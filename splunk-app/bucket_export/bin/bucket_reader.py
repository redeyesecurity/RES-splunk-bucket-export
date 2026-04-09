#!/usr/bin/env python3
"""
bucket_reader.py — Splunk bucket parser

Reads indexed Splunk buckets (hot/warm/cold) and extracts events.
Supports both journal.gz (compressed) and rawdata formats.

Bucket layout:
  db_<latest>_<earliest>_<id>/          # warm bucket
  hot_v1_<id>/                          # hot bucket
  colddb_<latest>_<earliest>_<id>/      # cold/frozen bucket
    rawdata/
      journal.gz                        # compressed event journal
      slicesv2.dat                      # slice metadata (optional)
    *.tsidx                             # time-series index files
    bucket_info.csv                     # bucket metadata
    .splunk/                            # internal metadata

Journal.gz format (reverse-engineered):
  - Standard gzip file containing concatenated event records
  - Each record: fixed header + variable-length raw event text
  - Header contains: timestamp, metadata offsets, event length
  - Events include _raw, _time, and field extraction metadata
"""

import gzip
import io
import logging
import os
import re
import struct
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

log = logging.getLogger("bucket_export.reader")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SplunkEvent:
    """A single extracted Splunk event."""
    _time: float
    _raw: str
    host: str = ""
    source: str = ""
    sourcetype: str = ""
    index: str = ""
    # Bucket metadata
    bucket_id: str = ""
    bucket_type: str = ""  # hot, warm, cold, frozen
    def to_dict(self) -> dict:
        return {
            "_time": self._time,
            "_raw": self._raw,
            "host": self.host,
            "source": self.source,
            "sourcetype": self.sourcetype,
            "index": self.index,
            "bucket_id": self.bucket_id,
            "bucket_type": self.bucket_type,
        }


@dataclass
class BucketInfo:
    """Metadata about a Splunk bucket."""
    path: Path
    bucket_id: str
    bucket_type: str  # hot, warm, cold, frozen
    index_name: str
    earliest: int = 0  # epoch
    latest: int = 0    # epoch
    event_count: int = 0
    size_bytes: int = 0
    journal_path: Optional[Path] = None
    is_complete: bool = False  # True if bucket is fully written (not hot)

# ---------------------------------------------------------------------------
# Bucket discovery
# ---------------------------------------------------------------------------

# Bucket directory name patterns
BUCKET_PATTERNS = {
    "hot":    re.compile(r"^hot_v\d+_(\d+)$"),
    "warm":   re.compile(r"^db_(\d+)_(\d+)_(\d+)$"),
    "cold":   re.compile(r"^colddb_(\d+)_(\d+)_(\d+)$"),
    "frozen": re.compile(r"^frozendb_(\d+)_(\d+)_(\d+)$"),
}


def discover_buckets(
    splunk_home: str = "/opt/splunk",
    indexes: Optional[List[str]] = None,
    bucket_types: Optional[List[str]] = None,
    min_time: float = 0,
    max_time: float = 0,
) -> List[BucketInfo]:
    """
    Scan Splunk's index directories and return a list of buckets.

    Args:
        splunk_home: Path to Splunk installation
        indexes: List of index names to scan (None = all)
        bucket_types: List of bucket types to include (None = all)
        min_time: Only include buckets with latest >= min_time (epoch)
        max_time: Only include buckets with earliest <= max_time (epoch)

    Returns:
        List of BucketInfo objects sorted by earliest time
    """
    if bucket_types is None:
        bucket_types = ["hot", "warm", "cold", "frozen"]

    buckets = []
    index_base = Path(splunk_home) / "var" / "lib" / "splunk"

    if not index_base.exists():
        log.warning(f"Splunk index base not found: {index_base}")
        return buckets
    # Scan each index directory
    for idx_dir in sorted(index_base.iterdir()):
        if not idx_dir.is_dir():
            continue
        idx_name = idx_dir.name
        if indexes and idx_name not in indexes:
            continue
        # Skip internal Splunk dirs
        if idx_name in ("authDb", "fishbucket", "hashDb", "persistentstorage",
                        "kvstore", "modinput"):
            continue

        # Look for bucket directories in db/, colddb/, frozendb/ subdirs
        # Standard layout: <index_dir>/db/, <index_dir>/colddb/
        search_dirs = [
            (idx_dir / "db", ["hot", "warm"]),
            (idx_dir / "colddb", ["cold"]),
            (idx_dir / "frozendb", ["frozen"]),
            # Also check the index dir itself (some layouts put buckets directly here)
            (idx_dir, ["hot", "warm", "cold", "frozen"]),
        ]

        for search_path, valid_types in search_dirs:
            if not search_path.exists() or not search_path.is_dir():
                continue
            for entry in sorted(search_path.iterdir()):
                if not entry.is_dir():
                    continue
                info = _parse_bucket_dir(entry, idx_name, valid_types, bucket_types)
                if info:
                    # Time filter
                    if min_time and info.latest < min_time:
                        continue
                    if max_time and info.earliest > max_time:
                        continue
                    buckets.append(info)

    buckets.sort(key=lambda b: b.earliest)
    log.info(f"Discovered {len(buckets)} buckets across "
             f"{len(set(b.index_name for b in buckets))} indexes")
    return buckets

def _parse_bucket_dir(
    path: Path,
    index_name: str,
    valid_types: List[str],
    allowed_types: List[str],
) -> Optional[BucketInfo]:
    """Parse a single bucket directory and return BucketInfo if valid."""
    name = path.name

    for btype, pattern in BUCKET_PATTERNS.items():
        if btype not in valid_types or btype not in allowed_types:
            continue
        m = pattern.match(name)
        if not m:
            continue

        # Extract time range from bucket name
        groups = m.groups()
        if btype == "hot":
            earliest, latest = 0, 0
            bucket_id = groups[0]
        else:
            latest = int(groups[0])
            earliest = int(groups[1])
            bucket_id = groups[2] if len(groups) > 2 else groups[0]

        # Find journal.gz
        journal = path / "rawdata" / "journal.gz"
        if not journal.exists():
            # Some buckets use uncompressed rawdata
            journal = path / "rawdata" / "journal.zst"
            if not journal.exists():
                journal = None

        # Calculate size
        size = 0
        if journal and journal.exists():
            size = journal.stat().st_size

        info = BucketInfo(
            path=path,
            bucket_id=bucket_id,
            bucket_type=btype,
            index_name=index_name,
            earliest=earliest,
            latest=latest,
            size_bytes=size,
            journal_path=journal,
            is_complete=(btype != "hot"),
        )

        # Try to get event count from bucket_info.csv
        info_csv = path / "bucket_info.csv"
        if info_csv.exists():
            try:
                with open(info_csv, "r") as f:
                    for line in f:
                        if "event_count" in line.lower():
                            parts = line.strip().split(",")
                            if len(parts) >= 2:
                                info.event_count = int(parts[1])
            except Exception:
                pass

        return info

    return None

# ---------------------------------------------------------------------------
# Journal reader (journal.gz / journal.zst)
# ---------------------------------------------------------------------------

# Splunk journal format (reverse-engineered from Splunk 9.1):
#
# The journal is a compressed stream (gzip or zstandard) containing
# "slices" — groups of events sharing common metadata.
#
# Structure:
#   [global header]
#     - host::hostname
#     - First source/sourcetype section
#   [slice N]
#     - source::/path/to/source
#     - sourcetype::type_name
#     - [event text blocks separated by binary framing]
#
# Each slice contains events from a single source/sourcetype combination.
# Events are raw text blocks (the _raw field) interleaved with binary
# metadata bytes (timestamps, field positions, etc.).
#
# The binary framing between events contains protobuf-encoded metadata
# but the raw event text is stored as plain UTF-8.

# Optional zstandard support
_zstd_available = False
try:
    import zstandard
    _zstd_available = True
except ImportError:
    pass


def read_bucket_events(
    bucket: BucketInfo,
    max_events: int = 0,
    source_filter: Optional[str] = None,
    sourcetype_filter: Optional[str] = None,
) -> Generator[SplunkEvent, None, None]:
    """
    Read all events from a Splunk bucket.

    Yields SplunkEvent objects. Handles both journal.gz and raw formats.

    Args:
        bucket: BucketInfo from discover_buckets()
        max_events: Stop after N events (0 = unlimited)
        source_filter: Only yield events matching this source pattern
        sourcetype_filter: Only yield events matching this sourcetype pattern
    """
    if not bucket.journal_path or not bucket.journal_path.exists():
        log.warning(f"No journal found for bucket {bucket.bucket_id}")
        return

    journal_path = bucket.journal_path
    log.info(f"Reading bucket {bucket.bucket_id} ({bucket.bucket_type}) "
             f"from {journal_path} ({bucket.size_bytes} bytes)")

    count = 0
    try:
        yield from _read_journal(
            journal_path, bucket, max_events,
            source_filter, sourcetype_filter
        )
    except Exception as e:
        log.error(f"Error reading bucket {bucket.bucket_id}: {e}")
        raise

def _read_journal(
    journal_path: Path,
    bucket: BucketInfo,
    max_events: int = 0,
    source_filter: Optional[str] = None,
    sourcetype_filter: Optional[str] = None,
) -> Generator[SplunkEvent, None, None]:
    """
    Parse a Splunk journal file (journal.gz or journal.zst).

    Splunk 9.x uses zstandard (.zst), older versions use gzip (.gz).
    The decompressed content contains "slices" — groups of events sharing
    common metadata (host, source, sourcetype). Each slice has:
      - source::/path and sourcetype::name markers
      - Event text blocks (the _raw field) interleaved with binary framing

    We extract metadata from markers and raw text from readable byte runs.
    """
    count = 0
    # --- Decompress ---
    suffix = str(journal_path).lower()
    try:
        if suffix.endswith(".zst"):
            if not _zstd_available:
                log.error("journal.zst found but zstandard not installed. "
                          "pip install zstandard")
                return
            with open(journal_path, "rb") as f:
                compressed = f.read()
            dctx = zstandard.ZstdDecompressor()
            data = dctx.decompress(compressed, max_output_size=500 * 1024 * 1024)
        else:
            with gzip.open(str(journal_path), "rb") as gz:
                data = gz.read()
    except Exception as e:
        log.error(f"Failed to decompress {journal_path}: {e}")
        return

    if len(data) < 20:
        log.warning(f"Journal too small ({len(data)} bytes): {journal_path}")
        return

    log.debug(f"Decompressed journal: {len(data):,} bytes from {journal_path.name}")

    # --- Extract global host ---
    global_host = ""
    hm = re.search(rb'host::([^\x00-\x05]+)', data)
    if hm:
        global_host = hm.group(1).decode("utf-8", errors="replace")

    # --- Find metadata sections (source/sourcetype boundaries) ---
    sections = _find_journal_sections(data)
    if not sections:
        # Fallback: treat entire journal as one section
        sections = [{"source": "", "sourcetype": "", "start": 0, "end": len(data)}]

    # --- Extract events from each section ---
    for sec in sections:
        if max_events and count >= max_events:
            break

        source = sec["source"]
        sourcetype = sec["sourcetype"]
        host = global_host

        # Apply metadata filters early
        if source_filter and source_filter not in source:
            continue
        if sourcetype_filter and sourcetype_filter not in sourcetype:
            continue

        chunk = data[sec["start"]:sec["end"]]

        for raw_text in _extract_text_blocks(chunk):
            if max_events and count >= max_events:
                break

            # Try to extract timestamp from the event text
            ts = _extract_timestamp_from_raw(raw_text)

            event = SplunkEvent(
                _time=ts,
                _raw=raw_text,
                host=host,
                source=source,
                sourcetype=sourcetype,
                index=bucket.index_name,
                bucket_id=bucket.bucket_id,
                bucket_type=bucket.bucket_type,
            )
            count += 1
            yield event

    log.info(f"Extracted {count} events from {journal_path.name} "
             f"({len(sections)} sections, {len(data):,} bytes decompressed)")


# Metadata field names that are NOT event text
_METADATA_PREFIXES = (
    "source::", "sourcetype::", "host::", "event",
    "timestartpos", "timeendpos", "date_second", "date_hour",
    "date_minute", "date_year", "date_month", "date_mday",
    "date_wday", "date_zone",
)

# Day/month names that appear as metadata values
_METADATA_VALUES = {
    "local", "monday", "tuesday", "wednesday", "thursday",
    "friday", "saturday", "sunday",
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
}


def _find_journal_sections(data: bytes) -> List[dict]:
    """
    Find metadata sections in a Splunk journal.

    Each section starts with a source:: marker followed by a sourcetype:: marker.
    Returns list of {source, sourcetype, start, end} dicts.
    """
    sections = []
    # Match source:: with optional prefix chars (#, $, @) — Splunk uses
    # $source::, #source::, @source:: as alternate section markers
    for m in re.finditer(rb'[$#@]?source::([^\x00-\x05]+)', data):
        source_val = m.group(1).decode("utf-8", errors="replace")
        # Clean: source value may run directly into "sourcetype::"
        if "sourcetype::" in source_val:
            source_val = source_val[:source_val.index("sourcetype::")]

        # Find sourcetype:: after this source (may be immediately adjacent
        # or within a few hundred bytes)
        search_start = m.start()
        st_match = re.search(rb'sourcetype::([^\x00-\x06]+)',
                             data[search_start:search_start + 500])
        sourcetype_val = ""
        if st_match:
            sourcetype_val = st_match.group(1).decode("utf-8", errors="replace")
            # Clean trailing metadata field names
            for sep in ("timestartpos", "timeendpos", "date_"):
                idx = sourcetype_val.find(sep)
                if idx >= 0:
                    sourcetype_val = sourcetype_val[:idx]
            sourcetype_val = sourcetype_val.strip("\x00\x06\x0a\r\n ")

        sections.append({
            "source": source_val,
            "sourcetype": sourcetype_val,
            "start": m.start(),
        })

    # Set end boundaries
    for i in range(len(sections)):
        if i + 1 < len(sections):
            sections[i]["end"] = sections[i + 1]["start"]
        else:
            sections[i]["end"] = len(data)

    return sections


def _extract_text_blocks(chunk: bytes, min_len: int = 15) -> Generator[str, None, None]:
    """
    Extract readable text blocks from a journal chunk.

    Events are stored as plain UTF-8 text interleaved with binary framing.
    We find contiguous runs of printable ASCII (+ common whitespace) and
    yield those that look like actual log events (not metadata).
    """
    i = 0
    length = len(chunk)
    while i < length:
        b = chunk[i]
        if b >= 0x20 and b < 0x7f:
            # Start of a printable text run
            k = i
            while k < length and (chunk[k] >= 0x20 or chunk[k] in (0x0a, 0x0d, 0x09)):
                k += 1
            text = chunk[i:k].decode("utf-8", errors="replace").strip()

            if len(text) >= min_len:
                text_lower = text.lower()
                # Skip metadata fields
                if not any(text_lower.startswith(p) for p in _METADATA_PREFIXES):
                    # Skip standalone metadata values (day/month names)
                    if text_lower not in _METADATA_VALUES:
                        # Skip source path refs with prefix markers
                        if not (len(text) > 1 and text[0] in ('#', '$', '@')
                                and "source::" in text):
                            yield text
            i = k
        else:
            i += 1


# Common timestamp patterns for parsing _time from raw event text
_TS_PATTERNS = [
    # ISO-ish: 2026-04-07 23:20:22-05
    re.compile(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})'),
    # Splunk internal: 04-07-2026 23:26:38.884 -0500
    re.compile(r'(\d{2}-\d{2}-\d{4})\s+(\d{2}:\d{2}:\d{2})'),
    # Syslog: Apr  7 23:19:15
    re.compile(r'([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})'),
]


def _extract_timestamp_from_raw(raw: str) -> float:
    """
    Best-effort timestamp extraction from raw event text.
    Returns epoch float, or current time as fallback.
    """
    # Pattern 1: YYYY-MM-DD HH:MM:SS
    m = _TS_PATTERNS[0].search(raw[:60])
    if m:
        try:
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)}",
                                   "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass

    # Pattern 2: MM-DD-YYYY HH:MM:SS
    m = _TS_PATTERNS[1].search(raw[:60])
    if m:
        try:
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)}",
                                   "%m-%d-%Y %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass

    # Pattern 3: Syslog (Mon DD HH:MM:SS — assume current year)
    m = _TS_PATTERNS[2].search(raw[:30])
    if m:
        try:
            year = datetime.now().year
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)} {year}",
                                   "%b %d %H:%M:%S %Y")
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass

    return time.time()


# ---------------------------------------------------------------------------
# Convenience / CLI
# ---------------------------------------------------------------------------

def summarize_buckets(buckets: List[BucketInfo]) -> str:
    """Return a human-readable summary of discovered buckets."""
    lines = [f"{'Type':<8} {'Index':<20} {'ID':<10} {'Events':>10} {'Size':>12} {'Time Range'}"]
    lines.append("-" * 90)
    for b in buckets:
        earliest_str = datetime.fromtimestamp(b.earliest, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if b.earliest else "N/A"
        latest_str = datetime.fromtimestamp(b.latest, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if b.latest else "N/A"
        size_mb = f"{b.size_bytes / 1_048_576:.1f} MB"
        lines.append(f"{b.bucket_type:<8} {b.index_name:<20} {b.bucket_id:<10} "
                      f"{b.event_count:>10} {size_mb:>12} {earliest_str} - {latest_str}")
    return "\n".join(lines)
