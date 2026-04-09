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
# Journal.gz reader
# ---------------------------------------------------------------------------

# Splunk journal.gz record header format (observed via hex analysis):
# The journal is a gzip stream of concatenated records.
# Each record has a variable-length header followed by raw event text.
#
# Record structure (approximate — varies by Splunk version):
#   [4 bytes] record_len (big-endian uint32, total record size)
#   [8 bytes] _time (big-endian int64, epoch seconds * 1000 or epoch float)
#   [variable] metadata fields (null-separated key=value pairs)
#   [variable] _raw event text (remainder of record)
#
# Metadata fields include: source, sourcetype, host, _cd, _indextime, etc.
# The exact layout depends on Splunk version — we use heuristics to detect.

# Known journal magic bytes / record delimiters
JOURNAL_RECORD_MAGIC = b"\x00\x00"  # Common prefix in record headers


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
        yield from _read_journal_gz(
            journal_path, bucket, max_events,
            source_filter, sourcetype_filter
        )
    except Exception as e:
        log.error(f"Error reading bucket {bucket.bucket_id}: {e}")
        raise

def _read_journal_gz(
    journal_path: Path,
    bucket: BucketInfo,
    max_events: int = 0,
    source_filter: Optional[str] = None,
    sourcetype_filter: Optional[str] = None,
) -> Generator[SplunkEvent, None, None]:
    """
    Parse a Splunk journal.gz file and yield individual events.

    Journal.gz is a gzip-compressed stream of concatenated records.
    Each record contains a header with metadata followed by the raw event.

    The format varies by Splunk version. We support two known layouts:
      Layout A (Splunk 7.x+):
        [4 bytes] record_length (big-endian uint32)
        [8 bytes] _time (big-endian double, epoch float)
        [variable] null-separated key=value metadata
        [variable] _raw text (after final null separator)

      Layout B (Splunk 6.x / older):
        [4 bytes] record_length (big-endian uint32)
        [4 bytes] _time (big-endian uint32, epoch seconds)
        [variable] metadata + _raw

    We auto-detect the layout by examining timestamp plausibility.
    """
    count = 0
    try:
        with gzip.open(str(journal_path), "rb") as gz:
            data = gz.read()
    except Exception as e:
        log.error(f"Failed to decompress {journal_path}: {e}")
        return

    if len(data) < 12:
        log.warning(f"Journal too small ({len(data)} bytes): {journal_path}")
        return

    log.debug(f"Decompressed journal: {len(data)} bytes")

    # Try to parse as concatenated records
    offset = 0
    errors = 0
    max_errors = 50  # bail if too many consecutive parse failures

    while offset < len(data) - 4:
        if max_events and count >= max_events:
            break
        if errors > max_errors:
            log.error(f"Too many parse errors ({errors}), stopping at offset {offset}")
            break

        # Read record length
        rec_len = struct.unpack(">I", data[offset:offset + 4])[0]

        # Sanity check record length
        if rec_len < 12 or rec_len > 10_000_000:
            # Try scanning forward for next valid record
            offset += 1
            errors += 1
            continue

        if offset + 4 + rec_len > len(data):
            # Truncated record at end of file
            log.debug(f"Truncated record at offset {offset}, "
                      f"rec_len={rec_len}, remaining={len(data) - offset}")
            break

        record = data[offset + 4: offset + 4 + rec_len]
        offset += 4 + rec_len
        errors = 0  # reset on successful frame

        # Parse record into event
        event = _parse_journal_record(record, bucket)
        if event is None:
            continue

        # Apply filters
        if source_filter and source_filter not in event.source:
            continue
        if sourcetype_filter and sourcetype_filter not in event.sourcetype:
            continue

        count += 1
        yield event

    log.info(f"Extracted {count} events from {journal_path.name} "
             f"(errors={errors}, final_offset={offset}/{len(data)})")


# Epoch range for sanity-checking timestamps (2000-01-01 to 2040-01-01)
_EPOCH_MIN = 946684800
_EPOCH_MAX = 2208988800


def _parse_journal_record(
    record: bytes,
    bucket: BucketInfo,
) -> Optional[SplunkEvent]:
    """
    Parse a single journal record into a SplunkEvent.

    Attempts two timestamp layouts:
      1. 8-byte double (Splunk 7.x+)
      2. 4-byte uint32 (Splunk 6.x)
    """
    if len(record) < 12:
        return None

    # --- Layout A: 8-byte double timestamp ---
    try:
        ts_double = struct.unpack(">d", record[0:8])[0]
        if _EPOCH_MIN <= ts_double <= _EPOCH_MAX:
            return _extract_event(record[8:], ts_double, bucket)
    except struct.error:
        pass

    # --- Layout B: 4-byte uint32 timestamp ---
    try:
        ts_int = struct.unpack(">I", record[0:4])[0]
        if _EPOCH_MIN <= ts_int <= _EPOCH_MAX:
            return _extract_event(record[4:], float(ts_int), bucket)
    except struct.error:
        pass

    # --- Layout C: skip 4-byte flags then 8-byte double ---
    try:
        ts_double = struct.unpack(">d", record[4:12])[0]
        if _EPOCH_MIN <= ts_double <= _EPOCH_MAX:
            return _extract_event(record[12:], ts_double, bucket)
    except struct.error:
        pass

    log.debug(f"Could not parse timestamp from record ({len(record)} bytes)")
    return None


def _extract_event(
    payload: bytes,
    timestamp: float,
    bucket: BucketInfo,
) -> Optional[SplunkEvent]:
    """
    Extract metadata and _raw from the post-timestamp payload.

    Payload structure: null-separated key=value pairs, followed by
    the raw event text after the last metadata field.
    """
    host = ""
    source = ""
    sourcetype = ""
    raw_text = ""

    # Split on null bytes to find metadata fields
    parts = payload.split(b"\x00")

    # Find where metadata ends and _raw begins
    # Metadata fields contain '=' signs; _raw typically does not start with one
    meta_end = 0
    for i, part in enumerate(parts):
        if not part:
            continue
        try:
            decoded = part.decode("utf-8", errors="replace")
        except Exception:
            decoded = part.decode("latin-1", errors="replace")

        if "=" in decoded and len(decoded) < 4096:
            # Looks like a metadata field
            key, _, val = decoded.partition("=")
            key = key.strip().lower()
            if key == "host":
                host = val
            elif key == "source":
                source = val
            elif key == "sourcetype":
                sourcetype = val
            meta_end = i + 1
        else:
            # This part doesn't look like metadata — it's _raw or start of _raw
            break

    # Everything from meta_end onward is the raw event
    raw_parts = parts[meta_end:]
    if raw_parts:
        try:
            raw_text = b"\x00".join(raw_parts).decode("utf-8", errors="replace").strip()
        except Exception:
            raw_text = b"\x00".join(raw_parts).decode("latin-1", errors="replace").strip()
    else:
        # Fallback: try entire payload as raw text
        try:
            raw_text = payload.decode("utf-8", errors="replace").strip()
        except Exception:
            raw_text = payload.decode("latin-1", errors="replace").strip()

    if not raw_text:
        return None

    return SplunkEvent(
        _time=timestamp,
        _raw=raw_text,
        host=host,
        source=source,
        sourcetype=sourcetype,
        index=bucket.index_name,
        bucket_id=bucket.bucket_id,
        bucket_type=bucket.bucket_type,
    )


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
