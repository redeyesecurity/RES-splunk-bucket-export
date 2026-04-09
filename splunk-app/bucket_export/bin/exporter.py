#!/usr/bin/env python3
"""
exporter.py — Main export orchestrator

Discovers Splunk buckets, reads events, transforms to OCSF,
writes to configured destinations, and tracks export state
to avoid re-processing.

State tracking uses a simple JSON file that records which
bucket IDs have been fully exported.
"""

import json
import logging
import os
import sys
import time
import yaml
from pathlib import Path
from typing import Dict, List, Optional

from bucket_reader import (
    BucketInfo, SplunkEvent, discover_buckets,
    read_bucket_events, summarize_buckets,
)
from ocsf_mapper import to_ocsf
from export_writer import BaseWriter, create_writers

log = logging.getLogger("bucket_export")


# ---------------------------------------------------------------------------
# State tracker — tracks which buckets have been exported
# ---------------------------------------------------------------------------

class ExportState:
    """
    Persistent state tracker for bucket exports.

    Stores a JSON file mapping bucket_id -> export metadata.
    Prevents re-exporting already-processed buckets.
    """

    def __init__(self, state_file: str = "export_state.json"):
        self._path = Path(state_file)
        self._state: Dict[str, dict] = {}
        self._load()

    def _load(self):
        if self._path.exists():
            try:
                with open(self._path, "r") as f:
                    self._state = json.load(f)
                log.info(f"Loaded export state: {len(self._state)} buckets tracked")
            except Exception as e:
                log.warning(f"Failed to load state file: {e}")
                self._state = {}

    def _save(self):
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "w") as f:
                json.dump(self._state, f, indent=2)
        except Exception as e:
            log.error(f"Failed to save state: {e}")

    def is_exported(self, bucket: BucketInfo) -> bool:
        """Check if a bucket has already been fully exported."""
        key = self._bucket_key(bucket)
        entry = self._state.get(key)
        if not entry:
            return False
        # Re-export if bucket size changed (new events in hot bucket)
        if bucket.size_bytes and entry.get("size_bytes") != bucket.size_bytes:
            return False
        return entry.get("status") == "complete"

    def mark_started(self, bucket: BucketInfo):
        key = self._bucket_key(bucket)
        self._state[key] = {
            "status": "in_progress",
            "started_at": time.time(),
            "size_bytes": bucket.size_bytes,
        }
        self._save()

    def mark_complete(self, bucket: BucketInfo, event_count: int):
        key = self._bucket_key(bucket)
        self._state[key] = {
            "status": "complete",
            "completed_at": time.time(),
            "event_count": event_count,
            "size_bytes": bucket.size_bytes,
        }
        self._save()

    def mark_failed(self, bucket: BucketInfo, error: str):
        key = self._bucket_key(bucket)
        self._state[key] = {
            "status": "failed",
            "failed_at": time.time(),
            "error": str(error)[:500],
            "size_bytes": bucket.size_bytes,
        }
        self._save()

    @staticmethod
    def _bucket_key(bucket: BucketInfo) -> str:
        return f"{bucket.index_name}/{bucket.bucket_type}/{bucket.bucket_id}"

    def summary(self) -> dict:
        counts = {"complete": 0, "in_progress": 0, "failed": 0}
        for entry in self._state.values():
            s = entry.get("status", "unknown")
            counts[s] = counts.get(s, 0) + 1
        return counts


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

class BucketExporter:
    """
    Main orchestrator that ties together:
      - Bucket discovery
      - Event reading
      - OCSF transformation
      - Multi-destination writing
      - State tracking
    """

    def __init__(self, config: dict):
        self.config = config
        self._splunk_home = config.get("splunk_home", "/opt/splunk")
        self._batch_size = config.get("batch_size", 500)
        self._ocsf_enabled = config.get("ocsf", {}).get("enabled", True)
        self._state = ExportState(
            config.get("state_file", "export_state.json")
        )
        self._writers: List[BaseWriter] = create_writers(
            config.get("writers", {})
        )
        self._stats = {
            "buckets_processed": 0,
            "buckets_skipped": 0,
            "events_exported": 0,
            "errors": 0,
        }

    def run(self,
            indexes: Optional[List[str]] = None,
            bucket_types: Optional[List[str]] = None,
            min_time: float = 0,
            max_time: float = 0,
            max_events_per_bucket: int = 0,
            dry_run: bool = False):
        """
        Main export loop.

        1. Discover buckets
        2. Skip already-exported
        3. Read events from each bucket
        4. Transform to OCSF (optional)
        5. Write to all configured destinations
        6. Track state
        """
        if not self._writers:
            log.error("No writers configured or initialized. Aborting.")
            return self._stats

        # Step 1: Discover
        log.info(f"Discovering buckets in {self._splunk_home}...")
        buckets = discover_buckets(
            splunk_home=self._splunk_home,
            indexes=indexes,
            bucket_types=bucket_types,
            min_time=min_time,
            max_time=max_time,
        )

        if not buckets:
            log.info("No buckets found matching criteria.")
            return self._stats

        log.info(f"Found {len(buckets)} buckets")
        if log.isEnabledFor(logging.DEBUG):
            log.debug("\n" + summarize_buckets(buckets))

        if dry_run:
            print(summarize_buckets(buckets))
            print(f"\nDry run — {len(buckets)} buckets would be exported.")
            return self._stats

        # Step 2-6: Process each bucket
        for bucket in buckets:
            if self._state.is_exported(bucket):
                self._stats["buckets_skipped"] += 1
                log.debug(f"Skipping already-exported bucket {bucket.bucket_id}")
                continue

            self._export_bucket(bucket, max_events_per_bucket)

        # Flush all writers
        for w in self._writers:
            try:
                w.flush()
            except Exception as e:
                log.error(f"Flush error: {e}")

        log.info(f"Export complete: {self._stats}")
        return self._stats

    def _export_bucket(self, bucket: BucketInfo, max_events: int = 0):
        """Export a single bucket through the pipeline."""
        log.info(f"Exporting bucket {bucket.index_name}/{bucket.bucket_type}/"
                 f"{bucket.bucket_id} ({bucket.size_bytes} bytes)")
        self._state.mark_started(bucket)
        event_count = 0
        batch: List[dict] = []

        try:
            for splunk_event in read_bucket_events(bucket, max_events=max_events):
                # Transform
                event_dict = splunk_event.to_dict()
                if self._ocsf_enabled:
                    event_dict = to_ocsf(event_dict)

                batch.append(event_dict)

                # Write when batch is full
                if len(batch) >= self._batch_size:
                    self._write_batch(batch)
                    event_count += len(batch)
                    batch = []

            # Write remaining events
            if batch:
                self._write_batch(batch)
                event_count += len(batch)

            self._state.mark_complete(bucket, event_count)
            self._stats["buckets_processed"] += 1
            self._stats["events_exported"] += event_count
            log.info(f"Bucket {bucket.bucket_id} complete: {event_count} events")

        except Exception as e:
            self._state.mark_failed(bucket, str(e))
            self._stats["errors"] += 1
            log.error(f"Bucket {bucket.bucket_id} failed: {e}")

    def _write_batch(self, batch: List[dict]):
        """Send a batch to all configured writers."""
        for writer in self._writers:
            try:
                writer.write_batch(batch)
            except Exception as e:
                self._stats["errors"] += 1
                log.error(f"Writer error ({type(writer).__name__}): {e}")

    def close(self):
        """Shut down all writers."""
        for w in self._writers:
            try:
                w.close()
            except Exception as e:
                log.error(f"Close error: {e}")


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(config_path: str = "config.yaml") -> dict:
    """Load YAML configuration file."""
    p = Path(config_path)
    if not p.exists():
        log.warning(f"Config file not found: {p}, using defaults")
        return {}
    with open(p, "r") as f:
        return yaml.safe_load(f) or {}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Export Splunk buckets to data lake destinations"
    )
    parser.add_argument("-c", "--config", default="config.yaml",
                        help="Path to config.yaml")
    parser.add_argument("--splunk-home", default=None,
                        help="Override SPLUNK_HOME")
    parser.add_argument("--indexes", nargs="*", default=None,
                        help="Index names to export")
    parser.add_argument("--bucket-types", nargs="*", default=None,
                        choices=["hot", "warm", "cold", "frozen"],
                        help="Bucket types to export")
    parser.add_argument("--max-events", type=int, default=0,
                        help="Max events per bucket (0=unlimited)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List buckets without exporting")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)s [%(levelname)s] %(message)s",
    )

    config = load_config(args.config)
    if args.splunk_home:
        config["splunk_home"] = args.splunk_home

    exporter = BucketExporter(config)
    try:
        stats = exporter.run(
            indexes=args.indexes,
            bucket_types=args.bucket_types,
            max_events_per_bucket=args.max_events,
            dry_run=args.dry_run,
        )
        print(json.dumps(stats, indent=2))
    finally:
        exporter.close()


if __name__ == "__main__":
    main()
