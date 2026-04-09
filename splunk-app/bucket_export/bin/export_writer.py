#!/usr/bin/env python3
"""
export_writer.py — Multi-destination export writers

Writes transformed events to various destinations:
  - Local Parquet files (priority)
  - S3-compatible storage (priority)
  - Local JSON files
  - Additional destinations via plugin interface

All writers implement the BaseWriter interface.
"""

import io
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

log = logging.getLogger("bucket_export.writer")


# ---------------------------------------------------------------------------
# Base writer interface
# ---------------------------------------------------------------------------

class BaseWriter(ABC):
    """Abstract base for all export destinations."""

    def __init__(self, config: dict):
        self.config = config
        self._event_count = 0
        self._byte_count = 0
        self._error_count = 0

    @abstractmethod
    def write_batch(self, events: List[dict]) -> int:
        """Write a batch of event dicts. Returns count written."""
        ...

    @abstractmethod
    def flush(self):
        """Flush any buffered data."""
        ...

    @abstractmethod
    def close(self):
        """Clean up resources."""
        ...

    @property
    def stats(self) -> dict:
        return {
            "events_written": self._event_count,
            "bytes_written": self._byte_count,
            "errors": self._error_count,
        }

    def _partition_key(self, event: dict) -> str:
        """Generate hive-style partition path from event timestamp."""
        ts = event.get("_time", time.time())
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        partition_by = self.config.get("partition_by", "hour")
        if partition_by == "day":
            return dt.strftime("year=%Y/month=%m/day=%d")
        elif partition_by == "hour":
            return dt.strftime("year=%Y/month=%m/day=%d/hour=%H")
        else:
            return dt.strftime("year=%Y/month=%m/day=%d")


# ---------------------------------------------------------------------------
# Parquet writer (local filesystem)
# ---------------------------------------------------------------------------

class ParquetWriter(BaseWriter):
    """
    Write events as Parquet files with hive-style partitioning.

    Config:
      path: /data/exports/parquet
      partition_by: hour | day
      compression: snappy | gzip | zstd
      row_group_size: 10000
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self._base_path = Path(config.get("path", "/tmp/bucket-export/parquet"))
        self._compression = config.get("compression", "snappy")
        self._row_group_size = config.get("row_group_size", 10000)
        self._buffer: Dict[str, List[dict]] = {}  # partition -> events
        self._pa = None
        self._pq = None
        self._init_pyarrow()

    def _init_pyarrow(self):
        """Lazy-import pyarrow to avoid hard dependency."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            self._pa = pa
            self._pq = pq
            log.info("PyArrow loaded for Parquet writer")
        except ImportError:
            log.warning("pyarrow not installed — Parquet writer disabled. "
                        "Install with: pip install pyarrow")

    def write_batch(self, events: List[dict]) -> int:
        if not self._pa:
            return 0
        count = 0
        for event in events:
            partition = self._partition_key(event)
            if partition not in self._buffer:
                self._buffer[partition] = []
            self._buffer[partition].append(event)
            count += 1
            # Flush partition if buffer exceeds row_group_size
            if len(self._buffer[partition]) >= self._row_group_size:
                self._flush_partition(partition)
        self._event_count += count
        return count

    def flush(self):
        for partition in list(self._buffer.keys()):
            self._flush_partition(partition)

    def _flush_partition(self, partition: str):
        events = self._buffer.pop(partition, [])
        if not events:
            return
        out_dir = self._base_path / partition
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = int(time.time() * 1000)
        out_file = out_dir / f"part-{ts}.parquet"

        try:
            table = self._pa.Table.from_pylist(events)
            self._pq.write_table(
                table,
                str(out_file),
                compression=self._compression,
                row_group_size=self._row_group_size,
            )
            file_size = out_file.stat().st_size
            self._byte_count += file_size
            log.info(f"Wrote {len(events)} events to {out_file} "
                     f"({file_size} bytes, {self._compression})")
        except Exception as e:
            self._error_count += 1
            log.error(f"Parquet write failed for {partition}: {e}")

    def close(self):
        self.flush()


# ---------------------------------------------------------------------------
# S3 writer
# ---------------------------------------------------------------------------

class S3Writer(BaseWriter):
    """
    Write events as Parquet (or JSON) to S3-compatible storage.

    Config:
      bucket: my-datalake-bucket
      prefix: splunk-export/
      region: us-east-1
      format: parquet | json
      compression: snappy | gzip | zstd
      partition_by: hour | day
      endpoint_url: (optional, for MinIO/R2/etc)
      access_key: (optional, falls back to IAM/env)
      secret_key: (optional, falls back to IAM/env)
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self._bucket_name = config.get("bucket", "")
        self._prefix = config.get("prefix", "splunk-export/").rstrip("/")
        self._region = config.get("region", "us-east-1")
        self._format = config.get("format", "parquet")
        self._compression = config.get("compression", "snappy")
        self._row_group_size = config.get("row_group_size", 10000)
        self._buffer: Dict[str, List[dict]] = {}
        self._s3 = None
        self._pa = None
        self._pq = None
        self._init_clients()

    def _init_clients(self):
        """Lazy-import boto3 and pyarrow."""
        try:
            import boto3
            kwargs = {"region_name": self._region}
            if self.config.get("endpoint_url"):
                kwargs["endpoint_url"] = self.config["endpoint_url"]
            if self.config.get("access_key"):
                kwargs["aws_access_key_id"] = self.config["access_key"]
                kwargs["aws_secret_access_key"] = self.config.get("secret_key", "")
            self._s3 = boto3.client("s3", **kwargs)
            log.info(f"S3 client initialized (bucket={self._bucket_name}, "
                     f"region={self._region})")
        except ImportError:
            log.warning("boto3 not installed — S3 writer disabled. "
                        "Install with: pip install boto3")

        if self._format == "parquet":
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq
                self._pa = pa
                self._pq = pq
            except ImportError:
                log.warning("pyarrow not installed, falling back to JSON for S3")
                self._format = "json"

    def write_batch(self, events: List[dict]) -> int:
        if not self._s3:
            return 0
        count = 0
        for event in events:
            partition = self._partition_key(event)
            if partition not in self._buffer:
                self._buffer[partition] = []
            self._buffer[partition].append(event)
            count += 1
            if len(self._buffer[partition]) >= self._row_group_size:
                self._flush_partition(partition)
        self._event_count += count
        return count

    def flush(self):
        for partition in list(self._buffer.keys()):
            self._flush_partition(partition)

    def _flush_partition(self, partition: str):
        events = self._buffer.pop(partition, [])
        if not events:
            return
        ts = int(time.time() * 1000)
        s3_key = f"{self._prefix}/{partition}/part-{ts}"

        try:
            if self._format == "parquet" and self._pa:
                s3_key += ".parquet"
                table = self._pa.Table.from_pylist(events)
                buf = io.BytesIO()
                self._pq.write_table(
                    table, buf,
                    compression=self._compression,
                    row_group_size=self._row_group_size,
                )
                body = buf.getvalue()
            else:
                s3_key += ".json.gz"
                import gzip as gz
                body = gz.compress(
                    "\n".join(json.dumps(e) for e in events).encode("utf-8")
                )

            self._s3.put_object(
                Bucket=self._bucket_name,
                Key=s3_key,
                Body=body,
            )
            self._byte_count += len(body)
            log.info(f"Uploaded {len(events)} events to s3://{self._bucket_name}/{s3_key} "
                     f"({len(body)} bytes)")
        except Exception as e:
            self._error_count += 1
            log.error(f"S3 upload failed for {s3_key}: {e}")

    def close(self):
        self.flush()


# ---------------------------------------------------------------------------
# JSON writer (local filesystem — simple fallback)
# ---------------------------------------------------------------------------

class JsonWriter(BaseWriter):
    """Write events as newline-delimited JSON files."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._base_path = Path(config.get("path", "/tmp/bucket-export/json"))
        self._compress = config.get("compress", False)
        self._batch_size = config.get("batch_size", 1000)
        self._buffer: Dict[str, List[dict]] = {}

    def write_batch(self, events: List[dict]) -> int:
        count = 0
        for event in events:
            partition = self._partition_key(event)
            if partition not in self._buffer:
                self._buffer[partition] = []
            self._buffer[partition].append(event)
            count += 1
            if len(self._buffer[partition]) >= self._batch_size:
                self._flush_partition(partition)
        self._event_count += count
        return count

    def flush(self):
        for partition in list(self._buffer.keys()):
            self._flush_partition(partition)

    def _flush_partition(self, partition: str):
        events = self._buffer.pop(partition, [])
        if not events:
            return
        out_dir = self._base_path / partition
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = int(time.time() * 1000)

        if self._compress:
            import gzip as gz
            out_file = out_dir / f"part-{ts}.json.gz"
            try:
                data = "\n".join(json.dumps(e) for e in events).encode("utf-8")
                with open(out_file, "wb") as f:
                    f.write(gz.compress(data))
                self._byte_count += out_file.stat().st_size
                log.info(f"Wrote {len(events)} events to {out_file}")
            except Exception as e:
                self._error_count += 1
                log.error(f"JSON write failed: {e}")
        else:
            out_file = out_dir / f"part-{ts}.json"
            try:
                with open(out_file, "w") as f:
                    for event in events:
                        f.write(json.dumps(event) + "\n")
                self._byte_count += out_file.stat().st_size
                log.info(f"Wrote {len(events)} events to {out_file}")
            except Exception as e:
                self._error_count += 1
                log.error(f"JSON write failed: {e}")

    def close(self):
        self.flush()


# ---------------------------------------------------------------------------
# Writer factory
# ---------------------------------------------------------------------------

WRITER_REGISTRY = {
    "parquet": ParquetWriter,
    "s3": S3Writer,
    "json": JsonWriter,
}


def create_writer(name: str, config: dict) -> BaseWriter:
    """
    Instantiate a writer by name.

    Args:
        name: Writer type (parquet, s3, json)
        config: Writer-specific configuration dict

    Returns:
        BaseWriter instance
    """
    cls = WRITER_REGISTRY.get(name.lower())
    if cls is None:
        raise ValueError(f"Unknown writer type: {name}. "
                         f"Available: {list(WRITER_REGISTRY.keys())}")
    return cls(config)


def create_writers(writers_config: dict) -> List[BaseWriter]:
    """
    Create multiple writers from config.

    Config format:
      writers:
        parquet:
          enabled: true
          path: /data/export/parquet
        s3:
          enabled: true
          bucket: my-bucket
    """
    writers = []
    for name, cfg in writers_config.items():
        if not cfg.get("enabled", True):
            continue
        try:
            w = create_writer(name, cfg)
            writers.append(w)
            log.info(f"Initialized writer: {name}")
        except Exception as e:
            log.error(f"Failed to initialize writer {name}: {e}")
    return writers
