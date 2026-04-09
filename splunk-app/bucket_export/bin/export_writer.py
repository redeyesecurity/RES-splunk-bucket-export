#!/usr/bin/env python3
"""
export_writer.py — Multi-destination export writers (all 11)

Destinations:
  1.  local-parquet   Hive-partitioned Parquet (pyarrow)
  2.  s3              S3-compatible (AWS/MinIO/R2) — Parquet or JSON
  3.  local-json      Newline-delimited JSON
  4.  kafka           Kafka / Confluent / Redpanda
  5.  loki            Grafana Loki push API
  6.  elasticsearch   Elasticsearch / OpenSearch bulk API
  7.  webhook         Generic HTTP POST
  8.  syslog          RFC 5424 TCP/UDP/TLS
  9.  azure-eventhubs Azure Event Hubs
  10. gcp-pubsub      Google Cloud Pub/Sub
  11. splunk-hec      Splunk HTTP Event Collector

All writers implement the BaseWriter interface.
"""

import io
import json
import logging
import os
import socket
import ssl
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

log = logging.getLogger("bucket_export.writer")

# ---------------------------------------------------------------------------
# Optional deps — imported lazily per writer
# ---------------------------------------------------------------------------
_pyarrow_available = False
_boto3_available = False
_kafka_available = False
_requests_available = False
_es_available = False
_azure_eh_available = False
_gcp_pubsub_available = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _pyarrow_available = True
except ImportError:
    pa = pq = None

try:
    import boto3
    _boto3_available = True
except ImportError:
    boto3 = None

try:
    import requests as _requests
    _requests_available = True
except ImportError:
    _requests = None

try:
    from confluent_kafka import Producer as _KafkaProducer
    _kafka_available = True
except ImportError:
    _KafkaProducer = None

try:
    from elasticsearch import Elasticsearch as _Elasticsearch
    from elasticsearch.helpers import bulk as _es_bulk
    _es_available = True
except ImportError:
    _Elasticsearch = _es_bulk = None

try:
    from azure.eventhub import EventHubProducerClient, EventData
    _azure_eh_available = True
except ImportError:
    EventHubProducerClient = EventData = None

try:
    from google.cloud import pubsub_v1
    _gcp_pubsub_available = True
except ImportError:
    pubsub_v1 = None


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
        ts = event.get("_time", event.get("time", time.time()))
        if isinstance(ts, str):
            try:
                ts = float(ts)
            except ValueError:
                ts = time.time()
        # Handle millisecond timestamps
        if ts > 1e12:
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        partition_by = self.config.get("partition_by", "hour")
        if partition_by == "day":
            return dt.strftime("year=%Y/month=%m/day=%d")
        elif partition_by == "hour":
            return dt.strftime("year=%Y/month=%m/day=%d/hour=%H")
        else:
            return dt.strftime("year=%Y/month=%m/day=%d")

    def _filename(self, ext: str) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = str(uuid.uuid4())[:8]
        return f"part-{ts}_{uid}.{ext}"


# ---------------------------------------------------------------------------
# Arrow table builder (shared by Parquet + S3)
# ---------------------------------------------------------------------------

def _events_to_arrow(events: list):
    """Convert list of dicts to pyarrow Table (nested dicts → JSON strings)."""
    if not events or not pa:
        return pa.table({}) if pa else None
    all_keys = sorted(set(k for e in events for k in e.keys()))
    columns = {k: [] for k in all_keys}
    for e in events:
        for k in all_keys:
            v = e.get(k)
            if isinstance(v, (dict, list)):
                v = json.dumps(v, default=str)
            elif v is None:
                v = ""
            else:
                v = str(v)
            columns[k].append(v)
    arrays = {k: pa.array(v, type=pa.string()) for k, v in columns.items()}
    return pa.table(arrays)


# ===================================================================
# 1. LOCAL PARQUET
# ===================================================================

class ParquetWriter(BaseWriter):
    """Hive-partitioned Parquet files on local filesystem."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._base_path = Path(config.get("path", "/tmp/bucket-export/parquet"))
        self._compression = config.get("compression", "snappy")
        self._row_group_size = config.get("row_group_size", 10000)
        self._buffer: Dict[str, List[dict]] = {}
        if not _pyarrow_available:
            log.warning("pyarrow not installed — Parquet writer disabled")

    def write_batch(self, events: List[dict]) -> int:
        if not _pyarrow_available:
            return 0
        count = 0
        for event in events:
            p = self._partition_key(event)
            self._buffer.setdefault(p, []).append(event)
            count += 1
            if len(self._buffer[p]) >= self._row_group_size:
                self._flush_partition(p)
        self._event_count += count
        return count

    def flush(self):
        for p in list(self._buffer.keys()):
            self._flush_partition(p)

    def _flush_partition(self, partition: str):
        events = self._buffer.pop(partition, [])
        if not events:
            return
        out_dir = self._base_path / partition
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / self._filename("parquet")
        try:
            table = _events_to_arrow(events)
            pq.write_table(table, str(out_file),
                           compression=self._compression,
                           row_group_size=self._row_group_size)
            self._byte_count += out_file.stat().st_size
            log.info(f"Parquet: {len(events)} events → {out_file}")
        except Exception as e:
            self._error_count += 1
            log.error(f"Parquet write failed: {e}")

    def close(self):
        self.flush()


# ===================================================================
# 2. S3 (AWS / MinIO / R2)
# ===================================================================

class S3Writer(BaseWriter):
    """Write Parquet or JSON to S3-compatible storage."""

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
        if not _boto3_available:
            log.warning("boto3 not installed — S3 writer disabled")
        else:
            kwargs = {"region_name": self._region}
            if config.get("endpoint_url"):
                kwargs["endpoint_url"] = config["endpoint_url"]
            if config.get("access_key"):
                kwargs["aws_access_key_id"] = config["access_key"]
                kwargs["aws_secret_access_key"] = config.get("secret_key", "")
            self._s3 = boto3.client("s3", **kwargs)
            log.info(f"S3 writer: bucket={self._bucket_name} region={self._region}")
        if self._format == "parquet" and not _pyarrow_available:
            log.warning("pyarrow not installed, S3 falling back to JSON")
            self._format = "json"

    def write_batch(self, events: List[dict]) -> int:
        if not self._s3:
            return 0
        count = 0
        for event in events:
            p = self._partition_key(event)
            self._buffer.setdefault(p, []).append(event)
            count += 1
            if len(self._buffer[p]) >= self._row_group_size:
                self._flush_partition(p)
        self._event_count += count
        return count

    def flush(self):
        for p in list(self._buffer.keys()):
            self._flush_partition(p)

    def _flush_partition(self, partition: str):
        events = self._buffer.pop(partition, [])
        if not events:
            return
        s3_key = f"{self._prefix}/{partition}/{self._filename('parquet' if self._format == 'parquet' else 'json.gz')}"
        try:
            if self._format == "parquet" and _pyarrow_available:
                table = _events_to_arrow(events)
                buf = io.BytesIO()
                pq.write_table(table, buf, compression=self._compression,
                               row_group_size=self._row_group_size)
                body = buf.getvalue()
            else:
                import gzip as gz
                body = gz.compress(
                    "\n".join(json.dumps(e, default=str) for e in events).encode("utf-8")
                )
            self._s3.put_object(Bucket=self._bucket_name, Key=s3_key, Body=body)
            self._byte_count += len(body)
            log.info(f"S3: {len(events)} events → s3://{self._bucket_name}/{s3_key}")
        except Exception as e:
            self._error_count += 1
            log.error(f"S3 upload failed: {e}")

    def close(self):
        self.flush()


# ===================================================================
# 3. LOCAL JSON (NDJSON)
# ===================================================================

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
            p = self._partition_key(event)
            self._buffer.setdefault(p, []).append(event)
            count += 1
            if len(self._buffer[p]) >= self._batch_size:
                self._flush_partition(p)
        self._event_count += count
        return count

    def flush(self):
        for p in list(self._buffer.keys()):
            self._flush_partition(p)

    def _flush_partition(self, partition: str):
        events = self._buffer.pop(partition, [])
        if not events:
            return
        out_dir = self._base_path / partition
        out_dir.mkdir(parents=True, exist_ok=True)
        if self._compress:
            import gzip as gz
            out_file = out_dir / self._filename("json.gz")
            data = "\n".join(json.dumps(e, default=str) for e in events).encode("utf-8")
            with open(out_file, "wb") as f:
                f.write(gz.compress(data))
        else:
            out_file = out_dir / self._filename("jsonl")
            with open(out_file, "w") as f:
                for event in events:
                    f.write(json.dumps(event, default=str) + "\n")
        try:
            self._byte_count += out_file.stat().st_size
            log.info(f"JSON: {len(events)} events → {out_file}")
        except Exception as e:
            self._error_count += 1
            log.error(f"JSON write failed: {e}")

    def close(self):
        self.flush()


# ===================================================================
# 4. KAFKA
# ===================================================================

class KafkaWriter(BaseWriter):
    """Write events to Kafka / Confluent / Redpanda."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._topic = config.get("topic", "security-events")
        self._producer = None
        if not _kafka_available:
            log.warning("confluent-kafka not installed — Kafka writer disabled")
            return
        producer_conf = {
            "bootstrap.servers": ",".join(config.get("brokers", ["localhost:9092"])),
            "compression.type": config.get("compression", "snappy"),
            "acks": str(config.get("acks", "all")),
            "batch.size": config.get("batch_size", 16384),
            "linger.ms": config.get("linger_ms", 5),
        }
        # SASL auth
        sasl = config.get("sasl", {})
        if sasl.get("enabled"):
            producer_conf["security.protocol"] = "SASL_SSL" if config.get("tls", {}).get("enabled") else "SASL_PLAINTEXT"
            producer_conf["sasl.mechanism"] = sasl.get("mechanism", "PLAIN")
            producer_conf["sasl.username"] = sasl.get("username", "")
            producer_conf["sasl.password"] = sasl.get("password", "")
        # TLS
        tls = config.get("tls", {})
        if tls.get("enabled"):
            if "security.protocol" not in producer_conf:
                producer_conf["security.protocol"] = "SSL"
            if tls.get("ca"):
                producer_conf["ssl.ca.location"] = tls["ca"]
            if tls.get("cert"):
                producer_conf["ssl.certificate.location"] = tls["cert"]
            if tls.get("key"):
                producer_conf["ssl.key.location"] = tls["key"]

        self._producer = _KafkaProducer(producer_conf)
        log.info(f"Kafka writer: brokers={producer_conf['bootstrap.servers']} topic={self._topic}")

    def _delivery_cb(self, err, msg):
        if err:
            self._error_count += 1
            log.error(f"Kafka delivery failed: {err}")

    def write_batch(self, events: List[dict]) -> int:
        if not self._producer:
            return 0
        count = 0
        for event in events:
            payload = json.dumps(event, default=str).encode("utf-8")
            self._producer.produce(
                self._topic, value=payload,
                callback=self._delivery_cb,
            )
            self._byte_count += len(payload)
            count += 1
        self._producer.poll(0)  # trigger delivery callbacks
        self._event_count += count
        return count

    def flush(self):
        if self._producer:
            self._producer.flush(timeout=10)

    def close(self):
        self.flush()


# ===================================================================
# 5. LOKI (Grafana Loki push API)
# ===================================================================

class LokiWriter(BaseWriter):
    """Push events to Grafana Loki via /loki/api/v1/push."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._url = config.get("url", "http://localhost:3100/loki/api/v1/push")
        self._labels = config.get("labels", {"job": "bucket-export"})
        self._batch_size = config.get("batch_size", 100)
        self._buffer: List[dict] = []
        # Auth
        auth = config.get("auth", {})
        self._auth_type = auth.get("type", "none")
        self._auth_headers = {}
        if self._auth_type == "basic":
            import base64
            cred = base64.b64encode(
                f"{auth.get('username', '')}:{auth.get('password', '')}".encode()
            ).decode()
            self._auth_headers["Authorization"] = f"Basic {cred}"
        elif self._auth_type == "bearer":
            self._auth_headers["Authorization"] = f"Bearer {auth.get('token', '')}"
        # TLS
        self._verify = True
        tls = config.get("tls", {})
        if tls.get("enabled"):
            self._verify = tls.get("ca", True) if tls.get("verify", True) else False
        if not _requests_available:
            log.warning("requests not installed — Loki writer disabled")

    def write_batch(self, events: List[dict]) -> int:
        self._buffer.extend(events)
        count = len(events)
        self._event_count += count
        if len(self._buffer) >= self._batch_size:
            self.flush()
        return count

    def flush(self):
        if not self._buffer or not _requests_available:
            return
        events = self._buffer
        self._buffer = []
        # Loki expects: {"streams": [{"stream": {labels}, "values": [[ts_ns, line], ...]}]}
        label_str = self._labels.copy()
        values = []
        for e in events:
            ts = e.get("time", e.get("_time", time.time()))
            if isinstance(ts, str):
                try:
                    ts = float(ts)
                except ValueError:
                    ts = time.time()
            if ts > 1e12:
                ts_ns = str(int(ts * 1_000_000))  # ms → ns
            else:
                ts_ns = str(int(ts * 1_000_000_000))  # s → ns
            line = json.dumps(e, default=str)
            values.append([ts_ns, line])

        payload = {"streams": [{"stream": label_str, "values": values}]}
        headers = {"Content-Type": "application/json"}
        headers.update(self._auth_headers)
        try:
            resp = _requests.post(self._url, json=payload,
                                  headers=headers, verify=self._verify, timeout=10)
            resp.raise_for_status()
            self._byte_count += len(json.dumps(payload))
            log.info(f"Loki: pushed {len(values)} entries")
        except Exception as e:
            self._error_count += 1
            log.error(f"Loki push failed: {e}")

    def close(self):
        self.flush()


# ===================================================================
# 6. ELASTICSEARCH / OPENSEARCH
# ===================================================================

class ElasticsearchWriter(BaseWriter):
    """Bulk-index events into Elasticsearch or OpenSearch."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._index = config.get("index", "security-events")
        self._date_pattern = config.get("index_date_pattern", "%Y.%m.%d")
        self._batch_size = config.get("batch_size", 500)
        self._buffer: List[dict] = []
        self._es = None
        if not _es_available:
            log.warning("elasticsearch not installed — ES writer disabled")
            return
        hosts = config.get("hosts", ["http://localhost:9200"])
        auth_cfg = config.get("auth", {})
        es_kwargs = {"hosts": hosts}
        if auth_cfg.get("type") == "basic":
            es_kwargs["basic_auth"] = (auth_cfg.get("username", ""),
                                       auth_cfg.get("password", ""))
        elif auth_cfg.get("type") == "api_key":
            es_kwargs["api_key"] = auth_cfg.get("api_key", "")
        tls = config.get("tls", {})
        if tls.get("ca"):
            es_kwargs["ca_certs"] = tls["ca"]
        if not tls.get("verify", True):
            es_kwargs["verify_certs"] = False
        self._es = _Elasticsearch(**es_kwargs)
        log.info(f"ES writer: hosts={hosts} index={self._index}")

    def _index_name(self, event: dict) -> str:
        ts = event.get("time", event.get("_time", time.time()))
        if isinstance(ts, str):
            try:
                ts = float(ts)
            except ValueError:
                ts = time.time()
        if ts > 1e12:
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        suffix = dt.strftime(self._date_pattern)
        return f"{self._index}-{suffix}"

    def write_batch(self, events: List[dict]) -> int:
        self._buffer.extend(events)
        count = len(events)
        self._event_count += count
        if len(self._buffer) >= self._batch_size:
            self.flush()
        return count

    def flush(self):
        if not self._buffer or not self._es:
            return
        events = self._buffer
        self._buffer = []
        actions = []
        for e in events:
            actions.append({
                "_index": self._index_name(e),
                "_source": e,
            })
        try:
            success, errors = _es_bulk(self._es, actions, raise_on_error=False)
            self._byte_count += sum(len(json.dumps(a["_source"], default=str)) for a in actions)
            if errors:
                self._error_count += len(errors)
                log.warning(f"ES bulk: {success} ok, {len(errors)} errors")
            else:
                log.info(f"ES: indexed {success} events")
        except Exception as e:
            self._error_count += len(events)
            log.error(f"ES bulk failed: {e}")

    def close(self):
        self.flush()


# ===================================================================
# 7. WEBHOOK (Generic HTTP POST)
# ===================================================================

class WebhookWriter(BaseWriter):
    """Send events via HTTP POST to any endpoint."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._url = config.get("url", "")
        self._method = config.get("method", "POST").upper()
        self._headers = config.get("headers", {"Content-Type": "application/json"})
        self._format = config.get("format", "json")  # json | ndjson
        self._batch_mode = config.get("batch", True)
        self._batch_size = config.get("batch_size", 100)
        self._buffer: List[dict] = []
        # Retry
        retry = config.get("retry", {})
        self._retry_enabled = retry.get("enabled", True)
        self._retry_max = retry.get("max_attempts", 3)
        self._retry_backoff = retry.get("backoff_ms", 1000) / 1000.0
        if not _requests_available:
            log.warning("requests not installed — Webhook writer disabled")

    def write_batch(self, events: List[dict]) -> int:
        self._buffer.extend(events)
        count = len(events)
        self._event_count += count
        if len(self._buffer) >= self._batch_size:
            self.flush()
        return count

    def flush(self):
        if not self._buffer or not _requests_available:
            return
        events = self._buffer
        self._buffer = []
        if self._batch_mode:
            self._send(events)
        else:
            for event in events:
                self._send([event])

    def _send(self, events: List[dict]):
        if self._format == "ndjson":
            body = "\n".join(json.dumps(e, default=str) for e in events)
            content_type = "application/x-ndjson"
        else:
            body = json.dumps(events if self._batch_mode else events[0], default=str)
            content_type = "application/json"

        headers = dict(self._headers)
        headers.setdefault("Content-Type", content_type)

        for attempt in range(self._retry_max if self._retry_enabled else 1):
            try:
                resp = _requests.request(
                    self._method, self._url,
                    data=body, headers=headers, timeout=30)
                resp.raise_for_status()
                self._byte_count += len(body)
                log.info(f"Webhook: sent {len(events)} events → {self._url}")
                return
            except Exception as e:
                if attempt < self._retry_max - 1:
                    time.sleep(self._retry_backoff * (attempt + 1))
                else:
                    self._error_count += 1
                    log.error(f"Webhook failed after {self._retry_max} attempts: {e}")

    def close(self):
        self.flush()


# ===================================================================
# 8. SYSLOG (RFC 5424 — TCP/UDP/TLS)
# ===================================================================

class SyslogWriter(BaseWriter):
    """Send events as RFC 5424 syslog messages."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._host = config.get("host", "localhost")
        self._port = config.get("port", 514)
        self._protocol = config.get("protocol", "tcp").lower()
        self._facility = config.get("facility", 16)  # local0
        self._sock = None
        self._connect()

    def _connect(self):
        try:
            if self._protocol == "udp":
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            else:
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if self._protocol == "tls":
                    ctx = ssl.create_default_context()
                    tls_cfg = self.config.get("tls", {})
                    if tls_cfg.get("ca"):
                        ctx.load_verify_locations(tls_cfg["ca"])
                    if not tls_cfg.get("verify", True):
                        ctx.check_hostname = False
                        ctx.verify_mode = ssl.CERT_NONE
                    self._sock = ctx.wrap_socket(self._sock, server_hostname=self._host)
                self._sock.connect((self._host, self._port))
            log.info(f"Syslog writer: {self._protocol}://{self._host}:{self._port}")
        except Exception as e:
            log.error(f"Syslog connect failed: {e}")
            self._sock = None

    def write_batch(self, events: List[dict]) -> int:
        if not self._sock:
            return 0
        count = 0
        for event in events:
            msg = self._format_rfc5424(event)
            try:
                if self._protocol == "udp":
                    self._sock.sendto(msg, (self._host, self._port))
                else:
                    self._sock.sendall(msg)
                self._byte_count += len(msg)
                count += 1
            except Exception as e:
                self._error_count += 1
                log.error(f"Syslog send failed: {e}")
                self._connect()  # reconnect
                break
        self._event_count += count
        return count

    def _format_rfc5424(self, event: dict) -> bytes:
        """Format as RFC 5424: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID MSG"""
        severity = min(event.get("severity_id", 6), 7)  # 6 = informational
        pri = self._facility * 8 + severity
        ts = event.get("time", event.get("_time", time.time()))
        if isinstance(ts, (int, float)):
            if ts > 1e12:
                ts = ts / 1000.0
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        hostname = event.get("host", event.get("metadata", {}).get("source", "-"))[:255] or "-"
        app_name = "bucket-export"
        msg = json.dumps(event, default=str)
        syslog_line = f"<{pri}>1 {timestamp} {hostname} {app_name} - - - {msg}\n"
        return syslog_line.encode("utf-8")

    def flush(self):
        pass  # syslog sends immediately per event

    def close(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass


# ===================================================================
# 9. AZURE EVENT HUBS
# ===================================================================

class AzureEventHubWriter(BaseWriter):
    """Send events to Azure Event Hubs."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._connection_string = config.get("connection_string", "")
        self._eventhub_name = config.get("eventhub_name", "")
        self._batch_size = config.get("batch_size", 100)
        self._buffer: List[dict] = []
        self._producer = None
        if not _azure_eh_available:
            log.warning("azure-eventhub not installed — Azure EH writer disabled")
            return
        if not self._connection_string:
            log.error("Azure Event Hub: connection_string required")
            return
        try:
            self._producer = EventHubProducerClient.from_connection_string(
                self._connection_string,
                eventhub_name=self._eventhub_name,
            )
            log.info(f"Azure Event Hub writer: hub={self._eventhub_name}")
        except Exception as e:
            log.error(f"Azure EH init failed: {e}")

    def write_batch(self, events: List[dict]) -> int:
        self._buffer.extend(events)
        count = len(events)
        self._event_count += count
        if len(self._buffer) >= self._batch_size:
            self.flush()
        return count

    def flush(self):
        if not self._buffer or not self._producer:
            return
        events = self._buffer
        self._buffer = []
        try:
            batch = self._producer.create_batch()
            for e in events:
                payload = json.dumps(e, default=str).encode("utf-8")
                try:
                    batch.add(EventData(payload))
                except ValueError:
                    # Batch full — send and start new
                    self._producer.send_batch(batch)
                    batch = self._producer.create_batch()
                    batch.add(EventData(payload))
                self._byte_count += len(payload)
            self._producer.send_batch(batch)
            log.info(f"Azure EH: sent {len(events)} events")
        except Exception as e:
            self._error_count += len(events)
            log.error(f"Azure EH send failed: {e}")

    def close(self):
        self.flush()
        if self._producer:
            try:
                self._producer.close()
            except Exception:
                pass


# ===================================================================
# 10. GCP PUB/SUB
# ===================================================================

class GCPPubSubWriter(BaseWriter):
    """Publish events to Google Cloud Pub/Sub."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._project = config.get("project_id", "")
        self._topic = config.get("topic", "")
        self._batch_size = config.get("batch_size", 100)
        self._buffer: List[dict] = []
        self._publisher = None
        self._topic_path = ""
        if not _gcp_pubsub_available:
            log.warning("google-cloud-pubsub not installed — GCP Pub/Sub writer disabled")
            return
        if not self._project or not self._topic:
            log.error("GCP Pub/Sub: project_id and topic required")
            return
        # Credentials: uses GOOGLE_APPLICATION_CREDENTIALS env var or default
        creds_path = config.get("credentials_file")
        if creds_path:
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", creds_path)
        try:
            self._publisher = pubsub_v1.PublisherClient()
            self._topic_path = self._publisher.topic_path(self._project, self._topic)
            log.info(f"GCP Pub/Sub writer: {self._topic_path}")
        except Exception as e:
            log.error(f"GCP Pub/Sub init failed: {e}")

    def write_batch(self, events: List[dict]) -> int:
        self._buffer.extend(events)
        count = len(events)
        self._event_count += count
        if len(self._buffer) >= self._batch_size:
            self.flush()
        return count

    def flush(self):
        if not self._buffer or not self._publisher:
            return
        events = self._buffer
        self._buffer = []
        futures = []
        for e in events:
            payload = json.dumps(e, default=str).encode("utf-8")
            future = self._publisher.publish(self._topic_path, data=payload)
            futures.append(future)
            self._byte_count += len(payload)
        # Wait for all publishes
        errors = 0
        for f in futures:
            try:
                f.result(timeout=30)
            except Exception as e:
                errors += 1
                log.error(f"GCP Pub/Sub publish failed: {e}")
        self._error_count += errors
        log.info(f"GCP Pub/Sub: published {len(events) - errors}/{len(events)} events")

    def close(self):
        self.flush()


# ===================================================================
# 11. SPLUNK HEC (HTTP Event Collector)
# ===================================================================

class SplunkHECWriter(BaseWriter):
    """Send events to Splunk via HTTP Event Collector."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._url = config.get("url", "").rstrip("/")
        self._token = config.get("token", "")
        self._index = config.get("index", "main")
        self._sourcetype = config.get("sourcetype", "bucket_export:ocsf")
        self._batch_size = config.get("batch_size", 100)
        self._buffer: List[dict] = []
        self._verify = config.get("tls", {}).get("verify", True)
        if not self._url or not self._token:
            log.error("Splunk HEC: url and token required")
        if not _requests_available:
            log.warning("requests not installed — Splunk HEC writer disabled")

    def write_batch(self, events: List[dict]) -> int:
        self._buffer.extend(events)
        count = len(events)
        self._event_count += count
        if len(self._buffer) >= self._batch_size:
            self.flush()
        return count

    def flush(self):
        if not self._buffer or not _requests_available or not self._url:
            return
        events = self._buffer
        self._buffer = []
        endpoint = f"{self._url}/services/collector/event"
        headers = {
            "Authorization": f"Splunk {self._token}",
            "Content-Type": "application/json",
        }
        # HEC accepts multiple JSON objects concatenated (no array, no newline required)
        payload_parts = []
        for e in events:
            ts = e.get("time", e.get("_time", time.time()))
            if isinstance(ts, str):
                try:
                    ts = float(ts)
                except ValueError:
                    ts = time.time()
            if ts > 1e12:
                ts = ts / 1000.0
            hec_event = {
                "time": ts,
                "index": self._index,
                "sourcetype": self._sourcetype,
                "event": e,
            }
            host = e.get("host", e.get("metadata", {}).get("source", ""))
            if host:
                hec_event["host"] = host
            payload_parts.append(json.dumps(hec_event, default=str))

        body = "\n".join(payload_parts)
        try:
            resp = _requests.post(endpoint, data=body, headers=headers,
                                  verify=self._verify, timeout=30)
            resp.raise_for_status()
            self._byte_count += len(body)
            log.info(f"Splunk HEC: sent {len(events)} events → {self._url}")
        except Exception as e:
            self._error_count += len(events)
            log.error(f"Splunk HEC failed: {e}")

    def close(self):
        self.flush()


# ===================================================================
# WRITER REGISTRY + FACTORY
# ===================================================================

WRITER_REGISTRY = {
    "parquet":          ParquetWriter,
    "local-parquet":    ParquetWriter,
    "s3":               S3Writer,
    "json":             JsonWriter,
    "local-json":       JsonWriter,
    "kafka":            KafkaWriter,
    "loki":             LokiWriter,
    "elasticsearch":    ElasticsearchWriter,
    "opensearch":       ElasticsearchWriter,
    "webhook":          WebhookWriter,
    "syslog":           SyslogWriter,
    "azure-eventhubs":  AzureEventHubWriter,
    "gcp-pubsub":       GCPPubSubWriter,
    "splunk-hec":       SplunkHECWriter,
}


def create_writer(name: str, config: dict) -> BaseWriter:
    """Instantiate a writer by name."""
    cls = WRITER_REGISTRY.get(name.lower())
    if cls is None:
        raise ValueError(f"Unknown writer type: {name}. "
                         f"Available: {sorted(set(WRITER_REGISTRY.keys()))}")
    return cls(config)


def create_writers(writers_config: dict) -> List[BaseWriter]:
    """
    Create multiple writers from config dict.

    Config format:
      writers:
        parquet:
          enabled: true
          path: /data/export/parquet
        s3:
          enabled: true
          bucket: my-bucket
        kafka:
          enabled: false
          brokers: [...]
    """
    writers = []
    for name, cfg in writers_config.items():
        if not isinstance(cfg, dict):
            continue
        if not cfg.get("enabled", True):
            continue
        try:
            w = create_writer(name, cfg)
            writers.append(w)
            log.info(f"Initialized writer: {name}")
        except Exception as e:
            log.error(f"Failed to initialize writer {name}: {e}")
    return writers
