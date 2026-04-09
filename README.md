# RES-splunk-bucket-export

Export already-indexed Splunk buckets to data lake destinations with optional OCSF transformation.

## Overview

This tool reads Splunk bucket directories (hot/warm/cold/frozen), decompresses `journal.gz` files, extracts individual events, optionally transforms them to OCSF v1.1, and writes to one or more destinations:

- **Local Parquet** — Hive-partitioned Parquet files (Snappy/Gzip/Zstd compression)
- **S3** — S3-compatible storage (AWS, MinIO, Cloudflare R2) as Parquet or compressed JSON
- **Local JSON** — Newline-delimited JSON files

Runs as either a **Splunk app** (scripted input on indexers) or **standalone** Python CLI.

## Architecture

```
Splunk Buckets → bucket_reader.py → ocsf_mapper.py → export_writer.py → Destinations
                                          ↑                                    ↓
                              (optional OCSF v1.1)              Parquet / S3 / JSON
                                                                       ↓
                                                            exporter.py (orchestrator)
                                                            + export_state.json (dedup)
```

## Quick Start

### Standalone Mode

```bash
pip install pyyaml pyarrow boto3
cp splunk-app/bucket_export/default/config.yaml config.yaml
# Edit config.yaml — set splunk_home, enable writers
python3 standalone/run_export.py --config config.yaml
```

### Splunk App Mode

```bash
cp -r splunk-app/bucket_export $SPLUNK_HOME/etc/apps/
cp $SPLUNK_HOME/etc/apps/bucket_export/default/config.yaml \
   $SPLUNK_HOME/etc/apps/bucket_export/local/config.yaml
# Edit local/config.yaml
# Enable the scripted input:
splunk edit input script://bucket_export.sh -disabled 0
splunk restart
```

### Dry Run (list buckets without exporting)

```bash
python3 standalone/run_export.py --config config.yaml --dry-run
```

## Configuration

See `splunk-app/bucket_export/default/config.yaml` for the full reference.
Key settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `splunk_home` | `/opt/splunk` | Path to Splunk installation |
| `batch_size` | `500` | Events per write batch |
| `ocsf.enabled` | `true` | Transform to OCSF v1.1 |
| `state_file` | `export_state.json` | Tracks exported buckets |
| `writers.parquet.path` | `/data/splunk-export/parquet` | Parquet output dir |
| `writers.s3.bucket` | — | S3 bucket name |
| `writers.s3.region` | `us-east-1` | AWS region |

## Modules

| File | Purpose |
|------|---------|
| `bucket_reader.py` | Discovers buckets, parses journal.gz, yields events |
| `ocsf_mapper.py` | Maps Splunk fields to OCSF v1.1 event classes |
| `export_writer.py` | Parquet, S3, and JSON writers with hive partitioning |
| `exporter.py` | Main orchestrator with state tracking and CLI |

## OCSF Classes Supported

Authentication (4002), Network Activity (3005), DNS Activity (3001),
HTTP Activity (3003), Process Activity (6001), File System Activity (1001),
Security Finding (2001), Account Change (4001), Unknown (0000 fallback).

## Requirements

- Python 3.8+
- `pyyaml` (required)
- `pyarrow` (for Parquet export)
- `boto3` (for S3 export)

## License

MIT
