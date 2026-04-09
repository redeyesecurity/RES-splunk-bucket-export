# Implementation Notes

## Journal Format (Reverse-Engineered from Splunk 9.1)

Splunk 9.1+ uses **zstandard** compression (`journal.zst`), older versions use gzip (`journal.gz`).

### Decompressed Structure

The journal contains **slices** — groups of events sharing common metadata:

```
[global header]
  host::hostname
[slice 1]
  $source::/path/to/source1
  sourcetype::type1
  [binary framing + event text blocks]
[slice 2]
  #source::/path/to/source2
  sourcetype::type2
  [binary framing + event text blocks]
[slice 3]
  @source::/path/to/source3
  ...
```

### Key Findings

1. **Source markers use prefix chars**: `$source::`, `#source::`, `@source::` — not plain `source::`
2. **Sourcetype follows source directly** with no null separator
3. **Host is global** — appears once at the top of the journal
4. **Events are plain UTF-8 text** interleaved with binary protobuf-like framing
5. **Binary framing between events** contains timestamps, field positions, date components
6. **3 mega-records** per journal (separated by `\x0a\x01` markers), not one-record-per-event

### Timestamp Extraction

Timestamps are extracted from the raw event text (not from binary framing) using pattern matching:
- ISO: `2026-04-07 23:20:22`
- Splunk internal: `04-07-2026 23:26:38.884`
- Syslog: `Apr  7 23:19:15`

The binary framing does contain encoded timestamps but their exact format varies. Text extraction is reliable.

### Bucket Types on Disk

```
/opt/splunk/var/lib/splunk/<index>/
  db/                          # warm + hot buckets
    db_<latest>_<earliest>_<id>/    # warm
    hot_v1_<id>/                    # hot
  colddb/                      # cold buckets
    colddb_<latest>_<earliest>_<id>/
```

Each bucket has `rawdata/journal.zst` (or `.gz`) and optional `slicesv2.dat`.

## Test Results (April 9, 2026)

### Environment
- Splunk indexer: Docker container on NAS (192.168.1.239), Splunk 9.1
- MinIO: localhost:9000 (bronze bucket)
- Mac: 192.168.1.61

### defaultdb (main index, warm bucket)
- Journal: 109KB compressed → 131KB decompressed
- **644 events** extracted across 3 sourcetypes:
  - `macos:install` (6 events)
  - `macos:system` (2 events)
  - `splunkd` (636 events)
- Host correctly extracted: `Mac.lucashouse.info`
- All events OCSF-mapped and written to:
  - Local Parquet: 35KB (snappy compressed)
  - Local JSON: 412KB (NDJSON)
  - MinIO S3: 2 Parquet files, 35KB total, hive-partitioned

### _internaldb (_internal index, warm bucket)
- Journal: 14.5MB compressed → 131KB decompressed
- **489 events** across 5 sourcetypes
- Host: `cedccfc08e0c` (Docker container ID)

## All 11 Destinations

| # | Writer | Dependencies | Status |
|---|--------|-------------|--------|
| 1 | Local Parquet | pyarrow | Tested ✓ |
| 2 | S3 (MinIO) | boto3 + pyarrow | Tested ✓ |
| 3 | Local JSON | (none) | Tested ✓ |
| 4 | Kafka | confluent-kafka | Implemented |
| 5 | Loki | requests | Implemented |
| 6 | Elasticsearch | elasticsearch | Implemented |
| 7 | Webhook | requests | Implemented |
| 8 | Syslog | (none, stdlib) | Implemented |
| 9 | Azure Event Hubs | azure-eventhub | Implemented |
| 10 | GCP Pub/Sub | google-cloud-pubsub | Implemented |
| 11 | Splunk HEC | requests | Implemented |

## Known Limitations

1. **Timestamp from text only** — binary framing timestamps not decoded yet
2. **Empty sourcetype** — some journal sections lack clear `source::` markers
3. **No incremental hot bucket support** — hot buckets are read fully, not tailed
4. **Single-threaded** — buckets processed sequentially (parallelism TBD)
