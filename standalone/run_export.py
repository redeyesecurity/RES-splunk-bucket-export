#!/usr/bin/env python3
"""
run_export.py — Standalone CLI for bucket export

Run outside of Splunk. Requires Python 3.8+ and optionally
pyarrow (Parquet) and boto3 (S3).

Usage:
  python3 run_export.py --config config.yaml --splunk-home /opt/splunk
  python3 run_export.py --dry-run --indexes main security
  python3 run_export.py --bucket-types warm cold --max-events 1000
"""

import os
import sys

# Add the Splunk app bin/ to path so we can import the modules
APP_BIN = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "splunk-app", "bucket_export", "bin"
)
sys.path.insert(0, APP_BIN)

from exporter import main

if __name__ == "__main__":
    main()
