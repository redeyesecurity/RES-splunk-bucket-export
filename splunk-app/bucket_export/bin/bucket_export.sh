#!/bin/sh
# bucket_export.sh — Launcher for the Splunk app scripted input
# Runs the exporter using Splunk's embedded Python

SPLUNK_HOME="${SPLUNK_HOME:-/opt/splunk}"
APP_DIR="${SPLUNK_HOME}/etc/apps/bucket_export"
CONFIG="${APP_DIR}/local/config.yaml"

if [ ! -f "$CONFIG" ]; then
    CONFIG="${APP_DIR}/default/config.yaml"
fi

exec "${SPLUNK_HOME}/bin/splunk" cmd python3 \
    "${APP_DIR}/bin/exporter.py" \
    --config "$CONFIG" \
    --splunk-home "$SPLUNK_HOME"
