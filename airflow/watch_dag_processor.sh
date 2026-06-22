#!/bin/bash

set -u

AIRFLOW_BIN="/home/asha/airflow_env/bin/airflow"
PROCESS_PATTERN="/home/asha/airflow_env/bin/airflow dag-processor"
PROCESS_LOG="/home/asha/airflow/airflow-dag-processor.log"
WATCHDOG_LOG="/home/asha/airflow/airflow-dag-processor-watchdog.log"
LOCK_FILE="/home/asha/airflow/airflow-dag-processor-watchdog.lock"
CHECK_INTERVAL_SECONDS=15

start_processor() {
    nohup "$AIRFLOW_BIN" dag-processor >> "$PROCESS_LOG" 2>&1 < /dev/null &
}

is_processor_running() {
    pgrep -f "$PROCESS_PATTERN" > /dev/null 2>&1
}

# Prevent duplicate watchdog instances.
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    exit 0
fi

# Stop legacy daemon-mode process if one is still alive.
pkill -f "$PROCESS_PATTERN -D" > /dev/null 2>&1 || true

if ! is_processor_running; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') dag-processor not running; starting." >> "$WATCHDOG_LOG"
    start_processor
fi

while true; do
    if ! is_processor_running; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') dag-processor exited; restarting." >> "$WATCHDOG_LOG"
        start_processor
    fi
    sleep "$CHECK_INTERVAL_SECONDS"
done
