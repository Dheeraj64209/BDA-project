#!/usr/bin/env bash
# Runs the weekly return-rate MapReduce job on YARN over every event the consumer has stored in HDFS.
# Usage (from the project folder):  docker compose exec resourcemanager bash /opt/jobs/run_job.sh
set -euo pipefail

INPUT=/data/returns/raw
OUTPUT=/data/returns/output

# MapReduce refuses to write into an existing folder, so clear the previous run's results first.
hdfs dfs -rm -r -f -skipTrash "$OUTPUT"

hadoop jar /opt/jobs/return-rate.jar ReturnRateJob "$INPUT" "$OUTPUT"

echo
echo "Result: $OUTPUT/part-r-00000  (week, category, orders, returns, return_rate, status)"
hdfs dfs -cat "$OUTPUT/part-r-*" | awk -F'\t' '
  { rows++ }
  $6 == "FLAGGED" { flagged++ }
  END { printf "%d category-weeks computed, %d flagged above the 10%% return-rate threshold\n", rows, flagged }'
echo "Dashboard: http://localhost:8501"
