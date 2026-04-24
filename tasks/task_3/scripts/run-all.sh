#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

OUT="results/results.csv"
mkdir -p results
rm -f "$OUT"

SIZES=(${SIZES:-128 1024 10240 102400})
RATES=(${RATES:-1000 5000 10000})
DURATION=${DURATION:-20s}
PRODUCERS=${PRODUCERS:-1}
CONSUMERS=${CONSUMERS:-1}
BROKERS=(${BROKERS:-rabbitmq redis})

export GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}

echo "Building bench..."
go build -o ./bin/bench ./cmd/bench

for broker in "${BROKERS[@]}"; do
  for size in "${SIZES[@]}"; do
    for rate in "${RATES[@]}"; do
      label="${broker}_s${size}_r${rate}"
      echo "=========================================="
      echo "RUN label=$label"
      echo "=========================================="
      ./bin/bench \
        -broker "$broker" \
        -size "$size" \
        -rate "$rate" \
        -duration "$DURATION" \
        -producers "$PRODUCERS" \
        -consumers "$CONSUMERS" \
        -queue "bench-$size" \
        -label "$label" \
        -out "$OUT" \
        || echo "(bench exited non-zero, continuing)"
      sleep 2
    done
  done
done

echo "Done. CSV: $OUT"
