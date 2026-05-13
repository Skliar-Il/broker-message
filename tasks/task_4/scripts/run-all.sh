#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

OUT="results/results.csv"
mkdir -p results
rm -f "$OUT" bench.db

STRATEGIES=(${STRATEGIES:-cache_aside write_through write_back})
DURATION=${DURATION:-20s}
RATE=${RATE:-5000}
WORKERS=${WORKERS:-4}
KEYS=${KEYS:-10000}
VALUE_SIZE=${VALUE_SIZE:-256}
WB_INTERVAL=${WB_INTERVAL:-200ms}
WB_BATCH=${WB_BATCH:-512}

# Mix definitions: name:read_ratio
declare -a MIXES=(
  "read_heavy:0.8"
  "balanced:0.5"
  "write_heavy:0.2"
)

export GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}

echo "Building bench..."
go build -o ./bin/bench ./cmd/bench

mkdir -p bin

for strategy in "${STRATEGIES[@]}"; do
  for mix_def in "${MIXES[@]}"; do
    mix="${mix_def%%:*}"
    read_ratio="${mix_def##*:}"
    label="${strategy}_${mix}"
    echo "=========================================="
    echo "RUN label=$label  strategy=$strategy  mix=$mix  read_ratio=$read_ratio"
    echo "=========================================="
    ./bin/bench \
      -strategy "$strategy" \
      -mix "$mix" \
      -read-ratio "$read_ratio" \
      -duration "$DURATION" \
      -rate "$RATE" \
      -workers "$WORKERS" \
      -keys "$KEYS" \
      -value-size "$VALUE_SIZE" \
      -wb-interval "$WB_INTERVAL" \
      -wb-batch "$WB_BATCH" \
      -db-path bench.db \
      -label "$label" \
      -out "$OUT" \
      || echo "(bench exited non-zero, continuing)"
    sleep 1
  done
done

echo ""
echo "Done. CSV: $OUT"
