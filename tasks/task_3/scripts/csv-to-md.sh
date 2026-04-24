#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

CSV="${1:-results/results.csv}"
OUT="${2:-results/results.md}"

if [ ! -f "$CSV" ]; then
  echo "no csv: $CSV" >&2
  exit 1
fi

{
  echo "# Результаты прогонов"
  echo
  echo "Источник: \`$CSV\`"
  echo
  echo "| broker | size B | rate/s | sent | recv | lost | thr msg/s | avg ms | p95 ms | p99 ms | max ms |"
  echo "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
  awk -F, 'NR>1 {printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
    $2,$3,$4,$8,$9,$10,$13,$14,$16,$17,$18}' "$CSV"
} > "$OUT"

echo "wrote $OUT"
