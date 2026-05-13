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
  echo "## Throughput и задержки"
  echo
  echo "| strategy | mix | read_ratio | ops | thr req/s | avg ms | p50 ms | p95 ms | p99 ms | max ms |"
  echo "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"
  awk -F, 'NR>1 {printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
    $2,$3,$4,$10,$13,$14,$15,$16,$17,$18}' "$CSV"

  echo
  echo "## Кеш и обращения к БД"
  echo
  echo "| strategy | mix | cache hits | cache misses | hit rate | db gets | db sets | db total |"
  echo "|---|---|---:|---:|---:|---:|---:|---:|"
  awk -F, 'NR>1 {
    hr = $21 * 100;
    printf "| %s | %s | %s | %s | %.1f%% | %s | %s | %s |\n",
    $2,$3,$19,$20,hr,$22,$23,$24}' "$CSV"

  echo
  echo "## Write-Back: накопление грязных записей"
  echo
  echo "| strategy | mix | wb max dirty | wb final dirty |"
  echo "|---|---|---:|---:|"
  awk -F, 'NR>1 {printf "| %s | %s | %s | %s |\n", $2,$3,$25,$26}' "$CSV"

} > "$OUT"

echo "wrote $OUT"
