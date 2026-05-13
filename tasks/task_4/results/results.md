# Результаты прогонов

Источник: `results/results.csv`

## Throughput и задержки

| strategy | mix | read_ratio | ops | thr req/s | avg ms | p50 ms | p95 ms | p99 ms | max ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| cache_aside | read_heavy | 0.80 | 102500 | 5125 | 0.198 | 0.147 | 0.380 | 0.654 | 21.637 |
| cache_aside | balanced | 0.50 | 102498 | 5125 | 0.263 | 0.235 | 0.516 | 0.898 | 21.114 |
| cache_aside | write_heavy | 0.20 | 102502 | 5125 | 0.210 | 0.183 | 0.402 | 0.894 | 72.642 |
| write_through | read_heavy | 0.80 | 102501 | 5125 | 0.118 | 0.099 | 0.193 | 0.380 | 31.136 |
| write_through | balanced | 0.50 | 102501 | 5125 | 0.219 | 0.166 | 0.499 | 1.140 | 24.606 |
| write_through | write_heavy | 0.20 | 102447 | 5122 | 0.224 | 0.179 | 0.447 | 1.078 | 178.170 |
| write_back | read_heavy | 0.80 | 102504 | 5125 | 0.138 | 0.129 | 0.192 | 0.302 | 12.966 |
| write_back | balanced | 0.50 | 102503 | 5125 | 0.221 | 0.202 | 0.353 | 0.653 | 26.382 |
| write_back | write_heavy | 0.20 | 102501 | 5125 | 0.142 | 0.134 | 0.210 | 0.337 | 18.659 |

## Кеш и обращения к БД

| strategy | mix | cache hits | cache misses | hit rate | db gets | db sets | db total |
|---|---|---:|---:|---:|---:|---:|---:|
| cache_aside | read_heavy | 59746 | 22120 | 73.0% | 22120 | 20634 | 42754 |
| cache_aside | balanced | 25897 | 25433 | 50.4% | 25433 | 51168 | 76601 |
| cache_aside | write_heavy | 4242 | 16237 | 20.7% | 16237 | 82023 | 98260 |
| write_through | read_heavy | 81907 | 0 | 100.0% | 0 | 20594 | 20594 |
| write_through | balanced | 51058 | 0 | 100.0% | 0 | 51443 | 51443 |
| write_through | write_heavy | 20546 | 0 | 100.0% | 0 | 81901 | 81901 |
| write_back | read_heavy | 81894 | 0 | 100.0% | 0 | 20358 | 20358 |
| write_back | balanced | 50981 | 0 | 100.0% | 0 | 49757 | 49757 |
| write_back | write_heavy | 20524 | 0 | 100.0% | 0 | 80200 | 80200 |

## Write-Back: накопление грязных записей

| strategy | mix | wb max dirty | wb final dirty |
|---|---|---:|---:|
| cache_aside | read_heavy | 0 | 0 |
| cache_aside | balanced | 0 | 0 |
| cache_aside | write_heavy | 0 | 0 |
| write_through | read_heavy | 0 | 0 |
| write_through | balanced | 0 | 0 |
| write_through | write_heavy | 0 | 0 |
| write_back | read_heavy | 512 | 13 |
| write_back | balanced | 513 | 115 |
| write_back | write_heavy | 513 | 58 |
