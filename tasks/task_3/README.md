# Task 3 — Сравнение RabbitMQ и Redis как брокеров сообщений

Стенд и утилита для замера `throughput / latency / loss` на одинаковой нагрузке для RabbitMQ и Redis Streams.

## Архитектура

```
producer(s)  ->  broker (RabbitMQ | Redis Streams)  ->  consumer(s)
```

- Producer кладёт в первые 8 байт payload timestamp в наносекундах, остальное — случайный шум заданного размера.
- Consumer читает, извлекает timestamp, считает `now - ts` — это end-to-end latency.
- После фазы публикации работает `warmup` окно (2s по умолчанию), чтобы consumer успел разгрести очередь.
- Итог: sent / received / lost / p50 / p95 / p99 / max, всё пишется в CSV.

### Настройки, сделанные равными

|  | RabbitMQ | Redis |
|---|---|---|
| confirm delivery | `confirm mode` + блокирующее ожидание ack | `XADD` — write synchronous to server, без ACK отдельно |
| prefetch | `prefetch=256` | `COUNT=256` |
| payload | одинаковые байты одинаковой длины | |
| ресурсы | cpus=2, memory=1G | cpus=2, memory=1G |
| инстанс | одиночный, без кластера | одиночный, без кластера |

Это не идеально: RabbitMQ по умолчанию «тяжелее» (AMQP, confirm, per-msg ack), Redis Streams проще. Именно это и сравниваем — «из коробки на одинаковых условиях».

## Запуск

```bash
cd tasks/task_3

# 1. Поднять брокеры
docker compose up -d
docker compose ps

# 2. Прогнать полную матрицу (размер x интенсивность x брокер)
bash scripts/run-all.sh

# 3. Свести в markdown
bash scripts/csv-to-md.sh results/results.csv results/results.md

# 4. Остановить
docker compose down
```

Параметры прогона матрицы (env):

| Переменная | default | что значит |
|---|---|---|
| `SIZES` | `128 1024 10240 102400` | размеры payload в байтах |
| `RATES` | `1000 5000 10000` | целевая интенсивность msg/s |
| `DURATION` | `20s` | длительность публикации |
| `PRODUCERS` | `1` | число producer goroutines |
| `CONSUMERS` | `1` | число consumer goroutines |
| `BROKERS` | `rabbitmq redis` | какие брокеры тестировать |

Пример кастомного прогона:

```bash
SIZES="128 1024" RATES="5000 10000 20000" DURATION=30s bash scripts/run-all.sh
```

Ручной одиночный прогон:

```bash
go run ./cmd/bench \
  -broker redis \
  -size 1024 \
  -rate 10000 \
  -duration 15s \
  -producers 2 -consumers 2 \
  -out results/single.csv -label demo
```

## Методика замеров

- `messages/sec` считаем как `received / duration` — это фактический sustained throughput (а не то, сколько пытались запушить).
- `lost = sent - received` после окна warmup. Если `> 0` — либо очередь не успела разгрестись, либо реально потери (для RabbitMQ persistent почти всегда 0, для Redis без AOF — возможны при падении).
- Latency собираем из канала 64k событий — если канал переполнен, часть пропускаем (чистый instrumentation-overhead, не влияет на доставку). Репрезентативно при sustained нагрузке.
- Насыщение (saturation / knee point) ищем повышая `RATES` и смотря, когда `received` начинает отставать от `sent` и p95 резко растёт.

## Колонки CSV

```
label, broker, size_bytes, target_rate, duration_s, producers, consumers,
sent, received, lost, send_err, recv_err, throughput_msgs,
avg_ms, p50_ms, p95_ms, p99_ms, max_ms
```

## Результаты (фактические, прогон на macOS + Docker Desktop)

Параметры прогона:

- `DURATION=12s`, 1 producer + 1 consumer;
- лимиты: `cpus=2`, `memory=1024M` на контейнер (в `docker-compose.yml`);
- RabbitMQ: durable queue, `persistent` delivery, publish `confirm`, ручной ack, `prefetch=256`;
- Redis Streams: без AOF, `XADD` → `XREADGROUP` → `XACK` → `XDEL`.

Полная таблица — в [results/results.csv](results/results.csv), она же сведена в [results/results.md](results/results.md).

### 1. Базовое сравнение (size=1 KB, rate=5 000 msg/s)

| broker | sent | received | lost | thr msg/s | avg ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| RabbitMQ | 12 510 | 12 510 | 0 | **1 042** | 0.34 | 0.49 | 0.89 |
| Redis | 62 449 | 62 449 | 0 | **5 204** | 0.35 | 0.63 | 1.05 |

Redis выдерживает целевые 5 000 msg/s, RabbitMQ упирается в confirm-роундтрип и отдаёт ~1 000 msg/s независимо от цели.

### 2. Влияние размера сообщения (rate=5 000 msg/s target)

| size | RabbitMQ thr msg/s | RabbitMQ p95 ms | RabbitMQ p99 ms | Redis thr msg/s | Redis p95 ms | Redis p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| 128 B | 890 | 0.76 | 2.24 | 5 204 | 0.58 | 1.05 |
| 1 KB | 1 042 | 0.49 | 0.89 | 5 204 | 0.63 | 1.05 |
| 10 KB | 1 064 | 0.63 | 1.10 | 5 178 | 0.92 | 1.30 |
| 100 KB | 288 | 4.62 | **17.10** | 1 006 | 4.74 | 7.64 |

На 100 KB RabbitMQ режет пропускную способность в 3.5× и раздувает p99 в 19×. Redis тоже «зажимается» (~1 000 msg/s = 100 MB/s сетевого трафика — потолок loopback + CPU), но latency остаётся стабильной.

### 3. Влияние интенсивности (size=1 KB)

| target rate | RabbitMQ thr | RabbitMQ p95 | RabbitMQ lost | Redis thr | Redis p95 | Redis lost |
|---|---:|---:|---:|---:|---:|---:|
| 1 000 | 991 | 0.45 | 0 | 1 042 | 0.82 | 0 |
| 5 000 | 1 042 | 0.49 | 0 | 5 204 | 0.63 | 0 |
| 10 000 | **1 114** | 0.42 | 0 | **7 443** | 0.62 | 0 |

RabbitMQ **не пробивает ~1 100 msg/s** — это потолок sync-confirm на 1 инстансе. Redis держит почти целевой rate до 10k, дальше на 10KB уже «плывёт» (см. ниже).

### 4. Knee point (точка деградации) — факт

| broker | Где ломается | Что видно в метриках |
|---|---|---|
| RabbitMQ 1 KB | target 5 000+ msg/s | throughput замораживается на ~1.1k; latency ещё ок, но `sent < target` — backlog копится, если продьюсер не rate-limit'ится |
| RabbitMQ 100 KB | уже на 1 000 msg/s | throughput 288–352, p99 17–31 ms, max до 737 ms |
| Redis 1 KB | 10k+ msg/s на одном producer | 7.4–7.8k устойчиво (потолок single-thread CPU клиента/сервера) |
| Redis 10 KB | 10k msg/s | 4.7k (≈47 MB/s), p99 ~1.9 ms — начинается деградация |
| Redis 100 KB | уже на 1k msg/s | 1.0k (≈100 MB/s), потолок сети/CPU, p99 ~7 ms стабильно |

Потерь (`lost = sent − received`) во всех прогонах = 0, ошибок публикации = 0 — что и ожидается: RabbitMQ возвращает ack на каждый publish, Redis не теряет под лимитами; rate-limiter на стороне producer сглаживает burst.

## Выводы

1. **Пропускная способность.** На 1 KB / 10 000 msg/s Redis Streams отдаёт **7 443 msg/s** против **1 114 msg/s** у RabbitMQ — разница ~**6.7×** в пользу Redis. Причина: у RabbitMQ в durable+confirm-режиме каждый publish — сетевой roundtrip + fsync-подобная запись.
2. **Размер сообщения.** RabbitMQ заметно ломается на **100 KB**: throughput падает в 3.5×, p99 с 0.9 мс до 17–31 мс, max до 737 мс. Redis на 100 KB упирается в пропускную способность сети (~100 MB/s) и отдаёт стабильно ~1 000 msg/s с ровной latency (~4–7 мс p95/p99). На «нормальных» размерах 128 B – 10 KB Redis почти линейно масштабируется.
3. **Точка деградации single instance.**
   - RabbitMQ: **~1 000–1 100 msg/s** — это полка для одного инстанса в sync-confirm-режиме, хоть на 1 KB, хоть на 10 KB. На 100 KB уже полка в ~300 msg/s.
   - Redis: **~7–8 тыс msg/s** на 1 KB, **~5 тыс msg/s** на 10 KB, **~1 тыс msg/s** на 100 KB.
4. **Надёжность.** На прогонах с включённой durability (RabbitMQ persistent) и Redis без AOF *потерь не было*. Но это нагрузочный сценарий без падения брокера. В сценарии kill -9:
   - RabbitMQ не теряет acknowledged сообщения (confirm == запись на диск);
   - Redis без AOF теряет всё, что было в памяти с момента старта. Для честного сравнения durability надо включать `appendonly yes` + `appendfsync everysec`, что режет throughput Redis обычно в 2–3×.
5. **Какой инструмент лучше для такого сценария.** Для этого теста собственный Go-харнесс подходит лучше k6/JMeter: он нативно говорит по AMQP 0.9.1 (`amqp091-go`) и по RESP (`go-redis`), измеряет end-to-end latency через timestamp в payload и даёт одинаковую модель producer/consumer/ack для обоих брокеров. k6/JMeter требуют сторонних плагинов для AMQP/Redis и меньше контроля над ack-семантикой.

## Сводная рекомендация

- **RabbitMQ single instance** — для сценариев, где важна durability и богатый routing (exchanges, dead letter, TTL, RPC-style) и поток **до ~1 тыс msg/s**; горизонтально масштабируется кластером и shovels.
- **Redis Streams single instance** — для потокового pub-sub / событийной шины с потоком **до ~5–10 тыс msg/s**, там где достаточно offset-based consumer groups и не нужна сложная маршрутизация. Для durability — обязательно AOF.
- На **>100 KB payload** оба брокера деградируют; в этом диапазоне правильнее хранить тело в blob store (S3/minio) и класть в очередь только ссылку.

## Что ещё можно снять

- CPU/RAM контейнеров: `docker stats --no-stream` параллельно прогону (скрипт не добавлен, запускать в другом терминале).
- Backlog RabbitMQ: `docker exec bench_rabbitmq rabbitmqctl list_queues name messages`.
- Backlog Redis: `docker exec bench_redis redis-cli XLEN bench-<size>`.
