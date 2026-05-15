# Multi pub/sub demo

Топология: **4 publisher + 5 consumer**.

```
demo/a  --[pub_a]-->  broker  -->  sub_a1
                                \--> sub_a2     (общий топик, 2 подписчика)
demo/b  --[pub_b]-->  broker  -->  sub_b       (1 на 1)
demo/c  --[pub_c]-->  broker  -->  sub_c       (1 на 1)
demo/d  --[pub_d]-->  broker  -->  sub_d       (1 на 1)
```

## Запуск

```bash
cd example/multi
docker compose up --build
```

UI:

- Admin: `http://localhost:8080` (`admin/admin`)
- Prometheus: `http://localhost:9091`
- Grafana: `http://localhost:3000` (`admin/admin`)

Контейнеры:

| Service | Роль | Topic | Client ID |
|---------|------|-------|-----------|
| broker  | сам брокер | — | — |
| prometheus | сбор метрик | — | — |
| grafana | дашборды | — | — |
| pub_a   | publisher  | `demo/a` | `pub_a` |
| pub_b   | publisher  | `demo/b` | `pub_b` |
| pub_c   | publisher  | `demo/c` | `pub_c` |
| pub_d   | publisher  | `demo/d` | `pub_d` |
| sub_a1  | consumer   | `demo/a` | `sub_a1` |
| sub_a2  | consumer   | `demo/a` | `sub_a2` |
| sub_b   | consumer   | `demo/b` | `sub_b`  |
| sub_c   | consumer   | `demo/c` | `sub_c`  |
| sub_d   | consumer   | `demo/d` | `sub_d`  |

Каждый publisher шлёт `[<client_id>] msg N` раз в секунду с QoS=1.
Каждый consumer пишет в stdout полученные сообщения с их `server_msg_id`.

## Что проверить

```bash
# Логи всех консьюмеров
docker compose logs -f sub_a1 sub_a2 sub_b sub_c sub_d

# Логи только общего топика — оба должны видеть одинаковые сообщения от pub_a
docker compose logs -f sub_a1 sub_a2

# Метрики брокера напрямую
curl http://localhost:9090/metrics | grep -E "mqtt_(publish|deliver)_total"

# Prometheus scrape targets
open http://localhost:9091/targets

# Admin UI с live tail
open http://localhost:8080

# Grafana dashboards
open http://localhost:3000
```

`sub_a1` и `sub_a2` получают **каждое** сообщение от `pub_a` (это и есть pub/sub на одном топике).
Остальные пары — изолированы своими топиками.

Пользователи в [config/users.yaml](../../config/users.yaml):

- `pub:pub` — публикует в `demo/#`;
- `sub:sub` — подписывается на `demo/#`.

## Остановить

```bash
docker compose down -v
```
