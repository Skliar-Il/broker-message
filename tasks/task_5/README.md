# Task 5 — Аномалии изоляции транзакций в SQL

Практическое воспроизведение четырёх классических аномалий изоляции на **MySQL 8.0 InnoDB** в Docker.

## Аномалии

| Аномалия | Уровень изоляции для воспроизведения | Устраняется начиная с |
|---|---|---|
| Dirty Read | READ UNCOMMITTED | READ COMMITTED |
| Non-Repeatable Read | READ COMMITTED | REPEATABLE READ |
| Phantom Read | READ COMMITTED | REPEATABLE READ (MVCC) / SERIALIZABLE |
| Lost Update | REPEATABLE READ (без блокировок) | SELECT FOR UPDATE / атомарный UPDATE |

## Стек

- MySQL 8.0 / InnoDB — единственная СУБД, позволяющая вживую показать все 4 аномалии:
  PostgreSQL не реализует грязное чтение даже на `READ UNCOMMITTED`.
- Docker Compose — изолированный контейнер с проброшенным портом `3307`.
- Bash + именованные FIFO-пайпы — два настоящих параллельных подключения к MySQL,
  воспроизводящих конкурентные транзакции.

## Запуск

```bash
cd tasks/task_5

# Поднять контейнер
docker compose up -d

# Прогнать все 4 сценария, результаты — в results/
bash scripts/repro.sh

# Остановить и удалить данные
docker compose down -v
```

Переменная окружения `MYSQL_ROOT_PASSWORD` (по умолчанию `secret`) задаёт пароль root.

## Схема данных

```sql
CREATE TABLE accounts (
  id      BIGINT PRIMARY KEY,
  owner   VARCHAR(64) NOT NULL,
  balance INT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE orders (
  id          BIGINT PRIMARY KEY AUTO_INCREMENT,
  customer_id BIGINT NOT NULL,
  amount      INT NOT NULL
) ENGINE=InnoDB;

-- Начальные данные
INSERT INTO accounts VALUES (1, 'alice', 100), (2, 'bob', 50);
```

Перед каждым сценарием данные сбрасываются к начальному состоянию (`reset_data` в `lib.sh`).

---

## 1. Dirty Read (грязное чтение)

### Что это

Транзакция читает данные, **изменённые другой транзакцией, которая ещё не зафиксирована**.
Если вторая транзакция откатится, первая уже «видела» значение, которого никогда не
существовало в зафиксированном состоянии БД.

### Условие воспроизведения

Сессия T2 устанавливает `READ UNCOMMITTED` — минимальный уровень изоляции, при котором
MySQL позволяет читать незафиксированные изменения других сессий.
T1 остаётся на дефолтном `REPEATABLE READ`.

### Шаги

| # | T1 | T2 |
|---|----|----|
| 1 | — | `SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED` |
| 2 | `BEGIN` | `BEGIN` |
| 3 | — | `SELECT balance` → **100** (контроль) |
| 4 | `UPDATE accounts SET balance=999 WHERE id=1` (без COMMIT) | — |
| 5 | — | `SELECT balance` → **999** (dirty read!) |
| 6 | `ROLLBACK` | — |
| 7 | — | `SELECT balance` → **100** (значение 999 никогда не существовало) |
| 8 | — | `COMMIT` |

### Фактический лог

Сессия T1 (`results/01_dirty_read.t1.log`):
```
--------------
UPDATE accounts SET balance=999 WHERE id=1
--------------

--------------
SELECT CONCAT('[T1] updated to 999, NOT committed yet') AS info
--------------
info
[T1] updated to 999, NOT committed yet
--------------
ROLLBACK
--------------

--------------
SELECT CONCAT('[T1] rolled back. committed balance: ', balance) AS info FROM accounts WHERE id=1
--------------
info
[T1] rolled back. committed balance: 100
```

Сессия T2 (`results/01_dirty_read.t2.log`):
```
--------------
SELECT CONCAT('[T2] read BEFORE T1 update: ', balance) AS info FROM accounts WHERE id=1
--------------
info
[T2] read BEFORE T1 update: 100
--------------
SELECT CONCAT('[T2] read WHILE T1 open (dirty read!): ', balance) AS info FROM accounts WHERE id=1
--------------
info
[T2] read WHILE T1 open (dirty read!): 999
--------------
SELECT CONCAT('[T2] read AFTER T1 rollback: ', balance, ' (was 999, now reverted)') AS info FROM accounts WHERE id=1
--------------
info
[T2] read AFTER T1 rollback: 100 (was 999, now reverted)
```

**T2 увидела `999` — значение, которое T1 никогда не зафиксировала. После отката T1 значение стало `100`.**

### Как избежать

Повысить уровень изоляции до `READ COMMITTED` (или выше). На `READ COMMITTED` сессия
видит только зафиксированные данные. Скрипт: [`sql/prevention/01_no_dirty_read.sql`](sql/prevention/01_no_dirty_read.sql).

```sql
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

---

## 2. Non-Repeatable Read (неповторяющееся чтение)

### Что это

Транзакция дважды читает **одну и ту же строку** в рамках одной транзакции и получает
разные значения, потому что между двумя чтениями другая транзакция изменила строку и
зафиксировала изменение.

### Условие воспроизведения

T1 работает на `READ COMMITTED`: каждый `SELECT` видит актуальный зафиксированный снимок,
а не снимок начала транзакции.

### Шаги

| # | T1 | T2 |
|---|----|----|
| 1 | `SET SESSION ... READ COMMITTED; BEGIN` | — |
| 2 | `SELECT balance` → **100** | — |
| 3 | — | `BEGIN; UPDATE SET balance=300; COMMIT` |
| 4 | `SELECT balance` → **300** (другой результат!) | — |
| 5 | `COMMIT` | — |

### Фактический лог

Сессия T1 (`results/02_non_repeatable_read.t1.log`):
```
--------------
SELECT CONCAT('[T1] 1st read: ', balance) AS info FROM accounts WHERE id=1
--------------
info
[T1] 1st read: 100
--------------
SELECT CONCAT('[T1] 2nd read: ', balance, ' (non-repeatable if != 100)') AS info FROM accounts WHERE id=1
--------------
info
[T1] 2nd read: 300 (non-repeatable if != 100)
```

Сессия T2 (`results/02_non_repeatable_read.t2.log`):
```
--------------
UPDATE accounts SET balance=300 WHERE id=1
--------------

--------------
COMMIT
--------------

--------------
SELECT CONCAT('[T2] committed balance=300') AS info
--------------
info
[T2] committed balance=300
```

**T1 дважды выполнила одинаковый SELECT в одной транзакции и получила 100, затем 300.**

### Как избежать

Повысить уровень до `REPEATABLE READ` (дефолт InnoDB). InnoDB создаёт MVCC-снимок
в момент первого `SELECT` транзакции; все последующие чтения работают по нему.
Скрипт: [`sql/prevention/02_no_non_repeatable.sql`](sql/prevention/02_no_non_repeatable.sql).

```sql
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
```

---

## 3. Phantom Read (чтение фантомов)

### Что это

Транзакция дважды выполняет **диапазонный запрос** (с `WHERE`, `COUNT`, `BETWEEN` и т.д.)
и получает разный набор строк: между запросами другая транзакция вставила (или удалила)
строки в этот диапазон и зафиксировала изменение.

### Условие воспроизведения

T1 работает на `READ COMMITTED`: MVCC-снимок обновляется на каждом `SELECT`,
поэтому новые зафиксированные строки становятся видны.

### Шаги

| # | T1 | T2 |
|---|----|----|
| 1 | `SET SESSION ... READ COMMITTED; BEGIN` | — |
| 2 | `SELECT COUNT(*) FROM orders WHERE amount>100` → **0** | — |
| 3 | — | `BEGIN; INSERT INTO orders(customer_id,amount) VALUES(1,500); COMMIT` |
| 4 | `SELECT COUNT(*) FROM orders WHERE amount>100` → **1** (фантом!) | — |
| 5 | `COMMIT` | — |

### Фактический лог

Сессия T1 (`results/03_phantom_read.t1.log`):
```
--------------
SELECT CONCAT('[T1] 1st count orders amount>100: ', COUNT(*)) AS info FROM orders WHERE amount>100
--------------
info
[T1] 1st count orders amount>100: 0
--------------
SELECT CONCAT('[T1] 2nd count orders amount>100: ', COUNT(*), ' (phantom if > 0)') AS info FROM orders WHERE amount>100
--------------
info
[T1] 2nd count orders amount>100: 1 (phantom if > 0)
```

Сессия T2 (`results/03_phantom_read.t2.log`):
```
--------------
INSERT INTO orders (customer_id, amount) VALUES (1, 500)
--------------

--------------
COMMIT
--------------

--------------
SELECT CONCAT('[T2] committed INSERT amount=500') AS info
--------------
info
[T2] committed INSERT amount=500
```

**T1 считала 0 строк, потом 1 — «фантомная» строка появилась между запросами.**

### Как избежать

Два подхода. Скрипт: [`sql/prevention/03_no_phantom.sql`](sql/prevention/03_no_phantom.sql).

**A. REPEATABLE READ + MVCC (non-locking SELECT).**
InnoDB фиксирует снимок при первом SELECT; последующие чтения не видят новых строк.
Подходит для большинства задач — дополнительных блокировок нет.

```sql
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
SELECT COUNT(*) FROM orders WHERE amount > 100;   -- фиксирует снимок
-- новые INSERT из других транзакций не видны
SELECT COUNT(*) FROM orders WHERE amount > 100;   -- тот же результат
COMMIT;
```

**B. `SELECT ... FOR UPDATE` + gap-lock.**
Устанавливает блокировку диапазона, запрещая другим транзакциям вставлять строки в него.
Используется когда нужно гарантировать отсутствие фантомов при последующем изменении.

```sql
BEGIN;
SELECT COUNT(*) FROM orders WHERE amount > 100 FOR UPDATE;
-- другие сессии не смогут INSERT amount>100 до нашего COMMIT
COMMIT;
```

---

## 4. Lost Update (потерянное обновление)

### Что это

Две транзакции параллельно читают одно значение, каждая вычисляет новое значение
на основе прочитанного, и обе записывают результат. Та, которая записывает последней,
перезаписывает изменение первой — **обновление первой транзакции теряется**.

Паттерн: **read → modify in app → write** без явных блокировок.

### Условие воспроизведения

Обе транзакции работают на `REPEATABLE READ` (дефолт) без `SELECT FOR UPDATE`.
InnoDB блокирует строку на время UPDATE, но не на время SELECT.
T2 успевает прочитать `balance=100` до того, как T1 запишет `110`.
Когда T2 выполняет `UPDATE SET balance=80`, она дожидается снятия row-lock T1
и перезаписывает `110 → 80`. Изменение T1 (+10) потеряно.

Ожидаемый корректный результат: 100 + 10 − 20 = **90**.
Фактический результат: **80**.

### Шаги

| # | T1 | T2 |
|---|----|----|
| 1 | `BEGIN` | `BEGIN` |
| 2 | `SELECT balance` → **100** | — |
| 3 | — | `SELECT balance` → **100** |
| 4 | `UPDATE SET balance=110` | — |
| 5 | `COMMIT` | — |
| 6 | — | `UPDATE SET balance=80` (ждёт row-lock, потом перезаписывает!) |
| 7 | — | `COMMIT` |
| 8 | `SELECT balance` → **80** (потеря +10) | — |

### Фактический лог

Сессия T1 (`results/04_lost_update.t1.log`):
```
--------------
SELECT CONCAT('[T1] read balance: ', balance) AS info FROM accounts WHERE id=1
--------------
info
[T1] read balance: 100
--------------
UPDATE accounts SET balance=110 WHERE id=1
--------------

--------------
SELECT CONCAT('[T1] set balance=110, committing') AS info
--------------
info
[T1] set balance=110, committing
--------------
COMMIT
--------------

--------------
SELECT CONCAT('[T1] final balance: ', balance, ' (expected 90, lost update if 80)') AS info FROM accounts WHERE id=1
--------------
info
[T1] final balance: 80 (expected 90, lost update if 80)
```

Сессия T2 (`results/04_lost_update.t2.log`):
```
--------------
SELECT CONCAT('[T2] read balance: ', balance) AS info FROM accounts WHERE id=1
--------------
info
[T2] read balance: 100
--------------
UPDATE accounts SET balance=80 WHERE id=1
--------------

--------------
SELECT CONCAT('[T2] set balance=80, committing (overwrites T1!)') AS info
--------------
info
[T2] set balance=80, committing (overwrites T1!)
--------------
COMMIT
--------------

--------------
SELECT CONCAT('[T2] final balance: ', balance) AS info FROM accounts WHERE id=1
--------------
info
[T2] final balance: 80
```

**Финальный баланс: 80. Должно быть 90. Прибавка T1 (+10) потеряна.**

### Как избежать

Скрипт: [`sql/prevention/04_no_lost_update.sql`](sql/prevention/04_no_lost_update.sql).

**Способ 1: `SELECT ... FOR UPDATE` (пессимистическая блокировка).**
Первая транзакция захватывает row-lock на SELECT. Вторая блокируется на этом же SELECT
и прочитает уже обновлённое значение, когда первая зафиксирует изменение.

```sql
BEGIN;
SELECT balance FROM accounts WHERE id=1 FOR UPDATE;  -- блокирует строку
-- другая транзакция не может прочитать через FOR UPDATE, пока мы открыты
UPDATE accounts SET balance = balance + 10 WHERE id=1;
COMMIT;
```

**Способ 2: Атомарный UPDATE (рекомендуемый).**
Вся арифметика выполняется в одном SQL-выражении. Нет фазы чтения в приложении —
нет race condition по определению.

```sql
BEGIN;
UPDATE accounts SET balance = balance + 10 WHERE id=1;
COMMIT;

BEGIN;
UPDATE accounts SET balance = balance - 20 WHERE id=1;
COMMIT;
-- Результат всегда корректен, порядок не важен
```

**Способ 3: Оптимистическая блокировка (через version).**
Добавить колонку `version INT`. При записи проверять, что версия не изменилась;
если `UPDATE` затронул 0 строк — повторить транзакцию.

```sql
-- ALTER TABLE accounts ADD COLUMN version INT NOT NULL DEFAULT 0;

-- Читаем с версией:
SELECT balance, version FROM accounts WHERE id=1;   -- → (100, 5)

-- Пишем только если version не изменился:
UPDATE accounts SET balance=110, version=6
WHERE id=1 AND version=5;
-- affected_rows=0 → кто-то успел раньше → retry
```

---

## Итоговая таблица уровней изоляции

| Уровень изоляции | Dirty Read | Non-Repeatable Read | Phantom Read | Lost Update |
|---|:---:|:---:|:---:|:---:|
| READ UNCOMMITTED | возможен | возможен | возможен | возможен |
| READ COMMITTED | устранён | возможен | возможен | возможен |
| REPEATABLE READ (InnoDB) | устранён | устранён | устранён\* | возможен\*\* |
| SERIALIZABLE | устранён | устранён | устранён | устранён |

\* InnoDB устраняет phantom для non-locking SELECT через MVCC. Для `SELECT FOR UPDATE`
нужны gap-locks (работают на REPEATABLE READ).

\*\* InnoDB на REPEATABLE READ не предотвращает lost update при паттерне read-modify-write
без `FOR UPDATE`. Используйте атомарный UPDATE или `SELECT FOR UPDATE`.

---

## Файлы

| Файл | Назначение |
|---|---|
| [`docker-compose.yml`](docker-compose.yml) | MySQL 8.0 контейнер |
| [`sql/00_init.sql`](sql/00_init.sql) | Схема и начальные данные |
| [`sql/01_dirty_read.t1.sql`](sql/01_dirty_read.t1.sql) | T1 для dirty read |
| [`sql/01_dirty_read.t2.sql`](sql/01_dirty_read.t2.sql) | T2 для dirty read |
| [`sql/02_non_repeatable.t1.sql`](sql/02_non_repeatable.t1.sql) | T1 для non-repeatable read |
| [`sql/02_non_repeatable.t2.sql`](sql/02_non_repeatable.t2.sql) | T2 для non-repeatable read |
| [`sql/03_phantom.t1.sql`](sql/03_phantom.t1.sql) | T1 для phantom read |
| [`sql/03_phantom.t2.sql`](sql/03_phantom.t2.sql) | T2 для phantom read |
| [`sql/04_lost_update.t1.sql`](sql/04_lost_update.t1.sql) | T1 для lost update |
| [`sql/04_lost_update.t2.sql`](sql/04_lost_update.t2.sql) | T2 для lost update |
| [`sql/prevention/`](sql/prevention/) | SQL-демонстрации способов устранения |
| [`scripts/lib.sh`](scripts/lib.sh) | Хелперы оркестрации (FIFO, логирование) |
| [`scripts/repro.sh`](scripts/repro.sh) | Точка запуска всех сценариев |
| [`results/01_dirty_read.t1.log`](results/01_dirty_read.t1.log) | Лог T1, сценарий 01 |
| [`results/01_dirty_read.t2.log`](results/01_dirty_read.t2.log) | Лог T2, сценарий 01 |
| [`results/01_dirty_read.timeline.md`](results/01_dirty_read.timeline.md) | Таймлайн шагов, сценарий 01 |
| [`results/02_non_repeatable_read.t1.log`](results/02_non_repeatable_read.t1.log) | Лог T1, сценарий 02 |
| [`results/02_non_repeatable_read.t2.log`](results/02_non_repeatable_read.t2.log) | Лог T2, сценарий 02 |
| [`results/02_non_repeatable_read.timeline.md`](results/02_non_repeatable_read.timeline.md) | Таймлайн шагов, сценарий 02 |
| [`results/03_phantom_read.t1.log`](results/03_phantom_read.t1.log) | Лог T1, сценарий 03 |
| [`results/03_phantom_read.t2.log`](results/03_phantom_read.t2.log) | Лог T2, сценарий 03 |
| [`results/03_phantom_read.timeline.md`](results/03_phantom_read.timeline.md) | Таймлайн шагов, сценарий 03 |
| [`results/04_lost_update.t1.log`](results/04_lost_update.t1.log) | Лог T1, сценарий 04 |
| [`results/04_lost_update.t2.log`](results/04_lost_update.t2.log) | Лог T2, сценарий 04 |
| [`results/04_lost_update.timeline.md`](results/04_lost_update.timeline.md) | Таймлайн шагов, сценарий 04 |
| [`results/tables_after.txt`](results/tables_after.txt) | Состояние таблиц после всех сценариев |
