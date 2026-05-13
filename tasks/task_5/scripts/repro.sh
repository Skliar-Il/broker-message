#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TASK_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$TASK_DIR"

source "${SCRIPT_DIR}/lib.sh"

# ── 1. Поднять контейнер ──────────────────────────────────────────────────────

echo "=== [repro] starting MySQL container ==="
docker compose up -d
wait_for_mysql

# ── 2. Инициализация схемы ────────────────────────────────────────────────────

echo "=== [repro] initialising schema ==="
mysql_exec < sql/00_init.sql
echo "[repro] schema initialised"

# ══════════════════════════════════════════════════════════════════════════════
# СЦЕНАРИЙ 01: Dirty Read
# Уровень T2: READ UNCOMMITTED
# T1 изменяет строку без COMMIT, T2 читает незафиксированное значение,
# затем T1 делает ROLLBACK — T2 уже «видела» значение, которого не было.
# ══════════════════════════════════════════════════════════════════════════════
echo "=== [repro] scenario 01: dirty_read ==="
reset_data

scenario_start "01_dirty_read"

# T2 устанавливает READ UNCOMMITTED до начала транзакции
step T2 "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;" 0.3

# T1 и T2 открывают транзакции
step T1 "BEGIN;" 0.3
step T2 "BEGIN;" 0.3

# T2 читает до изменения T1 (контрольное значение = 100)
step T2 "SELECT CONCAT('[T2] read BEFORE T1 update: ', balance) AS info FROM accounts WHERE id=1;" 0.5

# T1 изменяет строку (НЕ коммитит)
step T1 "UPDATE accounts SET balance=999 WHERE id=1;" 0.3
step T1 "SELECT CONCAT('[T1] updated to 999, NOT committed yet') AS info;" 0.3

# T2 читает снова — ДОЛЖНА увидеть 999 (dirty read)
step T2 "SELECT CONCAT('[T2] read WHILE T1 open (dirty read!): ', balance) AS info FROM accounts WHERE id=1;" 0.5

# T1 откатывает — значение 999 исчезает
step T1 "ROLLBACK;" 0.3
step T1 "SELECT CONCAT('[T1] rolled back. committed balance: ', balance) AS info FROM accounts WHERE id=1;" 0.3

# T2 читает снова — значение 999, которое она видела, никогда не существовало
step T2 "SELECT CONCAT('[T2] read AFTER T1 rollback: ', balance, ' (was 999, now reverted)') AS info FROM accounts WHERE id=1;" 0.5
step T2 "COMMIT;" 0.3

scenario_end

# ══════════════════════════════════════════════════════════════════════════════
# СЦЕНАРИЙ 02: Non-Repeatable Read
# Уровень T1: READ COMMITTED
# T1 читает строку дважды; между чтениями T2 коммитит UPDATE.
# ══════════════════════════════════════════════════════════════════════════════
echo "=== [repro] scenario 02: non_repeatable_read ==="
reset_data

scenario_start "02_non_repeatable_read"

step T1 "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;" 0.3
step T1 "BEGIN;" 0.3

# Первое чтение T1 — balance = 100
step T1 "SELECT CONCAT('[T1] 1st read: ', balance) AS info FROM accounts WHERE id=1;" 0.5

# T2 изменяет строку и коммитит
step T2 "BEGIN;" 0.2
step T2 "UPDATE accounts SET balance=300 WHERE id=1;" 0.3
step T2 "COMMIT;" 0.3
step T2 "SELECT CONCAT('[T2] committed balance=300') AS info;" 0.3

# Второе чтение T1 в той же транзакции — balance = 300 (non-repeatable read)
step T1 "SELECT CONCAT('[T1] 2nd read: ', balance, ' (non-repeatable if != 100)') AS info FROM accounts WHERE id=1;" 0.5
step T1 "COMMIT;" 0.3

scenario_end

# ══════════════════════════════════════════════════════════════════════════════
# СЦЕНАРИЙ 03: Phantom Read
# Уровень T1: READ COMMITTED
# T1 дважды считает строки по диапазону; между счётами T2 коммитит INSERT.
# ══════════════════════════════════════════════════════════════════════════════
echo "=== [repro] scenario 03: phantom_read ==="
reset_data

scenario_start "03_phantom_read"

step T1 "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;" 0.3
step T1 "BEGIN;" 0.3

# Первый счёт — 0 строк
step T1 "SELECT CONCAT('[T1] 1st count orders amount>100: ', COUNT(*)) AS info FROM orders WHERE amount>100;" 0.5

# T2 вставляет строку и коммитит
step T2 "BEGIN;" 0.2
step T2 "INSERT INTO orders (customer_id, amount) VALUES (1, 500);" 0.3
step T2 "COMMIT;" 0.3
step T2 "SELECT CONCAT('[T2] committed INSERT amount=500') AS info;" 0.3

# Второй счёт — 1 строка (фантом)
step T1 "SELECT CONCAT('[T1] 2nd count orders amount>100: ', COUNT(*), ' (phantom if > 0)') AS info FROM orders WHERE amount>100;" 0.5
step T1 "COMMIT;" 0.3

scenario_end

# ══════════════════════════════════════════════════════════════════════════════
# СЦЕНАРИЙ 04: Lost Update
# Уровень: REPEATABLE READ (дефолт InnoDB), без явных блокировок.
# T1 и T2 оба читают balance=100, вычисляют новое значение, пишут.
# T2 коммитит последней и перезаписывает изменение T1.
# ══════════════════════════════════════════════════════════════════════════════
echo "=== [repro] scenario 04: lost_update ==="
reset_data

scenario_start "04_lost_update"

# Оба открывают транзакции и читают balance=100
step T1 "BEGIN;" 0.2
step T2 "BEGIN;" 0.2

step T1 "SELECT CONCAT('[T1] read balance: ', balance) AS info FROM accounts WHERE id=1;" 0.4
step T2 "SELECT CONCAT('[T2] read balance: ', balance) AS info FROM accounts WHERE id=1;" 0.4

# T1 вычисляет 100+10=110 и записывает; T2 ещё не коммитила
step T1 "UPDATE accounts SET balance=110 WHERE id=1;" 0.3
step T1 "SELECT CONCAT('[T1] set balance=110, committing') AS info;" 0.3
step T1 "COMMIT;" 0.4

# T2 вычисляет 100-20=80 (по старому snapshot), записывает поверх T1
# UPDATE подождёт освобождения row-lock, потом перепишет 110 → 80
step T2 "UPDATE accounts SET balance=80 WHERE id=1;" 0.5
step T2 "SELECT CONCAT('[T2] set balance=80, committing (overwrites T1!)') AS info;" 0.3
step T2 "COMMIT;" 0.4

step T1 "SELECT CONCAT('[T1] final balance: ', balance, ' (expected 90, lost update if 80)') AS info FROM accounts WHERE id=1;" 0.3
step T2 "SELECT CONCAT('[T2] final balance: ', balance) AS info FROM accounts WHERE id=1;" 0.3

scenario_end

# ── Дамп финального состояния таблиц ─────────────────────────────────────────
echo "=== [repro] dumping final table state ==="
{
    echo "=== accounts ==="
    mysql_exec "${DB}" -e "SELECT * FROM accounts;"
    echo ""
    echo "=== orders ==="
    mysql_exec "${DB}" -e "SELECT * FROM orders;"
} > "${RESULTS_DIR}/tables_after.txt"

echo ""
echo "=== [repro] ALL DONE ==="
echo "Results written to: ${RESULTS_DIR}/"
ls -1 "${RESULTS_DIR}/"
