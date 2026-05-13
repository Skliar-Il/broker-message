#!/usr/bin/env bash
# lib.sh — вспомогательные функции для repro.sh.
# Подключается через: source "$(dirname "$0")/lib.sh"

set -euo pipefail

MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-secret}"
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3307}"
DB="isolation_lab"

RESULTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/results"
mkdir -p "$RESULTS_DIR"

# ── mysql helper ─────────────────────────────────────────────────────────────

mysql_exec() {
    # Выполняет SQL-команды через docker compose exec (без -T убирает tty-escape).
    docker compose exec -T mysql \
        mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" \
              --batch --raw --silent \
              "$@"
}

# ── Ожидание готовности MySQL ─────────────────────────────────────────────────

wait_for_mysql() {
    local max_attempts=40
    local attempt=0
    echo "[lib] waiting for MySQL to be ready..."
    until mysql_exec -e "SELECT 1" >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [[ $attempt -ge $max_attempts ]]; then
            echo "[lib] ERROR: MySQL did not become ready in time" >&2
            exit 1
        fi
        sleep 1
    done
    echo "[lib] MySQL is ready (attempt $attempt)"
}

# ── Переменные текущего сценария ──────────────────────────────────────────────

SCENARIO_NAME=""
TIMELINE_FILE=""
T1_LOG=""
T2_LOG=""
T1_FD=3
T2_FD=4
T1_FIFO=""
T2_FIFO=""
T1_PID=""
T2_PID=""

# ── Запуск сценария ───────────────────────────────────────────────────────────

# scenario_start <name>
# Создаёт FIFO-пайпы, запускает два mysql-клиента в фоне,
# открывает файловые дескрипторы 3 (T1) и 4 (T2).
scenario_start() {
    SCENARIO_NAME="$1"
    TIMELINE_FILE="${RESULTS_DIR}/${SCENARIO_NAME}.timeline.md"
    T1_LOG="${RESULTS_DIR}/${SCENARIO_NAME}.t1.log"
    T2_LOG="${RESULTS_DIR}/${SCENARIO_NAME}.t2.log"

    T1_FIFO="/tmp/isolation_t1_$$.fifo"
    T2_FIFO="/tmp/isolation_t2_$$.fifo"

    rm -f "$T1_FIFO" "$T2_FIFO"
    mkfifo "$T1_FIFO" "$T2_FIFO"

    # Заголовки лог-файлов
    echo "=== T1 log: ${SCENARIO_NAME} ===" > "$T1_LOG"
    echo "=== T2 log: ${SCENARIO_NAME} ===" > "$T2_LOG"

    # Заголовок таймлайна
    {
        echo "# Timeline: ${SCENARIO_NAME}"
        echo ""
        echo "| # | Session | SQL |"
        echo "|---|---------|-----|"
    } > "$TIMELINE_FILE"

    # Запускаем клиентов, каждый читает из своего FIFO
    docker compose exec -T mysql \
        mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" \
              --batch --raw --verbose \
              "${DB}" \
        < "$T1_FIFO" >> "$T1_LOG" 2>&1 &
    T1_PID=$!

    docker compose exec -T mysql \
        mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" \
              --batch --raw --verbose \
              "${DB}" \
        < "$T2_FIFO" >> "$T2_LOG" 2>&1 &
    T2_PID=$!

    # Открываем дескрипторы для записи в FIFO
    eval "exec ${T1_FD}>${T1_FIFO}"
    eval "exec ${T2_FD}>${T2_FIFO}"

    _STEP_N=0
    echo "[lib] scenario '${SCENARIO_NAME}' started (T1 pid=${T1_PID}, T2 pid=${T2_PID})"
}

# ── Отправка шага транзакции ──────────────────────────────────────────────────

# step <T1|T2> <SQL>
# Записывает SQL в таймлайн и отправляет в нужную сессию.
# После отправки делает небольшую паузу для детерминизма порядка шагов.
_STEP_N=0
step() {
    local session="$1"
    local sql="$2"
    local delay="${3:-0.4}"

    _STEP_N=$((_STEP_N + 1))

    # Таймлайн
    echo "| ${_STEP_N} | **${session}** | \`${sql}\` |" >> "$TIMELINE_FILE"

    # Отправка в нужную сессию
    if [[ "$session" == "T1" ]]; then
        echo "${sql}" >&${T1_FD}
    else
        echo "${sql}" >&${T2_FD}
    fi

    sleep "$delay"
}

# ── Завершение сценария ───────────────────────────────────────────────────────

scenario_end() {
    # Закрываем FIFO — клиенты получат EOF и завершатся
    eval "exec ${T1_FD}>&-" 2>/dev/null || true
    eval "exec ${T2_FD}>&-" 2>/dev/null || true

    # Ждём завершения клиентов
    wait "$T1_PID" 2>/dev/null || true
    wait "$T2_PID" 2>/dev/null || true

    rm -f "$T1_FIFO" "$T2_FIFO"

    echo "[lib] scenario '${SCENARIO_NAME}' done"
    echo "[lib] logs: ${T1_LOG}  ${T2_LOG}"
    echo "[lib] timeline: ${TIMELINE_FILE}"
    echo ""
}

# ── Сброс данных между сценариями ────────────────────────────────────────────

reset_data() {
    mysql_exec "${DB}" <<'SQL'
DELETE FROM orders;
DELETE FROM accounts;
ALTER TABLE orders AUTO_INCREMENT = 1;
INSERT INTO accounts VALUES (1, 'alice', 100), (2, 'bob', 50);
SQL
    echo "[lib] data reset: accounts restored, orders cleared"
}
