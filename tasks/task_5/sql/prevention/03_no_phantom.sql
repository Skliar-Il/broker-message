-- Предотвращение Phantom Read: два подхода.
--
-- Подход A: REPEATABLE READ + non-locking SELECT.
-- InnoDB использует MVCC-снимок: диапазонный SELECT не видит
-- новых строк, вставленных и зафиксированных после начала транзакции.
--
-- Подход B: SELECT ... FOR UPDATE (locking read).
-- Устанавливает gap-locks на диапазон, блокируя вставку новых строк
-- в этот диапазон из других транзакций до окончания текущей.

USE isolation_lab;

-- === Подход A: REPEATABLE READ + MVCC ===
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;

SELECT CONCAT('[prevention-A] 1st count (MVCC snapshot): ', COUNT(*)) AS info
FROM orders WHERE amount > 100;

-- Другая транзакция может вставить строку и зафиксировать —
-- но наш COUNT останется прежним (MVCC-снимок фиксирует видимость строк).
SELECT CONCAT('[prevention-A] 2nd count (same snapshot): ', COUNT(*),
              ' (should be same as 1st)') AS info
FROM orders WHERE amount > 100;

COMMIT;

-- === Подход B: FOR UPDATE + gap-lock ===
BEGIN;

-- Locking read: ставит record-lock + gap-lock на диапазон amount > 100.
-- Другая транзакция, пытающаяся вставить строку в этот диапазон, будет заблокирована.
SELECT COUNT(*) AS cnt_with_lock
FROM orders WHERE amount > 100
FOR UPDATE;

-- Пока мы держим эту транзакцию открытой, INSERT в диапазон amount > 100
-- из другой сессии будет ждать нашей блокировки.
COMMIT;
