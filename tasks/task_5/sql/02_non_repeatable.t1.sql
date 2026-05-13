-- Сценарий 02: Non-Repeatable Read — транзакция T1 (наблюдатель)
-- Уровень изоляции: READ COMMITTED — каждый SELECT видит актуальный снимок.
-- T1 читает одну и ту же строку дважды в рамках одной транзакции,
-- но между чтениями T2 успевает зафиксировать изменение.

USE isolation_lab;

SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;

-- Шаг 1: первое чтение — ожидаем 100
SELECT CONCAT('[T1] first read: balance = ', balance) AS info
FROM accounts WHERE id = 1;

-- (T2 выполнит UPDATE + COMMIT между этими двумя SELECT)

-- Шаг 2: второе чтение — ожидаем 300 (non-repeatable!)
SELECT CONCAT('[T1] second read (non-repeatable!): balance = ', balance) AS info
FROM accounts WHERE id = 1;

COMMIT;
