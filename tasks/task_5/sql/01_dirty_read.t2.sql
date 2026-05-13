-- Сценарий 01: Dirty Read — транзакция T2 (наблюдатель)
-- Уровень изоляции: READ UNCOMMITTED — позволяет видеть незафиксированные изменения.

USE isolation_lab;

-- Шаг 1: снижаем уровень изоляции до минимума
SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

BEGIN;

-- Шаг 2: читаем ДО того, как T1 изменит данные (контрольное значение)
SELECT CONCAT('[T2] balance BEFORE T1 UPDATE: ', balance) AS info
FROM accounts WHERE id = 1;

-- Шаг 3: читаем ПОКА T1 держит незафиксированное изменение
-- Ожидаем увидеть 999 — это и есть dirty read
SELECT CONCAT('[T2] balance WHILE T1 is open (dirty read!): ', balance) AS info
FROM accounts WHERE id = 1;

-- Шаг 4: читаем ПОСЛЕ ROLLBACK T1
-- Ожидаем снова 100 — значение, которое T2 уже «видела» как 999, исчезло
SELECT CONCAT('[T2] balance AFTER T1 ROLLBACK: ', balance) AS info
FROM accounts WHERE id = 1;

COMMIT;
