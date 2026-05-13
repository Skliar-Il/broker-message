-- Сценарий 03: Phantom Read — транзакция T1 (наблюдатель)
-- Уровень изоляции: READ COMMITTED.
-- T1 дважды выполняет диапазонный запрос в рамках одной транзакции
-- и получает разное число строк, потому что T2 вставила новую строку между запросами.

USE isolation_lab;

SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;

-- Первый счёт: ожидаем 0 заказов с amount > 100
SELECT CONCAT('[T1] 1st count orders amount>100: ', COUNT(*)) AS info
FROM orders WHERE amount > 100;

-- (T2 в это время делает INSERT + COMMIT)

-- Второй счёт в той же транзакции
-- Ожидаем 1 — новая строка появилась: phantom read
SELECT CONCAT('[T1] 2nd count orders amount>100: ', COUNT(*),
              ' (phantom read if > 0)') AS info
FROM orders WHERE amount > 100;

COMMIT;
