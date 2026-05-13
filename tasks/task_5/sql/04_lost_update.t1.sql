-- Сценарий 04: Lost Update — транзакция T1 (первый счётчик: +10)
-- Уровень изоляции: REPEATABLE READ (дефолт InnoDB).
-- Паттерн read-modify-write без явных блокировок:
-- приложение читает значение, вычисляет новое в памяти, записывает.
-- Если T2 делает то же самое параллельно — одно из изменений теряется.

USE isolation_lab;

-- REPEATABLE READ — дефолт, явно устанавливаем для наглядности
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;

-- Читаем текущий баланс (100)
SELECT CONCAT('[T1] read balance: ', balance) AS info
FROM accounts WHERE id = 1;

-- (T2 читает то же значение параллельно)
-- (T2 вычисляет и записывает 100 - 20 = 80, коммитит)

-- Имитация работы приложения: прочитали 100, вычислили 100 + 10 = 110
-- Записываем результат вычисления (не атомарный инкремент!)
UPDATE accounts SET balance = 110 WHERE id = 1;
SELECT CONCAT('[T1] set balance to 110, committing') AS info;

COMMIT;

SELECT CONCAT('[T1] committed. Final balance should be 90, but may be 80 (lost update!)') AS info;
SELECT CONCAT('[T1] actual final balance: ', balance) AS info
FROM accounts WHERE id = 1;
