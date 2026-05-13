-- Предотвращение Dirty Read: повысить уровень изоляции до READ COMMITTED.
-- На READ COMMITTED сессия видит только зафиксированные данные.
-- Даже если другая транзакция изменила строку и не откатилась,
-- читающая транзакция получит последнее ЗАФИКСИРОВАННОЕ значение.

USE isolation_lab;

-- Устанавливаем READ COMMITTED (минимальный уровень, защищающий от dirty read)
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;

-- Читаем баланс: получим только зафиксированные данные (100),
-- даже если другая транзакция держит незафиксированное значение 999.
SELECT CONCAT('[prevention] balance (no dirty read): ', balance) AS info
FROM accounts WHERE id = 1;

COMMIT;
