-- Предотвращение Non-Repeatable Read: уровень REPEATABLE READ (дефолт InnoDB).
-- InnoDB создаёт снимок (consistent read view) в момент первого SELECT транзакции.
-- Все последующие чтения в той же транзакции работают по этому снимку
-- и не видят изменений, зафиксированных другими транзакциями после снимка.

USE isolation_lab;

SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;

-- Первое чтение создаёт снимок. balance = 100
SELECT CONCAT('[prevention] 1st read: ', balance) AS info
FROM accounts WHERE id = 1;

-- Даже если другая транзакция сделает UPDATE SET balance=300 и зафиксирует —
-- второе чтение всё равно вернёт 100 (из снимка).
SELECT CONCAT('[prevention] 2nd read: ', balance,
              ' (should still be 100)') AS info
FROM accounts WHERE id = 1;

COMMIT;
