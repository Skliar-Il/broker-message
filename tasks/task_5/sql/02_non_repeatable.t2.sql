-- Сценарий 02: Non-Repeatable Read — транзакция T2 (агрессор)
-- T2 изменяет строку и ФИКСИРУЕТ изменение между двумя чтениями T1.

USE isolation_lab;

BEGIN;

-- Изменяем balance пока T1 находится между своими двумя SELECT
UPDATE accounts SET balance = 300 WHERE id = 1;
SELECT CONCAT('[T2] updated balance to 300, committing') AS info;

COMMIT;

SELECT CONCAT('[T2] committed. T1 2nd read should now see 300') AS info;
