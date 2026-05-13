-- Сценарий 03: Phantom Read — транзакция T2 (агрессор)
-- T2 вставляет новую строку в диапазон, который читает T1, и ФИКСИРУЕТ изменение.

USE isolation_lab;

BEGIN;

INSERT INTO orders (customer_id, amount) VALUES (1, 500);
SELECT CONCAT('[T2] inserted order amount=500, committing') AS info;

COMMIT;

SELECT CONCAT('[T2] committed. T1 2nd count should now see 1') AS info;
