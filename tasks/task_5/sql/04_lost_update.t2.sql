-- Сценарий 04: Lost Update — транзакция T2 (второй счётчик: -20)
-- T2 читает то же значение что T1, вычисляет своё изменение и перезаписывает.
-- Изменение T1 (+10) будет потеряно, потому что T2 не видела его.

USE isolation_lab;

SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;

-- Читаем текущий баланс (100) — тот же снимок, что у T1
SELECT CONCAT('[T2] read balance: ', balance) AS info
FROM accounts WHERE id = 1;

-- (T1 ещё не записала своё изменение)

-- Имитация работы приложения: прочитали 100, вычислили 100 - 20 = 80
-- T2 коммитит позже T1, поэтому перезапишет результат T1
UPDATE accounts SET balance = 80 WHERE id = 1;
-- UPDATE будет заблокирован до COMMIT T1, потом выполнится и перепишет 110 → 80
SELECT CONCAT('[T2] set balance to 80, committing') AS info;

COMMIT;

SELECT CONCAT('[T2] committed. Lost +10 from T1: balance is 80, expected 90') AS info;
SELECT CONCAT('[T2] actual final balance: ', balance) AS info
FROM accounts WHERE id = 1;
