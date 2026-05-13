-- Сценарий 01: Dirty Read — транзакция T1 (агрессор)
-- Уровень изоляции: по умолчанию (REPEATABLE READ).
-- T1 изменяет строку, но НЕ фиксирует изменение.
-- T2 (READ UNCOMMITTED) сможет прочитать это незафиксированное значение.

USE isolation_lab;

-- Шаг 1: начало транзакции T1
BEGIN;

-- Шаг 2: изменяем баланс alice — изменение ещё не зафиксировано
UPDATE accounts SET balance = 999 WHERE id = 1;
SELECT CONCAT('[T1] balance after UPDATE (not committed yet): ', balance) AS info
FROM accounts WHERE id = 1;

-- Шаг 3: держим транзакцию открытой — T2 должна успеть прочитать грязные данные.
-- (следующий шаг управляется из repro.sh через FIFO)
SELECT '[T1] waiting before ROLLBACK...' AS info;

-- Шаг 4: откатываем — значение 999 никогда не должно было существовать
ROLLBACK;

SELECT CONCAT('[T1] balance after ROLLBACK (committed value): ', balance) AS info
FROM accounts WHERE id = 1;
