-- Предотвращение Lost Update: три практических подхода.

USE isolation_lab;

-- === Подход 1: SELECT ... FOR UPDATE (пессимистическая блокировка) ===
-- Первая транзакция, сделавшая SELECT FOR UPDATE, блокирует строку.
-- Вторая транзакция будет ждать освобождения блокировки перед своим SELECT FOR UPDATE.
-- После ожидания она прочитает уже обновлённое значение и вычислит корректный результат.
BEGIN;
SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;
-- Приложение видит актуальное значение и применяет изменение поверх него.
UPDATE accounts SET balance = balance + 10 WHERE id = 1;
COMMIT;


-- === Подход 2: Атомарный UPDATE (предпочтительный способ) ===
-- Нет фазы чтения в приложении — вся арифметика в одном SQL-выражении.
-- UPDATE атомарен на уровне строки, race condition невозможен по определению.
BEGIN;
UPDATE accounts SET balance = balance - 20 WHERE id = 1;
COMMIT;

SELECT CONCAT('[prevention-2] balance after atomic updates: ', balance) AS info
FROM accounts WHERE id = 1;


-- === Подход 3: Оптимистическая блокировка через version ===
-- Требует колонки version в таблице (здесь — демонстрация концепции).
-- ALTER TABLE accounts ADD COLUMN version INT NOT NULL DEFAULT 0;
--
-- Читаем значение вместе с version:
--   SELECT balance, version FROM accounts WHERE id = 1;  → (balance=100, version=5)
-- Применяем изменение только если version не изменился:
--   UPDATE accounts SET balance = 110, version = 6
--   WHERE id = 1 AND version = 5;
-- Если affected_rows = 0 — кто-то успел раньше, повторяем транзакцию.
SELECT '[prevention-3] optimistic locking: retry if UPDATE affects 0 rows' AS note;
