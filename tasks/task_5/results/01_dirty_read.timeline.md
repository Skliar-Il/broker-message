# Timeline: 01_dirty_read

| # | Session | SQL |
|---|---------|-----|
| 1 | **T2** | `SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;` |
| 2 | **T1** | `BEGIN;` |
| 3 | **T2** | `BEGIN;` |
| 4 | **T2** | `SELECT CONCAT('[T2] read BEFORE T1 update: ', balance) AS info FROM accounts WHERE id=1;` |
| 5 | **T1** | `UPDATE accounts SET balance=999 WHERE id=1;` |
| 6 | **T1** | `SELECT CONCAT('[T1] updated to 999, NOT committed yet') AS info;` |
| 7 | **T2** | `SELECT CONCAT('[T2] read WHILE T1 open (dirty read!): ', balance) AS info FROM accounts WHERE id=1;` |
| 8 | **T1** | `ROLLBACK;` |
| 9 | **T1** | `SELECT CONCAT('[T1] rolled back. committed balance: ', balance) AS info FROM accounts WHERE id=1;` |
| 10 | **T2** | `SELECT CONCAT('[T2] read AFTER T1 rollback: ', balance, ' (was 999, now reverted)') AS info FROM accounts WHERE id=1;` |
| 11 | **T2** | `COMMIT;` |
