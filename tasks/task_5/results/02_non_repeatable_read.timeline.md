# Timeline: 02_non_repeatable_read

| # | Session | SQL |
|---|---------|-----|
| 1 | **T1** | `SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;` |
| 2 | **T1** | `BEGIN;` |
| 3 | **T1** | `SELECT CONCAT('[T1] 1st read: ', balance) AS info FROM accounts WHERE id=1;` |
| 4 | **T2** | `BEGIN;` |
| 5 | **T2** | `UPDATE accounts SET balance=300 WHERE id=1;` |
| 6 | **T2** | `COMMIT;` |
| 7 | **T2** | `SELECT CONCAT('[T2] committed balance=300') AS info;` |
| 8 | **T1** | `SELECT CONCAT('[T1] 2nd read: ', balance, ' (non-repeatable if != 100)') AS info FROM accounts WHERE id=1;` |
| 9 | **T1** | `COMMIT;` |
