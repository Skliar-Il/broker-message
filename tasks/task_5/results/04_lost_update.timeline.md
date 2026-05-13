# Timeline: 04_lost_update

| # | Session | SQL |
|---|---------|-----|
| 1 | **T1** | `BEGIN;` |
| 2 | **T2** | `BEGIN;` |
| 3 | **T1** | `SELECT CONCAT('[T1] read balance: ', balance) AS info FROM accounts WHERE id=1;` |
| 4 | **T2** | `SELECT CONCAT('[T2] read balance: ', balance) AS info FROM accounts WHERE id=1;` |
| 5 | **T1** | `UPDATE accounts SET balance=110 WHERE id=1;` |
| 6 | **T1** | `SELECT CONCAT('[T1] set balance=110, committing') AS info;` |
| 7 | **T1** | `COMMIT;` |
| 8 | **T2** | `UPDATE accounts SET balance=80 WHERE id=1;` |
| 9 | **T2** | `SELECT CONCAT('[T2] set balance=80, committing (overwrites T1!)') AS info;` |
| 10 | **T2** | `COMMIT;` |
| 11 | **T1** | `SELECT CONCAT('[T1] final balance: ', balance, ' (expected 90, lost update if 80)') AS info FROM accounts WHERE id=1;` |
| 12 | **T2** | `SELECT CONCAT('[T2] final balance: ', balance) AS info FROM accounts WHERE id=1;` |
