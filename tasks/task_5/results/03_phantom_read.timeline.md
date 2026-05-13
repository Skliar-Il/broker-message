# Timeline: 03_phantom_read

| # | Session | SQL |
|---|---------|-----|
| 1 | **T1** | `SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;` |
| 2 | **T1** | `BEGIN;` |
| 3 | **T1** | `SELECT CONCAT('[T1] 1st count orders amount>100: ', COUNT(*)) AS info FROM orders WHERE amount>100;` |
| 4 | **T2** | `BEGIN;` |
| 5 | **T2** | `INSERT INTO orders (customer_id, amount) VALUES (1, 500);` |
| 6 | **T2** | `COMMIT;` |
| 7 | **T2** | `SELECT CONCAT('[T2] committed INSERT amount=500') AS info;` |
| 8 | **T1** | `SELECT CONCAT('[T1] 2nd count orders amount>100: ', COUNT(*), ' (phantom if > 0)') AS info FROM orders WHERE amount>100;` |
| 9 | **T1** | `COMMIT;` |
