-- Инициализация стенда: база, таблицы, начальные данные.
-- Скрипт идемпотентен: можно выполнять повторно (DROP IF EXISTS + INSERT IGNORE).

CREATE DATABASE IF NOT EXISTS isolation_lab
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE isolation_lab;

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS accounts;

CREATE TABLE accounts (
  id      BIGINT       PRIMARY KEY,
  owner   VARCHAR(64)  NOT NULL,
  balance INT          NOT NULL
) ENGINE=InnoDB;

CREATE TABLE orders (
  id          BIGINT PRIMARY KEY AUTO_INCREMENT,
  customer_id BIGINT NOT NULL,
  amount      INT    NOT NULL
) ENGINE=InnoDB;

-- Начальные данные: два счёта, ни одного заказа.
INSERT INTO accounts VALUES (1, 'alice', 100),
                             (2, 'bob',   50);
