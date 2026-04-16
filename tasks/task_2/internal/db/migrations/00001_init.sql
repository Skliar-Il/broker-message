-- +goose Up
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE products (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    price        NUMERIC(12, 2) NOT NULL CHECK (price >= 0)
);

CREATE TABLE orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers (customer_id),
    order_date   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0)
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INT NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    product_id    INT NOT NULL REFERENCES products (product_id),
    quantity      INT NOT NULL CHECK (quantity > 0),
    subtotal      NUMERIC(12, 2) NOT NULL CHECK (subtotal >= 0),
    UNIQUE (order_id, product_id)
);

INSERT INTO customers (first_name, last_name, email)
VALUES ('Иван', 'Иванов', 'ivan@example.com'),
       ('Мария', 'Петрова', 'maria@example.com');

INSERT INTO products (product_name, price)
VALUES ('Ноутбук', 89999.00),
       ('Мышь', 1500.50),
       ('Клавиатура', 3500.00);

-- +goose Down
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
