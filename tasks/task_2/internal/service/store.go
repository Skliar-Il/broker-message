package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

type OrderLineIn struct {
	ProductID int32 `json:"product_id"`
	Quantity  int32 `json:"quantity"`
}

type PlaceOrderResult struct {
	OrderID     int32   `json:"order_id"`
	TotalAmount float64 `json:"total_amount"`
	CustomerID  int32   `json:"customer_id"`
}

func (s *Store) PlaceOrder(ctx context.Context, customerID int32, lines []OrderLineIn) (*PlaceOrderResult, error) {
	if len(lines) == 0 {
		return nil, errors.New("empty order")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var orderID int32
	if err := tx.QueryRow(ctx,
		`INSERT INTO orders (customer_id, total_amount) VALUES ($1, 0) RETURNING order_id`,
		customerID,
	).Scan(&orderID); err != nil {
		return nil, fmt.Errorf("insert order: %w", err)
	}

	for _, line := range lines {
		if line.Quantity <= 0 {
			return nil, errors.New("quantity must be positive")
		}
		var price float64
		err := tx.QueryRow(ctx,
			`SELECT price::float8 FROM products WHERE product_id = $1 FOR UPDATE`,
			line.ProductID,
		).Scan(&price)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("product %d not found", line.ProductID)
			}
			return nil, fmt.Errorf("lock product: %w", err)
		}
		subtotal := price * float64(line.Quantity)
		_, err = tx.Exec(ctx,
			`INSERT INTO order_items (order_id, product_id, quantity, subtotal) VALUES ($1, $2, $3, $4)`,
			orderID, line.ProductID, line.Quantity, subtotal,
		)
		if err != nil {
			return nil, fmt.Errorf("insert order_item: %w", err)
		}
	}

	var total float64
	if err := tx.QueryRow(ctx,
		`UPDATE orders SET total_amount = (
			SELECT COALESCE(SUM(subtotal), 0) FROM order_items WHERE order_id = $1
		) WHERE order_id = $1 RETURNING total_amount::float8`,
		orderID,
	).Scan(&total); err != nil {
		return nil, fmt.Errorf("update order total: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &PlaceOrderResult{OrderID: orderID, TotalAmount: total, CustomerID: customerID}, nil
}

func (s *Store) UpdateCustomerEmail(ctx context.Context, customerID int32, email string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE customers SET email = $1 WHERE customer_id = $2`,
		email, customerID,
	)
	if err != nil {
		return fmt.Errorf("update email: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.New("customer not found")
	}
	return tx.Commit(ctx)
}

type NewProduct struct {
	Name  string  `json:"product_name"`
	Price float64 `json:"price"`
}

type ProductCreated struct {
	ProductID int32   `json:"product_id"`
	Name      string  `json:"product_name"`
	Price     float64 `json:"price"`
}

func (s *Store) CreateProduct(ctx context.Context, p NewProduct) (*ProductCreated, error) {
	if p.Price < 0 {
		return nil, errors.New("price must be non-negative")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var id int32
	var name string
	var price float64
	if err := tx.QueryRow(ctx,
		`INSERT INTO products (product_name, price) VALUES ($1, $2) RETURNING product_id, product_name, price::float8`,
		p.Name, p.Price,
	).Scan(&id, &name, &price); err != nil {
		return nil, fmt.Errorf("insert product: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &ProductCreated{ProductID: id, Name: name, Price: price}, nil
}

type CustomerRow struct {
	CustomerID int32  `json:"customer_id"`
	FirstName  string `json:"first_name"`
	LastName   string `json:"last_name"`
	Email      string `json:"email"`
}

func (s *Store) QueryCustomers(ctx context.Context) ([]CustomerRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT customer_id, first_name, last_name, email FROM customers ORDER BY customer_id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CustomerRow
	for rows.Next() {
		var r CustomerRow
		if err := rows.Scan(&r.CustomerID, &r.FirstName, &r.LastName, &r.Email); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type ProductRow struct {
	ProductID   int32   `json:"product_id"`
	ProductName string  `json:"product_name"`
	Price       float64 `json:"price"`
}

func (s *Store) QueryProducts(ctx context.Context) ([]ProductRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT product_id, product_name, price::float8 FROM products ORDER BY product_id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProductRow
	for rows.Next() {
		var r ProductRow
		if err := rows.Scan(&r.ProductID, &r.ProductName, &r.Price); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
