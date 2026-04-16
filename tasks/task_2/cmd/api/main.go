package main

import (
	"context"
	"embed"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Skliar-Il/broker-message/tasks/task_2/internal/db"
	"github.com/Skliar-Il/broker-message/tasks/task_2/internal/handlers"
	"github.com/Skliar-Il/broker-message/tasks/task_2/internal/service"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/swagger"
)

//go:embed swagger.json
var swaggerFS embed.FS

func main() {
	spec, err := swaggerFS.ReadFile("swagger.json")
	if err != nil {
		log.Fatal("swagger spec: ", err)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://store:store@localhost:5432/store?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		log.Fatal("db pool: ", err)
	}
	defer pool.Close()

	if err := db.RunMigrations(ctx, pool); err != nil {
		log.Fatal("migrations: ", err)
	}

	app := fiber.New(fiber.Config{
		AppName: "task2-store",
	})
	app.Use(recover.New())
	app.Use(logger.New())

	h := handlers.New(service.NewStore(pool))

	app.Get("/swagger/doc.json", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "application/json")
		return c.Send(spec)
	})
	app.Get("/swagger/*", swagger.New(swagger.Config{
		URL:         "/swagger/doc.json",
		DeepLinking: true,
	}))

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	v1 := app.Group("/api/v1")
	v1.Post("/orders", h.PlaceOrder)
	v1.Patch("/customers/:id/email", h.UpdateCustomerEmail)
	v1.Post("/products", h.CreateProduct)
	v1.Get("/customers", h.ListCustomers)
	v1.Get("/products", h.ListProducts)

	go func() {
		addr := ":8080"
		if p := os.Getenv("PORT"); p != "" {
			addr = ":" + p
		}
		if err := app.Listen(addr); err != nil {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shCancel()
	_ = app.ShutdownWithContext(shCtx)
}
