package handlers

import (
	"strconv"

	"github.com/Skliar-Il/broker-message/tasks/task_2/internal/service"
	"github.com/gofiber/fiber/v2"
)

type Handlers struct {
	Store *service.Store
}

func New(s *service.Store) *Handlers {
	return &Handlers{Store: s}
}

type placeOrderReq struct {
	CustomerID int32                 `json:"customer_id"`
	Items      []service.OrderLineIn `json:"items"`
}

func (h *Handlers) PlaceOrder(c *fiber.Ctx) error {
	var req placeOrderReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	res, err := h.Store.PlaceOrder(c.Context(), req.CustomerID, req.Items)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	return c.Status(fiber.StatusCreated).JSON(res)
}

type patchEmailReq struct {
	Email string `json:"email"`
}

func (h *Handlers) UpdateCustomerEmail(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid id"})
	}
	var req patchEmailReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	if req.Email == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "email required"})
	}
	if err := h.Store.UpdateCustomerEmail(c.Context(), int32(id), req.Email); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func (h *Handlers) CreateProduct(c *fiber.Ctx) error {
	var req service.NewProduct
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "product_name required"})
	}
	res, err := h.Store.CreateProduct(c.Context(), req)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	return c.Status(fiber.StatusCreated).JSON(res)
}

func (h *Handlers) ListCustomers(c *fiber.Ctx) error {
	rows, err := h.Store.QueryCustomers(c.Context())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(rows)
}

func (h *Handlers) ListProducts(c *fiber.Ctx) error {
	rows, err := h.Store.QueryProducts(c.Context())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(rows)
}
