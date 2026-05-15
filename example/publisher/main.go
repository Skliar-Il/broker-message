package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Skliar-Il/broker-message/sdk/go/brokermq"
)

func main() {
	addr := envOr("BROKER_ADDR", "localhost:1883")
	topic := envOr("TOPIC", "hello")
	clientID := envOr("CLIENT_ID", "example-publisher")
	qos := qosFromEnv("QOS", 1)
	user := os.Getenv("MQTT_USER")
	pass := os.Getenv("MQTT_PASS")

	opts := []brokermq.Option{
		brokermq.WithClientID(clientID),
		brokermq.WithQoS(qos),
	}
	if user != "" {
		opts = append(opts, brokermq.WithCredentials(user, pass))
	}

	ctx := context.Background()
	client, err := brokermq.Connect(ctx, addr, opts...)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	log.Printf("publisher %q -> topic %q on %s", clientID, topic, addr)
	for i := 1; ; i++ {
		payload := []byte(fmt.Sprintf("[%s] msg %d", clientID, i))
		if err := client.Publish(ctx, topic, payload); err != nil {
			log.Fatalf("publish: %v", err)
		}
		log.Printf("sent: %s", payload)
		time.Sleep(time.Second)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func qosFromEnv(key string, def byte) byte {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > 1 {
		return def
	}
	return byte(n)
}
