package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/Skliar-Il/broker-message/sdk/go/brokermq"
)

func main() {
	addr := envOr("BROKER_ADDR", "localhost:1883")
	topic := envOr("TOPIC", "hello")
	qos := qosFromEnv("QOS", 1)
	user := os.Getenv("MQTT_USER")
	pass := os.Getenv("MQTT_PASS")

	opts := []brokermq.Option{
		brokermq.WithClientID(envOr("CLIENT_ID", "example-consumer")),
		brokermq.WithQoS(qos),
		brokermq.WithSeenCacheSize(20_000),
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

	err = client.Subscribe(ctx, topic, func(m brokermq.Message) {
		fmt.Printf("[%s] server_msg=%s payload=%s\n", m.Topic, m.ServerMsgID, m.Payload)
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	select {}
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
