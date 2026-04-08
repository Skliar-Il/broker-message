package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Skliar-Il/broker-message/core/transport/mqtt"
	"github.com/rs/zerolog"
)

const (
	mqttAddr  = ":1883"
	topicsDir = "storage/topics"
)

func main() {
	log := zerolog.New(
		zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339},
	).With().Timestamp().Logger()

	log.Info().Msg("broker: starting")

	baseDir := filepath.Join(".", topicsDir)
	registry := mqtt.NewRegistry(baseDir, log)
	defer func() {
		log.Info().Msg("broker: shutting down, closing all topics")
		if err := registry.Close(); err != nil {
			log.Error().Err(err).Msg("broker: close registry error")
		}
		log.Info().Msg("broker: stopped")
	}()

	srv := mqtt.NewServer(mqttAddr, registry, log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info().Str("addr", mqttAddr).Str("topics_dir", baseDir).Msg("broker: ready")

	if err := srv.ListenAndServe(ctx); err != nil {
		log.Fatal().Err(err).Msg("broker: server error")
	}
}
