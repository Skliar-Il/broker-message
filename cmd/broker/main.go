package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Skliar-Il/broker-message/core/admin"
	"github.com/Skliar-Il/broker-message/core/auth"
	"github.com/Skliar-Il/broker-message/core/brokerhub"
	"github.com/Skliar-Il/broker-message/core/config"
	"github.com/Skliar-Il/broker-message/core/security"
	"github.com/Skliar-Il/broker-message/core/storage"
	"github.com/Skliar-Il/broker-message/core/transport/mqtt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

func main() {
	cfgPath := flag.String("config", "config/broker.yaml", "path to broker config")
	flag.Parse()

	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal().Err(err).Msg("load config")
	}

	_ = auth.WriteDefaultDev("config")

	topicsDir := filepath.Join(".", cfg.TopicsDir)
	metaDir := filepath.Join(".", cfg.MetaDir)
	_ = os.MkdirAll(topicsDir, 0o755)
	_ = os.MkdirAll(metaDir, 0o755)

	metaDB, err := storage.OpenBadger(metaDir)
	if err != nil {
		log.Fatal().Err(err).Msg("open meta db")
	}

	hub := brokerhub.NewHub(metaDir, metaDB, cfg.DedupCapacity, cfg.DedupTTL, log)
	registry := mqtt.NewRegistry(topicsDir, hub, log)
	if err := registry.Load(); err != nil {
		log.Fatal().Err(err).Msg("load topics")
	}

	authStore, err := auth.Load(cfg.UsersFile)
	if err != nil {
		log.Warn().Err(err).Msg("auth disabled (users file missing)")
		authStore = nil
	}

	tlsCfg, err := security.LoadServerTLS(security.TLSConfig{
		CertFile: cfg.TLSCert,
		KeyFile:  cfg.TLSKey,
		CAFile:   cfg.TLSCA,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("tls config")
	}

	mqttSrv := mqtt.NewServer(cfg.MQTTAddr, cfg.MQTTTLSAddr, tlsCfg, registry, authStore, cfg.AuthRequired && authStore != nil, log)
	adminSrv := admin.New(registry, mqttSrv, hub, cfg.AdminUser, cfg.AdminPassword, log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	defer func() {
		_ = registry.Close()
		_ = hub.Close()
	}()

	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		log.Info().Str("addr", cfg.MetricsAddr).Msg("metrics listening")
		if err := http.ListenAndServe(cfg.MetricsAddr, metricsMux); err != nil {
			log.Error().Err(err).Msg("metrics server")
		}
	}()

	go func() {
		log.Info().Str("addr", cfg.AdminAddr).Msg("admin UI listening")
		if err := http.ListenAndServe(cfg.AdminAddr, adminSrv.Handler()); err != nil {
			log.Error().Err(err).Msg("admin server")
		}
	}()

	log.Info().
		Str("mqtt", cfg.MQTTAddr).
		Str("mqtt_tls", cfg.MQTTTLSAddr).
		Msg("broker ready")

	if err := mqttSrv.ListenAndServe(ctx); err != nil {
		log.Fatal().Err(err).Msg("mqtt server")
	}
}
