package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ConnectionsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_connections_active",
		Help: "Active MQTT client connections",
	})
	PublishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mqtt_publish_total",
		Help: "MQTT publish operations",
	}, []string{"topic", "qos", "result"})
	PublishDuplicates = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mqtt_publish_duplicates_total",
		Help: "Duplicate idempotency publishes suppressed",
	}, []string{"topic"})
	DeliverTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mqtt_deliver_total",
		Help: "MQTT message deliveries to subscribers",
	}, []string{"topic", "qos", "result"})
	RetransmitTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mqtt_retransmit_total",
		Help: "MQTT QoS1 retransmissions",
	}, []string{"topic"})
	InflightMessages = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_inflight_messages",
		Help: "QoS1 inflight messages awaiting PUBACK",
	})
	DedupCacheSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "broker_dedup_cache_size",
		Help: "Entries in idempotency dedup cache",
	})
	TopicSeq = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "broker_topic_seq",
		Help: "Current sequence number per topic",
	}, []string{"topic"})
)
