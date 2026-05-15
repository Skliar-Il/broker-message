package brokerhub

import (
	"time"

	"github.com/Skliar-Il/broker-message/core/dedup"
	"github.com/Skliar-Il/broker-message/core/storage"
	"github.com/rs/zerolog"
)

// Hub holds shared broker services used by MQTT and admin layers.
type Hub struct {
	BaseDir    string
	Dedup      *dedup.Store
	MetaDB     *storage.Badger
	DedupTTL   time.Duration
	DedupCap   int
	Log        zerolog.Logger
}

func NewHub(baseDir string, metaDB *storage.Badger, dedupCap int, dedupTTL time.Duration, log zerolog.Logger) *Hub {
	return &Hub{
		BaseDir:  baseDir,
		Dedup:    dedup.New(metaDB, dedupCap, dedupTTL, log),
		MetaDB:   metaDB,
		DedupTTL: dedupTTL,
		DedupCap: dedupCap,
		Log:      log,
	}
}

func (h *Hub) Close() error {
	if h.MetaDB != nil {
		return h.MetaDB.Close()
	}
	return nil
}
