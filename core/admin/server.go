package admin

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Skliar-Il/broker-message/core/brokerhub"
	"github.com/Skliar-Il/broker-message/core/envelope"
	"github.com/Skliar-Il/broker-message/core/topic"
	"github.com/Skliar-Il/broker-message/core/transport/mqtt"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

type Server struct {
	mux        *http.ServeMux
	registry   *mqtt.Registry
	mqttSrv    *mqtt.Server
	hub        *brokerhub.Hub
	adminUser  string
	adminPass  string
	started    time.Time
	sessions   map[string]time.Time
	sessMu     sync.RWMutex
	csrfTokens map[string]time.Time
	csrfMu     sync.RWMutex
	log        zerolog.Logger
	upgrader   websocket.Upgrader
}

func New(registry *mqtt.Registry, mqttSrv *mqtt.Server, hub *brokerhub.Hub, adminUser, adminPass string, log zerolog.Logger) *Server {
	s := &Server{
		mux:        http.NewServeMux(),
		registry:   registry,
		mqttSrv:    mqttSrv,
		hub:        hub,
		adminUser:  adminUser,
		adminPass:  adminPass,
		started:    time.Now(),
		sessions:   make(map[string]time.Time),
		csrfTokens: make(map[string]time.Time),
		log:        log.With().Str("component", "admin").Logger(),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.serve)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/login" && r.Method == http.MethodPost {
		s.handleLogin(w, r)
		return
	}
	if !s.authenticated(r) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	if r.Method != http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/admin/") {
		if !s.validCSRF(r) {
			http.Error(w, "csrf", http.StatusForbidden)
			return
		}
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/api/state", s.handleState)
	s.mux.HandleFunc("/api/clients", s.handleClients)
	s.mux.HandleFunc("/api/topics", s.handleTopics)
	s.mux.HandleFunc("/api/topics/", s.handleTopicSub)
	s.mux.HandleFunc("/api/db", s.handleDB)
	s.mux.HandleFunc("/api/admin/restart", s.handleRestart)
	s.mux.HandleFunc("/", s.handleUI)
}

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, uiHTML)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	dedupSize := 0
	if s.hub != nil && s.hub.Dedup != nil {
		dedupSize = s.hub.Dedup.Size()
	}
	writeJSON(w, map[string]any{
		"uptime_sec":   time.Since(s.started).Seconds(),
		"topics":       len(s.registry.ListTopics()),
		"dedup_size":   dedupSize,
		"dedup_ttl":    s.hub.DedupTTL.String(),
		"started_at":   s.started,
	})
}

func (s *Server) handleClients(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.mqttSrv.ListSessions())
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	names := s.registry.ListTopics()
	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		t, ok := s.registry.Get(n)
		if !ok {
			continue
		}
		out = append(out, map[string]any{
			"name": n,
			"seq":  t.CurrentSeq(),
		})
	}
	writeJSON(w, out)
}

func (s *Server) handleTopicSub(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/topics/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	name := parts[0]
	t, ok := s.registry.Get(name)
	if !ok {
		t2, err := s.registry.GetOrCreate(name)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		t = t2
	}

	if len(parts) == 1 {
		writeJSON(w, map[string]any{"name": name, "seq": t.CurrentSeq()})
		return
	}

	switch parts[1] {
	case "messages":
		from, _ := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 50
		}
		msgs, err := t.GetMessages(from, limit)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, msgs)
	case "offsets":
		// list offsets keys from badger scan - simplified: return current seq only
		writeJSON(w, map[string]any{"topic": name, "seq": t.CurrentSeq()})
	case "tail":
		if r.Header.Get("Upgrade") == "websocket" {
			s.handleTailWS(w, r, t, name)
			return
		}
		http.Error(w, "websocket required", 400)
	default:
		if parts[1] == "replay" && r.Method == http.MethodPost {
			s.handleReplay(w, r, t, name)
			return
		}
		if parts[1] == "purge" && r.Method == http.MethodPost {
			if err := t.Purge(); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			writeJSON(w, map[string]string{"status": "purged"})
			return
		}
		http.NotFound(w, r)
	}
}

func (s *Server) handleTailWS(w http.ResponseWriter, r *http.Request, t *topic.Topic, name string) {
	ch, unsub := t.Subscribe()
	defer unsub()
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			_ = conn.WriteJSON(map[string]any{
				"seq":           m.Seq,
				"topic":         m.Topic,
				"payload":       string(m.Payload),
				"server_msg_id": m.ServerMsgID.String(),
			})
		}
	}
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request, t *topic.Topic, name string) {
	from, _ := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
	to, _ := strconv.ParseUint(r.URL.Query().Get("to"), 10, 64)
	if to == 0 {
		to = t.CurrentSeq()
	}
	msgs, err := t.GetMessages(from, int(to-from+1))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	replayed := 0
	for _, m := range msgs {
		env := envelope.Envelope{
			IdempotencyID: m.IdempotencyID,
			ServerMsgID:   m.ServerMsgID,
			PublishTS:     m.Timestamp,
			Payload:       m.Payload,
		}
		if _, err := t.Publish(env); err == nil {
			replayed++
		}
	}
	writeJSON(w, map[string]any{"replayed": replayed})
}

func (s *Server) handleDB(w http.ResponseWriter, r *http.Request) {
	topicNames := s.registry.ListTopics()
	topicDBs := make([]map[string]any, 0, len(topicNames))
	for _, name := range topicNames {
		seq := uint64(0)
		if t, ok := s.registry.Get(name); ok {
			seq = t.CurrentSeq()
		}
		topicDBs = append(topicDBs, map[string]any{
			"topic": name,
			"dir":   s.registry.TopicDataDir(name),
			"seq":   seq,
		})
	}

	writeJSON(w, map[string]any{
		"meta_dir":   s.hub.BaseDir,
		"topics_dir": s.registry.BaseDir(),
		"topics":     topicNames,
		"topic_dbs":  topicDBs,
	})
}

func (s *Server) handleRestart(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "restarting"})
	go func() {
		time.Sleep(200 * time.Millisecond)
		s.mqttSrv.Shutdown()
	}()
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != s.adminUser || pass != s.adminPass {
		http.Error(w, "unauthorized", 401)
		return
	}
	tok := randomToken()
	s.sessMu.Lock()
	s.sessions[tok] = time.Now().Add(24 * time.Hour)
	s.sessMu.Unlock()
	csrf := randomToken()
	s.csrfMu.Lock()
	s.csrfTokens[csrf] = time.Now().Add(24 * time.Hour)
	s.csrfMu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "bmq_session", Value: tok, Path: "/", HttpOnly: true})
	writeJSON(w, map[string]string{"csrf": csrf})
}

func (s *Server) authenticated(r *http.Request) bool {
	c, err := r.Cookie("bmq_session")
	if err != nil {
		return false
	}
	s.sessMu.RLock()
	exp, ok := s.sessions[c.Value]
	s.sessMu.RUnlock()
	return ok && time.Now().Before(exp)
}

func (s *Server) validCSRF(r *http.Request) bool {
	tok := r.Header.Get("X-CSRF-Token")
	if tok == "" {
		return false
	}
	s.csrfMu.RLock()
	exp, ok := s.csrfTokens[tok]
	s.csrfMu.RUnlock()
	return ok && time.Now().Before(exp)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func randomToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
