package mqtt

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Skliar-Il/broker-message/core/auth"
	"github.com/Skliar-Il/broker-message/core/brokerhub"
	"github.com/Skliar-Il/broker-message/core/topic"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

type Registry struct {
	mu      sync.RWMutex
	topics  map[string]*topic.Topic
	baseDir string
	hub     *brokerhub.Hub
	log     zerolog.Logger
}

func NewRegistry(baseDir string, hub *brokerhub.Hub, log zerolog.Logger) *Registry {
	return &Registry{
		topics:  make(map[string]*topic.Topic),
		baseDir: baseDir,
		hub:     hub,
		log:     log.With().Str("component", "registry").Logger(),
	}
}

func sanitizeTopicDir(name string) string {
	r := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return r.Replace(name)
}

func (r *Registry) Hub() *brokerhub.Hub { return r.hub }

func (r *Registry) BaseDir() string { return r.baseDir }

func (r *Registry) TopicDataDir(name string) string {
	return filepath.Join(r.baseDir, sanitizeTopicDir(name))
}

func (r *Registry) Load() error {
	entries, err := os.ReadDir(r.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			r.log.Info().Str("dir", r.baseDir).Msg("registry: base dir does not exist yet, nothing to load")
			return nil
		}
		return errors.Wrapf(err, "registry: read base dir %q", r.baseDir)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	loaded := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(r.baseDir, e.Name())
		t, err := topic.LoadFromDir(dir, r.log)
		if err != nil {
			r.log.Error().Err(err).Str("dir", dir).Msg("registry: load topic failed, skipping")
			continue
		}
		if t.Name() == "" {
			r.log.Warn().Str("dir", dir).Msg("registry: topic dir has empty name metadata, skipping")
			_ = t.Close()
			continue
		}
		if _, dup := r.topics[t.Name()]; dup {
			r.log.Warn().Str("topic", t.Name()).Msg("registry: duplicate topic on load, closing extra")
			_ = t.Close()
			continue
		}
		r.topics[t.Name()] = t
		loaded++
	}
	r.log.Info().Int("topics", loaded).Msg("registry: load complete")
	return nil
}

func (r *Registry) ListTopics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.topics))
	for name := range r.topics {
		out = append(out, name)
	}
	return out
}

func (r *Registry) Get(name string) (*topic.Topic, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.topics[name]
	return t, ok
}

func (r *Registry) GetOrCreate(name string) (*topic.Topic, error) {
	r.mu.RLock()
	t, ok := r.topics[name]
	r.mu.RUnlock()
	if ok {
		return t, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok = r.topics[name]; ok {
		return t, nil
	}

	dir := filepath.Join(r.baseDir, sanitizeTopicDir(name))
	t, err := topic.New(name, dir, r.log)
	if err != nil {
		return nil, errors.Wrapf(err, "registry: create topic %q", name)
	}
	r.topics[name] = t
	r.log.Info().Str("topic", name).Str("dir", dir).Msg("registry: new topic created")
	return t, nil
}

func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for name, t := range r.topics {
		if err := t.Close(); err != nil {
			r.log.Error().Err(err).Str("topic", name).Msg("registry: close topic error")
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

type SessionInfo struct {
	ClientID   string
	RemoteAddr string
	Username   string
	Connected  time.Time
	Topics     []string
}

type Server struct {
	addrPlain string
	addrTLS   string
	tlsConfig *tls.Config
	registry  *Registry
	auth      *auth.Store
	authReq   bool
	wg        sync.WaitGroup
	sessMu    sync.RWMutex
	sessions  map[*session]SessionInfo
	log       zerolog.Logger
}

func NewServer(addrPlain, addrTLS string, tlsCfg *tls.Config, registry *Registry, authStore *auth.Store, authRequired bool, log zerolog.Logger) *Server {
	return &Server{
		addrPlain: addrPlain,
		addrTLS:   addrTLS,
		tlsConfig: tlsCfg,
		registry:  registry,
		auth:      authStore,
		authReq:   authRequired,
		sessions:  make(map[*session]SessionInfo),
		log:       log.With().Str("component", "mqtt_server").Logger(),
	}
}

func (s *Server) Registry() *Registry { return s.registry }

func (s *Server) registerSession(sess *session, info SessionInfo) {
	s.sessMu.Lock()
	s.sessions[sess] = info
	s.sessMu.Unlock()
}

func (s *Server) updateSessionIdentity(sess *session, clientID, username string) {
	s.sessMu.Lock()
	defer s.sessMu.Unlock()
	info, ok := s.sessions[sess]
	if !ok {
		return
	}
	info.ClientID = clientID
	info.Username = username
	s.sessions[sess] = info
}

func (s *Server) updateSessionTopics(sess *session, topics []string) {
	s.sessMu.Lock()
	defer s.sessMu.Unlock()
	info, ok := s.sessions[sess]
	if !ok {
		return
	}
	info.Topics = topics
	s.sessions[sess] = info
}

func (s *Server) unregisterSession(sess *session) {
	s.sessMu.Lock()
	delete(s.sessions, sess)
	s.sessMu.Unlock()
}

func (s *Server) ListSessions() []SessionInfo {
	s.sessMu.RLock()
	defer s.sessMu.RUnlock()
	out := make([]SessionInfo, 0, len(s.sessions))
	for _, info := range s.sessions {
		out = append(out, info)
	}
	return out
}

func (s *Server) KickClient(clientID string) int {
	s.sessMu.Lock()
	var kicked []*session
	for sess, info := range s.sessions {
		if info.ClientID == clientID {
			kicked = append(kicked, sess)
		}
	}
	s.sessMu.Unlock()
	for _, sess := range kicked {
		sess.Close()
	}
	return len(kicked)
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	errCh := make(chan error, 2)

	if s.addrPlain != "" {
		go func() {
			ln, err := net.Listen("tcp", s.addrPlain)
			if err != nil {
				errCh <- errors.Wrap(err, "listen plain")
				return
			}
			s.log.Info().Str("addr", s.addrPlain).Msg("server: MQTT plain listening")
			errCh <- s.serveListener(ctx, ln)
		}()
	}

	if s.addrTLS != "" && s.tlsConfig != nil {
		go func() {
			ln, err := tls.Listen("tcp", s.addrTLS, s.tlsConfig)
			if err != nil {
				errCh <- errors.Wrap(err, "listen tls")
				return
			}
			s.log.Info().Str("addr", s.addrTLS).Msg("server: MQTT TLS listening")
			errCh <- s.serveListener(ctx, ln)
		}()
	}

	select {
	case <-ctx.Done():
		s.wg.Wait()
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *Server) serveListener(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.wg.Wait()
				return nil
			default:
				return errors.Wrap(err, "accept")
			}
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			sess := newSession(c, s, s.registry, s.auth, s.authReq, s.log)
			s.registerSession(sess, SessionInfo{
				RemoteAddr: c.RemoteAddr().String(),
				Connected:  time.Now(),
				Topics:     []string{},
			})
			defer s.unregisterSession(sess)
			sess.Handle(ctx)
		}(conn)
	}
}

func (s *Server) Shutdown() {
	s.sessMu.Lock()
	for sess := range s.sessions {
		sess.Close()
	}
	s.sessMu.Unlock()
	s.wg.Wait()
}
