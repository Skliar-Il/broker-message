package mqtt

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Skliar-Il/broker-message/core/topic"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

type Registry struct {
	mu      sync.RWMutex
	topics  map[string]*topic.Topic
	baseDir string
	log     zerolog.Logger
}

func NewRegistry(baseDir string, log zerolog.Logger) *Registry {
	return &Registry{
		topics:  make(map[string]*topic.Topic),
		baseDir: baseDir,
		log:     log.With().Str("component", "registry").Logger(),
	}
}

func sanitizeTopicDir(name string) string {
	r := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return r.Replace(name)
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
		r.log.Info().Str("topic", t.Name()).Str("dir", dir).Uint64("seq", t.CurrentSeq()).Msg("registry: topic loaded from disk")
	}
	r.log.Info().Int("topics", loaded).Msg("registry: load complete")
	return nil
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

type Server struct {
	addr     string
	listener net.Listener
	registry *Registry
	wg       sync.WaitGroup
	connsMu  sync.Mutex
	conns    map[net.Conn]struct{}
	log      zerolog.Logger
}

func NewServer(addr string, registry *Registry, log zerolog.Logger) *Server {
	return &Server{
		addr:     addr,
		registry: registry,
		conns:    make(map[net.Conn]struct{}),
		log:      log.With().Str("component", "mqtt_server").Logger(),
	}
}

func (s *Server) trackConn(c net.Conn) {
	s.connsMu.Lock()
	s.conns[c] = struct{}{}
	s.connsMu.Unlock()
}

func (s *Server) untrackConn(c net.Conn) {
	s.connsMu.Lock()
	delete(s.conns, c)
	s.connsMu.Unlock()
}

func (s *Server) closeAllConns() int {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	n := len(s.conns)
	for c := range s.conns {
		_ = c.Close()
	}
	s.conns = make(map[net.Conn]struct{})
	return n
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return errors.Wrap(err, "listen")
	}
	s.listener = ln
	s.log.Info().Str("addr", s.addr).Msg("server: listening for MQTT connections")

	go func() {
		<-ctx.Done()
		s.log.Info().Msg("server: shutdown signal received, closing listener and client connections")
		_ = ln.Close()
		n := s.closeAllConns()
		s.log.Info().Int("closed_conns", n).Msg("server: client connections closed")
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.wg.Wait()
				s.log.Info().Msg("server: all sessions finished")
				return nil
			default:
				s.log.Error().Err(err).Msg("server: accept error")
				return errors.Wrap(err, "accept")
			}
		}

		s.trackConn(conn)
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer s.untrackConn(c)
			sess := newSession(c, s.registry, s.log)
			sess.Handle(ctx)
		}(conn)
	}
}

func (s *Server) Shutdown() {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.closeAllConns()
	s.wg.Wait()
}
