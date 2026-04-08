package mqtt

import (
	"context"
	"net"
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
	log      zerolog.Logger
}

func NewServer(addr string, registry *Registry, log zerolog.Logger) *Server {
	return &Server{
		addr:     addr,
		registry: registry,
		log:      log.With().Str("component", "mqtt_server").Logger(),
	}
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
		_ = ln.Close()
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

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			sess := newSession(c, s.registry, s.log)
			sess.Handle(ctx)
		}(conn)
	}
}

func (s *Server) Shutdown() {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
}
