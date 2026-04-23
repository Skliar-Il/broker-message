package mqtt

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/Skliar-Il/broker-message/core/topic"
	"github.com/rs/zerolog"
)

const catchUpLimit = 500

type session struct {
	conn     net.Conn
	connMu   sync.Mutex
	clientID string
	registry *Registry
	subs     map[string]func()
	subsMu   sync.Mutex
	log      zerolog.Logger
}

func newSession(conn net.Conn, registry *Registry, log zerolog.Logger) *session {
	return &session{
		conn:     conn,
		registry: registry,
		subs:     make(map[string]func()),
		log:      log.With().Str("remote", conn.RemoteAddr().String()).Logger(),
	}
}

func (s *session) Handle(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		s.cleanupSubs()
		s.conn.Close()
		s.log.Info().Str("client_id", s.clientID).Msg("session: closed")
	}()

	r := bufio.NewReader(s.conn)

	pkt, err := ReadPacket(r)
	if err != nil {
		s.log.Error().Err(err).Msg("session: read first packet failed")
		return
	}
	if pkt.Type != TypeConnect || pkt.Connect == nil {
		s.log.Error().Uint8("type", pkt.Type).Msg("session: first packet is not CONNECT")
		return
	}
	if err := s.handleConnect(pkt.Connect); err != nil {
		s.log.Error().Err(err).Msg("session: CONNECT handling failed")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pkt, err := ReadPacket(r)
		if err != nil {
			s.log.Warn().Err(err).Msg("session: read error, closing")
			return
		}
		if err := s.dispatch(ctx, pkt); err != nil {
			s.log.Warn().Err(err).Msg("session: dispatch error, closing")
			return
		}
	}
}

func (s *session) dispatch(ctx context.Context, pkt *Packet) error {
	switch pkt.Type {
	case TypePublish:
		return s.handlePublish(pkt.Publish)
	case TypeSubscribe:
		return s.handleSubscribe(ctx, pkt.Subscribe)
	case TypeUnsubscribe:
		return s.handleUnsubscribe(pkt.Unsubscribe)
	case TypePubAck:
		return nil
	case TypePingReq:
		return s.write(func() error { return WritePingResp(s.conn) })
	case TypeDisconnect:
		return fmt.Errorf("client sent DISCONNECT")
	default:
		s.log.Warn().Uint8("type", pkt.Type).Msg("session: unhandled packet type")
		return nil
	}
}

func (s *session) handleConnect(cp *ConnectPacket) error {
	s.clientID = cp.ClientID
	if s.clientID == "" {
		s.clientID = fmt.Sprintf("auto-%s", s.conn.RemoteAddr().String())
	}
	s.log = s.log.With().Str("client_id", s.clientID).Logger()
	s.log.Info().
		Bool("clean_session", cp.CleanSession).
		Uint16("keep_alive", cp.KeepAlive).
		Msg("session: CONNECT accepted")
	return s.write(func() error {
		return WriteConnAck(s.conn, false, ConnAccepted)
	})
}

func (s *session) handlePublish(pp *PublishPacket) error {
	t, err := s.registry.GetOrCreate(pp.TopicName)
	if err != nil {
		return fmt.Errorf("get topic %q: %w", pp.TopicName, err)
	}
	if err := t.Publish(pp.Payload); err != nil {
		return fmt.Errorf("publish to topic %q: %w", pp.TopicName, err)
	}
	s.log.Debug().
		Str("topic", pp.TopicName).
		Int("bytes", len(pp.Payload)).
		Uint8("qos", pp.QoS).
		Msg("session: PUBLISH processed")
	if pp.QoS == 1 {
		return s.write(func() error { return WritePubAck(s.conn, pp.PacketID) })
	}
	return nil
}

func (s *session) handleSubscribe(ctx context.Context, sp *SubscribePacket) error {
	retCodes := make([]byte, len(sp.Topics))
	type pending struct {
		t       *topic.Topic
		topicFn string
		msgCh   <-chan *topic.Message
		catchUp []*topic.Message
	}
	pendings := make([]pending, 0, len(sp.Topics))

	for i, tf := range sp.Topics {
		t, err := s.registry.GetOrCreate(tf.Filter)
		if err != nil {
			return fmt.Errorf("get topic %q: %w", tf.Filter, err)
		}
		msgCh, unsub := t.Subscribe()

		s.subsMu.Lock()
		if old, ok := s.subs[tf.Filter]; ok {
			old()
		}
		s.subs[tf.Filter] = unsub
		s.subsMu.Unlock()

		offs := t.Offsets()
		offset := offs.Get(s.clientID)
		var msgs []*topic.Message
		if offset < t.CurrentSeq() {
			ms, err := t.GetMessages(offset+1, catchUpLimit)
			if err != nil {
				s.log.Warn().Err(err).Str("topic", tf.Filter).Msg("session: catch-up fetch failed")
			} else {
				msgs = ms
			}
		}

		pendings = append(pendings, pending{t: t, topicFn: tf.Filter, msgCh: msgCh, catchUp: msgs})

		retCodes[i] = tf.QoS & 0x01
		s.log.Info().
			Str("filter", tf.Filter).
			Uint8("qos", tf.QoS).
			Int("catch_up", len(msgs)).
			Uint64("from_offset", offset).
			Uint64("current_seq", t.CurrentSeq()).
			Msg("session: SUBSCRIBE ok")
	}

	if err := s.write(func() error { return WriteSubAck(s.conn, sp.PacketID, retCodes) }); err != nil {
		return err
	}

	for _, p := range pendings {
		offs := p.t.Offsets()
		for _, m := range p.catchUp {
			m := m
			if err := s.write(func() error {
				return WritePublish(s.conn, m.Topic, m.Payload, 0, 0)
			}); err != nil {
				return err
			}
			offs.Set(s.clientID, m.Seq)
		}
	}

	for _, p := range pendings {
		p := p
		go func() {
			toffs := p.t.Offsets()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-p.msgCh:
					if !ok {
						return
					}
					if msg.Seq <= toffs.Get(s.clientID) {
						continue
					}
					if err := s.write(func() error {
						return WritePublish(s.conn, msg.Topic, msg.Payload, 0, 0)
					}); err != nil {
						s.log.Warn().Err(err).Str("topic", p.topicFn).Msg("session: delivery write error")
						return
					}
					toffs.Set(s.clientID, msg.Seq)
				}
			}
		}()
	}

	return nil
}

func (s *session) handleUnsubscribe(up *UnsubscribePacket) error {
	s.subsMu.Lock()
	for _, topicName := range up.Topics {
		if unsub, ok := s.subs[topicName]; ok {
			unsub()
			delete(s.subs, topicName)
			s.log.Info().Str("topic", topicName).Msg("session: UNSUBSCRIBE ok")
		}
	}
	s.subsMu.Unlock()
	return s.write(func() error { return WriteUnsubAck(s.conn, up.PacketID) })
}

func (s *session) write(fn func() error) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return fn()
}

func (s *session) cleanupSubs() {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for topicName, unsub := range s.subs {
		unsub()
		s.log.Debug().Str("topic", topicName).Msg("session: cleanup subscription")
	}
	s.subs = make(map[string]func())
}
