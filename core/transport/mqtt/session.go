package mqtt

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Skliar-Il/broker-message/core/auth"
	"github.com/Skliar-Il/broker-message/core/envelope"
	"github.com/Skliar-Il/broker-message/core/metrics"
	"github.com/Skliar-Il/broker-message/core/topic"
	"github.com/rs/zerolog"
)

const (
	catchUpLimit        = 500
	retransmitTimeout   = 10 * time.Second
	retransmitTickEvery = time.Second
)

type inflightMsg struct {
	topicName string
	seq       uint64
	wire      []byte
	qos       byte
	sentAt    time.Time
	topicRef  *topic.Topic
}

type session struct {
	conn       net.Conn
	connMu     sync.Mutex
	closed     bool
	closeMu    sync.Mutex
	owner      *Server
	clientID   string
	user       *auth.User
	registry   *Registry
	authStore  *auth.Store
	authReq    bool
	subs       map[string]func()
	subsMu     sync.Mutex
	subQoS     map[string]byte
	inflight   map[uint16]*inflightMsg
	inflightMu sync.Mutex
	nextPktID  uint16
	log        zerolog.Logger
}

func newSession(conn net.Conn, owner *Server, registry *Registry, authStore *auth.Store, authRequired bool, log zerolog.Logger) *session {
	return &session{
		conn:      conn,
		owner:     owner,
		registry:  registry,
		authStore: authStore,
		authReq:   authRequired,
		subs:      make(map[string]func()),
		subQoS:    make(map[string]byte),
		inflight:  make(map[uint16]*inflightMsg),
		nextPktID: 1,
		log:       log.With().Str("remote", conn.RemoteAddr().String()).Logger(),
	}
}

func (s *session) Close() {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return
	}
	s.closed = true
	s.closeMu.Unlock()
	_ = s.conn.Close()
}

func (s *session) allocPacketID() uint16 {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	for {
		id := s.nextPktID
		s.nextPktID++
		if s.nextPktID == 0 {
			s.nextPktID = 1
		}
		if _, used := s.inflight[id]; !used {
			return id
		}
	}
}

func (s *session) Handle(ctx context.Context) {
	metrics.ConnectionsActive.Inc()
	defer metrics.ConnectionsActive.Dec()

	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		s.cleanupSubs()
		s.Close()
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

	go s.retransmitLoop(ctx)

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
		return s.handlePubAck(pkt.PubAck)
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

	if s.authReq && s.authStore != nil {
		u, ok := s.authStore.Authenticate(cp.Username, cp.Password)
		if !ok {
			_ = s.write(func() error { return WriteConnAck(s.conn, false, ConnRefusedBadCredentials) })
			return fmt.Errorf("auth failed for user %q", cp.Username)
		}
		s.user = u
	}

	username := cp.Username
	if s.user != nil {
		username = s.user.Name
	}
	if s.owner != nil {
		s.owner.updateSessionIdentity(s, s.clientID, username)
	}

	s.log.Info().Str("user", cp.Username).Msg("session: CONNECT accepted")
	return s.write(func() error {
		return WriteConnAck(s.conn, false, ConnAccepted)
	})
}

func (s *session) handlePublish(pp *PublishPacket) error {
	if s.user != nil && !s.user.CanPublish(pp.TopicName) {
		metrics.PublishTotal.WithLabelValues(pp.TopicName, fmt.Sprintf("%d", pp.QoS), "denied").Inc()
		return fmt.Errorf("publish denied for topic %q", pp.TopicName)
	}

	env, err := envelope.Decode(pp.Payload)
	if err != nil {
		env = envelope.NewPublish(envelope.Envelope{}.IdempotencyID, pp.Payload)
	}

	t, err := s.registry.GetOrCreate(pp.TopicName)
	if err != nil {
		metrics.PublishTotal.WithLabelValues(pp.TopicName, fmt.Sprintf("%d", pp.QoS), "error").Inc()
		return fmt.Errorf("get topic %q: %w", pp.TopicName, err)
	}

	hub := s.registry.Hub()
	if hub != nil && hub.Dedup != nil {
		if _, dup := hub.Dedup.Check(env.IdempotencyID); dup {
			metrics.PublishDuplicates.WithLabelValues(pp.TopicName).Inc()
			metrics.PublishTotal.WithLabelValues(pp.TopicName, fmt.Sprintf("%d", pp.QoS), "duplicate").Inc()
			metrics.DedupCacheSize.Set(float64(hub.Dedup.Size()))
			if pp.QoS == 1 {
				return s.write(func() error { return WritePubAck(s.conn, pp.PacketID) })
			}
			return nil
		}
	}

	result, err := t.Publish(env)
	if err != nil {
		metrics.PublishTotal.WithLabelValues(pp.TopicName, fmt.Sprintf("%d", pp.QoS), "error").Inc()
		return fmt.Errorf("publish to topic %q: %w", pp.TopicName, err)
	}

	if hub != nil && hub.Dedup != nil && result.Msg != nil {
		hub.Dedup.Remember(env.IdempotencyID, result.Msg.ServerMsgID, result.Msg.Seq)
		metrics.DedupCacheSize.Set(float64(hub.Dedup.Size()))
	}

	metrics.PublishTotal.WithLabelValues(pp.TopicName, fmt.Sprintf("%d", pp.QoS), "ok").Inc()
	metrics.TopicSeq.WithLabelValues(pp.TopicName).Set(float64(t.CurrentSeq()))

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
		qos     byte
	}
	pendings := make([]pending, 0, len(sp.Topics))

	for i, tf := range sp.Topics {
		if s.user != nil && !s.user.CanSubscribe(tf.Filter) {
			retCodes[i] = 0x80
			continue
		}
		t, err := s.registry.GetOrCreate(tf.Filter)
		if err != nil {
			return fmt.Errorf("get topic %q: %w", tf.Filter, err)
		}
		msgCh, unsub := t.Subscribe()

		effectiveQoS := tf.QoS
		if effectiveQoS > 1 {
			effectiveQoS = 1
		}

		s.subsMu.Lock()
		if old, ok := s.subs[tf.Filter]; ok {
			old()
		}
		s.subs[tf.Filter] = unsub
		s.subQoS[tf.Filter] = effectiveQoS
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

		pendings = append(pendings, pending{t: t, topicFn: tf.Filter, msgCh: msgCh, catchUp: msgs, qos: effectiveQoS})
		retCodes[i] = effectiveQoS
	}

	if err := s.write(func() error { return WriteSubAck(s.conn, sp.PacketID, retCodes) }); err != nil {
		return err
	}
	s.syncSessionTopics()

	for _, p := range pendings {
		offs := p.t.Offsets()
		for _, m := range p.catchUp {
			if err := s.deliverMessage(p.t, p.topicFn, m, p.qos, offs); err != nil {
				return err
			}
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
					if err := s.deliverMessage(p.t, p.topicFn, msg, p.qos, toffs); err != nil {
						s.log.Warn().Err(err).Str("topic", p.topicFn).Msg("session: delivery write error")
						return
					}
				}
			}
		}()
	}

	return nil
}

func (s *session) deliverMessage(t *topic.Topic, topicFn string, m *topic.Message, qos byte, offs interface {
	Get(string) uint64
	Set(string, uint64)
}) error {
	wire := m.RawWire
	if len(wire) == 0 {
		env := envelope.NewPublish(m.IdempotencyID, m.Payload).WithServerMsgID(m.ServerMsgID)
		wire = env.Encode()
	}
	if qos == 1 {
		pid := s.allocPacketID()
		s.inflightMu.Lock()
		s.inflight[pid] = &inflightMsg{
			topicName: m.Topic,
			seq:       m.Seq,
			wire:      wire,
			qos:       1,
			sentAt:    time.Now(),
			topicRef:  t,
		}
		metrics.InflightMessages.Inc()
		s.inflightMu.Unlock()
		err := s.write(func() error {
			return WritePublish(s.conn, m.Topic, wire, 1, pid, false)
		})
		if err != nil {
			metrics.DeliverTotal.WithLabelValues(topicFn, "1", "error").Inc()
			return err
		}
		metrics.DeliverTotal.WithLabelValues(topicFn, "1", "ok").Inc()
		return nil
	}
	if err := s.write(func() error {
		return WritePublish(s.conn, m.Topic, wire, 0, 0, false)
	}); err != nil {
		metrics.DeliverTotal.WithLabelValues(topicFn, "0", "error").Inc()
		return err
	}
	metrics.DeliverTotal.WithLabelValues(topicFn, "0", "ok").Inc()
	offs.Set(s.clientID, m.Seq)
	return nil
}

func (s *session) handlePubAck(pa *PubAckPacket) error {
	s.inflightMu.Lock()
	msg, ok := s.inflight[pa.PacketID]
	if ok {
		delete(s.inflight, pa.PacketID)
		metrics.InflightMessages.Dec()
	}
	s.inflightMu.Unlock()

	if !ok {
		s.log.Warn().Uint16("pid", pa.PacketID).Msg("session: PUBACK for unknown packet id")
		return nil
	}

	msg.topicRef.Offsets().Set(s.clientID, msg.seq)
	return nil
}

func (s *session) retransmitLoop(ctx context.Context) {
	ticker := time.NewTicker(retransmitTickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			s.inflightMu.Lock()
			var toRetransmit []struct {
				pid uint16
				msg *inflightMsg
			}
			for pid, msg := range s.inflight {
				if now.Sub(msg.sentAt) >= retransmitTimeout {
					toRetransmit = append(toRetransmit, struct {
						pid uint16
						msg *inflightMsg
					}{pid, msg})
				}
			}
			s.inflightMu.Unlock()

			for _, item := range toRetransmit {
				if err := s.write(func() error {
					return WritePublish(s.conn, item.msg.topicName, item.msg.wire, item.msg.qos, item.pid, true)
				}); err != nil {
					continue
				}
				metrics.RetransmitTotal.WithLabelValues(item.msg.topicName).Inc()
				s.inflightMu.Lock()
				if m, ok := s.inflight[item.pid]; ok {
					m.sentAt = time.Now()
				}
				s.inflightMu.Unlock()
			}
		}
	}
}

func (s *session) handleUnsubscribe(up *UnsubscribePacket) error {
	s.subsMu.Lock()
	for _, topicName := range up.Topics {
		if unsub, ok := s.subs[topicName]; ok {
			unsub()
			delete(s.subs, topicName)
			delete(s.subQoS, topicName)
		}
	}
	s.subsMu.Unlock()
	s.syncSessionTopics()
	return s.write(func() error { return WriteUnsubAck(s.conn, up.PacketID) })
}

func (s *session) write(fn func() error) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return fn()
}

func (s *session) cleanupSubs() {
	s.subsMu.Lock()
	for topicName, unsub := range s.subs {
		unsub()
		s.log.Debug().Str("topic", topicName).Msg("session: cleanup subscription")
	}
	s.subs = make(map[string]func())
	s.subQoS = make(map[string]byte)
	s.subsMu.Unlock()

	s.syncSessionTopics()

	s.inflightMu.Lock()
	s.inflight = make(map[uint16]*inflightMsg)
	s.inflightMu.Unlock()
}

func (s *session) syncSessionTopics() {
	if s.owner == nil {
		return
	}
	s.subsMu.Lock()
	topics := make([]string, 0, len(s.subs))
	for topicName := range s.subs {
		topics = append(topics, topicName)
	}
	s.subsMu.Unlock()
	s.owner.updateSessionTopics(s, topics)
}
