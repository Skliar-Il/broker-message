package brokermq

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Skliar-Il/broker-message/core/envelope"
	"github.com/google/uuid"
)

// Message delivered to subscriber handlers.
type Message struct {
	Topic         string
	Payload       []byte
	Seq           uint64
	IdempotencyID uuid.UUID
	ServerMsgID   uuid.UUID
	PublishTS     time.Time
}

type Client struct {
	addr    string
	opts    Options
	conn    net.Conn
	reader  *bufio.Reader
	connMu  sync.Mutex
	seen    *SeenCache
	nextPID uint16
	pidMu   sync.Mutex
}

func Connect(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}
	c := &Client{addr: addr, opts: o, seen: NewSeenCache(o.SeenCacheSize)}
	if err := c.dial(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) dial(ctx context.Context) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return err
	}
	if len(c.addr) >= 4 && c.addr[len(c.addr)-4:] == "8883" {
		// optional TLS when port suggests MQTTS (insecure skip for dev)
		tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return err
		}
		conn = tlsConn
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	return c.connectMQTT()
}

func (c *Client) connectMQTT() error {
	var vh []byte
	vh = append(vh, 0, 4)
	vh = append(vh, []byte("MQTT")...)
	vh = append(vh, 4)
	flags := byte(0x02)
	if c.opts.Username != "" {
		flags |= 0x80
		if c.opts.Password != "" {
			flags |= 0x40
		}
	}
	vh = append(vh, flags)
	vh = append(vh, 0, 60)
	cid := []byte(c.opts.ClientID)
	vh = append(vh, byte(len(cid)>>8), byte(len(cid)))
	vh = append(vh, cid...)
	if c.opts.Username != "" {
		u := []byte(c.opts.Username)
		vh = append(vh, byte(len(u)>>8), byte(len(u)))
		vh = append(vh, u...)
	}
	if c.opts.Password != "" {
		p := []byte(c.opts.Password)
		vh = append(vh, byte(len(p)>>8), byte(len(p)))
		vh = append(vh, p...)
	}
	pkt := []byte{0x10}
	pkt = append(pkt, encodeRemaining(len(vh))...)
	pkt = append(pkt, vh...)
	if _, err := c.conn.Write(pkt); err != nil {
		return err
	}
	buf := make([]byte, 4)
	if _, err := readFull(c.conn, buf); err != nil {
		return err
	}
	if buf[3] != 0 {
		return fmt.Errorf("connack refused: %d", buf[3])
	}
	return nil
}

// Publish sends a message with automatic idempotency key unless WithIdempotencyKey was set on Connect options via Publish option pattern.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte, opts ...Option) error {
	o := c.opts
	for _, fn := range opts {
		fn(&o)
	}
	idem := o.IdempotencyID
	if idem == uuid.Nil {
		idem = uuid.Must(uuid.NewV7())
	}
	env := envelope.NewPublish(idem, payload)
	wire := env.Encode()

	qos := o.QoS
	var pid uint16
	if qos > 0 {
		c.pidMu.Lock()
		pid = c.nextPID
		c.nextPID++
		if c.nextPID == 0 {
			c.nextPID = 1
		}
		c.pidMu.Unlock()
	}
	return c.writePublish(topic, wire, qos, pid, false)
}

func (c *Client) Subscribe(ctx context.Context, topicFilter string, handler func(Message)) error {
	if err := c.sendSubscribe(topicFilter, c.opts.QoS); err != nil {
		return err
	}
	if _, _, err := c.readPacket(); err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			pktType, flags, body, err := c.readPacketFlags()
			if err != nil {
				return
			}
			if pktType != 3 {
				continue
			}
			topic, wire, pid := parsePublishBody(body, (flags>>1)&0x03)
			env, err := envelope.Decode(wire)
			if err != nil {
				env = envelope.Envelope{Payload: wire}
			}
			if c.seen.Seen(env.ServerMsgID) {
				if pid > 0 {
					_ = c.sendPubAck(pid)
				}
				continue
			}
			handler(Message{
				Topic:         topic,
				Payload:       env.Payload,
				IdempotencyID: env.IdempotencyID,
				ServerMsgID:   env.ServerMsgID,
				PublishTS:     env.PublishTS,
			})
			if pid > 0 {
				_ = c.sendPubAck(pid)
			}
		}
	}()
	return nil
}

func (c *Client) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) writePublish(topic string, payload []byte, qos byte, pid uint16, dup bool) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	tb := []byte(topic)
	var vh []byte
	vh = append(vh, byte(len(tb)>>8), byte(len(tb)))
	vh = append(vh, tb...)
	if qos > 0 {
		vh = append(vh, byte(pid>>8), byte(pid))
	}
	flags := (qos & 0x03) << 1
	if dup {
		flags |= 0x08
	}
	pkt := []byte{(0x03 << 4) | flags}
	pkt = append(pkt, encodeRemaining(len(vh)+len(payload))...)
	pkt = append(pkt, vh...)
	pkt = append(pkt, payload...)
	_, err := c.conn.Write(pkt)
	return err
}

func (c *Client) sendSubscribe(topic string, qos byte) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	tb := []byte(topic)
	var body []byte
	body = append(body, 0, 1)
	body = append(body, byte(len(tb)>>8), byte(len(tb)))
	body = append(body, tb...)
	body = append(body, qos&0x03)
	pkt := []byte{0x82}
	pkt = append(pkt, encodeRemaining(len(body))...)
	pkt = append(pkt, body...)
	_, err := c.conn.Write(pkt)
	return err
}

func (c *Client) sendPubAck(pid uint16) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	_, err := c.conn.Write([]byte{0x40, 0x02, byte(pid >> 8), byte(pid)})
	return err
}

func (c *Client) readPacket() (byte, []byte, error) {
	t, _, b, err := c.readPacketFlags()
	return t, b, err
}

func (c *Client) readPacketFlags() (pktType, flags byte, body []byte, err error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	h := make([]byte, 1)
	if _, err = readFull(c.reader, h); err != nil {
		return
	}
	pktType = h[0] >> 4
	flags = h[0] & 0x0F
	rem, e := readRemainingLength(c.reader)
	if e != nil {
		err = e
		return
	}
	body = make([]byte, rem)
	if rem > 0 {
		_, err = readFull(c.reader, body)
	}
	return
}

func parsePublishBody(body []byte, qos byte) (topic string, payload []byte, pid uint16) {
	if len(body) < 2 {
		return
	}
	tl := int(binary.BigEndian.Uint16(body[:2]))
	if 2+tl > len(body) {
		return
	}
	pos := 2 + tl
	topic = string(body[2:pos])
	if qos > 0 && pos+2 <= len(body) {
		pid = binary.BigEndian.Uint16(body[pos : pos+2])
		pos += 2
	}
	payload = body[pos:]
	return
}

func encodeRemaining(length int) []byte {
	var buf []byte
	for {
		b := byte(length & 0x7F)
		length >>= 7
		if length > 0 {
			b |= 0x80
		}
		buf = append(buf, b)
		if length == 0 {
			break
		}
	}
	return buf
}

func readRemainingLength(r io.Reader) (int, error) {
	var length, shift int
	buf := make([]byte, 1)
	for {
		if _, err := readFull(r, buf); err != nil {
			return 0, err
		}
		length |= int(buf[0]&0x7F) << shift
		if buf[0]&0x80 == 0 {
			break
		}
		shift += 7
	}
	return length, nil
}

func readFull(r io.Reader, buf []byte) (int, error) {
	return io.ReadFull(r, buf)
}
