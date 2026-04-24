package mqtt

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

const (
	TypeConnect     byte = 1
	TypeConnAck     byte = 2
	TypePublish     byte = 3
	TypePubAck      byte = 4
	TypeSubscribe   byte = 8
	TypeSubAck      byte = 9
	TypeUnsubscribe byte = 10
	TypeUnsubAck    byte = 11
	TypePingReq     byte = 12
	TypePingResp    byte = 13
	TypeDisconnect  byte = 14
)

const (
	ConnAccepted              byte = 0x00
	ConnRefusedBadProtocol    byte = 0x01
	ConnRefusedIDRejected     byte = 0x02
	ConnRefusedUnavailable    byte = 0x03
	ConnRefusedBadCredentials byte = 0x04
	ConnRefusedNotAuthorised  byte = 0x05
)

type ConnectPacket struct {
	ClientID     string
	Username     string
	Password     string
	CleanSession bool
	KeepAlive    uint16
}

type PublishPacket struct {
	TopicName string
	PacketID  uint16
	QoS       byte
	Retain    bool
	Dup       bool
	Payload   []byte
}

type TopicFilter struct {
	Filter string
	QoS    byte
}

type SubscribePacket struct {
	PacketID uint16
	Topics   []TopicFilter
}

type UnsubscribePacket struct {
	PacketID uint16
	Topics   []string
}

type PubAckPacket struct {
	PacketID uint16
}

type Packet struct {
	Type        byte
	Connect     *ConnectPacket
	Publish     *PublishPacket
	PubAck      *PubAckPacket
	Subscribe   *SubscribePacket
	Unsubscribe *UnsubscribePacket
}

func ReadPacket(r io.Reader) (*Packet, error) {
	header := make([]byte, 1)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, errors.Wrap(err, "read fixed-header byte")
	}
	pktType := header[0] >> 4
	flags := header[0] & 0x0F

	remaining, err := readRemainingLength(r)
	if err != nil {
		return nil, errors.Wrap(err, "read remaining length")
	}

	body := make([]byte, remaining)
	if remaining > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, errors.Wrap(err, "read packet body")
		}
	}

	pkt := &Packet{Type: pktType}
	switch pktType {
	case TypeConnect:
		cp, err := parseConnect(body)
		if err != nil {
			return nil, errors.Wrap(err, "parse CONNECT")
		}
		pkt.Connect = cp
	case TypePublish:
		pp, err := parsePublish(flags, body)
		if err != nil {
			return nil, errors.Wrap(err, "parse PUBLISH")
		}
		pkt.Publish = pp
	case TypePubAck:
		if len(body) < 2 {
			return nil, errors.New("PUBACK: body too short")
		}
		pkt.PubAck = &PubAckPacket{PacketID: binary.BigEndian.Uint16(body[0:2])}
	case TypeSubscribe:
		sp, err := parseSubscribe(body)
		if err != nil {
			return nil, errors.Wrap(err, "parse SUBSCRIBE")
		}
		pkt.Subscribe = sp
	case TypeUnsubscribe:
		up, err := parseUnsubscribe(body)
		if err != nil {
			return nil, errors.Wrap(err, "parse UNSUBSCRIBE")
		}
		pkt.Unsubscribe = up
	case TypePingReq, TypeDisconnect:
	default:
		return nil, fmt.Errorf("unknown MQTT packet type: %d", pktType)
	}
	return pkt, nil
}

func readRemainingLength(r io.Reader) (int, error) {
	var length, shift int
	buf := make([]byte, 1)
	for {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		length |= int(buf[0]&0x7F) << shift
		if buf[0]&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 21 {
			return 0, errors.New("remaining length: overflow (more than 4 bytes)")
		}
	}
	return length, nil
}

func readString(b []byte, pos int) (string, int, error) {
	if pos+2 > len(b) {
		return "", pos, errors.New("string: buffer too short for length prefix")
	}
	l := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	if pos+l > len(b) {
		return "", pos, fmt.Errorf("string: buffer too short for data (need %d, have %d)", l, len(b)-pos)
	}
	return string(b[pos : pos+l]), pos + l, nil
}

func parseConnect(body []byte) (*ConnectPacket, error) {
	pos := 0

	protoName, p, err := readString(body, pos)
	if err != nil {
		return nil, errors.Wrap(err, "protocol name")
	}
	pos = p
	if protoName != "MQTT" && protoName != "MQIsdp" {
		return nil, fmt.Errorf("unsupported protocol name: %q", protoName)
	}

	if pos >= len(body) {
		return nil, errors.New("CONNECT: missing protocol level")
	}
	pos++

	if pos >= len(body) {
		return nil, errors.New("CONNECT: missing connect flags")
	}
	connectFlags := body[pos]
	pos++

	cleanSession := connectFlags&0x02 != 0
	usernameFlag := connectFlags&0x80 != 0
	passwordFlag := connectFlags&0x40 != 0
	willFlag := connectFlags&0x04 != 0

	if pos+2 > len(body) {
		return nil, errors.New("CONNECT: missing keep-alive")
	}
	keepAlive := binary.BigEndian.Uint16(body[pos : pos+2])
	pos += 2

	clientID, p, err := readString(body, pos)
	if err != nil {
		return nil, errors.Wrap(err, "CONNECT: client ID")
	}
	pos = p

	if willFlag {
		_, p, err = readString(body, pos)
		if err != nil {
			return nil, errors.Wrap(err, "CONNECT: will topic")
		}
		pos = p
		_, p, err = readString(body, pos)
		if err != nil {
			return nil, errors.Wrap(err, "CONNECT: will message")
		}
		pos = p
	}

	var username string
	if usernameFlag {
		username, p, err = readString(body, pos)
		if err != nil {
			return nil, errors.Wrap(err, "CONNECT: username")
		}
		pos = p
	}

	var password string
	if passwordFlag {
		password, _, err = readString(body, pos)
		if err != nil {
			return nil, errors.Wrap(err, "CONNECT: password")
		}
	}

	return &ConnectPacket{
		ClientID:     clientID,
		Username:     username,
		Password:     password,
		CleanSession: cleanSession,
		KeepAlive:    keepAlive,
	}, nil
}

func parsePublish(flags byte, body []byte) (*PublishPacket, error) {
	dup := flags&0x08 != 0
	qos := (flags >> 1) & 0x03
	retain := flags&0x01 != 0
	pos := 0

	topicName, p, err := readString(body, pos)
	if err != nil {
		return nil, errors.Wrap(err, "PUBLISH: topic name")
	}
	pos = p

	var packetID uint16
	if qos > 0 {
		if pos+2 > len(body) {
			return nil, errors.New("PUBLISH: missing packet identifier for QoS > 0")
		}
		packetID = binary.BigEndian.Uint16(body[pos : pos+2])
		pos += 2
	}

	payload := make([]byte, len(body)-pos)
	copy(payload, body[pos:])

	return &PublishPacket{
		TopicName: topicName,
		PacketID:  packetID,
		QoS:       qos,
		Retain:    retain,
		Dup:       dup,
		Payload:   payload,
	}, nil
}

func parseSubscribe(body []byte) (*SubscribePacket, error) {
	if len(body) < 2 {
		return nil, errors.New("SUBSCRIBE: body too short")
	}
	packetID := binary.BigEndian.Uint16(body[0:2])
	pos := 2

	var topics []TopicFilter
	for pos < len(body) {
		filter, p, err := readString(body, pos)
		if err != nil {
			return nil, errors.Wrap(err, "SUBSCRIBE: topic filter")
		}
		pos = p
		if pos >= len(body) {
			return nil, errors.New("SUBSCRIBE: missing QoS byte")
		}
		qos := body[pos] & 0x03
		pos++
		topics = append(topics, TopicFilter{Filter: filter, QoS: qos})
	}
	return &SubscribePacket{PacketID: packetID, Topics: topics}, nil
}

func parseUnsubscribe(body []byte) (*UnsubscribePacket, error) {
	if len(body) < 2 {
		return nil, errors.New("UNSUBSCRIBE: body too short")
	}
	packetID := binary.BigEndian.Uint16(body[0:2])
	pos := 2

	var topics []string
	for pos < len(body) {
		t, p, err := readString(body, pos)
		if err != nil {
			return nil, errors.Wrap(err, "UNSUBSCRIBE: topic filter")
		}
		pos = p
		topics = append(topics, t)
	}
	return &UnsubscribePacket{PacketID: packetID, Topics: topics}, nil
}

func WriteConnAck(w io.Writer, sessionPresent bool, returnCode byte) error {
	sp := byte(0)
	if sessionPresent {
		sp = 1
	}
	_, err := w.Write([]byte{TypeConnAck << 4, 0x02, sp, returnCode})
	return errors.Wrap(err, "write CONNACK")
}

func WritePublish(w io.Writer, topicName string, payload []byte, qos byte, packetID uint16, dup bool) error {
	flags := qos << 1
	if dup {
		flags |= 0x08
	}
	varHeader := encodeString(topicName)
	if qos > 0 {
		varHeader = append(varHeader, byte(packetID>>8), byte(packetID))
	}

	buf := []byte{(TypePublish << 4) | flags}
	buf = append(buf, encodeRemainingLength(len(varHeader)+len(payload))...)
	buf = append(buf, varHeader...)
	buf = append(buf, payload...)
	_, err := w.Write(buf)
	return errors.Wrap(err, "write PUBLISH")
}

func WritePubAck(w io.Writer, packetID uint16) error {
	_, err := w.Write([]byte{TypePubAck << 4, 0x02, byte(packetID >> 8), byte(packetID)})
	return errors.Wrap(err, "write PUBACK")
}

func WriteSubAck(w io.Writer, packetID uint16, returnCodes []byte) error {
	body := append([]byte{byte(packetID >> 8), byte(packetID)}, returnCodes...)
	buf := []byte{TypeSubAck << 4}
	buf = append(buf, encodeRemainingLength(len(body))...)
	buf = append(buf, body...)
	_, err := w.Write(buf)
	return errors.Wrap(err, "write SUBACK")
}

func WriteUnsubAck(w io.Writer, packetID uint16) error {
	_, err := w.Write([]byte{TypeUnsubAck << 4, 0x02, byte(packetID >> 8), byte(packetID)})
	return errors.Wrap(err, "write UNSUBACK")
}

func WritePingResp(w io.Writer) error {
	_, err := w.Write([]byte{TypePingResp << 4, 0x00})
	return errors.Wrap(err, "write PINGRESP")
}

func encodeString(s string) []byte {
	b := []byte(s)
	return append([]byte{byte(len(b) >> 8), byte(len(b))}, b...)
}

func encodeRemainingLength(length int) []byte {
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
