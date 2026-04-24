package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
)

const (
	brokerAddr = "localhost:1883"
	topicName  = "hello"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	qos := byte(0)
	if envOr("QOS", "0") == "1" {
		qos = 1
	}
	clientID := envOr("CLIENT_ID", "example-consumer")
	log.Printf("consumer starting client_id=%s QoS=%d", clientID, qos)

	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := sendConnect(conn, clientID); err != nil {
		log.Fatalf("CONNECT: %v", err)
	}
	if err := readConnAck(conn); err != nil {
		log.Fatalf("CONNACK: %v", err)
	}
	log.Println("connected to broker")

	if err := sendSubscribe(conn, topicName, qos); err != nil {
		log.Fatalf("SUBSCRIBE: %v", err)
	}
	if err := readSubAck(conn); err != nil {
		log.Fatalf("SUBACK: %v", err)
	}
	log.Printf("subscribed to %q (QoS=%d), waiting for messages...\n", topicName, qos)

	for {
		pktType, flags, body, err := readPacketWithFlags(conn)
		if err != nil {
			log.Fatalf("read: %v", err)
		}
		if pktType != 3 {
			continue
		}

		msgQoS := (flags >> 1) & 0x03
		topic, msg, pid := parsePublish(body, msgQoS)
		fmt.Printf("[%s] %s\n", topic, msg)

		if msgQoS >= 1 && pid > 0 {
			if err := sendPubAck(conn, pid); err != nil {
				log.Fatalf("PUBACK: %v", err)
			}
		}
	}
}

func sendConnect(conn net.Conn, clientID string) error {
	var vh []byte
	vh = append(vh, 0, 4)
	vh = append(vh, []byte("MQTT")...)
	vh = append(vh, 4)
	vh = append(vh, 0x02)
	vh = append(vh, 0, 60)
	cid := []byte(clientID)
	vh = append(vh, byte(len(cid)>>8), byte(len(cid)))
	vh = append(vh, cid...)

	pkt := []byte{0x10}
	pkt = append(pkt, encodeRemaining(len(vh))...)
	pkt = append(pkt, vh...)
	_, err := conn.Write(pkt)
	return err
}

func readConnAck(conn net.Conn) error {
	buf := make([]byte, 4)
	if _, err := readFull(conn, buf); err != nil {
		return err
	}
	if buf[0]>>4 != 2 {
		return fmt.Errorf("expected CONNACK, got type %d", buf[0]>>4)
	}
	if buf[3] != 0 {
		return fmt.Errorf("connection refused: code %d", buf[3])
	}
	return nil
}

func sendSubscribe(conn net.Conn, topic string, qos byte) error {
	tb := []byte(topic)
	var body []byte
	body = append(body, 0, 1)
	body = append(body, byte(len(tb)>>8), byte(len(tb)))
	body = append(body, tb...)
	body = append(body, qos&0x03)

	pkt := []byte{0x82}
	pkt = append(pkt, encodeRemaining(len(body))...)
	pkt = append(pkt, body...)
	_, err := conn.Write(pkt)
	return err
}

func readSubAck(conn net.Conn) error {
	_, _, _, err := readPacketWithFlags(conn)
	return err
}

func sendPubAck(conn net.Conn, pid uint16) error {
	pkt := []byte{0x40, 0x02, byte(pid >> 8), byte(pid)}
	_, err := conn.Write(pkt)
	return err
}

func readPacketWithFlags(conn net.Conn) (pktType byte, flags byte, body []byte, err error) {
	header := make([]byte, 1)
	if _, err = readFull(conn, header); err != nil {
		return
	}
	pktType = header[0] >> 4
	flags = header[0] & 0x0F

	remaining, e := readRemainingLength(conn)
	if e != nil {
		err = e
		return
	}
	body = make([]byte, remaining)
	if remaining > 0 {
		if _, e := readFull(conn, body); e != nil {
			err = e
			return
		}
	}
	return
}

func readRemainingLength(conn net.Conn) (int, error) {
	var length, shift int
	buf := make([]byte, 1)
	for {
		if _, err := readFull(conn, buf); err != nil {
			return 0, err
		}
		length |= int(buf[0]&0x7F) << shift
		if buf[0]&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 21 {
			return 0, fmt.Errorf("remaining length overflow")
		}
	}
	return length, nil
}

func parsePublish(body []byte, qos byte) (string, string, uint16) {
	if len(body) < 2 {
		return "", "", 0
	}
	topicLen := int(binary.BigEndian.Uint16(body[:2]))
	if 2+topicLen > len(body) {
		return "", "", 0
	}
	topic := string(body[2 : 2+topicLen])
	pos := 2 + topicLen

	var pid uint16
	if qos > 0 {
		if pos+2 > len(body) {
			return topic, "", 0
		}
		pid = binary.BigEndian.Uint16(body[pos : pos+2])
		pos += 2
	}

	payload := string(body[pos:])
	return topic, payload, pid
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

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
