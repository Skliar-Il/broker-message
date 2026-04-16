package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
)

const (
	brokerAddr = "localhost:1883"
	topicName  = "hello"
)

func main() {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := sendConnect(conn, "example-consumer"); err != nil {
		log.Fatalf("CONNECT: %v", err)
	}
	if err := readConnAck(conn); err != nil {
		log.Fatalf("CONNACK: %v", err)
	}
	log.Println("connected to broker")

	if err := sendSubscribe(conn, topicName); err != nil {
		log.Fatalf("SUBSCRIBE: %v", err)
	}
	if err := readSubAck(conn); err != nil {
		log.Fatalf("SUBACK: %v", err)
	}
	log.Printf("subscribed to %q, waiting for messages...\n", topicName)

	for {
		pktType, payload, err := readPacket(conn)
		if err != nil {
			log.Fatalf("read: %v", err)
		}
		if pktType == 3 {
			topic, msg := parsePublish(payload)
			fmt.Printf("[%s] %s\n", topic, msg)
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

func sendSubscribe(conn net.Conn, topic string) error {
	tb := []byte(topic)
	var body []byte
	body = append(body, 0, 1)
	body = append(body, byte(len(tb)>>8), byte(len(tb)))
	body = append(body, tb...)
	body = append(body, 0)

	pkt := []byte{0x82}
	pkt = append(pkt, encodeRemaining(len(body))...)
	pkt = append(pkt, body...)
	_, err := conn.Write(pkt)
	return err
}

func readSubAck(conn net.Conn) error {
	_, _, err := readPacket(conn)
	return err
}

func readPacket(conn net.Conn) (byte, []byte, error) {
	header := make([]byte, 1)
	if _, err := readFull(conn, header); err != nil {
		return 0, nil, err
	}
	pktType := header[0] >> 4

	remaining, err := readRemainingLength(conn)
	if err != nil {
		return 0, nil, err
	}
	body := make([]byte, remaining)
	if remaining > 0 {
		if _, err := readFull(conn, body); err != nil {
			return 0, nil, err
		}
	}
	return pktType, body, nil
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

func parsePublish(body []byte) (string, string) {
	if len(body) < 2 {
		return "", ""
	}
	topicLen := int(binary.BigEndian.Uint16(body[:2]))
	if 2+topicLen > len(body) {
		return "", ""
	}
	topic := string(body[2 : 2+topicLen])
	payload := string(body[2+topicLen:])
	return topic, payload
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
