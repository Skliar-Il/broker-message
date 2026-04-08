package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"
)

const (
	brokerAddr = "localhost:1883"
	topicName  = "demo/hello"
)

func main() {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := sendConnect(conn, "example-publisher"); err != nil {
		log.Fatalf("CONNECT: %v", err)
	}
	if err := readConnAck(conn); err != nil {
		log.Fatalf("CONNACK: %v", err)
	}
	log.Println("connected to broker")

	for i := 1; ; i++ {
		payload := fmt.Sprintf("hello world %d", i)
		if err := sendPublish(conn, topicName, []byte(payload)); err != nil {
			log.Fatalf("PUBLISH: %v", err)
		}
		log.Printf("published: %s", payload)
		time.Sleep(time.Second)
	}
}

func sendConnect(conn net.Conn, clientID string) error {
	var varHeader []byte

	varHeader = append(varHeader, 0, 4)
	varHeader = append(varHeader, []byte("MQTT")...)
	varHeader = append(varHeader, 4)
	varHeader = append(varHeader, 0x02)
	varHeader = append(varHeader, 0, 60)

	cid := []byte(clientID)
	varHeader = append(varHeader, byte(len(cid)>>8), byte(len(cid)))
	varHeader = append(varHeader, cid...)

	pkt := []byte{0x10}
	pkt = append(pkt, encodeRemaining(len(varHeader))...)
	pkt = append(pkt, varHeader...)
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

func sendPublish(conn net.Conn, topic string, payload []byte) error {
	topicBytes := []byte(topic)
	varHeader := make([]byte, 2+len(topicBytes))
	binary.BigEndian.PutUint16(varHeader[:2], uint16(len(topicBytes)))
	copy(varHeader[2:], topicBytes)

	remaining := len(varHeader) + len(payload)
	pkt := []byte{0x30}
	pkt = append(pkt, encodeRemaining(remaining)...)
	pkt = append(pkt, varHeader...)
	pkt = append(pkt, payload...)
	_, err := conn.Write(pkt)
	return err
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
