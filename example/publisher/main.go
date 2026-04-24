package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	brokerAddr     = "localhost:1883"
	topicName      = "hello"
	ackTimeout     = 5 * time.Second
	retransmitTick = time.Second
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type pendingMsg struct {
	payload []byte
	sentAt  time.Time
	dup     bool
}

func main() {
	qos := byte(0)
	if envOr("QOS", "0") == "1" {
		qos = 1
	}
	log.Printf("publisher starting with QoS=%d", qos)

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

	if qos == 0 {
		for i := 1; ; i++ {
			payload := fmt.Sprintf("hello world %d", i)
			if err := sendPublish(conn, topicName, []byte(payload), 0, 0, false); err != nil {
				log.Fatalf("PUBLISH: %v", err)
			}
			log.Printf("published (qos=0): %s", payload)
			time.Sleep(time.Second)
		}
	}

	var (
		mu      sync.Mutex
		pending        = make(map[uint16]*pendingMsg)
		nextPid uint16 = 1
	)

	go func() {
		for {
			pktType, body, err := readPacket(conn)
			if err != nil {
				log.Printf("read packet error: %v", err)
				return
			}
			if pktType == 4 && len(body) >= 2 {
				pid := binary.BigEndian.Uint16(body[0:2])
				mu.Lock()
				if _, ok := pending[pid]; ok {
					delete(pending, pid)
					log.Printf("PUBACK received pid=%d", pid)
				}
				mu.Unlock()
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(retransmitTick)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			mu.Lock()
			for pid, msg := range pending {
				if now.Sub(msg.sentAt) >= ackTimeout {
					pid := pid
					msg := msg
					if err := sendPublish(conn, topicName, msg.payload, 1, pid, true); err != nil {
						log.Printf("retransmit write error pid=%d: %v", pid, err)
						continue
					}
					msg.sentAt = time.Now()
					msg.dup = true
					log.Printf("retransmitted pid=%d DUP=1 payload=%s", pid, msg.payload)
				}
			}
			mu.Unlock()
		}
	}()

	for i := 1; ; i++ {
		payload := []byte(fmt.Sprintf("hello world %d", i))
		mu.Lock()
		pid := nextPid
		nextPid++
		if nextPid == 0 {
			nextPid = 1
		}
		pending[pid] = &pendingMsg{payload: payload, sentAt: time.Now()}
		mu.Unlock()

		if err := sendPublish(conn, topicName, payload, 1, pid, false); err != nil {
			log.Fatalf("PUBLISH: %v", err)
		}
		log.Printf("published (qos=1) pid=%d: %s", pid, payload)
		time.Sleep(time.Second)
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

func sendPublish(conn net.Conn, topic string, payload []byte, qos byte, pid uint16, dup bool) error {
	topicBytes := []byte(topic)
	var varHeader []byte
	varHeader = append(varHeader, byte(len(topicBytes)>>8), byte(len(topicBytes)))
	varHeader = append(varHeader, topicBytes...)
	if qos > 0 {
		varHeader = append(varHeader, byte(pid>>8), byte(pid))
	}

	flags := (qos & 0x03) << 1
	if dup {
		flags |= 0x08
	}

	pkt := []byte{(0x03 << 4) | flags}
	pkt = append(pkt, encodeRemaining(len(varHeader)+len(payload))...)
	pkt = append(pkt, varHeader...)
	pkt = append(pkt, payload...)
	_, err := conn.Write(pkt)
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
