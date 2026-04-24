package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Skliar-Il/broker-message/tasks/task_3/internal/broker"
	"golang.org/x/time/rate"
)

type cliFlags struct {
	Broker    string
	URL       string
	Queue     string
	Size      int
	Rate      int
	Duration  time.Duration
	Producers int
	Consumers int
	Warmup    time.Duration
	OutCSV    string
	Label     string
}

func parseFlags() cliFlags {
	var f cliFlags
	flag.StringVar(&f.Broker, "broker", "rabbitmq", "broker: rabbitmq|redis")
	flag.StringVar(&f.URL, "url", "", "connection url (default by broker)")
	flag.StringVar(&f.Queue, "queue", "bench", "queue/stream name")
	flag.IntVar(&f.Size, "size", 128, "message payload size in bytes")
	flag.IntVar(&f.Rate, "rate", 1000, "target msgs/sec per producer group (0 = unbounded)")
	flag.DurationVar(&f.Duration, "duration", 20*time.Second, "test duration")
	flag.IntVar(&f.Producers, "producers", 1, "number of producer goroutines")
	flag.IntVar(&f.Consumers, "consumers", 1, "number of consumer goroutines")
	flag.DurationVar(&f.Warmup, "warmup", 2*time.Second, "drain window after producers stop")
	flag.StringVar(&f.OutCSV, "out", "", "append one result line to this CSV file")
	flag.StringVar(&f.Label, "label", "", "extra label for the result row")
	flag.Parse()

	if f.URL == "" {
		switch f.Broker {
		case "rabbitmq":
			f.URL = "amqp://guest:guest@localhost:5672/"
		case "redis":
			f.URL = "redis://localhost:6379/0"
		}
	}
	return f
}

func main() {
	f := parseFlags()
	if f.Size < 8 {
		log.Fatalf("size must be >= 8 (first 8 bytes carry timestamp)")
	}

	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	client, err := newClient(rootCtx, f)
	if err != nil {
		log.Fatalf("broker init: %v", err)
	}
	defer client.Close()

	if err := client.Reset(rootCtx); err != nil {
		log.Fatalf("reset queue: %v", err)
	}

	log.Printf("bench start broker=%s size=%dB rate=%d/s duration=%s producers=%d consumers=%d",
		f.Broker, f.Size, f.Rate, f.Duration, f.Producers, f.Consumers)

	var (
		sent      atomic.Int64
		recv      atomic.Int64
		sendErr   atomic.Int64
		recvErr   atomic.Int64
		latencies = make(chan time.Duration, 1<<16)
	)

	consumerCtx, stopConsumer := context.WithCancel(rootCtx)
	defer stopConsumer()

	var cwg sync.WaitGroup
	for i := 0; i < f.Consumers; i++ {
		cwg.Add(1)
		go func(id int) {
			defer cwg.Done()
			err := client.Consume(consumerCtx, func(payload []byte) {
				recv.Add(1)
				if len(payload) >= 8 {
					tsNs := int64(binary.BigEndian.Uint64(payload[:8]))
					if tsNs > 0 {
						lat := time.Since(time.Unix(0, tsNs))
						if lat > 0 {
							select {
							case latencies <- lat:
							default:
							}
						}
					}
				}
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				recvErr.Add(1)
				log.Printf("consumer %d: %v", id, err)
			}
		}(i)
	}

	producerDeadline := time.Now().Add(f.Duration)
	var pwg sync.WaitGroup
	for i := 0; i < f.Producers; i++ {
		pwg.Add(1)
		go func(id int) {
			defer pwg.Done()
			var limiter *rate.Limiter
			if f.Rate > 0 {
				per := f.Rate / f.Producers
				if per < 1 {
					per = 1
				}
				limiter = rate.NewLimiter(rate.Limit(per), per/2+1)
			}
			buf := make([]byte, f.Size)
			rand.New(rand.NewSource(time.Now().UnixNano() + int64(id))).Read(buf[8:])
			for time.Now().Before(producerDeadline) {
				if rootCtx.Err() != nil {
					return
				}
				if limiter != nil {
					if err := limiter.Wait(rootCtx); err != nil {
						return
					}
				}
				binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UnixNano()))
				if err := client.Publish(rootCtx, buf); err != nil {
					sendErr.Add(1)
					continue
				}
				sent.Add(1)
			}
		}(i)
	}

	tickerDone := make(chan struct{})
	go func() {
		defer close(tickerDone)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var lastSent, lastRecv int64
		for {
			select {
			case <-tickerDone:
				return
			case <-rootCtx.Done():
				return
			case <-t.C:
				s, r := sent.Load(), recv.Load()
				log.Printf("progress sent=%d (+%d/s) recv=%d (+%d/s) sendErr=%d recvErr=%d",
					s, s-lastSent, r, r-lastRecv, sendErr.Load(), recvErr.Load())
				lastSent, lastRecv = s, r
				if time.Now().After(producerDeadline.Add(f.Warmup)) {
					return
				}
			}
		}
	}()

	pwg.Wait()
	log.Printf("producers done, draining for %s...", f.Warmup)

	drainCtx, drainCancel := context.WithTimeout(context.Background(), f.Warmup)
	<-drainCtx.Done()
	drainCancel()

	stopConsumer()
	cwg.Wait()
	close(latencies)

	lats := make([]time.Duration, 0, len(latencies))
	for l := range latencies {
		lats = append(lats, l)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		idx := int(float64(len(lats)-1) * p)
		return lats[idx]
	}
	var avg time.Duration
	if len(lats) > 0 {
		var sum time.Duration
		for _, l := range lats {
			sum += l
		}
		avg = sum / time.Duration(len(lats))
	}

	totalSent := sent.Load()
	totalRecv := recv.Load()
	lost := totalSent - totalRecv
	if lost < 0 {
		lost = 0
	}
	throughput := float64(totalRecv) / f.Duration.Seconds()

	fmt.Println("----- RESULT -----")
	fmt.Printf("broker:       %s\n", f.Broker)
	fmt.Printf("size:         %d B\n", f.Size)
	fmt.Printf("target rate:  %d msg/s\n", f.Rate)
	fmt.Printf("duration:     %s\n", f.Duration)
	fmt.Printf("producers:    %d\n", f.Producers)
	fmt.Printf("consumers:    %d\n", f.Consumers)
	fmt.Printf("sent:         %d\n", totalSent)
	fmt.Printf("received:     %d\n", totalRecv)
	fmt.Printf("lost:         %d (%.2f%%)\n", lost, percent(lost, totalSent))
	fmt.Printf("send errors:  %d\n", sendErr.Load())
	fmt.Printf("recv errors:  %d\n", recvErr.Load())
	fmt.Printf("throughput:   %.0f msg/s (recv/duration)\n", throughput)
	fmt.Printf("latency avg:  %s\n", avg)
	fmt.Printf("latency p50:  %s\n", pct(0.50))
	fmt.Printf("latency p95:  %s\n", pct(0.95))
	fmt.Printf("latency p99:  %s\n", pct(0.99))
	fmt.Printf("latency max:  %s\n", pct(1.0))
	fmt.Println("------------------")

	if f.OutCSV != "" {
		writeCSV(f, totalSent, totalRecv, lost, sendErr.Load(), recvErr.Load(), throughput, avg, pct(0.50), pct(0.95), pct(0.99), pct(1.0))
	}
}

func percent(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return 100.0 * float64(part) / float64(total)
}

func writeCSV(f cliFlags, sent, recv, lost, sendErr, recvErr int64, thr float64, avg, p50, p95, p99, pmax time.Duration) {
	needHeader := true
	if st, err := os.Stat(f.OutCSV); err == nil && st.Size() > 0 {
		needHeader = false
	}
	fh, err := os.OpenFile(f.OutCSV, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("csv open: %v", err)
		return
	}
	defer fh.Close()
	if needHeader {
		fmt.Fprintln(fh, "label,broker,size_bytes,target_rate,duration_s,producers,consumers,sent,received,lost,send_err,recv_err,throughput_msgs,avg_ms,p50_ms,p95_ms,p99_ms,max_ms")
	}
	fmt.Fprintf(fh, "%s,%s,%d,%d,%.0f,%d,%d,%d,%d,%d,%d,%d,%.0f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
		f.Label, f.Broker, f.Size, f.Rate, f.Duration.Seconds(), f.Producers, f.Consumers,
		sent, recv, lost, sendErr, recvErr, thr,
		ms(avg), ms(p50), ms(p95), ms(p99), ms(pmax),
	)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

func newClient(ctx context.Context, f cliFlags) (broker.Client, error) {
	switch f.Broker {
	case "rabbitmq":
		return broker.NewRabbit(ctx, f.URL, f.Queue)
	case "redis":
		return broker.NewRedis(ctx, f.URL, f.Queue)
	default:
		return nil, fmt.Errorf("unknown broker %q", f.Broker)
	}
}
