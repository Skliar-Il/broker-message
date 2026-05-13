package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/cache"
	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/db"
	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/metrics"
	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/store"
	"golang.org/x/time/rate"
)

type cliFlags struct {
	Strategy  string
	RedisURL  string
	DBPath    string
	Keys      int
	ValueSize int
	ReadRatio float64
	Rate      int
	Duration  time.Duration
	Workers   int
	WBInterval time.Duration
	WBBatch    int
	CacheTTL   time.Duration
	OutCSV    string
	Label     string
	Mix       string
}

func parseFlags() cliFlags {
	var f cliFlags
	flag.StringVar(&f.Strategy, "strategy", "cache_aside", "cache strategy: cache_aside|write_through|write_back")
	flag.StringVar(&f.RedisURL, "redis-url", "redis://localhost:6379/0", "Redis connection URL")
	flag.StringVar(&f.DBPath, "db-path", "bench.db", "SQLite database file path")
	flag.IntVar(&f.Keys, "keys", 10000, "number of distinct keys in the dataset")
	flag.IntVar(&f.ValueSize, "value-size", 256, "size of each value in bytes")
	flag.Float64Var(&f.ReadRatio, "read-ratio", 0.8, "fraction of operations that are reads (0..1)")
	flag.IntVar(&f.Rate, "rate", 5000, "target requests/sec (0 = unlimited)")
	flag.DurationVar(&f.Duration, "duration", 20*time.Second, "benchmark duration")
	flag.IntVar(&f.Workers, "workers", 4, "number of concurrent worker goroutines")
	flag.DurationVar(&f.WBInterval, "wb-interval", 200*time.Millisecond, "Write-Back flush interval")
	flag.IntVar(&f.WBBatch, "wb-batch", 512, "Write-Back batch flush threshold (dirty key count)")
	flag.DurationVar(&f.CacheTTL, "cache-ttl", 60*time.Second, "Redis key TTL")
	flag.StringVar(&f.OutCSV, "out", "", "append result row to this CSV file")
	flag.StringVar(&f.Label, "label", "", "extra label for the result row")
	flag.StringVar(&f.Mix, "mix", "custom", "mix name (read_heavy|balanced|write_heavy)")
	flag.Parse()
	return f
}

func main() {
	f := parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := &metrics.M{}

	// Build cache and DB wrappers.
	redisCache, err := cache.NewRedis(ctx, f.RedisURL, m, f.CacheTTL)
	if err != nil {
		log.Fatalf("redis init: %v", err)
	}

	sqlite, err := db.NewSQLite(f.DBPath, m)
	if err != nil {
		log.Fatalf("sqlite init: %v", err)
	}

	// Reset state so every run starts clean.
	if err := redisCache.FlushAll(ctx); err != nil {
		log.Fatalf("redis flush: %v", err)
	}
	if err := sqlite.Reset(ctx); err != nil {
		log.Fatalf("sqlite reset: %v", err)
	}

	// Build the store implementation.
	var st store.Store
	switch f.Strategy {
	case "cache_aside":
		st = store.NewCacheAside(redisCache, sqlite)
	case "write_through":
		st = store.NewWriteThrough(redisCache, sqlite)
	case "write_back":
		st = store.NewWriteBack(redisCache, sqlite, m, store.WriteBackConfig{
			FlushInterval: f.WBInterval,
			BatchSize:     f.WBBatch,
		})
	default:
		log.Fatalf("unknown strategy %q", f.Strategy)
	}
	defer st.Close()

	// Prefill: insert all keys into both DB and cache so the dataset is identical
	// for every strategy at the start of the benchmark.
	log.Printf("prefilling %d keys (strategy=%s)...", f.Keys, f.Strategy)
	value := randString(f.ValueSize)
	keys := make([]string, f.Keys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%08d", i)
	}
	prefillCtx := ctx
	for _, k := range keys {
		if err := st.Set(prefillCtx, k, value); err != nil {
			log.Fatalf("prefill set: %v", err)
		}
	}
	// For write_back, flush prefill to DB immediately so every strategy
	// has the same data in the DB before the benchmark starts.
	if err := st.Flush(ctx); err != nil {
		log.Fatalf("prefill flush: %v", err)
	}
	// Reset metrics after prefill so they only reflect the benchmark.
	m.Reset()
	log.Printf("prefill done, starting benchmark...")

	// ---- Benchmark ----
	var (
		totalOps  atomic.Int64
		totalReads  atomic.Int64
		totalWrites atomic.Int64
		latencies = make(chan time.Duration, 1<<17)
	)

	deadline := time.Now().Add(f.Duration)
	var wg sync.WaitGroup

	for w := 0; w < f.Workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			val := randString(f.ValueSize)

			var limiter *rate.Limiter
			if f.Rate > 0 {
				perWorker := f.Rate / f.Workers
				if perWorker < 1 {
					perWorker = 1
				}
				limiter = rate.NewLimiter(rate.Limit(perWorker), perWorker/2+1)
			}

			for time.Now().Before(deadline) {
				if limiter != nil {
					if err := limiter.Wait(ctx); err != nil {
						return
					}
				}

				key := keys[rng.Intn(len(keys))]
				isRead := rng.Float64() < f.ReadRatio

				start := time.Now()
				if isRead {
					_, _ = st.Get(ctx, key)
					totalReads.Add(1)
				} else {
					_ = st.Set(ctx, key, val)
					totalWrites.Add(1)
				}
				lat := time.Since(start)
				totalOps.Add(1)
				select {
				case latencies <- lat:
				default:
				}
			}
		}(w)
	}

	// Progress ticker.
	tickerDone := make(chan struct{})
	go func() {
		defer close(tickerDone)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var lastOps int64
		for {
			select {
			case <-t.C:
				ops := totalOps.Load()
				rps := ops - lastOps
				lastOps = ops
				hits := m.CacheHits.Load()
				misses := m.CacheMisses.Load()
				hitRate := float64(0)
				if hits+misses > 0 {
					hitRate = float64(hits) / float64(hits+misses) * 100
				}
				dbTotal := m.DBGets.Load() + m.DBSets.Load()
				log.Printf("progress ops=%d rps=%d hit_rate=%.1f%% db_calls=%d",
					ops, rps, hitRate, dbTotal)
				if time.Now().After(deadline) {
					return
				}
			}
		}
	}()

	wg.Wait()
	<-tickerDone

	// Record dirty queue size before final flush (Write-Back insight).
	if wb, ok := st.(*store.WriteBack); ok {
		m.WBFinalDirty.Store(wb.DirtyLen())
	}

	// Final flush for Write-Back.
	if err := st.Flush(ctx); err != nil {
		log.Printf("final flush: %v", err)
	}

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
		return lats[int(float64(len(lats)-1)*p)]
	}
	var avg time.Duration
	if len(lats) > 0 {
		var sum time.Duration
		for _, l := range lats {
			sum += l
		}
		avg = sum / time.Duration(len(lats))
	}

	ops := totalOps.Load()
	reads := totalReads.Load()
	writes := totalWrites.Load()
	throughput := float64(ops) / f.Duration.Seconds()
	hits := m.CacheHits.Load()
	misses := m.CacheMisses.Load()
	hitRatePct := float64(0)
	if hits+misses > 0 {
		hitRatePct = float64(hits) / float64(hits+misses) * 100
	}
	dbGets := m.DBGets.Load()
	dbSets := m.DBSets.Load()

	fmt.Println("----- RESULT -----")
	fmt.Printf("strategy:      %s\n", f.Strategy)
	fmt.Printf("mix:           %s (read_ratio=%.2f)\n", f.Mix, f.ReadRatio)
	fmt.Printf("duration:      %s\n", f.Duration)
	fmt.Printf("target rate:   %d req/s\n", f.Rate)
	fmt.Printf("workers:       %d\n", f.Workers)
	fmt.Printf("keys:          %d\n", f.Keys)
	fmt.Printf("value size:    %d B\n", f.ValueSize)
	fmt.Printf("total ops:     %d\n", ops)
	fmt.Printf("reads:         %d\n", reads)
	fmt.Printf("writes:        %d\n", writes)
	fmt.Printf("throughput:    %.0f req/s\n", throughput)
	fmt.Printf("latency avg:   %s\n", avg)
	fmt.Printf("latency p50:   %s\n", pct(0.50))
	fmt.Printf("latency p95:   %s\n", pct(0.95))
	fmt.Printf("latency p99:   %s\n", pct(0.99))
	fmt.Printf("latency max:   %s\n", pct(1.0))
	fmt.Printf("cache hits:    %d\n", hits)
	fmt.Printf("cache misses:  %d\n", misses)
	fmt.Printf("hit rate:      %.2f%%\n", hitRatePct)
	fmt.Printf("db gets:       %d\n", dbGets)
	fmt.Printf("db sets:       %d\n", dbSets)
	fmt.Printf("db total:      %d\n", dbGets+dbSets)
	fmt.Printf("wb max dirty:  %d\n", m.WBMaxDirty.Load())
	fmt.Printf("wb final dirty:%d\n", m.WBFinalDirty.Load())
	fmt.Println("------------------")

	if f.OutCSV != "" {
		writeCSV(f, ops, reads, writes, throughput, avg, pct(0.50), pct(0.95), pct(0.99), pct(1.0),
			hits, misses, hitRatePct, dbGets, dbSets, m.WBMaxDirty.Load(), m.WBFinalDirty.Load())
	}
}

func randString(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

func writeCSV(f cliFlags, ops, reads, writes int64, thr float64,
	avg, p50, p95, p99, pmax time.Duration,
	hits, misses int64, hitRate float64,
	dbGets, dbSets, wbMax, wbFinal int64) {

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
		fmt.Fprintln(fh, "label,strategy,mix,read_ratio,duration_s,target_rate,workers,keys,value_size,"+
			"requests,reads,writes,throughput_rps,"+
			"avg_ms,p50_ms,p95_ms,p99_ms,max_ms,"+
			"cache_hits,cache_misses,hit_rate,"+
			"db_gets,db_sets,db_total,"+
			"wb_max_dirty,wb_final_dirty")
	}
	fmt.Fprintf(fh, "%s,%s,%s,%.2f,%.0f,%d,%d,%d,%d,"+
		"%d,%d,%d,%.0f,"+
		"%.3f,%.3f,%.3f,%.3f,%.3f,"+
		"%d,%d,%.4f,"+
		"%d,%d,%d,"+
		"%d,%d\n",
		f.Label, f.Strategy, f.Mix, f.ReadRatio, f.Duration.Seconds(), f.Rate, f.Workers, f.Keys, f.ValueSize,
		ops, reads, writes, thr,
		ms(avg), ms(p50), ms(p95), ms(p99), ms(pmax),
		hits, misses, hitRate/100.0,
		dbGets, dbSets, dbGets+dbSets,
		wbMax, wbFinal,
	)
}
