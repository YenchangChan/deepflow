// go-baseline is a throughput harness mirroring examples/baseline.rs but using
// elastic/go-lumber for both server and client. Same payload, same batch size,
// same measurement methodology — directly comparable to the Rust harness.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	cliv2 "github.com/elastic/go-lumber/client/v2"
	srvv2 "github.com/elastic/go-lumber/server/v2"
)

func main() {
	clients := flag.Int("clients", 1, "number of concurrent clients")
	durationS := flag.Int("duration", 10, "measurement duration seconds")
	eventSize := flag.Int("event-size", 250, "approximate JSON event size in bytes")
	batchSize := flag.Int("batch", 128, "events per send call")
	flag.Parse()

	fmt.Fprintf(os.Stderr, "baseline: clients=%d duration=%ds event-size=%d batch=%d\n",
		*clients, *durationS, *eventSize, *batchSize)

	// Build one batch of synthetic events; reuse it across all sends.
	event := makeEvent(*eventSize)
	serializedSize := approxJSONSize(event)
	fmt.Fprintf(os.Stderr, "event JSON size: %d bytes\n", serializedSize)
	batch := make([]interface{}, *batchSize)
	for i := range batch {
		batch[i] = event
	}

	// ----- Server -----
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	addr := listener.Addr().String()

	srv, err := srvv2.NewWithListener(listener)
	must(err)

	// Drain and ack everything in the background.
	go func() {
		for batch := range srv.ReceiveChan() {
			batch.ACK()
		}
	}()

	// ----- Spawn clients -----
	var (
		stop         atomic.Bool
		totalEvents  atomic.Uint64
		totalBatches atomic.Uint64
		wg           sync.WaitGroup
	)

	for i := 0; i < *clients; i++ {
		conn, err := net.Dial("tcp", addr)
		must(err)
		// Disable Nagle to match the Rust harness setup.
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		base, err := cliv2.NewWithConn(conn)
		must(err)
		cl, err := cliv2.NewSyncClientWith(base)
		must(err)

		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if _, err := cl.Send(batch); err != nil {
					fmt.Fprintf(os.Stderr, "client error: %v\n", err)
					return
				}
				totalEvents.Add(uint64(len(batch)))
				totalBatches.Add(1)
			}
			_ = cl.Close()
		}()
	}

	// ----- Steady-state measurement window -----
	time.Sleep(500 * time.Millisecond) // warmup

	cpuStart := cpuTime()
	eventsStart := totalEvents.Load()
	wallStart := time.Now()

	time.Sleep(time.Duration(*durationS) * time.Second)

	wall := time.Since(wallStart)
	cpu := cpuTime() - cpuStart
	events := totalEvents.Load() - eventsStart
	batches := totalBatches.Load()

	stop.Store(true)
	wg.Wait()
	_ = srv.Close()

	// ----- Report -----
	secs := wall.Seconds()
	eps := float64(events) / secs
	mbps := float64(events) * float64(serializedSize) / secs / 1048576.0
	cpuPct := cpu.Seconds() / secs * 100.0
	rssMiB := float64(peakRSSKiB()) / 1024.0

	fmt.Println("---")
	fmt.Printf("clients         : %d\n", *clients)
	fmt.Printf("duration_s      : %.3f\n", secs)
	fmt.Printf("event_bytes     : %d\n", serializedSize)
	fmt.Printf("batch_size      : %d\n", *batchSize)
	fmt.Printf("total_events    : %d\n", events)
	fmt.Printf("total_batches   : %d\n", batches)
	fmt.Printf("events_per_sec  : %.0f\n", eps)
	fmt.Printf("payload_MiB_per_s: %.2f\n", mbps)
	fmt.Printf("cpu_total_pct   : %.1f\n", cpuPct)
	fmt.Printf("peak_rss_MiB    : %.1f\n", rssMiB)
}

func makeEvent(targetSize int) map[string]interface{} {
	skeleton := `{"timestamp":"2026-04-09T08:30:00Z","level":"info","host":"server-01","msg":""}`
	pad := targetSize - len(skeleton)
	if pad < 0 {
		pad = 0
	}
	return map[string]interface{}{
		"timestamp": "2026-04-09T08:30:00Z",
		"level":     "info",
		"host":      "server-01",
		"msg":       strings.Repeat("x", pad),
	}
}

func approxJSONSize(ev map[string]interface{}) int {
	// Mirrors what go-lumber's encoder will produce. The skeleton + padded msg.
	return len(`{"host":"server-01","level":"info","msg":""}`) +
		len(`,"timestamp":"2026-04-09T08:30:00Z"`) +
		len(ev["msg"].(string))
}

func cpuTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	user := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	sys := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return user + sys
}

func peakRSSKiB() uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				var v uint64
				_, _ = fmt.Sscanf(fields[1], "%d", &v)
				return v
			}
		}
	}
	return 0
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
