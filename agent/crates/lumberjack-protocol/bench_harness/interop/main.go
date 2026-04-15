// interop is a small bridge binary used by the Rust integration tests in
// tests/interop.rs to talk to elastic/go-lumber as both a server and a client.
//
// Two modes:
//
//   --mode server --expect N
//     Listens on 127.0.0.1:0, prints "PORT=<n>" on stdout, waits until exactly
//     N events have been received across any number of batches, then writes a
//     single line "EVENTS=<json-array>" with the events as received.
//
//   --mode client --addr host:port --count N
//     Dials the address and sends N events of the form {"i": <index>} through
//     a synchronous v2 client, then exits 0.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"

	cliv2 "github.com/elastic/go-lumber/client/v2"
	srvv2 "github.com/elastic/go-lumber/server/v2"
)

func main() {
	mode := flag.String("mode", "", "server | client")
	expect := flag.Int("expect", 0, "server: number of events to receive before reporting")
	addr := flag.String("addr", "", "client: target address")
	count := flag.Int("count", 0, "client: number of events to send")
	flag.Parse()

	switch *mode {
	case "server":
		runServer(*expect)
	case "client":
		runClient(*addr, *count)
	default:
		fmt.Fprintln(os.Stderr, "usage: interop --mode server|client ...")
		os.Exit(2)
	}
}

func runServer(expect int) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		die(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	fmt.Printf("PORT=%d\n", port)
	os.Stdout.Sync()

	srv, err := srvv2.NewWithListener(listener)
	if err != nil {
		die(err)
	}

	collected := make([]interface{}, 0, expect)
	for len(collected) < expect {
		batch, ok := <-srv.ReceiveChan()
		if !ok {
			die(fmt.Errorf("server channel closed before receiving %d events", expect))
		}
		collected = append(collected, batch.Events...)
		batch.ACK()
	}

	out, err := json.Marshal(collected)
	if err != nil {
		die(err)
	}
	fmt.Printf("EVENTS=%s\n", out)
	os.Stdout.Sync()
	_ = srv.Close()
}

func runClient(addr string, count int) {
	if addr == "" || count <= 0 {
		die(fmt.Errorf("client mode requires --addr and --count > 0"))
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		die(err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	base, err := cliv2.NewWithConn(conn)
	if err != nil {
		die(err)
	}
	cl, err := cliv2.NewSyncClientWith(base)
	if err != nil {
		die(err)
	}

	events := make([]interface{}, count)
	for i := 0; i < count; i++ {
		events[i] = map[string]interface{}{"i": i, "src": "go-lumber"}
	}
	if _, err := cl.Send(events); err != nil {
		die(err)
	}
	_ = cl.Close()
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
