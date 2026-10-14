// Command minikv-bench is a small load generator for RESP servers (minikv or Redis), in the
// spirit of redis-benchmark: N clients, optional pipelining, latency percentiles per command.
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/omergazi34/minikv/internal/resp"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "server address")
	clients := flag.Int("c", 50, "concurrent clients")
	requests := flag.Int("n", 200_000, "requests per test")
	pipeline := flag.Int("P", 1, "commands per pipelined batch")
	size := flag.Int("d", 16, "value size in bytes")
	keyspace := flag.Int("r", 100_000, "number of distinct keys")
	tests := flag.String("t", "ping,set,get,incr,lpush,hset", "comma-separated tests")
	flag.Parse()

	value := strings.Repeat("x", *size)
	fmt.Printf("minikv-bench: %d clients, %d requests, pipeline %d, %d-byte values, %d keys\n\n",
		*clients, *requests, *pipeline, *size, *keyspace)
	fmt.Printf("%-6s %12s %10s %10s %10s\n", "test", "ops/sec", "p50", "p99", "max")
	for _, name := range strings.Split(*tests, ",") {
		gen := generator(strings.TrimSpace(name), value, *keyspace)
		if gen == nil {
			fmt.Fprintf(os.Stderr, "unknown test %q\n", name)
			os.Exit(2)
		}
		res, err := run(*addr, *clients, *requests, *pipeline, gen)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Printf("%-6s %12.0f %10s %10s %10s\n", name, res.opsPerSec,
			res.p50.Round(time.Microsecond), res.p99.Round(time.Microsecond), res.max.Round(time.Microsecond))
	}
}

func generator(name, value string, keyspace int) func(*rand.Rand) []string {
	key := func(r *rand.Rand, prefix string) string { return prefix + fmt.Sprint(r.IntN(keyspace)) }
	switch name {
	case "ping":
		return func(*rand.Rand) []string { return []string{"PING"} }
	case "set":
		return func(r *rand.Rand) []string { return []string{"SET", key(r, "key:"), value} }
	case "get":
		return func(r *rand.Rand) []string { return []string{"GET", key(r, "key:")} }
	case "incr":
		return func(r *rand.Rand) []string { return []string{"INCR", key(r, "counter:")} }
	case "lpush":
		return func(r *rand.Rand) []string { return []string{"LPUSH", "list:" + fmt.Sprint(r.IntN(100)), value} }
	case "hset":
		return func(r *rand.Rand) []string {
			return []string{"HSET", "hash:" + fmt.Sprint(r.IntN(100)), key(r, "f"), value}
		}
	}
	return nil
}

type result struct {
	opsPerSec     float64
	p50, p99, max time.Duration
}

// run splits the requests across clients. Each client sends batches of `pipeline` commands
// and records the round-trip time of every batch; that RTT is the latency each command in
// the batch experienced.
func run(addr string, clients, requests, pipeline int, gen func(*rand.Rand) []string) (result, error) {
	perClient := max(requests/clients, pipeline)
	latencies := make([][]time.Duration, clients)
	errs := make(chan error, clients)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := resp.Dial(addr)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()
			r := rand.New(rand.NewPCG(uint64(i), 42))
			for sent := 0; sent < perClient; sent += pipeline {
				t0 := time.Now()
				for range pipeline {
					if err := c.Send(gen(r)...); err != nil {
						errs <- err
						return
					}
				}
				if err := c.Flush(); err != nil {
					errs <- err
					return
				}
				for range pipeline {
					v, err := c.Receive()
					if err == nil {
						err = v.Err()
					}
					if err != nil {
						errs <- err
						return
					}
				}
				latencies[i] = append(latencies[i], time.Since(t0))
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errs)
	if err := <-errs; err != nil {
		return result{}, err
	}
	all := slices.Concat(latencies...)
	slices.Sort(all)
	pct := func(p float64) time.Duration { return all[min(len(all)-1, int(p*float64(len(all))))] }
	total := perClient / pipeline * pipeline * clients
	return result{
		opsPerSec: float64(total) / elapsed.Seconds(),
		p50:       pct(0.50),
		p99:       pct(0.99),
		max:       all[len(all)-1],
	}, nil
}
