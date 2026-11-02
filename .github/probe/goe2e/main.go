package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	nfs "github.com/norma-core/normfs/normfs_go"
	"github.com/norma-core/normfs/normfs_go/uintn"
)

func payload(i, n int) []byte {
	b := make([]byte, n)
	for j := range b {
		b[j] = byte(i*31 + j*7)
	}
	return b
}

func fail(f string, a ...any) { fmt.Printf("FAIL "+f+"\n", a...); os.Exit(1) }

// Station puts large records only in queues its video rule matches.
var prefix = func() string {
	if p := os.Getenv("E2E_PREFIX"); p != "" {
		return p
	}
	return "e2e/"
}()

var sizes = []int{1, 17, 1024, 4000, 65535, 65536, 65537, 200 * 1024, 1 << 20, 3 << 20}

func packed() [][]byte {
	pack := make([][]byte, 50)
	for i := range pack {
		pack[i] = payload(100+i, 10+i*100)
	}
	return pack
}

// verify reads back, after a restart, what a full run left in e2e/main.
func verify(c nfs.Client) {
	var want [][]byte
	for i, n := range sizes {
		want = append(want, payload(i, n))
	}
	want = append(want, packed()...)
	want = append(want, []byte("after idle"))
	r := c.ReadFromTail(prefix + "main", uintn.FromU64(uint64(len(want))), uint64(len(want)), 1, 16)
	got := 0
	for e := range r.Data {
		if e.Err != nil {
			fail("verify entry: %v", e.Err)
		}
		if got >= len(want) || !bytes.Equal(e.Data, want[got]) {
			fail("verify %d: %d bytes", got, len(e.Data))
		}
		got++
	}
	if r.Err != nil || got != len(want) {
		fail("verify: err=%v got=%d want=%d", r.Err, got, len(want))
	}
	fmt.Printf("after restart: %d entries read back intact: OK\n", got)
}

func main() {
	addr := os.Args[1]
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	c, err := nfs.NewSingleConnClient(addr, log)
	if err != nil {
		fail("connect: %v", err)
	}
	if len(os.Args) > 2 && os.Args[2] == "verify" {
		verify(c)
		return
	}
	q := prefix + "main"
	var want [][]byte
	var ids []uintn.UintN
	for i, n := range sizes {
		p := payload(i, n)
		id, err := c.Enqueue(q, p)
		if err != nil {
			fail("enqueue %d: %v", n, err)
		}
		want = append(want, p)
		ids = append(ids, id)
	}
	pack := packed()
	pids, err := c.EnqueuePack(q, pack)
	if err != nil || len(pids) != len(pack) {
		fail("pack: %v %d", err, len(pids))
	}
	want = append(want, pack...)
	ids = append(ids, pids...)

	r := c.ReadFromOffset(q, ids[0], uint64(len(want)), 1, 16)
	got := 0
	for e := range r.Data {
		if e.Err != nil {
			fail("read entry: %v", e.Err)
		}
		if !bytes.Equal(e.Data, want[got]) {
			fail("read %d: %d bytes, want %d", got, len(e.Data), len(want[got]))
		}
		got++
	}
	if r.Err != nil || got != len(want) {
		fail("read from offset: err=%v got=%d want=%d", r.Err, got, len(want))
	}
	r = c.ReadFromTail(q, uintn.FromU8(5), 5, 1, 0)
	got = 0
	var tl []int
	for e := range r.Data {
		tl = append(tl, len(e.Data))
		got++
	}
	fmt.Printf("tail 5 lengths: %v\n", tl)
	if r.Err != nil || got != 5 {
		fail("read from tail: err=%v got=%d", r.Err, got)
	}
	r = c.ReadFromOffset(prefix + "missing", ids[0], 1, 1, 0)
	for range r.Data {
	}
	fmt.Printf("missing queue read err: %v\n", r.Err)

	fq := prefix + "follow"
	if _, err := c.Enqueue(fq, []byte("seed")); err != nil {
		fail("seed: %v", err)
	}
	ch := make(chan nfs.StreamEntry, 64)
	c.Follow(fq, ch)
	time.Sleep(300 * time.Millisecond)
	var fwant [][]byte
	for i := 0; i < 20; i++ {
		p := payload(500+i, 100+i*3000)
		fwant = append(fwant, p)
		if _, err := c.Enqueue(fq, p); err != nil {
			fail("follow enqueue: %v", err)
		}
	}
	seen := 0
	deadline := time.After(10 * time.Second)
	for seen < len(fwant) {
		select {
		case e := <-ch:
			if bytes.Equal(e.Data, []byte("seed")) {
				continue
			}
			if !bytes.Equal(e.Data, fwant[seen]) {
				fail("follow %d mismatch: %d bytes", seen, len(e.Data))
			}
			seen++
		case <-deadline:
			fail("follow: got %d of %d", seen, len(fwant))
		}
	}
	fmt.Printf("single client: %d enqueued+read back, tail 5, follow %d: OK\n", len(want), seen)

	// Idle past several keepalive intervals, then use the connection again.
	time.Sleep(5 * time.Second)
	if _, err := c.Enqueue(q, []byte("after idle")); err != nil {
		fail("after idle: %v", err)
	}

	const N, M = 200, 200
	var errs, ok atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for k := 0; k < N; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			cc, err := nfs.NewSingleConnClient(addr, log)
			if err != nil {
				errs.Add(M)
				return
			}
			qq := fmt.Sprintf(prefix+"load/%d", k%20)
			for i := 0; i < M; i++ {
				if _, err := cc.Enqueue(qq, payload(k, 64+i)); err != nil {
					errs.Add(1)
				} else {
					ok.Add(1)
				}
			}
		}(k)
	}
	wg.Wait()
	fmt.Printf("load: %d clients x %d enqueues: ok=%d errors=%d in %.1fs\n", N, M, ok.Load(), errs.Load(), time.Since(start).Seconds())
	if errs.Load() != 0 {
		os.Exit(1)
	}
}
