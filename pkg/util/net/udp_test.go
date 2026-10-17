package net

import (
	"net"
	"runtime"
	"testing"
	"time"
)

// TestUDPListener_NoBufferLeak verifies that UDPListener returns its read
// buffers to the pool.
//
// The read loop allocates one 1450-byte buffer per incoming packet via
// pool.GetBuf(1450). Before the fix the buffer was never returned to the pool:
// FakeUDPConn.Read consumed packets but never called pool.PutBuf, and dropped
// packets (a full channel) were discarded. Because the test below does not
// consume packets fast enough to empty the 20-slot channel, the read loop is
// forced to keep ~20 buffers alive and allocates a fresh one for every packet
// it reads. Each 1450-byte buffer that is not reused therefore shows up as a
// new heap allocation (~1.4MB for 1000 packets).
//
// After the fix, Read returns each consumed buffer to the pool (defer
// pool.PutBuf), so steady-state packets reuse the same buffers and the number
// of new 1450-byte allocations collapses toward zero.
//
// We measure with runtime.MemStats.TotalAlloc, which only ever grows, so the
// delta is stable and independent of garbage collection.
func TestUDPListener_NoBufferLeak(t *testing.T) {
	const (
		numPackets = 1000
		port       = 15000
	)

	l, err := ListenUDP("127.0.0.1", port)
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer l.Close()
	serverAddr := l.Addr()

	// Use a *fixed* client port (DialUDP binds one local port) so that every
	// packet is attributed to the same remote address and therefore to a single
	// FakeUDPConn. This keeps the read loop's per-packet bookkeeping (conn
	// creation, map lookups) out of the measured steady state.
	clientConn, err := net.DialUDP("udp", nil, serverAddr.(*net.UDPAddr))
	if err != nil {
		t.Fatalf("client DialUDP: %v", err)
	}
	defer clientConn.Close()

	// Warmup: let the read loop spin up and let the first pool.GetBuf miss
	// (make) happen before we start measuring.
	for i := range 4 {
		if _, err := clientConn.Write([]byte("warmup")); err != nil {
			t.Fatalf("warmup send %d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Consume connections from Accept in the background. acceptCh is
	// unbuffered, so the read loop blocks here between packets; the consumer
	// goroutine does NOT read from the accepted conns, so the 20-slot packets
	// channel fills up. This mirrors a real frps that accepts connections but
	// reads from them slowly: it is exactly the situation in which the read
	// loop's buffers must be returned to the pool to avoid leaking.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := l.Accept(); err != nil {
				return
			}
		}
	}()

	sendBurst := func(n int) {
		for range n {
			if _, err := clientConn.Write([]byte("x")); err != nil {
				t.Fatalf("send: %v", err)
			}
		}
	}

	// Let the machine settle before measuring.
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	// Measured phase: send a burst of 1000 packets. The read loop will
	// allocate a 1450-byte buffer for each (after the first reuses a pooled
	// buffer if and only if one was returned).
	sendBurst(numPackets)

	// Give the read loop time to read all the packets (each read is gated on
	// the unbuffered acceptCh, which the consumer drains).
	time.Sleep(300 * time.Millisecond)

	runtime.GC()
	runtime.ReadMemStats(&after)

	allocDelta := after.TotalAlloc - before.TotalAlloc
	t.Logf("TotalAlloc delta for %d packets: %d bytes (≈ %.2f bytes/packet)",
		numPackets, allocDelta, float64(allocDelta)/float64(numPackets))

	// Before the fix the delta is dominated by ~numPackets * 1450 bytes of
	// leaked buffers (> ~1.4MB). After the fix the buffers are reused, so the
	// delta should be a small fraction of that. Threshold: at most ~20% of the
	// full-leak size.
	leakThreshold := uint64(numPackets) * 1450 / 5
	if allocDelta > leakThreshold {
		t.Errorf("buffer leak detected: TotalAlloc grew by %d bytes for %d packets (threshold %d). "+
			"Expected buffers to be reused when returned to the pool.",
			allocDelta, numPackets, leakThreshold)
	}

	// Stop the accept consumer and let it observe the closed listener.
	l.Close()
	<-done
}
