package main

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	adht "github.com/anacrolix/dht/v2"
	"github.com/die-net/dhtproxy/peercache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactPeerFromNodeAddr(t *testing.T) {
	peer, ok := compactPeerFromNodeAddr(net.ParseIP("127.0.0.1"), 6881)
	if assert.True(t, ok) {
		assert.Equal(t, []byte{127, 0, 0, 1, 0x1a, 0xe1}, []byte(peer))
	}

	// IPv4-mapped IPv6 carries the same 4 bytes.
	peer, ok = compactPeerFromNodeAddr(net.ParseIP("::ffff:10.0.0.1"), 5000)
	if assert.True(t, ok) {
		assert.Equal(t, []byte{10, 0, 0, 1, 0x13, 0x88}, []byte(peer))
	}

	_, ok = compactPeerFromNodeAddr(net.ParseIP("::1"), 6881)
	assert.False(t, ok, "IPv6 has no compact form, like the local pool")

	_, ok = compactPeerFromNodeAddr(nil, 6881)
	assert.False(t, ok)

	_, ok = compactPeerFromNodeAddr(net.ParseIP("10.0.0.1"), 0)
	assert.False(t, ok)

	_, ok = compactPeerFromNodeAddr(net.ParseIP("10.0.0.1"), 65536)
	assert.False(t, ok)
}

func TestCompactPeersFromNodeAddrs(t *testing.T) {
	peers := []adht.Peer{
		{IP: net.ParseIP("10.0.0.1"), Port: 5000}, // 5000 = 0x1388
		{IP: net.ParseIP("::1"), Port: 6881},      // dropped: IPv6
		{IP: net.ParseIP("10.0.0.2"), Port: 0},    // dropped: bad port
		{IP: net.ParseIP("10.0.0.3"), Port: 5001}, // 5001 = 0x1389
	}

	got := compactPeersFromNodeAddrs(peers)
	assert.Equal(t, []string{
		string([]byte{10, 0, 0, 1, 0x13, 0x88}),
		string([]byte{10, 0, 0, 3, 0x13, 0x89}),
	}, got)

	assert.Empty(t, compactPeersFromNodeAddrs(nil))
}

// fakeBackend records fan-out without touching the network.
type fakeBackend struct {
	name       string
	requests   [][20]byte
	closeCalls int
	closeErr   error
}

func (f *fakeBackend) Name() string { return f.name }

func (f *fakeBackend) Request(ih [20]byte) { f.requests = append(f.requests, ih) }

func (f *fakeBackend) Close() error {
	f.closeCalls++
	return f.closeErr
}

func TestBothBackendFanout(t *testing.T) {
	a := &fakeBackend{name: "old"}
	b := &fakeBackend{name: "new"}
	m := &bothBackend{a: a, b: b}

	assert.Equal(t, "both", m.Name())

	var ih [20]byte
	copy(ih[:], "0123456789abcdefghij")
	m.Request(ih)

	assert.Equal(t, [][20]byte{ih}, a.requests, "old backend must see the request")
	assert.Equal(t, [][20]byte{ih}, b.requests, "new backend must see the request")

	assert.NoError(t, m.Close())
	assert.Equal(t, 1, a.closeCalls)
	assert.Equal(t, 1, b.closeCalls)
}

func TestBothBackendCloseError(t *testing.T) {
	a := &fakeBackend{name: "old", closeErr: errors.New("old boom")}
	b := &fakeBackend{name: "new"}
	m := &bothBackend{a: a, b: b}

	err := m.Close()
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "old boom")
	}
	assert.Equal(t, 1, b.closeCalls, "new backend must still close when old fails")
}

func TestNewPeerBackendUnknownMode(t *testing.T) {
	_, err := newPeerBackend("bogus", 0, 8, 0, 0, nil)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "bogus")
	}
}

// newStubBackend builds a newBackend with no server/UDP: traversals run
// through starter. The gate channel blocks started traversals until the
// test closes it; every starter call is counted.
func newStubBackend(t *testing.T, gate <-chan struct{}, calls *int64) *newBackend {
	t.Helper()
	c, err := peercache.New(16, 200)
	require.NoError(t, err)
	b := &newBackend{
		cache:          c,
		requestTimeout: time.Minute,
		active:         make(map[[20]byte]struct{}),
		sem:            make(chan struct{}, maxConcurrentNewTraversals),
	}
	b.starter = func(ih [20]byte) (<-chan adht.PeersValues, func(), error) {
		atomic.AddInt64(calls, 1)
		<-gate
		ch := make(chan adht.PeersValues)
		close(ch)
		return ch, func() {}, nil
	}
	return b
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// N concurrent Requests for one infohash with a blocked traversal must
// start exactly one traversal (singleflight); after it finishes, a new
// Request may start exactly one more.
func TestNewBackendSingleflight(t *testing.T) {
	gate := make(chan struct{})
	var calls int64
	b := newStubBackend(t, gate, &calls)

	var ih [20]byte
	copy(ih[:], "0123456789abcdefghij")

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Request(ih)
		}()
	}
	wg.Wait()

	waitFor(t, "single traversal to start", func() bool {
		return atomic.LoadInt64(&calls) == 1
	})
	// Give stragglers a chance to wrongly start more traversals.
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int64(1), atomic.LoadInt64(&calls),
		"concurrent Requests for one infohash must share one traversal")

	close(gate)
	waitFor(t, "traversal slot to release", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.active) == 0
	})

	b.Request(ih)
	waitFor(t, "second traversal after release", func() bool {
		return atomic.LoadInt64(&calls) == 2
	})
}

// M distinct infohashes with blocked starters must leave at most cap
// traversals active; the excess is skipped, not queued (no new starter
// calls appear after the gate opens).
func TestNewBackendTraversalCap(t *testing.T) {
	gate := make(chan struct{})
	var calls int64
	b := newStubBackend(t, gate, &calls)

	const m = maxConcurrentNewTraversals + 8
	for i := 0; i < m; i++ {
		var ih [20]byte
		ih[0] = byte(i >> 8)
		ih[1] = byte(i)
		copy(ih[2:], "distinct-infohash!!")
		b.Request(ih)
	}

	waitFor(t, "cap worth of traversals to start", func() bool {
		return atomic.LoadInt64(&calls) == int64(maxConcurrentNewTraversals)
	})
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int64(maxConcurrentNewTraversals), atomic.LoadInt64(&calls),
		"at most cap traversals may run concurrently")

	close(gate)
	waitFor(t, "all traversal slots to release", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.active) == 0
	})
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int64(maxConcurrentNewTraversals), atomic.LoadInt64(&calls),
		"skipped Requests must not queue follow-up traversals")
}
