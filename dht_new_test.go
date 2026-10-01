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

func TestCompactPeer6FromNodeAddr(t *testing.T) {
	peer, ok := compactPeer6FromNodeAddr(net.ParseIP("2001:db8::1"), 6881)
	if assert.True(t, ok) {
		want := append([]byte(net.ParseIP("2001:db8::1").To16()), 0x1a, 0xe1)
		assert.Equal(t, want, []byte(peer))
		assert.Len(t, peer, 18)
	}

	_, ok = compactPeer6FromNodeAddr(net.ParseIP("10.0.0.1"), 6881)
	assert.False(t, ok, "IPv4 stays on the v4 path")

	_, ok = compactPeer6FromNodeAddr(net.ParseIP("::ffff:10.0.0.1"), 6881)
	assert.False(t, ok, "IPv4-mapped IPv6 stays on the v4 path, never double-counted")

	_, ok = compactPeer6FromNodeAddr(nil, 6881)
	assert.False(t, ok)

	_, ok = compactPeer6FromNodeAddr(net.ParseIP("2001:db8::1"), 0)
	assert.False(t, ok)

	_, ok = compactPeer6FromNodeAddr(net.ParseIP("2001:db8::1"), 65536)
	assert.False(t, ok)
}

func TestCompactPeers6FromNodeAddrs(t *testing.T) {
	peers := []adht.Peer{
		{IP: net.ParseIP("2001:db8::1"), Port: 6881},
		{IP: net.ParseIP("10.0.0.1"), Port: 5000},        // dropped: IPv4
		{IP: net.ParseIP("2001:db8::2"), Port: 0},        // dropped: bad port
		{IP: net.ParseIP("::ffff:10.0.0.3"), Port: 5001}, // dropped: mapped
		{IP: net.ParseIP("2001:db8::3"), Port: 5001},     // 5001 = 0x1389
	}

	got := compactPeers6FromNodeAddrs(peers)
	require.Len(t, got, 2)
	for _, s := range got {
		assert.Len(t, s, 18)
	}
	want3 := append([]byte(net.ParseIP("2001:db8::3").To16()), 0x13, 0x89)
	assert.Equal(t, string(want3), got[1])

	assert.Empty(t, compactPeers6FromNodeAddrs(nil))
}

func TestFilterDHT6Addrs(t *testing.T) {
	addrs := []adht.Addr{
		adht.NewAddr(&net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}),
		adht.NewAddr(&net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 6881}),
		adht.NewAddr(&net.UDPAddr{IP: net.ParseIP("::ffff:10.0.0.2"), Port: 6881}),
	}

	got := filterDHT6Addrs(addrs)
	require.Len(t, got, 1)
	assert.True(t, got[0].IP().Equal(net.ParseIP("2001:db8::1")))

	assert.Empty(t, filterDHT6Addrs(nil))
	assert.Empty(t, filterDHT6Addrs(addrs[:1]), "v4-only input keeps nothing")
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

// batchStarter returns a traversalStarter that emits one batch then ends,
// with no network. Calls are counted when calls is non-nil.
func batchStarter(batch []adht.Peer, calls *int64) traversalStarter {
	return func(ih [20]byte) (<-chan adht.PeersValues, func(), error) {
		if calls != nil {
			atomic.AddInt64(calls, 1)
		}
		ch := make(chan adht.PeersValues, 1)
		ch <- adht.PeersValues{Peers: batch}
		close(ch)
		return ch, func() {}, nil
	}
}

// gateStarter returns a traversalStarter that blocks until gate closes,
// then ends with no batches. Every call is counted.
func gateStarter(gate <-chan struct{}, calls *int64) traversalStarter {
	return func(ih [20]byte) (<-chan adht.PeersValues, func(), error) {
		atomic.AddInt64(calls, 1)
		<-gate
		ch := make(chan adht.PeersValues)
		close(ch)
		return ch, func() {}, nil
	}
}

// newStubBackendV6 builds a newBackend with no server/UDP and both families
// stubbed: v4 emits v4batch, v6 emits v6batch.
func newStubBackendV6(t *testing.T, v4batch, v6batch []adht.Peer) *newBackend {
	t.Helper()
	c, err := peercache.New(16, 200)
	require.NoError(t, err)
	b := &newBackend{
		cache:          c,
		requestTimeout: time.Minute,
		active:         make(map[[20]byte]struct{}),
		sem:            make(chan struct{}, maxConcurrentNewTraversals),
	}
	b.starter = batchStarter(v4batch, nil)
	b.starter6 = batchStarter(v6batch, nil)
	return b
}

// One Request fans out to v4+v6 under the shared budget: the 6-byte v4
// entry and the 18-byte v6 entry both land in the shared cache, and the one
// returnedPeers counter covers both families.
func TestNewBackendV6Fanout(t *testing.T) {
	v4batch := []adht.Peer{{IP: net.ParseIP("10.0.0.1"), Port: 5000}} // 5000 = 0x1388
	v6batch := []adht.Peer{{IP: net.ParseIP("2001:db8::1"), Port: 6881}}
	b := newStubBackendV6(t, v4batch, v6batch)

	var ih [20]byte
	copy(ih[:], "0123456789abcdefghij")
	b.Request(ih)

	waitFor(t, "v4+v6 peers in cache", func() bool {
		peers, ok := b.cache.Get(string(ih[:]))
		return ok && len(peers) == 2
	})
	peers, ok := b.cache.Get(string(ih[:]))
	require.True(t, ok, "both families share the one peercache")
	require.Len(t, peers, 2)
	var saw4, saw6 bool
	for _, p := range peers {
		switch len(p) {
		case 6:
			saw4 = true
			assert.Equal(t, []byte{10, 0, 0, 1, 0x13, 0x88}, []byte(p))
		case 18:
			saw6 = true
			assert.Equal(t, append([]byte(net.ParseIP("2001:db8::1").To16()), 0x1a, 0xe1), []byte(p))
		default:
			t.Fatalf("unexpected compact length %d", len(p))
		}
	}
	assert.True(t, saw4 && saw6, "v4 and v6 batches must both land")
	assert.Equal(t, int64(2), atomic.LoadInt64(&b.returnedPeers),
		"returnedPeers covers v4+v6 together")
}

// N concurrent Requests for one infohash with blocked traversals must start
// exactly one v4 and one v6 traversal (singleflight covers both families).
func TestNewBackendSingleflightCoversV6(t *testing.T) {
	gate := make(chan struct{})
	var calls4, calls6 int64
	c, err := peercache.New(16, 200)
	require.NoError(t, err)
	b := &newBackend{
		cache:          c,
		requestTimeout: time.Minute,
		active:         make(map[[20]byte]struct{}),
		sem:            make(chan struct{}, maxConcurrentNewTraversals),
	}
	b.starter = gateStarter(gate, &calls4)
	b.starter6 = gateStarter(gate, &calls6)

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

	waitFor(t, "one v4+v6 traversal pair to start", func() bool {
		return atomic.LoadInt64(&calls4) == 1 && atomic.LoadInt64(&calls6) == 1
	})
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int64(1), atomic.LoadInt64(&calls4))
	assert.Equal(t, int64(1), atomic.LoadInt64(&calls6),
		"singleflight must dedupe across families for the same ih")

	close(gate)
	waitFor(t, "traversal slot to release", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.active) == 0
	})
}

// M distinct infohashes with blocked starters must admit at most cap
// Requests; each admitted Request holds ONE sem slot for BOTH families, so
// the cap is shared, not doubled — and the excess starts nothing.
func TestNewBackendCapShared(t *testing.T) {
	gate := make(chan struct{})
	var calls4, calls6 int64
	c, err := peercache.New(16, 200)
	require.NoError(t, err)
	b := &newBackend{
		cache:          c,
		requestTimeout: time.Minute,
		active:         make(map[[20]byte]struct{}),
		sem:            make(chan struct{}, maxConcurrentNewTraversals),
	}
	b.starter = gateStarter(gate, &calls4)
	b.starter6 = gateStarter(gate, &calls6)

	const m = maxConcurrentNewTraversals + 8
	for i := 0; i < m; i++ {
		var ih [20]byte
		ih[0] = byte(i >> 8)
		ih[1] = byte(i)
		copy(ih[2:], "distinct-infohash!!")
		b.Request(ih)
	}

	waitFor(t, "cap worth of v4 traversals to start", func() bool {
		return atomic.LoadInt64(&calls4) == int64(maxConcurrentNewTraversals)
	})
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int64(maxConcurrentNewTraversals), atomic.LoadInt64(&calls4))
	assert.Equal(t, int64(maxConcurrentNewTraversals), atomic.LoadInt64(&calls6),
		"each admitted Request runs one v4 + one v6 traversal on ONE shared slot")

	close(gate)
	waitFor(t, "all traversal slots to release", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.active) == 0
	})
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int64(maxConcurrentNewTraversals), atomic.LoadInt64(&calls4))
	assert.Equal(t, int64(maxConcurrentNewTraversals), atomic.LoadInt64(&calls6),
		"skipped Requests must not queue follow-up traversals in either family")
}

// A failing v6 traversal degrades: v4 results still land, the request is
// still served from cache, nothing panics or exits.
func TestNewBackendV6ErrorDegrades(t *testing.T) {
	v4batch := []adht.Peer{{IP: net.ParseIP("10.0.0.9"), Port: 5000}}
	c, err := peercache.New(16, 200)
	require.NoError(t, err)
	b := &newBackend{
		cache:          c,
		requestTimeout: time.Minute,
		active:         make(map[[20]byte]struct{}),
		sem:            make(chan struct{}, maxConcurrentNewTraversals),
	}
	b.starter = batchStarter(v4batch, nil)
	b.starter6 = func(ih [20]byte) (<-chan adht.PeersValues, func(), error) {
		return nil, nil, errors.New("no v6 route")
	}

	var ih [20]byte
	copy(ih[:], "0123456789abcdefghij")
	b.Request(ih)

	waitFor(t, "v4 peers in cache despite v6 failure", func() bool {
		peers, ok := b.cache.Get(string(ih[:]))
		return ok && len(peers) == 1
	})
	peers, ok := b.cache.Get(string(ih[:]))
	require.True(t, ok)
	require.Len(t, peers, 1)
	assert.Len(t, peers[0], 6)
}

// Close on a stub (nil servers) must not panic; in prod it tears down both
// the v4 and the v6 servers.
func TestNewBackendCloseV6(t *testing.T) {
	b := newStubBackendV6(t, nil, nil)
	assert.NoError(t, b.Close())
	assert.Nil(t, b.server)
	assert.Nil(t, b.server6)
}
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

func TestTableSummaryNoServers(t *testing.T) {
	assert.Equal(t, "", (&newBackend{}).tableSummary(),
		"no servers means nothing to report (stats line keeps old format)")
}

// newRealServer builds a live anacrolix Server on loopback for table-size
// assertions. TableMaintainer is deliberately NOT started: no network
// traffic, just a bound socket and an empty table.
func newRealServer(t *testing.T, network, addr string) *adht.Server {
	t.Helper()
	conn, err := net.ListenPacket(network, addr)
	if err != nil {
		t.Skipf("loopback %s unavailable: %v", network, err)
	}
	cfg := adht.NewDefaultServerConfig()
	cfg.Conn = conn
	srv, err := adht.NewServer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { srv.Close() })
	return srv
}

func TestTableSummaryRealServers(t *testing.T) {
	b := &newBackend{
		server:  newRealServer(t, "udp", "127.0.0.1:0"),
		server6: newRealServer(t, "udp6", "[::1]:0"),
	}
	assert.Equal(t, "table4=0 table6=0", b.tableSummary())
}

func TestTableSummaryV6Off(t *testing.T) {
	b := &newBackend{server: newRealServer(t, "udp", "127.0.0.1:0")}
	assert.Equal(t, "table4=0 table6=off", b.tableSummary())
}
