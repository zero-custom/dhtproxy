package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	adht "github.com/anacrolix/dht/v2"
	"github.com/die-net/dhtproxy/peercache"
)

// backendStats emits one cumulative log line per minute per backend
// (requests seen, peers forwarded into the shared cache) so old/new can
// be compared in dual-run mode without extra dependencies.
type backendStats struct {
	name     string
	requests *int64
	peers    *int64
	stop     chan struct{}
}

func startBackendStats(name string, requests, peers *int64) *backendStats {
	s := &backendStats{name: name, requests: requests, peers: peers, stop: make(chan struct{})}
	go s.loop()
	return s
}

func (s *backendStats) loop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			log.Printf("dht backend stats backend=%s requests=%d peers=%d",
				s.name, atomic.LoadInt64(s.requests), atomic.LoadInt64(s.peers))
		case <-s.stop:
			return
		}
	}
}

func (s *backendStats) close() {
	close(s.stop)
}

// compactPeerFromNodeAddr encodes one 6-byte compact peer (IPv4 + big-endian
// port) from an anacrolix peer. It reports false for invalid ports and
// non-IPv4 addresses, mirroring the local pool's IPv4-only rule so tracker
// merge semantics stay unchanged.
func compactPeerFromNodeAddr(ip net.IP, port int) (string, bool) {
	if port <= 0 || port > 65535 {
		return "", false
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return "", false
	}
	b := make([]byte, 6)
	copy(b, ip4)
	b[4] = byte(port >> 8)
	b[5] = byte(port)
	return string(b), true
}

// compactPeersFromNodeAddrs converts one get_peers batch, dropping peers
// that have no compact IPv4 form. Pure function, safe to unit test.
func compactPeersFromNodeAddrs(peers []adht.Peer) []string {
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		if s, ok := compactPeerFromNodeAddr(p.IP, p.Port); ok {
			out = append(out, s)
		}
	}
	return out
}

// maxConcurrentNewTraversals bounds distinct-infohash AnnounceTraversal
// operations in flight on the new backend. Shared-infohash load fans out to
// thousands of Requests; without a cap each one held a minute-long
// traversal (goroutines + UDP + timers) until the process starved. Excess
// Requests while full are skipped: results land in the shared cache where
// the tracker's poll picks them up for all waiters.
const maxConcurrentNewTraversals = 32

// traversalStarter starts one discovery round for ih and returns its peer
// batch stream plus an idempotent close func. It is a field (not a direct
// AnnounceTraversal call) so tests can stub it without UDP or the library.
type traversalStarter func(ih [20]byte) (peers <-chan adht.PeersValues, close func(), err error)

// newBackend is the anacrolix/dht PeerBackend. Each Request starts one
// AnnounceTraversal in the background and forwards every PeersValues batch
// into the shared peercache; the tracker picks results up via its cache
// poll, exactly like the old backend. Concurrent traversals are singleflight
// per infohash (duplicate Requests share the in-flight one via the shared
// cache) and bounded by maxConcurrentNewTraversals; excess Requests are
// skipped, never queued.
type newBackend struct {
	server         *adht.Server
	cache          *peercache.Cache
	requestTimeout time.Duration
	requests       int64
	returnedPeers  int64
	stats          *backendStats
	starter        traversalStarter

	mu          sync.Mutex
	active      map[[20]byte]struct{}
	sem         chan struct{}
	lastSkipLog time.Time
}

func (b *newBackend) Name() string { return "new" }

// NewNewBackend starts an anacrolix DHT server on the given UDP port
// (0 = ephemeral) with the library's default bootstrap nodes and table
// maintenance. TableMaintainer bootstraps on its own, so no explicit
// Bootstrap call is needed.
func NewNewBackend(port int, requestTimeout time.Duration, c *peercache.Cache) (*newBackend, error) {
	conn, err := net.ListenPacket("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("new DHT backend: listen udp :%d: %w", port, err)
	}

	cfg := adht.NewDefaultServerConfig()
	cfg.Conn = conn

	srv, err := adht.NewServer(cfg)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("new DHT backend: new server: %w", err)
	}

	b := &newBackend{
		server:         srv,
		cache:          c,
		requestTimeout: requestTimeout,
		active:         make(map[[20]byte]struct{}),
		sem:            make(chan struct{}, maxConcurrentNewTraversals),
	}
	b.starter = b.startRealTraversal
	b.stats = startBackendStats("new", &b.requests, &b.returnedPeers)

	log.Printf("new DHT backend started port=%d requestTimeout=%s maxTraversals=%d",
		port, b.effectiveTimeout(), maxConcurrentNewTraversals)

	go srv.TableMaintainer()

	return b, nil
}

// effectiveTimeout is the per-traversal bound, defaulting to one minute.
func (b *newBackend) effectiveTimeout() time.Duration {
	if b.requestTimeout <= 0 {
		return time.Minute
	}
	return b.requestTimeout
}

// startRealTraversal runs one AnnounceTraversal against the live server.
func (b *newBackend) startRealTraversal(ih [20]byte) (<-chan adht.PeersValues, func(), error) {
	a, err := b.server.AnnounceTraversal(ih)
	if err != nil {
		return nil, nil, err
	}
	return a.Peers, a.Close, nil
}

func (b *newBackend) Request(ih [20]byte) {
	atomic.AddInt64(&b.requests, 1)

	b.mu.Lock()
	// Singleflight: a traversal for ih is already running; its results
	// land in the shared cache where the tracker's poll picks them up for
	// every waiter, so starting another one only burns goroutines/UDP.
	if _, ok := b.active[ih]; ok {
		b.mu.Unlock()
		return
	}
	// Bound distinct-infohash traversals; skip (never queue, never log
	// per-request) while full.
	select {
	case b.sem <- struct{}{}:
	default:
		if time.Since(b.lastSkipLog) >= time.Minute {
			b.lastSkipLog = time.Now()
			log.Printf("new DHT backend: traversal cap reached (%d), skipping requests",
				maxConcurrentNewTraversals)
		}
		b.mu.Unlock()
		return
	}
	b.active[ih] = struct{}{}
	b.mu.Unlock()

	go b.lookup(ih, b.effectiveTimeout())
}

func (b *newBackend) lookup(ih [20]byte, timeout time.Duration) {
	defer func() {
		b.mu.Lock()
		delete(b.active, ih)
		b.mu.Unlock()
		<-b.sem
	}()

	peersCh, closeFn, err := b.starter(ih)
	if err != nil {
		log.Print("new DHT backend: announce: ", err)
		return
	}
	// Bound the discovery round: closing the traversal ends the Peers
	// stream, which ends the range below. No process exit on timeout.
	timer := time.AfterFunc(timeout, closeFn)
	defer timer.Stop()
	defer closeFn()

	for pv := range peersCh {
		compacts := compactPeersFromNodeAddrs(pv.Peers)
		if len(compacts) == 0 {
			continue
		}
		atomic.AddInt64(&b.returnedPeers, int64(len(compacts)))
		b.cache.Add(string(ih[:]), compacts)
	}
}

func (b *newBackend) Close() error {
	if b.stats != nil {
		b.stats.close()
		b.stats = nil
	}
	b.server.Close()
	return nil
}

// bothBackend fans one Request out to two backends. Both write into the
// same shared peercache (dedupe inside Add), so merge semantics are
// unchanged; per-backend stats lines tell old/new apart.
type bothBackend struct {
	a, b PeerBackend
}

func (m *bothBackend) Name() string { return "both" }

func (m *bothBackend) Request(ih [20]byte) {
	m.a.Request(ih)
	m.b.Request(ih)
}

func (m *bothBackend) Close() error {
	errA := m.a.Close()
	errB := m.b.Close()
	switch {
	case errA != nil && errB != nil:
		return fmt.Errorf("close both backends: old: %v, new: %v", errA, errB)
	case errA != nil:
		return fmt.Errorf("close old backend: %w", errA)
	case errB != nil:
		return fmt.Errorf("close new backend: %w", errB)
	default:
		return nil
	}
}

// newPeerBackend builds the backend selected by --dhtBackend. In both mode
// the old backend keeps the configured UDP port and the new backend takes
// an ephemeral one, since only one socket can own a port.
func newPeerBackend(mode string, port, numTargetPeers int, resetInterval, requestTimeout time.Duration, c *peercache.Cache) (PeerBackend, error) {
	switch mode {
	case "old":
		return NewOldBackend(port, numTargetPeers, resetInterval, requestTimeout, c)
	case "new":
		return NewNewBackend(port, requestTimeout, c)
	case "both":
		old, err := NewOldBackend(port, numTargetPeers, resetInterval, requestTimeout, c)
		if err != nil {
			return nil, err
		}
		nb, err := NewNewBackend(0, requestTimeout, c)
		if err != nil {
			_ = old.Close()
			return nil, err
		}
		return &bothBackend{a: old, b: nb}, nil
	default:
		return nil, fmt.Errorf("unknown --dhtBackend %q: want old|new|both", mode)
	}
}
