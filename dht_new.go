package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
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
	// extra appends to the stats line when non-nil and non-empty
	// (e.g. routing table sizes); nil keeps the original line format.
	extra func() string
	stop  chan struct{}
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
			line := fmt.Sprintf("dht backend stats backend=%s requests=%d peers=%d",
				s.name, atomic.LoadInt64(s.requests), atomic.LoadInt64(s.peers))
			if s.extra != nil {
				if extra := s.extra(); extra != "" {
					line += " " + extra
				}
			}
			log.Print(line)
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

// compactPeer6FromNodeAddr encodes one 18-byte compact peer (IPv6 +
// big-endian port, BEP 7) from an anacrolix peer. It reports false for
// invalid ports and non-IPv6 addresses: IPv4 and IPv4-mapped IPv6 stay on
// the v4 path so families never double-count the same peer.
func compactPeer6FromNodeAddr(ip net.IP, port int) (string, bool) {
	if port <= 0 || port > 65535 {
		return "", false
	}
	if ip.To4() != nil {
		return "", false
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return "", false
	}
	b := make([]byte, 18)
	copy(b, ip16)
	b[16] = byte(port >> 8)
	b[17] = byte(port)
	return string(b), true
}

// compactPeers6FromNodeAddrs converts one get_peers batch, dropping peers
// that have no compact IPv6 form. Pure function, safe to unit test.
func compactPeers6FromNodeAddrs(peers []adht.Peer) []string {
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		if s, ok := compactPeer6FromNodeAddr(p.IP, p.Port); ok {
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

// dht6BootstrapHostPorts is the bootstrap pool the DHT6 server resolves at
// startup: the anacrolix library's DefaultGlobalBootstrapHostPorts
// (dht.go), filtered to IPv6-capable addrs below. router.silotis.us is that
// list's documented IPv6 anchor; the remaining defaults are IPv4-only and
// are dropped by the filter. Kept as a var so tests and operators can
// override it without touching the library (MPL-2.0: consume unmodified).
var dht6BootstrapHostPorts = adht.DefaultGlobalBootstrapHostPorts

// filterDHT6Addrs keeps only IPv6-capable addrs for the DHT6 server.
// Pure function, safe to unit test.
func filterDHT6Addrs(addrs []adht.Addr) []adht.Addr {
	v6 := make([]adht.Addr, 0, len(addrs))
	for _, a := range addrs {
		ip := a.IP()
		if ip == nil || ip.To4() != nil || ip.To16() == nil {
			continue
		}
		v6 = append(v6, a)
	}
	return v6
}

// resolveDHT6StartingNodes resolves hostPorts and filters to IPv6-capable
// bootstrap addrs. Zero-result is a degrade signal (skip v6), never fatal.
func resolveDHT6StartingNodes(hostPorts []string) ([]adht.Addr, error) {
	addrs, err := adht.ResolveHostPorts(hostPorts)
	if err != nil {
		return nil, err
	}
	v6 := filterDHT6Addrs(addrs)
	if len(v6) == 0 {
		return nil, fmt.Errorf("no IPv6 bootstrap addrs resolved from %d hosts", len(hostPorts))
	}
	return v6, nil
}

// startDHT6Server builds the second anacrolix Server for the DHT6 network:
// own udp6 socket (ephemeral port, one socket per port), fresh NodeId (never
// shared with the v4 server), IPv6-filtered StartingNodes resolved at
// startup. The caller owns TableMaintainer + Close wiring. It returns
// (nil, err) on v4-only hosts (no IPv6 socket) or unresolvable bootstrap;
// the caller degrades to v4-only, never failing startup or a request.
func startDHT6Server() (*adht.Server, error) {
	conn, err := net.ListenPacket("udp6", ":0")
	if err != nil {
		return nil, fmt.Errorf("listen udp6 :0: %w", err)
	}
	nodes, err := resolveDHT6StartingNodes(dht6BootstrapHostPorts)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	log.Printf("new DHT backend: DHT6 bootstrap addrs=%d %v", len(nodes), nodes)
	cfg := adht.NewDefaultServerConfig()
	cfg.Conn = conn
	cfg.NodeId = adht.RandomNodeID() // fresh identity: one Server, one network.
	cfg.StartingNodes = func() ([]adht.Addr, error) { return nodes, nil }
	srv, err := adht.NewServer(cfg)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("new v6 server: %w", err)
	}
	return srv, nil
}

// traversalStarter starts one discovery round for ih and returns its peer
// batch stream plus an idempotent close func. It is a field (not a direct
// AnnounceTraversal call) so tests can stub it without UDP or the library.
type traversalStarter func(ih [20]byte) (peers <-chan adht.PeersValues, close func(), err error)

// newBackend is the anacrolix/dht PeerBackend. Each Request starts one
// AnnounceTraversal per family (v4, plus v6 when the DHT6 server started)
// in the background and forwards every PeersValues batch into the shared
// peercache; the tracker picks results up via its cache poll, exactly like
// the old backend. Concurrent traversals are singleflight per infohash
// (duplicate Requests share the in-flight ones via the shared cache) and
// bounded by maxConcurrentNewTraversals, one budget shared across families,
// not doubled; excess Requests are skipped, never queued.
type newBackend struct {
	server         *adht.Server
	server6        *adht.Server // nil on v4-only hosts: v6 skipped, v4 byte-identical.
	cache          *peercache.Cache
	requestTimeout time.Duration
	requests       int64
	returnedPeers  int64 // covers v4+v6 together (one counter, see runTraversal).
	stats          *backendStats
	starter        traversalStarter
	starter6       traversalStarter // nil when server6 is nil; same seam as v4.
	nodesFile      string           // "" = persistence disabled, zero behavior change.
	saveStop       chan struct{}    // nil unless the 15min save sweeper runs.

	mu           sync.Mutex
	active       map[[20]byte]struct{}
	sem          chan struct{}
	lastSkipLog  time.Time
	lastV6ErrLog time.Time
}

func (b *newBackend) Name() string { return "new" }

// NewNewBackend starts an anacrolix DHT server on the given UDP port
// (0 = ephemeral) with the library's default bootstrap nodes and table
// maintenance. TableMaintainer bootstraps on its own, so no explicit
// Bootstrap call is needed. nodesFile is the v4 routing-table persist path
// ("" disables persistence); the v6 path derives a .v6 suffix. When set,
// file nodes load into the tables before either TableMaintainer starts, so
// the first bootstrap already uses them, and a sweeper re-saves every
// nodeSaveInterval.
func NewNewBackend(port int, requestTimeout time.Duration, c *peercache.Cache, nodesFile string) (*newBackend, error) {
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
		nodesFile:      nodesFile,
		active:         make(map[[20]byte]struct{}),
		sem:            make(chan struct{}, maxConcurrentNewTraversals),
	}
	b.starter = b.startRealTraversal
	b.stats = startBackendStats("new", &b.requests, &b.returnedPeers)
	b.stats.extra = b.tableSummary

	log.Printf("new DHT backend started port=%d requestTimeout=%s maxTraversals=%d",
		port, b.effectiveTimeout(), maxConcurrentNewTraversals)

	// DHT6 (Variant A: second Server): degrade to v4-only when there is no
	// IPv6 socket or no AAAA bootstrap. One line at startup, never fatal,
	// never per-request; v4 behavior stays byte-identical.
	if srv6, err := startDHT6Server(); err != nil {
		log.Print("new DHT backend: DHT6 disabled: ", err)
	} else {
		b.server6 = srv6
		b.starter6 = b.startRealTraversal6
		log.Printf("new DHT backend: DHT6 enabled addr=%s", srv6.Addr())
	}

	// Persisted routing tables load BEFORE either TableMaintainer starts so
	// the first bootstrap already uses file nodes. Empty path = disabled,
	// zero behavior change.
	if nodesFile != "" {
		n4, err := loadNodes(srv, nodesFile, false)
		if err != nil {
			log.Print("new DHT backend: load nodes v4: ", err)
		}
		n6 := 0
		if b.server6 != nil {
			var err6 error
			n6, err6 = loadNodes(b.server6, deriveV6Path(nodesFile), true)
			if err6 != nil {
				log.Print("new DHT backend: load nodes v6: ", err6)
			}
		}
		log.Printf("new DHT backend: loaded nodes v4=%d v6=%d", n4, n6)
		b.startNodeSaver()
	}

	go srv.TableMaintainer()
	if b.server6 != nil {
		go b.server6.TableMaintainer()
	}

	return b, nil
}

// tableSize renders one routing table size for the stats line; "off"
// means that family has no server (v6 degrade path). NumNodes locks
// internally, so this is safe to call from the stats goroutine.
func tableSize(srv *adht.Server) string {
	if srv == nil {
		return "off"
	}
	return strconv.Itoa(srv.NumNodes())
}

// tableSummary reports both routing tables for the per-minute stats
// line, so an empty v6 table (dead bootstrap) is distinguishable from
// missing observability. Empty only when neither server exists.
func (b *newBackend) tableSummary() string {
	if b.server == nil && b.server6 == nil {
		return ""
	}
	return fmt.Sprintf("table4=%s table6=%s",
		tableSize(b.server), tableSize(b.server6))
}

// effectiveTimeout is the per-traversal bound, defaulting to one minute.
func (b *newBackend) effectiveTimeout() time.Duration {
	if b.requestTimeout <= 0 {
		return time.Minute
	}
	return b.requestTimeout
}

// startRealTraversal runs one AnnounceTraversal against the live v4 server.
func (b *newBackend) startRealTraversal(ih [20]byte) (<-chan adht.PeersValues, func(), error) {
	a, err := b.server.AnnounceTraversal(ih)
	if err != nil {
		return nil, nil, err
	}
	return a.Peers, a.Close, nil
}

// startRealTraversal6 runs one AnnounceTraversal against the live DHT6
// server. Only wired as starter6 when server6 started; nil server6 means no
// v6 discovery at all.
func (b *newBackend) startRealTraversal6(ih [20]byte) (<-chan adht.PeersValues, func(), error) {
	a, err := b.server6.AnnounceTraversal(ih)
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

	// One sem slot and one singleflight entry cover both families: v4+v6
	// fan out under the shared budget, never doubling it.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.runTraversal(ih, b.starter, timeout, false)
	}()
	if b.starter6 != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.runTraversal(ih, b.starter6, timeout, true)
		}()
	}
	wg.Wait()
}

// runTraversal runs one discovery round and forwards compact batches into
// the shared peercache. v6 converts to 18-byte entries; v4 stays 6-byte.
// Errors and timeouts are warnings only, never fatal.
func (b *newBackend) runTraversal(ih [20]byte, starter traversalStarter, timeout time.Duration, v6 bool) {
	peersCh, closeFn, err := starter(ih)
	if err != nil {
		b.logTraversalErr(err, v6)
		return
	}
	// Bound the discovery round: closing the traversal ends the Peers
	// stream, which ends the range below. No process exit on timeout.
	timer := time.AfterFunc(timeout, closeFn)
	defer timer.Stop()
	defer closeFn()

	for pv := range peersCh {
		var compacts []string
		if v6 {
			compacts = compactPeers6FromNodeAddrs(pv.Peers)
		} else {
			compacts = compactPeersFromNodeAddrs(pv.Peers)
		}
		if len(compacts) == 0 {
			continue
		}
		atomic.AddInt64(&b.returnedPeers, int64(len(compacts))) // returnedPeers covers v4+v6 together.
		b.cache.Add(string(ih[:]), compacts)
	}
}

// logTraversalErr warns on discovery failure. v4 logs every failure (as
// before); v6 failures are throttled to one line per minute so v4-only
// hosts don't spam the log while v4 keeps serving from cache.
func (b *newBackend) logTraversalErr(err error, v6 bool) {
	if !v6 {
		log.Print("new DHT backend: announce: ", err)
		return
	}
	b.mu.Lock()
	throttled := time.Since(b.lastV6ErrLog) < time.Minute
	if !throttled {
		b.lastV6ErrLog = time.Now()
	}
	b.mu.Unlock()
	if !throttled {
		log.Print("new DHT backend (v6): announce: ", err)
	}
}

// SaveNodes persists the v4 (and v6, when present) routing tables,
// best-effort: first error returned for the caller to log, never fatal.
// No-op when persistence is disabled (empty nodesFile).
func (b *newBackend) SaveNodes() error {
	if b.nodesFile == "" {
		return nil
	}
	var firstErr error
	if b.server != nil {
		if err := saveNodesAtomic(b.server, b.nodesFile); err != nil {
			firstErr = err
		}
	}
	if b.server6 != nil {
		if err := saveNodesAtomic(b.server6, deriveV6Path(b.nodesFile)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// startNodeSaver launches the periodic routing-table persist loop in its
// own goroutine (backendStats-style ticker + stop channel, not shared with
// the stats loop). Failures only warn; the request path never blocks on it.
func (b *newBackend) startNodeSaver() {
	stop := make(chan struct{})
	b.saveStop = stop
	go b.nodeSaveLoop(stop)
}

func (b *newBackend) nodeSaveLoop(stop <-chan struct{}) {
	t := time.NewTicker(nodeSaveInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := b.SaveNodes(); err != nil {
				log.Print("new DHT backend: save nodes: ", err)
			} else {
				log.Printf("new DHT backend: saved nodes file=%s", b.nodesFile)
			}
		case <-stop:
			return
		}
	}
}

func (b *newBackend) Close() error {
	if b.stats != nil {
		b.stats.close()
		b.stats = nil
	}
	if b.saveStop != nil {
		close(b.saveStop)
		b.saveStop = nil
	}
	// NOTE: server pointers are intentionally NOT nilled. In-flight
	// lookups read b.server/b.server6 via the starter methods without
	// holding mu; nil-ing here would race with those reads (nil deref
	// panic, and -race read/write report). Server.Close is idempotent
	// (mutex + atomic closed flag), so double Close is safe, and a
	// post-Close Request degrades to a logged traversal error, never a
	// panic.
	if b.server != nil {
		b.server.Close()
	}
	if b.server6 != nil {
		b.server6.Close()
	}
	return nil
}
