package main

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/die-net/dhtproxy/peercache"

	"github.com/nictuku/dht"
)

func init() {
	// Current list of bootstrap nodes from:
	// https://git.deluge-torrent.org/deluge/tree/deluge/core/preferencesmanager.py#n274
	dht.DefaultConfig.DHTRouters = "router.bittorrent.com:6881,router.utorrent.com:6881,router.bitcomet.com:6881,dht.transmissionbt.com:6881,dht.aelitis.com:6881"

	// Don't rate-limit (by silently dropping) packets by default.
	// Assume we want the info.
	dht.DefaultConfig.RateLimit = -1

	dht.RegisterFlags(nil)
}

// PeerBackend is the seam between the tracker frontend and DHT discovery.
// Implementations trigger asynchronous lookups and drain compact peers
// (6 bytes: IPv4 + big-endian port) into the shared peercache; results are
// picked up by the tracker's cache poll. trackerHandler must only use this
// interface, never a concrete backend or DHT library type.
type PeerBackend interface {
	// Name identifies the backend ("old", "new") for metrics and switches.
	Name() string
	// Request triggers one discovery round for ih. It must not block the
	// caller and must never terminate the process on timeouts.
	Request(ih [20]byte)
	// Close stops background work.
	Close() error
}

// oldBackend is the nictuku/dht implementation, kept 1:1 apart from the
// fatal watchdogs (downgraded to warnings) so a replacement backend can
// take over behind the PeerBackend interface.
type oldBackend struct {
	port           int
	numTargetPeers int
	requestTimeout time.Duration
	node           *dht.DHT
	c              *peercache.Cache
	resetter       *time.Ticker
	// requests counts Request calls; returnedPeers counts compact peers
	// forwarded into the shared cache. Read with atomic.LoadInt64; the
	// per-minute stats line reports both (see backendStats).
	requests      int64
	returnedPeers int64
	stats         *backendStats
}

func (d *oldBackend) Name() string { return "old" }

// DhtNode is kept as an alias so existing references keep compiling during
// the migration; new code should use PeerBackend.
type DhtNode = oldBackend

func NewOldBackend(port, numTargetPeers int, resetInterval, requestTimeout time.Duration, c *peercache.Cache) (*oldBackend, error) {
	d := &oldBackend{
		port:           port,
		numTargetPeers: numTargetPeers,
		requestTimeout: requestTimeout,
		c:              c,
	}

	if err := d.Reset(); err != nil {
		return nil, err
	}

	d.stats = startBackendStats("old", &d.requests, &d.returnedPeers)

	if resetInterval > 0 {
		d.resetter = time.NewTicker(resetInterval)
		go d.doResets()
	}

	return d, nil
}

// NewDhtNode is kept as an alias during the migration; new code should
// call NewOldBackend.
func NewDhtNode(port, numTargetPeers int, resetInterval time.Duration, c *peercache.Cache) (*oldBackend, error) {
	return NewOldBackend(port, numTargetPeers, resetInterval, time.Minute, c)
}

func (d *oldBackend) Reset() error {
	d.stop()

	conf := dht.NewConfig()
	conf.Port = d.port
	conf.NumTargetPeers = d.numTargetPeers

	node, err := dht.New(conf)
	if err != nil {
		return err
	}

	d.node = node

	go func() { _ = d.node.Run() }()

	go d.drainResults(d.c)

	return nil
}

func (d *oldBackend) doResets() {
	for range d.resetter.C {
		if err := d.Reset(); err != nil {
			log.Print("DHT reset failed (keeping old node): ", err)
		}
	}
}

func (d *oldBackend) drainResults(c *peercache.Cache) {
	for r := range d.node.PeersRequestResults {
		for ih, peers := range r {
			atomic.AddInt64(&d.returnedPeers, int64(len(peers)))
			c.Add(string(ih), peers)
		}
	}
}

func (d *oldBackend) Request(ih [20]byte) {
	atomic.AddInt64(&d.requests, 1)
	d.Find(dht.InfoHash(string(ih[:])))
}

func (d *oldBackend) Find(ih dht.InfoHash) {
	// TODO: This is still racy vs Reset()
	if d.node != nil {
		timeout := d.requestTimeout
		if timeout <= 0 {
			timeout = time.Minute
		}
		timer := time.AfterFunc(timeout, func() {
			log.Print("d.node.PeersRequest() took longer than ", timeout, ".")
		})
		defer timer.Stop()

		d.node.PeersRequest(string(ih), false)
	}
}

func (d *oldBackend) Close() error {
	d.Stop()
	return nil
}

func (d *oldBackend) Stop() {
	if d.resetter != nil {
		d.resetter.Stop()
		d.resetter = nil
	}
	if d.stats != nil {
		d.stats.close()
		d.stats = nil
	}
	d.stop()
}

func (d *oldBackend) stop() {
	if d.node != nil {
		timeout := d.requestTimeout
		if timeout <= 0 {
			timeout = time.Minute
		}
		timer := time.AfterFunc(timeout, func() {
			log.Print("d.node.Stop() took longer than ", timeout, ".")
		})
		defer timer.Stop()

		d.node.Stop()
		d.node = nil
	}
}
