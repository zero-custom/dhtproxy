package peercache

import (
	"math/rand"
	"sync"
	"time"

	simplelru "github.com/hashicorp/golang-lru/simplelru"
)

// peerEntry is one compact peer (6 bytes: IPv4 + big-endian port).
// DHT-sourced peers have left == -1 (unknown); locally announced peers
// carry the client's reported `left` value.
type peerEntry struct {
	compact  string
	lastSeen time.Time
	left     int64
}

type peerList struct {
	peers []peerEntry
}

func (p *peerList) Size() int {
	return len(p.peers)
}

type Cache struct {
	listLimit int
	ttl       time.Duration
	mu        sync.Mutex // This can't be a RWMutex because lru.Get() reorders the list.
	lru       *simplelru.LRU
	stop      chan struct{}
	stopOnce  sync.Once
}

func New(size, listLimit int) (*Cache, error) {
	return NewWithTTL(size, listLimit, 0)
}

// NewWithTTL creates a cache whose entries expire ttl after lastSeen.
// A non-positive ttl disables expiry (and the background sweeper),
// preserving the original behavior for existing callers and tests.
func NewWithTTL(size, listLimit int, ttl time.Duration) (*Cache, error) {
	lru, err := simplelru.NewLRU(size, nil)
	if err != nil {
		return nil, err
	}

	c := &Cache{
		lru:       lru,
		listLimit: listLimit,
		ttl:       ttl,
	}

	if ttl > 0 {
		c.stop = make(chan struct{})
		go c.sweepLoop(ttl / 2)
	}

	return c, nil
}

// Close stops the background sweeper. Safe to call on caches created by
// New (no sweeper running) and safe to call more than once.
func (c *Cache) Close() {
	c.stopOnce.Do(func() {
		if c.stop != nil {
			close(c.stop)
		}
	})
}

func (c *Cache) sweepLoop(interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.sweep(time.Now())
		case <-c.stop:
			return
		}
	}
}

// sweep drops expired entries; callers must not hold c.mu (it locks).
func (c *Cache) sweep(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, k := range c.lru.Keys() {
		v, ok := c.lru.Peek(k)
		if !ok {
			continue
		}
		list, ok := v.(*peerList)
		if !ok || list == nil {
			continue
		}
		kept := list.peers[:0]
		for _, e := range list.peers {
			if !c.expired(e, now) {
				kept = append(kept, e)
			}
		}
		// Clear the tail so dropped strings can be collected.
		for i := len(kept); i < len(list.peers); i++ {
			list.peers[i] = peerEntry{}
		}
		list.peers = kept
		if len(kept) == 0 {
			c.lru.Remove(k)
		}
	}
}

func (c *Cache) expired(e peerEntry, now time.Time) bool {
	return c.ttl > 0 && now.Sub(e.lastSeen) >= c.ttl
}

// Add records DHT-sourced peers (left unknown). Re-announced peers only
// refresh lastSeen; a locally recorded `left` value is preserved.
func (c *Cache) Add(ih string, peers []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	list, ok := c.get(ih)
	if !ok || list == nil {
		list = &peerList{}
	}

peers:
	for _, peer := range peers {
		for i, e := range list.peers {
			if e.compact == peer {
				list.peers[i].lastSeen = now
				continue peers
			}
		}

		// Append peers up to listLimit, then randomly replace one.
		if len(list.peers) < c.listLimit {
			list.peers = append(list.peers, peerEntry{compact: peer, lastSeen: now, left: -1})
		} else {
			list.peers[rand.Intn(len(list.peers))] = peerEntry{compact: peer, lastSeen: now, left: -1}
		}
	}

	c.lru.Add(ih, list)
}

// Upsert records one locally announced peer, refreshing lastSeen and left
// when the peer is already present.
func (c *Cache) Upsert(ih, peer string, left int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	list, ok := c.get(ih)
	if !ok || list == nil {
		list = &peerList{}
	}

	for i, e := range list.peers {
		if e.compact == peer {
			list.peers[i].lastSeen = now
			list.peers[i].left = left
			c.lru.Add(ih, list)
			return
		}
	}

	if len(list.peers) < c.listLimit {
		list.peers = append(list.peers, peerEntry{compact: peer, lastSeen: now, left: left})
	} else {
		list.peers[rand.Intn(len(list.peers))] = peerEntry{compact: peer, lastSeen: now, left: left}
	}

	c.lru.Add(ih, list)
}

// Delete removes one peer (announce `stopped`). Keys left with no peers
// are dropped so the LRU bound keeps counting live infohashes.
func (c *Cache) Delete(ih, peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	list, ok := c.get(ih)
	if !ok || list == nil {
		return
	}

	kept := list.peers[:0]
	for _, e := range list.peers {
		if e.compact != peer {
			kept = append(kept, e)
		}
	}
	for i := len(kept); i < len(list.peers); i++ {
		list.peers[i] = peerEntry{}
	}
	list.peers = kept

	if len(kept) == 0 {
		c.lru.Remove(ih)
		return
	}
	c.lru.Add(ih, list)
}

func (c *Cache) Get(ih string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	list, ok := c.get(ih)
	if !ok || list == nil {
		return nil, false
	}

	now := time.Now()
	peers := make([]string, 0, len(list.peers))
	kept := list.peers[:0]
	for _, e := range list.peers {
		if c.expired(e, now) {
			continue
		}
		kept = append(kept, e)
		peers = append(peers, e.compact)
	}
	for i := len(kept); i < len(list.peers); i++ {
		list.peers[i] = peerEntry{}
	}
	list.peers = kept

	if len(peers) == 0 {
		c.lru.Remove(ih)
		return nil, false
	}
	return peers, true
}

func (c *Cache) get(ih string) (*peerList, bool) {
	p, ok := c.lru.Get(ih)
	if !ok {
		return nil, false
	}

	list, ok := p.(*peerList)
	return list, ok
}
