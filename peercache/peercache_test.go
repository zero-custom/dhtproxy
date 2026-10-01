package peercache

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var entries = []struct {
	key   string
	peers []string
}{
	{"1", []string{"one"}},
	{"2", []string{"two"}},
	{"3", []string{"three"}},
	{"4", []string{"four"}},
	{"5", []string{"five", "six", "seven", "eight"}},
}

func TestCache(t *testing.T) {
	c, err := New(10, 4)
	assert.NoError(t, err)

	for _, e := range entries {
		c.Add(e.key, e.peers)
	}

	_, ok := c.Get("missing")
	assert.False(t, ok)

	for _, e := range entries {
		peers, ok := c.Get(e.key)
		if assert.True(t, ok) {
			assert.Equal(t, e.peers, peers)
		}
	}
}

func TestSize(t *testing.T) {
	c, err := New(3, 2)
	assert.NoError(t, err)

	for _, e := range entries {
		c.Add(e.key, e.peers)
	}

	count := 0
	for _, e := range entries {
		if _, ok := c.Get(e.key); ok {
			count++
		}
	}
	assert.Equal(t, 3, count, "Should only find 3 entries")
}

func TestLimit(t *testing.T) {
	c, err := New(10, 4)
	assert.NoError(t, err)

	c.Add("1", []string{"one", "two", "three", "four", "five", "six"})
	peers, ok := c.Get("1")
	assert.Equal(t, 4, len(peers), "len(peers) should be 4.")
	assert.True(t, ok)

	for _, p := range []string{"one", "two", "three", "four", "five", "six"} {
		c.Add("2", []string{p})
	}
	peers, ok = c.Get("2")
	assert.Equal(t, 4, len(peers), "len(peers) should be 4.")
	assert.True(t, ok)
}

func TestRace(t *testing.T) {
	c, err := New(100000, 4)
	assert.NoError(t, err)

	wg := sync.WaitGroup{}
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			testRaceWorker(c)
			wg.Done()
		}()
	}
	wg.Wait()
}

func testRaceWorker(c *Cache) {
	peers := []string{"asdf"}

	for n := 0; n < 1000; n++ {
		_, _ = c.Get(randKey(100))
		c.Add(randKey(100), peers)
	}
}

func randKey(n int32) string {
	return strconv.Itoa(int(rand.Int31n(n)))
}

func TestUpsertDelete(t *testing.T) {
	c, err := New(10, 4)
	assert.NoError(t, err)
	defer c.Close()

	c.Upsert("ih", "peer1", 100)
	peers, ok := c.Get("ih")
	if assert.True(t, ok) {
		assert.Equal(t, []string{"peer1"}, peers)
	}

	c.Upsert("ih", "peer2", 0)
	peers, ok = c.Get("ih")
	if assert.True(t, ok) {
		assert.Equal(t, []string{"peer1", "peer2"}, peers)
	}

	// Refreshing an existing peer keeps a single entry.
	c.Upsert("ih", "peer1", 0)
	peers, ok = c.Get("ih")
	if assert.True(t, ok) {
		assert.Equal(t, []string{"peer1", "peer2"}, peers)
	}

	c.Delete("ih", "peer1")
	peers, ok = c.Get("ih")
	if assert.True(t, ok) {
		assert.Equal(t, []string{"peer2"}, peers)
	}

	// Deleting the last peer drops the key.
	c.Delete("ih", "peer2")
	_, ok = c.Get("ih")
	assert.False(t, ok)

	// Deleting a missing key is a no-op.
	c.Delete("ih", "peer2")
	c.Delete("missing", "peer2")
}

func TestTTLExpiry(t *testing.T) {
	c, err := NewWithTTL(10, 4, 60*time.Millisecond)
	assert.NoError(t, err)
	defer c.Close()

	c.Upsert("ih", "peer1", 0)
	_, ok := c.Get("ih")
	assert.True(t, ok)

	time.Sleep(150 * time.Millisecond)
	_, ok = c.Get("ih")
	assert.False(t, ok)
}

func TestAddExpiry(t *testing.T) {
	c, err := NewWithTTL(10, 4, 60*time.Millisecond)
	assert.NoError(t, err)
	defer c.Close()

	c.Add("ih", []string{"peer1"})
	_, ok := c.Get("ih")
	assert.True(t, ok)

	time.Sleep(150 * time.Millisecond)
	_, ok = c.Get("ih")
	assert.False(t, ok)
}

func TestUpsertRefresh(t *testing.T) {

	c, err := NewWithTTL(10, 4, 200*time.Millisecond)
	assert.NoError(t, err)
	defer c.Close()

	c.Upsert("ih", "peer1", 5)
	time.Sleep(100 * time.Millisecond)
	c.Upsert("ih", "peer1", 0)
	time.Sleep(100 * time.Millisecond)

	// Age since refresh is ~100ms < 200ms TTL: still alive.
	_, ok := c.Get("ih")
	assert.True(t, ok)

	time.Sleep(250 * time.Millisecond)
	_, ok = c.Get("ih")
	assert.False(t, ok)
}

func TestListLimit200(t *testing.T) {
	c, err := New(10, 200)
	assert.NoError(t, err)
	defer c.Close()

	for i := 0; i < 201; i++ {
		c.Upsert("ih", fmt.Sprintf("peer-%03d", i), 0)
	}
	peers, ok := c.Get("ih")
	if assert.True(t, ok) {
		assert.Len(t, peers, 200)
	}
}
