package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/die-net/dhtproxy/peercache"
	"github.com/stretchr/testify/assert"
)

func TestTruncatePeers(t *testing.T) {
	peers := []string{"a", "b", "c", "d"}

	assert.Equal(t, []string{"a", "b"}, truncatePeers(peers, 2, 200))
	assert.Equal(t, peers, truncatePeers(peers, 0, 200))
	assert.Equal(t, peers, truncatePeers(peers, -1, 200))
	assert.Equal(t, peers, truncatePeers(peers, 10, 200))
	assert.Equal(t, []string{"a", "b", "c"}, truncatePeers(peers, 0, 3))
	assert.Equal(t, []string{"a", "b"}, truncatePeers(peers, 2, 3))
	assert.Equal(t, peers, truncatePeers(peers, 0, 0))
	assert.Empty(t, truncatePeers(nil, 2, 200))
}

func TestCompactPeerFromAddr(t *testing.T) {
	peer, ok := compactPeerFromAddr("127.0.0.1:1234", 6881)
	if assert.True(t, ok) {
		assert.Equal(t, []byte{127, 0, 0, 1, 0x1a, 0xe1}, []byte(peer))
	}

	_, ok = compactPeerFromAddr("[::1]:1234", 6881)
	assert.False(t, ok)

	_, ok = compactPeerFromAddr("127.0.0.1:1234", 0)
	assert.False(t, ok)

	_, ok = compactPeerFromAddr("127.0.0.1:1234", 65536)
	assert.False(t, ok)

	_, ok = compactPeerFromAddr("nonsense", 6881)
	assert.False(t, ok)
}

// withTestGlobals swaps the server globals for an isolated cache and a
// no-op DHT node (nil inner node makes Find a no-op), restoring both after
// the test. It lets handler tests run without network.
func withTestGlobals(t *testing.T) {
	t.Helper()
	oldCache, oldNode := peerCache, dhtNode
	c, err := peercache.New(10, 200)
	assert.NoError(t, err)
	peerCache = c
	dhtNode = &DhtNode{}
	t.Cleanup(func() {
		peerCache, dhtNode = oldCache, oldNode
	})
}

func doAnnounce(t *testing.T, remoteAddr, ih, port, event string) string {
	t.Helper()
	u := "/announce?compact=1&info_hash=" + ih + "&port=" + port + "&event=" + event + "&left=0&numwant=50"
	req := httptest.NewRequest(http.MethodGet, u, nil)
	req.RemoteAddr = remoteAddr
	rec := httptest.NewRecorder()
	trackerHandler(rec, req)
	if !assert.Equal(t, http.StatusOK, rec.Code) {
		t.FailNow()
	}
	return rec.Body.String()
}

func TestAnnounceMergeAB(t *testing.T) {
	withTestGlobals(t)
	ih := strings.Repeat("a", 20)

	// 5000 = 0x1388, 5001 = 0x1389.
	peerA := string([]byte{10, 0, 0, 1, 0x13, 0x88})
	peerB := string([]byte{10, 0, 0, 2, 0x13, 0x89})

	doAnnounce(t, "10.0.0.1:9999", ih, "5000", "started")
	body := doAnnounce(t, "10.0.0.2:9999", ih, "5001", "started")

	assert.Contains(t, body, peerA, "B must see A's locally announced peer")
	assert.Contains(t, body, peerB, "no self-dedup: B sees itself")
}

func TestStoppedRemoves(t *testing.T) {
	withTestGlobals(t)
	ih := strings.Repeat("b", 20)

	peerA := string([]byte{10, 0, 0, 1, 0x13, 0x88})
	peerB := string([]byte{10, 0, 0, 2, 0x13, 0x89})

	doAnnounce(t, "10.0.0.1:9999", ih, "5000", "started")
	doAnnounce(t, "10.0.0.1:9999", ih, "5000", "stopped")
	body := doAnnounce(t, "10.0.0.2:9999", ih, "5001", "started")

	assert.NotContains(t, body, peerA, "stopped peer must disappear")
	assert.Contains(t, body, peerB)
}
