package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zero-custom/dhtproxy/peercache"
	bencode "github.com/jackpal/bencode-go"
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

	// IPv6 now records an 18-byte compact entry (covered in depth below).
	peer6, ok6 := compactPeerFromAddr("[::1]:1234", 6881)
	assert.True(t, ok6)
	assert.Len(t, peer6, 18)

	_, ok = compactPeerFromAddr("127.0.0.1:1234", 0)
	assert.False(t, ok)

	_, ok = compactPeerFromAddr("127.0.0.1:1234", 65536)
	assert.False(t, ok)

	_, ok = compactPeerFromAddr("nonsense", 6881)
	assert.False(t, ok)
}

// compactV6 builds the expected 18-byte compact entry for tests.
func compactV6(t *testing.T, ip string, port int) string {
	t.Helper()
	b := make([]byte, 18)
	copy(b, net.ParseIP(ip).To16())
	b[16] = byte(port >> 8)
	b[17] = byte(port)
	return string(b)
}

func TestCompactPeerFromAddrIPv6(t *testing.T) {
	// Bracket form: ::1 port 6881 (0x1ae1).
	peer, ok := compactPeerFromAddr("[::1]:1234", 6881)
	if assert.True(t, ok) {
		assert.Equal(t, append(make([]byte, 15), 1, 0x1a, 0xe1), []byte(peer))
	}

	// Full form.
	peer, ok = compactPeerFromAddr("[2001:db8::1]:9999", 5000)
	if assert.True(t, ok) {
		assert.Equal(t, compactV6(t, "2001:db8::1", 5000), peer)
		assert.Len(t, peer, 18)
	}

	// IPv4-mapped IPv6 keeps the 6-byte encoding.
	peer, ok = compactPeerFromAddr("[::ffff:127.0.0.1]:9999", 6881)
	if assert.True(t, ok) {
		assert.Equal(t, []byte{127, 0, 0, 1, 0x1a, 0xe1}, []byte(peer))
	}

	// Unbracketed v6 confuses SplitHostPort -> unusable.
	_, ok = compactPeerFromAddr("2001:db8::1", 6881)
	assert.False(t, ok)

	_, ok = compactPeerFromAddr("[::1]:1234", 0)
	assert.False(t, ok)

	_, ok = compactPeerFromAddr("[::1]:1234", 65536)
	assert.False(t, ok)

	_, ok = compactPeerFromAddr("[::1", 6881)
	assert.False(t, ok)
}

func TestIPv6SkippedCountsUnusableOnly(t *testing.T) {
	atomic.StoreInt64(&ipv6Skipped, 0)

	_, ok := compactPeerFromAddr("[2001:db8::1]:9999", 6881)
	assert.True(t, ok)
	assert.Zero(t, atomic.LoadInt64(&ipv6Skipped), "recorded v6 must not bump ipv6Skipped")

	_, ok = compactPeerFromAddr("127.0.0.1:1234", 6881)
	assert.True(t, ok)
	assert.Zero(t, atomic.LoadInt64(&ipv6Skipped), "recorded v4 must not bump ipv6Skipped")

	_, ok = compactPeerFromAddr("nonsense", 6881)
	assert.False(t, ok)
	assert.Equal(t, int64(1), atomic.LoadInt64(&ipv6Skipped), "garbage host is unusable")

	_, ok = compactPeerFromAddr("[::1]:1234", 0)
	assert.False(t, ok)
	assert.Equal(t, int64(2), atomic.LoadInt64(&ipv6Skipped), "bad port is unusable")
}

func TestSplitPeers(t *testing.T) {
	v4a := string([]byte{10, 0, 0, 1, 0x13, 0x88})
	v4b := string([]byte{10, 0, 0, 2, 0x13, 0x89})
	v6a := compactV6(t, "2001:db8::1", 5000)
	v6b := compactV6(t, "2001:db8::2", 5001)
	odd := string([]byte{1, 2, 3})

	v4, v6 := splitPeers([]string{v4a, v6a, v4b, v6b, odd})
	assert.Equal(t, []string{v4a, v4b}, v4)
	assert.Equal(t, []string{v6a, v6b}, v6, "order preserved, odd length dropped")

	v4, v6 = splitPeers(nil)
	assert.Empty(t, v4)
	assert.Empty(t, v6)
}

func TestTruncateSplitPeers(t *testing.T) {
	v4a := string([]byte{10, 0, 0, 1, 0x13, 0x88})
	v4b := string([]byte{10, 0, 0, 2, 0x13, 0x89})
	v4c := string([]byte{10, 0, 0, 3, 0x13, 0x8a})
	v6a := compactV6(t, "2001:db8::1", 5000)
	v6b := compactV6(t, "2001:db8::2", 5001)
	mixed := []string{v4a, v6a, v4b, v6b, v4c}

	// Shared numwant budget, v4 filled first.
	v4, v6 := truncateSplitPeers(mixed, 4, 200)
	assert.Equal(t, []string{v4a, v4b, v4c}, v4)
	assert.Equal(t, []string{v6a}, v6)

	// Budget exhausted by v4 alone.
	v4, v6 = truncateSplitPeers(mixed, 2, 200)
	assert.Equal(t, []string{v4a, v4b}, v4)
	assert.Empty(t, v6)

	// No numwant limit: each half capped by maxWant only.
	v4, v6 = truncateSplitPeers(mixed, 0, 2)
	assert.Equal(t, []string{v4a, v4b}, v4)
	assert.Equal(t, []string{v6a, v6b}, v6)

	// v4-only input matches truncatePeers exactly (byte-identical serve).
	v4only := []string{v4a, v4b, v4c}
	for _, numwant := range []int{-1, 0, 1, 2, 10} {
		v4, v6 = truncateSplitPeers(v4only, numwant, 200)
		assert.Equal(t, truncatePeers(v4only, numwant, 200), v4, "numwant=%d", numwant)
		assert.Empty(t, v6, "numwant=%d", numwant)
		assert.Equal(t, strings.Join(truncatePeers(v4only, numwant, 200), ""),
			strings.Join(v4, ""), "joined bytes identical, numwant=%d", numwant)
	}
}

// fakeBackend is a no-op PeerBackend so handler tests run without network.
type fakeBackend struct{}

func (f *fakeBackend) Name() string       { return "fake" }
func (f *fakeBackend) Request(_ [20]byte) {}
func (f *fakeBackend) Close() error       { return nil }

// withTestGlobals swaps the server globals for an isolated cache and a
// no-op DHT backend, restoring both after the test. It lets handler tests
// run without network.
func withTestGlobals(t *testing.T) {
	t.Helper()
	oldCache, oldNode := peerCache, dhtNode
	c, err := peercache.New(10, 200)
	assert.NoError(t, err)
	peerCache = c
	dhtNode = &fakeBackend{}
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

func decodeTrackerResponse(t *testing.T, body string) TrackerResponse {
	t.Helper()
	var resp TrackerResponse
	assert.NoError(t, bencode.Unmarshal(strings.NewReader(body), &resp))
	return resp
}

func TestAnnounceMergeABv6(t *testing.T) {
	withTestGlobals(t)
	ih := strings.Repeat("c", 20)

	peerA := compactV6(t, "2001:db8::1", 5000)
	peerB := compactV6(t, "2001:db8::2", 5001)

	doAnnounce(t, "[2001:db8::1]:9999", ih, "5000", "started")
	body := doAnnounce(t, "[2001:db8::2]:9999", ih, "5001", "started")

	assert.Contains(t, body, peerA, "B must see A's v6 peer in peers6")
	assert.Contains(t, body, peerB, "no self-dedup: B sees itself")
	assert.Contains(t, body, "peers6")

	resp := decodeTrackerResponse(t, body)
	assert.Contains(t, resp.Peers6, peerA)
	assert.Contains(t, resp.Peers6, peerB)
	assert.Empty(t, resp.Peers, "no v4 peers announced")
	assert.Equal(t, 2, resp.Incomplete)
}

func TestStoppedRemovesV6(t *testing.T) {
	withTestGlobals(t)
	ih := strings.Repeat("d", 20)

	peerA := compactV6(t, "2001:db8::1", 5000)
	peerB := compactV6(t, "2001:db8::2", 5001)

	doAnnounce(t, "[2001:db8::1]:9999", ih, "5000", "started")
	doAnnounce(t, "[2001:db8::1]:9999", ih, "5000", "stopped")
	body := doAnnounce(t, "[2001:db8::2]:9999", ih, "5001", "started")

	assert.NotContains(t, body, peerA, "stopped v6 peer must disappear")
	assert.Contains(t, body, peerB)

	resp := decodeTrackerResponse(t, body)
	assert.NotContains(t, resp.Peers6, peerA)
	assert.Contains(t, resp.Peers6, peerB)
}

func TestAnnounceMixedV4V6Split(t *testing.T) {
	withTestGlobals(t)
	ih := strings.Repeat("e", 20)

	peerV4 := string([]byte{10, 0, 0, 1, 0x13, 0x88})
	peerV6 := compactV6(t, "2001:db8::1", 5000)

	doAnnounce(t, "10.0.0.1:9999", ih, "5000", "started")
	body := doAnnounce(t, "[2001:db8::1]:9999", ih, "5000", "started")

	resp := decodeTrackerResponse(t, body)
	assert.Contains(t, resp.Peers, peerV4, "v4 entry served via peers")
	assert.Contains(t, resp.Peers6, peerV6, "v6 entry served via peers6")
	assert.NotContains(t, resp.Peers, peerV6)
	assert.NotContains(t, resp.Peers6, peerV4)
	assert.Equal(t, 2, resp.Incomplete)
}

func TestV4OnlyOmitsPeers6(t *testing.T) {
	withTestGlobals(t)
	ih := strings.Repeat("f", 20)

	doAnnounce(t, "10.0.0.1:9999", ih, "5000", "started")
	body := doAnnounce(t, "10.0.0.2:9999", ih, "5001", "started")

	assert.NotContains(t, body, "peers6", "v4-only swarms stay byte-identical: no peers6 key")
}
