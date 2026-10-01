package main

import (
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	bencode "github.com/jackpal/bencode-go"
	"github.com/nictuku/dht"
)

type TrackerResponse struct {
	Interval    int64  "interval"                   //nolint:govet // Bencode-go uses non-comformant struct tags
	MinInterval int64  "min interval"               //nolint:govet // Bencode-go uses non-comformant struct tags
	Complete    int    "complete"                   //nolint:govet // Bencode-go uses non-comformant struct tags
	Incomplete  int    "incomplete"                 //nolint:govet // Bencode-go uses non-comformant struct tags
	Peers       string "peers"                      //nolint:govet // Bencode-go uses non-comformant struct tags
	Peers6      string `bencode:"peers6,omitempty"` // Omitted when empty so v4-only swarms stay byte-identical.
}

// ipv6Skipped counts announces that could not be recorded in the local pool
// because the source address or port was unusable (unparseable host,
// non-IP host, or port outside 1-65535). Successfully recorded IPv6
// announces are not counted. Read with atomic.LoadInt64.
var ipv6Skipped int64

// compactPeerFromAddr encodes a compact peer from a connection RemoteAddr
// and an announce port: 6 bytes (IPv4 + big-endian port) for IPv4 sources,
// 18 bytes (16-byte IPv6 + big-endian port, BEP 7) for IPv6 sources
// (bracket form included — net.SplitHostPort already handles it). It
// reports false when the port is invalid or the host is unusable; callers
// then serve DHT results only.
func compactPeerFromAddr(remoteAddr string, port int) (string, bool) {
	if port <= 0 || port > 65535 {
		atomic.AddInt64(&ipv6Skipped, 1)
		return "", false
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		atomic.AddInt64(&ipv6Skipped, 1)
		return "", false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		atomic.AddInt64(&ipv6Skipped, 1)
		return "", false
	}
	if ip4 := ip.To4(); ip4 != nil {
		b := make([]byte, 6)
		copy(b, ip4)
		b[4] = byte(port >> 8)
		b[5] = byte(port)
		return string(b), true
	}
	ip16 := ip.To16()
	if ip16 == nil {
		atomic.AddInt64(&ipv6Skipped, 1)
		return "", false
	}
	b := make([]byte, 18)
	copy(b, ip16)
	b[16] = byte(port >> 8)
	b[17] = byte(port)
	return string(b), true
}

// truncatePeers caps the merged peer list at numwant (when positive) and
// always at maxWant, preserving the existing no-limit behavior when both
// are non-positive.
func truncatePeers(peers []string, numwant, maxWant int) []string {
	limit := len(peers)
	if maxWant > 0 && limit > maxWant {
		limit = maxWant
	}
	if numwant > 0 && numwant < limit {
		limit = numwant
	}
	return peers[:limit]
}

// splitPeers partitions a merged peer list by compact-entry length: 6-byte
// entries go to v4 (peers), 18-byte entries to v6 (peers6, BEP 7). Entries
// of any other length are dropped defensively.
func splitPeers(peers []string) (v4, v6 []string) {
	for _, p := range peers {
		switch len(p) {
		case 6:
			v4 = append(v4, p)
		case 18:
			v6 = append(v6, p)
		}
	}
	return v4, v6
}

// truncateSplitPeers partitions the merged list and caps both halves against
// a shared numwant budget, v4 filled first; each half is additionally capped
// by maxWant. A v4-only input yields exactly the truncatePeers output, so
// pre-IPv6 swarms respond byte-identically.
func truncateSplitPeers(peers []string, numwant, maxWant int) (v4, v6 []string) {
	v4all, v6all := splitPeers(peers)
	v4 = truncatePeers(v4all, numwant, maxWant)
	if numwant <= 0 {
		v6 = truncatePeers(v6all, 0, maxWant)
		return v4, v6
	}
	budget := numwant - len(v4)
	if budget <= 0 {
		return v4, nil
	}
	v6 = truncatePeers(v6all, budget, maxWant)
	return v4, v6
}

// recordAnnounce folds one client's announce into the local pool so later
// clients behind dhtproxy can discover it. It never fails the request:
// unparseable input only skips local recording.
func recordAnnounce(r *http.Request, ih string) {
	port, _ := strconv.Atoi(r.FormValue("port"))
	peer, ok := compactPeerFromAddr(r.RemoteAddr, port)
	if !ok {
		return
	}

	event := r.FormValue("event")
	if event == "stopped" {
		peerCache.Delete(ih, peer)
		return
	}

	// `completed` means the client holds the full data: record left=0
	// regardless of the reported value (presence-only, no effect on return).
	if event == "completed" {
		peerCache.Upsert(ih, peer, 0)
		return
	}

	left := int64(-1)
	if s := r.FormValue("left"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil && n >= 0 {
			left = n
		}
	}
	peerCache.Upsert(ih, peer, left)
}

func trackerHandler(w http.ResponseWriter, r *http.Request) {
	if r.FormValue("compact") != "1" {
		http.Error(w, "Only compact protocol supported.", 400)
		return
	}

	infoHash := dht.InfoHash(r.FormValue("info_hash"))
	if len(infoHash) != 20 {
		http.Error(w, "Bad info_hash.", 400)
		return
	}

	response := TrackerResponse{
		Interval:    300,
		MinInterval: 60,
	}

	if *poolTTL > 0 {
		recordAnnounce(r, string(infoHash))
	}

	peers, ok := peerCache.Get(string(infoHash))

	var ih [20]byte
	copy(ih[:], string(infoHash))
	dhtNode.Request(ih)

	if !ok || len(peers) == 0 {
		response.Interval = 30
		response.MinInterval = 10

		time.Sleep(5 * time.Second)

		peers, ok = peerCache.Get(string(infoHash))
	}

	if ok && len(peers) > 0 {
		numwant, _ := strconv.Atoi(r.FormValue("numwant"))
		v4, v6 := truncateSplitPeers(peers, numwant, *maxWant)
		response.Incomplete = len(v4) + len(v6)
		response.Peers = strings.Join(v4, "")
		response.Peers6 = strings.Join(v6, "")
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	if err := bencode.Marshal(w, response); err != nil {
		http.Error(w, err.Error(), 500)
	}
}
