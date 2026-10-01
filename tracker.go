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
	Interval    int64  "interval"     //nolint:govet // Bencode-go uses non-comformant struct tags
	MinInterval int64  "min interval" //nolint:govet // Bencode-go uses non-comformant struct tags
	Complete    int    "complete"     //nolint:govet // Bencode-go uses non-comformant struct tags
	Incomplete  int    "incomplete"   //nolint:govet // Bencode-go uses non-comformant struct tags
	Peers       string "peers"        //nolint:govet // Bencode-go uses non-comformant struct tags
}

// ipv6Skipped counts announces whose source address is not IPv4 and were
// therefore not recorded in the local pool. Read with atomic.LoadInt64.
var ipv6Skipped int64

// compactPeerFromAddr encodes a 6-byte compact peer (IPv4 + big-endian
// port) from a connection RemoteAddr and an announce port. It reports false
// when the port is invalid or the address is not IPv4; callers then serve
// DHT results only.
func compactPeerFromAddr(remoteAddr string, port int) (string, bool) {
	if port <= 0 || port > 65535 {
		return "", false
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return "", false
	}
	ip4 := net.ParseIP(host).To4()
	if ip4 == nil {
		atomic.AddInt64(&ipv6Skipped, 1)
		return "", false
	}
	b := make([]byte, 6)
	copy(b, ip4)
	b[4] = byte(port >> 8)
	b[5] = byte(port)
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

	dhtNode.Find(infoHash)

	if !ok || len(peers) == 0 {
		response.Interval = 30
		response.MinInterval = 10

		time.Sleep(5 * time.Second)

		peers, ok = peerCache.Get(string(infoHash))
	}

	if ok && len(peers) > 0 {
		numwant, _ := strconv.Atoi(r.FormValue("numwant"))
		peers = truncatePeers(peers, numwant, *maxWant)
		response.Incomplete = len(peers)
		response.Peers = strings.Join(peers, "")
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	if err := bencode.Marshal(w, response); err != nil {
		http.Error(w, err.Error(), 500)
	}
}
