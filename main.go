package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // TODO: Expose this on a different port.
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zero-custom/dhtproxy/peercache"
)

var (
	listenAddr        = flag.String("listen", ":6969", "The [IP]:port to listen for incoming HTTP requests.")
	debugAddr         = flag.String("debugListen", "", "The [IP]:port to listen for pprof HTTP requests. (\"\" = disable)")
	dhtPortUDP        = flag.Int("dhtPortUDP", 0, "The UDP port number to use for DHT requests")
	dhtRequestTimeout = flag.Duration("dhtRequestTimeout", time.Minute, "Per-request timeout for DHT discovery/Stop before logging a warning (never fatal)")
	peerCacheSize     = flag.Int("peerCacheSize", 16384, "The max number of infohashes to keep a list of peers for.")
	maxWant           = flag.Int("maxWant", 200, "The largest number of peers to return in one request.")
	poolTTL           = flag.Duration("poolTTL", 30*time.Minute, "How long locally announced peers are kept (0 = disable the local pool, DHT-only behavior).")
	dhtNodesFile      = flag.String("dhtNodesFile", "", "Path to a file for persisting DHT routing-table nodes across restarts (\"\" = disable); the v6 table derives a .v6 suffix.")

	peerCache *peercache.Cache
	dhtNode   PeerBackend
)

func main() {
	flag.Parse()

	setRlimitFromFlags()

	var err error
	peerCache, err = peercache.NewWithTTL(*peerCacheSize, *maxWant, *poolTTL)
	if err != nil {
		log.Fatal(err)
	}

	dhtNode, err = NewNewBackend(*dhtPortUDP, *dhtRequestTimeout, peerCache, *dhtNodesFile)
	if err != nil {
		log.Fatal(err)
	}

	if *debugAddr != "" {
		// Serve /debug/pprof/* on default mux
		go func() {
			srv := &http.Server{
				Addr:         *debugAddr,
				ReadTimeout:  10 * time.Second,
				WriteTimeout: 30 * time.Second,
				IdleTimeout:  240 * time.Second,
				Handler:      http.DefaultServeMux,
			}
			log.Fatal(srv.ListenAndServe())
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", robotsDisallowHandler)
	mux.HandleFunc("/announce", trackerHandler)

	srv := &http.Server{
		Addr:         *listenAddr,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  240 * time.Second,
		Handler:      mux,
	}
	// Run the tracker in the background so SIGTERM/SIGINT can persist the
	// DHT routing tables and close the backend before the process exits.
	srvErr := make(chan error, 1)
	go func() {
		srvErr <- srv.ListenAndServe()
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)

	select {
	case sig := <-sigs:
		log.Printf("received signal %s: saving DHT nodes", sig)
		if nb, ok := dhtNode.(*newBackend); ok {
			if err := nb.SaveNodes(); err != nil {
				log.Print("save DHT nodes: ", err)
			}
		}
		if err := dhtNode.Close(); err != nil {
			log.Print("close DHT backend: ", err)
		}
	case err := <-srvErr:
		log.Fatal(err)
	}
}

func robotsDisallowHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("User-agent: *\nDisallow: /\n"))
}
