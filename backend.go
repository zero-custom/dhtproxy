package main

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
