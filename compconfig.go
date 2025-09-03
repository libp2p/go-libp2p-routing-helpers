package routinghelpers

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p-routing-helpers/tracing"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"
)

const tracer = tracing.Tracer("go-libp2p-routing-helpers")

type ParallelRouter struct {
	Timeout      time.Duration
	Router       routing.Routing
	ExecuteAfter time.Duration
	// DoNotWaitForSearchValue is experimental while we wait for a better solution.
	DoNotWaitForSearchValue bool
	IgnoreError             bool
}

type SequentialRouter struct {
	Timeout     time.Duration
	IgnoreError bool
	Router      routing.Routing
}

type ProvideManyRouter interface {
	ProvideMany(ctx context.Context, keys []multihash.Multihash) error
}

type ReadyAbleRouter interface {
	Ready() bool
}

type ComposableRouter interface {
	Routers() []routing.Routing
}

// A DHTRouter provides DHT-specific operations.
type DHTRouter interface {
	// GetClosestPeers returns the DHT closest peers to the given peer ID.
	// If empty, it will use the content router's peer ID
	// (self). `closerThan` (optional) forces the resulting records to be
	// closer to `PeerID` than to `closerThan`. `count` specifies how many
	// records to return ([1,100], with 20 as default when set to 0).
	GetClosestPeers(ctx context.Context, pid peer.ID, closerThan peer.ID, count int) (<-chan peer.AddrInfo, error)
}
