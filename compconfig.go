package routinghelpers

import (
	"time"

	"github.com/libp2p/go-libp2p-core/routing"
)

type ParallelRouter struct {
	Timeout      time.Duration
	IgnoreError  bool
	Router       routing.Routing
	ExecuteAfter time.Duration
}

type SequentialRouter struct {
	Timeout     time.Duration
	IgnoreError bool
	Router      routing.Routing
}
