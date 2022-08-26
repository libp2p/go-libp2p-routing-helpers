package composable

import (
	"time"

	"github.com/libp2p/go-libp2p-core/routing"
)

type ParallelRouter struct {
	config
	ExecuteAfter time.Duration
}

type SequentialRouter struct {
	config
}

type config struct {
	Timeout     time.Duration
	IgnoreError bool
	Router      routing.Routing
}
