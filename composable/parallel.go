package composable

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
)

var _ routing.Routing = &Parallel{}

type Parallel struct {
	timeout time.Duration
	routers []*ParallelRouter
}

func NewParallel(timeout time.Duration, routers []*ParallelRouter) *Parallel {
	return &Parallel{
		timeout: timeout,
		routers: routers,
	}
}

func (r *Parallel) Provide(context.Context, cid.Cid, bool) error {
	return nil
}

func (r *Parallel) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	return nil
}

func (r *Parallel) FindPeer(context.Context, peer.ID) (peer.AddrInfo, error) {
	return peer.AddrInfo{}, nil
}

func (r *Parallel) PutValue(context.Context, string, []byte, ...routing.Option) error {
	return nil
}

func (r *Parallel) GetValue(context.Context, string, ...routing.Option) ([]byte, error) {
	return nil, nil
}

func (r *Parallel) SearchValue(context.Context, string, ...routing.Option) (<-chan []byte, error) {
	return nil, nil
}

func (r *Parallel) Bootstrap(context.Context) error {
	return nil
}
