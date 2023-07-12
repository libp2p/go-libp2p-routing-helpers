package routinghelpers

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

// nothing is like [Null] but it never reach quorum for streaming responses.
type nothing struct {
	Null
}

// SearchValue always returns ErrNotFound
func (nr nothing) SearchValue(ctx context.Context, _ string, _ ...routing.Option) (<-chan []byte, error) {
	return makeChannelThatDoNothingAndIsClosedOnCancel[[]byte](ctx), nil
}

// FindProvidersAsync always returns a closed channel
func (nr nothing) FindProvidersAsync(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	return makeChannelThatDoNothingAndIsClosedOnCancel[peer.AddrInfo](ctx)
}

func makeChannelThatDoNothingAndIsClosedOnCancel[T any](ctx context.Context) <-chan T {
	ch := make(chan T)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

var _ routing.Routing = nothing{}
