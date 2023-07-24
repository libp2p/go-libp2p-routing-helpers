package routinghelpers

import (
	"context"

	"github.com/libp2p/go-libp2p/core/routing"
)

// nothing is like [Null] but it never reach quorum for SearchValue.
type nothing struct {
	Null
}

// SearchValue always returns ErrNotFound
func (nr nothing) SearchValue(ctx context.Context, _ string, _ ...routing.Option) (<-chan []byte, error) {
	ch := make(chan []byte)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}
