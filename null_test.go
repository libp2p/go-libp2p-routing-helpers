package routinghelpers

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

func TestNull(t *testing.T) {
	t.Parallel()

	var n Null
	ctx := context.Background()
	if err := n.PutValue(ctx, "anything", nil); err != routing.ErrNotSupported {
		t.Fatal(err)
	}
	if _, err := n.GetValue(ctx, "anything", nil); err != routing.ErrNotFound {
		t.Fatal(err)
	}
	if err := n.Provide(ctx, cid.Cid{}, false); err != routing.ErrNotSupported {
		t.Fatal(err)
	}
	if _, ok := <-n.FindProvidersAsync(ctx, cid.Cid{}, 10); ok {
		t.Fatal("expected no values")
	}
	if _, err := n.FindPeer(ctx, peer.ID("thing")); err != routing.ErrNotFound {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
}
