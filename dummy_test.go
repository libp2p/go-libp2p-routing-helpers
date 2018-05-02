package routinghelpers

import (
	"context"
	"errors"
	"strings"
	"sync"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	routing "github.com/libp2p/go-libp2p-routing"
	ropts "github.com/libp2p/go-libp2p-routing/options"
)

type dummyValueStore sync.Map

func (d *dummyValueStore) PutValue(ctx context.Context, key string, value []byte, opts ...ropts.Option) error {
	if strings.HasPrefix(key, "/notsupported/") {
		return routing.ErrNotSupported
	}
	if strings.HasPrefix(key, "/error/") {
		return errors.New(key[len("/error/"):])
	}
	(*sync.Map)(d).Store(key, value)
	return nil
}

func (d *dummyValueStore) GetValue(ctx context.Context, key string, opts ...ropts.Option) ([]byte, error) {
	if v, ok := (*sync.Map)(d).Load(key); ok {
		return v.([]byte), nil
	}
	return nil, routing.ErrNotFound
}

type dummyProvider struct {
	cids      map[string][]peer.ID
	providing sync.Map
}

func (d *dummyProvider) FindProvidersAsync(ctx context.Context, c *cid.Cid, count int) <-chan pstore.PeerInfo {
	peers := d.cids[c.KeyString()]
	if len(peers) > count {
		peers = peers[:count]
	}
	out := make(chan pstore.PeerInfo, len(peers))
	for _, p := range peers {
		out <- pstore.PeerInfo{ID: p}
	}
	close(out)
	return out
}

func (d *dummyProvider) Provide(ctx context.Context, c *cid.Cid, local bool) error {
	d.providing.Store(c.KeyString(), local)
	return nil
}
