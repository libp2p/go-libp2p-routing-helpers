package routinghelpers

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"

	cid "github.com/ipfs/go-cid"
)

type testCloser struct {
	closed int
}

func (closer *testCloser) Close() error {
	closer.closed++
	return nil
}

type failValueStore struct{}

var failValueErr = errors.New("fail valuestore error")

func (f failValueStore) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	return failValueErr
}
func (f failValueStore) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	return nil, failValueErr
}

func (f failValueStore) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	return nil, failValueErr
}

type dummyValueStore sync.Map

func (d *dummyValueStore) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	if strings.HasPrefix(key, "/notsupported/") {
		return routing.ErrNotSupported
	}
	if strings.HasPrefix(key, "/error/") {
		return errors.New(key[len("/error/"):])
	}
	if strings.HasPrefix(key, "/stall/") {
		<-ctx.Done()
		return ctx.Err()
	}
	(*sync.Map)(d).Store(key, value)
	return nil
}

func (d *dummyValueStore) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	if strings.HasPrefix(key, "/error/") {
		return nil, errors.New(key[len("/error/"):])
	}
	if strings.HasPrefix(key, "/stall/") {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if v, ok := (*sync.Map)(d).Load(key); ok {
		return v.([]byte), nil
	}
	return nil, routing.ErrNotFound
}

func (d *dummyValueStore) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	out := make(chan []byte)
	if strings.HasPrefix(key, "/error/") {
		return nil, errors.New(key[len("/error/"):])
	}

	go func() {
		defer close(out)
		v, err := d.GetValue(ctx, key, opts...)
		if err == nil {
			select {
			case out <- v:
			case <-ctx.Done():
			}
		}
	}()
	return out, nil
}

type dummyProvider map[string][]peer.ID

func (d dummyProvider) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	peers := d[c.KeyString()]
	if count > 0 && len(peers) > count {
		peers = peers[:count]
	}
	out := make(chan peer.AddrInfo)
	go func() {
		defer close(out)
		for _, p := range peers {
			if p == "stall" {
				<-ctx.Done()
				return
			}
			select {
			case out <- peer.AddrInfo{ID: p}:
			case <-ctx.Done():
			}
		}
	}()
	return out
}

func (d dummyProvider) Provide(ctx context.Context, c cid.Cid, local bool) error {
	return routing.ErrNotSupported
}

type cbProvider func(c cid.Cid, local bool) error

func (d cbProvider) Provide(ctx context.Context, c cid.Cid, local bool) error {
	return d(c, local)
}

func (d cbProvider) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo)
	close(ch)
	return ch
}

type dummyPeerRouter map[peer.ID]struct{}

func (d dummyPeerRouter) FindPeer(ctx context.Context, p peer.ID) (peer.AddrInfo, error) {
	if _, ok := d[p]; ok {
		return peer.AddrInfo{ID: p}, nil
	}
	return peer.AddrInfo{}, routing.ErrNotFound
}
