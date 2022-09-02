package routinghelpers

import (
	"context"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
)

var _ routing.Routing = &ComposableSequential{}

type ComposableSequential struct {
	routers []*SequentialRouter
}

func NewComposableSequential(routers []*SequentialRouter) *ComposableSequential {
	return &ComposableSequential{
		routers: routers,
	}
}

// Provide calls Provide method per each router sequentially.
// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *ComposableSequential) Provide(ctx context.Context, cid cid.Cid, provide bool) error {
	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		if err := router.Router.Provide(ctx, cid, provide); err != nil &&
			!router.IgnoreError {
			cancel()
			return err
		}
		cancel()
	}

	return nil
}

// FindProvidersAsync calls FindProvidersAsync per each router sequentially.
// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
// If count is set, the channel will return up to count results, stopping routers iteration.
func (r *ComposableSequential) FindProvidersAsync(ctx context.Context, cid cid.Cid, count int) <-chan peer.AddrInfo {
	chanOut := make(chan peer.AddrInfo)
	var totalCount int64
	go func() {
		for _, router := range r.routers {
			ctx, cancel := context.WithTimeout(ctx, router.Timeout)
			defer cancel()
			rch := router.Router.FindProvidersAsync(ctx, cid, count)
		f:
			for {
				select {
				case <-ctx.Done():
					break f
				case v, ok := <-rch:
					if !ok {
						break f
					}
					if totalCount >= int64(count) {
						close(chanOut)
						return
					}

					select {
					case <-ctx.Done():
						break f
					case chanOut <- v:
						atomic.AddInt64(&totalCount, 1)
					}
				}
			}
		}
	}()

	return chanOut
}

// FindPeer calls FindPeer per each router sequentially.
// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *ComposableSequential) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		defer cancel()
		addrInfo, err := router.Router.FindPeer(ctx, pid)
		if err != nil &&
			!router.IgnoreError {
			return peer.AddrInfo{}, err
		}

		if err == nil {
			return addrInfo, nil
		}
	}

	return peer.AddrInfo{}, routing.ErrNotFound
}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *ComposableSequential) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		defer cancel()
		if err := router.Router.PutValue(ctx, key, val, opts...); err != nil &&
			!router.IgnoreError {
			return err
		}
	}

	return nil
}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *ComposableSequential) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		defer cancel()
		val, err := router.Router.GetValue(ctx, key, opts...)
		if err != nil &&
			!router.IgnoreError {
			return nil, err
		}

		if val == nil {
			continue
		}

		return val, nil
	}

	return nil, routing.ErrNotFound
}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *ComposableSequential) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	chanOut := make(chan []byte)
	var chans []<-chan []byte
	var cancels []context.CancelFunc

	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		rch, err := router.Router.SearchValue(ctx, key, opts...)
		if err != nil &&
			!router.IgnoreError {
			cancel()
			return nil, err
		}

		cancels = append(cancels, cancel)
		chans = append(chans, rch)
	}

	go func() {
		for i := 0; i < len(chans); i++ {
			if chans[i] == nil {
				cancels[i]()
				continue
			}

		f:
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-chans[i]:
					if !ok {
						break f
					}
					chanOut <- v
				}
			}

			cancels[i]()
		}
	}()

	return chanOut, nil

}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *ComposableSequential) Bootstrap(ctx context.Context) error {
	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		defer cancel()
		if err := router.Router.Bootstrap(ctx); err != nil &&
			!router.IgnoreError {
			return err
		}
	}

	return nil
}
