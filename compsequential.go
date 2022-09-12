package routinghelpers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
)

var _ routing.Routing = &composableSequential{}

type composableSequential struct {
	routers []*SequentialRouter
}

func NewComposableSequential(routers []*SequentialRouter) *composableSequential {
	return &composableSequential{
		routers: routers,
	}
}

// Provide calls Provide method per each router sequentially.
// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) Provide(ctx context.Context, cid cid.Cid, provide bool) error {
	return executeSequential(ctx, r.routers, func(ctx context.Context, r routing.Routing) error {
		return r.Provide(ctx, cid, provide)
	})
}

// FindProvidersAsync calls FindProvidersAsync per each router sequentially.
// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
// If count is set, the channel will return up to count results, stopping routers iteration.
func (r *composableSequential) FindProvidersAsync(ctx context.Context, cid cid.Cid, count int) <-chan peer.AddrInfo {
	var totalCount int64
	ch, _ := getChannelOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (<-chan peer.AddrInfo, error) {
			return r.FindProvidersAsync(ctx, cid, count), nil
		},
		func() bool {
			return atomic.AddInt64(&totalCount, 1) > int64(count) && count != 0
		},
	)

	return ch
}

// FindPeer calls FindPeer per each router sequentially.
// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	return getValueOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (peer.AddrInfo, bool, error) {
			addr, err := r.FindPeer(ctx, pid)
			return addr, addr.ID == "", err
		},
	)
}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	return executeSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.PutValue(ctx, key, val, opts...)
		})
}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	return getValueOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) ([]byte, bool, error) {
			val, err := r.GetValue(ctx, key, opts...)
			return val, len(val) == 0, err
		},
	)
}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	return getChannelOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (<-chan []byte, error) {
			return r.SearchValue(ctx, key, opts...)
		},
		func() bool { return false },
	)

}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) Bootstrap(ctx context.Context) error {
	return executeSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.Bootstrap(ctx)
		},
	)
}

func getValueOrErrorSequential[T any](
	ctx context.Context,
	routers []*SequentialRouter,
	f func(context.Context, routing.Routing) (T, bool, error),
) (value T, err error) {
	for _, router := range routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		defer cancel()
		value, empty, err := f(ctx, router.Router)
		if err != nil &&
			!errors.Is(err, routing.ErrNotFound) &&
			!router.IgnoreError {
			return value, err
		}

		if empty {
			continue
		}

		return value, nil
	}

	return value, routing.ErrNotFound
}

func executeSequential(
	ctx context.Context,
	routers []*SequentialRouter,
	f func(context.Context, routing.Routing,
	) error) error {
	for _, router := range routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		if err := f(ctx, router.Router); err != nil &&
			!errors.Is(err, routing.ErrNotFound) &&
			!router.IgnoreError {
			cancel()
			return err
		}
		cancel()
	}

	return nil
}

func getChannelOrErrorSequential[T any](
	ctx context.Context,
	routers []*SequentialRouter,
	f func(context.Context, routing.Routing) (<-chan T, error),
	shouldStop func() bool,
) (chan T, error) {
	chanOut := make(chan T)
	var chans []<-chan T
	var cancels []context.CancelFunc

	for _, router := range routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		rch, err := f(ctx, router.Router)
		if err != nil &&
			!errors.Is(err, routing.ErrNotFound) &&
			!router.IgnoreError {
			cancel()
			return nil, err
		}

		cancels = append(cancels, cancel)
		chans = append(chans, rch)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < len(chans); i++ {
			if chans[i] == nil {
				cancels[i]()
				continue
			}

		f:
			for {
				select {
				case <-ctx.Done():
					break f
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

	go func() {
		wg.Wait()
		close(chanOut)
	}()

	return chanOut, nil
}
