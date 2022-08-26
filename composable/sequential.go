package composable

import (
	"context"
	"errors"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multiaddr"
)

var _ routing.Routing = &Sequential{}

type Sequential struct {
	routers []*SequentialRouter
}

func NewSequential(routers []*SequentialRouter) *Sequential {
	return &Sequential{
		routers: routers,
	}
}

func (r *Sequential) Provide(ctx context.Context, cid cid.Cid, provide bool) error {
	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		if err := router.Router.Provide(ctx, cid, provide); err != nil &&
			!errors.Is(err, routing.ErrNotSupported) &&
			!router.IgnoreError {
			cancel()
			return err
		}
		cancel()
	}

	return nil
}

func (r *Sequential) FindProvidersAsync(ctx context.Context, cid cid.Cid, count int) <-chan peer.AddrInfo {
	chanOut := make(chan peer.AddrInfo)
	sentCount := 0
	go func() {
		for _, router := range r.routers {
			ctx, cancel := context.WithTimeout(ctx, router.Timeout)
			rch := router.Router.FindProvidersAsync(ctx, cid, count)

			var g sync.WaitGroup
			g.Add(1)
			go func() {
				defer g.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case v, ok := <-rch:
						if !ok {
							break
						}
						if sentCount >= count {
							break
						}

						chanOut <- v
						sentCount++
					}
				}
			}()

			g.Wait()
			cancel()
		}
	}()

	return chanOut
}

func (r *Sequential) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	maSet := make(map[multiaddr.Multiaddr]struct{})
	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		addrInfo, err := router.Router.FindPeer(ctx, pid)
		if err != nil &&
			!errors.Is(err, routing.ErrNotSupported) &&
			!router.IgnoreError {
			cancel()
			return peer.AddrInfo{}, err
		}
		cancel()

		// TODO addrInfo.ID can be different between requests?

		for _, addr := range addrInfo.Addrs {
			maSet[addr] = struct{}{}
		}
	}

	var mas []multiaddr.Multiaddr
	for addr := range maSet {
		mas = append(mas, addr)

	}

	out := peer.AddrInfo{
		ID:    pid,
		Addrs: mas,
	}

	return out, nil
}

func (r *Sequential) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		if err := router.Router.PutValue(ctx, key, val, opts...); err != nil &&
			!errors.Is(err, routing.ErrNotSupported) &&
			!router.IgnoreError {

			cancel()
			return err
		}
		cancel()
	}

	return nil
}

func (r *Sequential) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		val, err := router.Router.GetValue(ctx, key, opts...)
		if err != nil &&
			!errors.Is(err, routing.ErrNotSupported) &&
			!router.IgnoreError {
			cancel()
			return nil, err
		}
		cancel()

		if val == nil {
			continue
		}

		return val, nil
	}

	return nil, routing.ErrNotFound
}

func (r *Sequential) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	chanOut := make(chan []byte)
	var chans []<-chan []byte
	var cancels []context.CancelFunc

	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		rch, err := router.Router.SearchValue(ctx, key, opts...)
		if err != nil && !errors.Is(err, routing.ErrNotSupported) && !router.IgnoreError {
			cancel()
			return nil, err
		}

		cancels = append(cancels, cancel)
		chans = append(chans, rch)
	}

	go func() {
		for i := 0; i < len(chans); i++ {
			defer cancels[i]()
			if chans[i] == nil {
				continue
			}

			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-chans[i]:
					if !ok {
						break
					}
					chanOut <- v
				}
			}
		}
	}()

	return chanOut, nil

}

func (r *Sequential) Bootstrap(ctx context.Context) error {
	for _, router := range r.routers {
		ctx, cancel := context.WithTimeout(ctx, router.Timeout)
		if err := router.Router.Bootstrap(ctx); err != nil && !errors.Is(err, routing.ErrNotSupported) && !router.IgnoreError {
			cancel()
			return err
		}
		cancel()
	}

	return nil
}
