package routinghelpers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
)

var _ routing.Routing = &Parallel{}

type ComposableParallel struct {
	routers []*ParallelRouter
}

func NewComposableParallel(routers []*ParallelRouter) *ComposableParallel {
	return &ComposableParallel{
		routers: routers,
	}
}

func (r *ComposableParallel) Provide(ctx context.Context, cid cid.Cid, provide bool) error {
	var wg sync.WaitGroup
	errCh := make(chan error)
	closeCh := make(chan bool)
	for _, r := range r.routers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				if ctx.Err() != nil && !r.IgnoreError {
					errCh <- ctx.Err()
				}
			case <-time.After(r.ExecuteAfter):
				ctx, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				err := r.Router.Provide(ctx, cid, provide)
				if err != nil &&
					!errors.Is(err, routing.ErrNotSupported) &&
					!r.IgnoreError {
					errCh <- err
				}
			case <-closeCh:
				return
			}
		}()
	}

	go func() {
		wg.Wait()
		close(closeCh)
	}()

	var errOut error
	select {
	case err := <-errCh:
		errOut = multierror.Append(errOut, err)
	case <-closeCh:
	}

	return errOut
}

func (r *ComposableParallel) FindProvidersAsync(ctx context.Context, cid cid.Cid, count int) <-chan peer.AddrInfo {
	addrChanOut := make(chan peer.AddrInfo)
	var totalCount int64
	for _, r := range r.routers {
		r := r
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.ExecuteAfter):
				ctx, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				addrChan := r.Router.FindProvidersAsync(ctx, cid, count)
				for {
					select {
					case <-ctx.Done():
						return
					case addr, ok := <-addrChan:
						if !ok {
							return
						}

						if atomic.AddInt64(&totalCount, 1) > int64(count) {
							return
						}

						addrChanOut <- addr
					}
				}
			}
		}()
	}

	return addrChanOut
}

// FindPeer
func (r *ComposableParallel) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	outCh := make(chan peer.AddrInfo)
	errCh := make(chan error)
	closeCh := make(chan bool)
	var wg sync.WaitGroup
	for _, r := range r.routers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				if ctx.Err() != nil && !r.IgnoreError {
					errCh <- ctx.Err()
				}
			case <-time.After(r.ExecuteAfter):
				ctx, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				addr, err := r.Router.FindPeer(ctx, id)
				if err != nil &&
					!errors.Is(err, routing.ErrNotSupported) &&
					!errors.Is(err, routing.ErrNotFound) &&
					!r.IgnoreError {
					errCh <- err
					return
				}

				if addr.ID == peer.ID("") {
					return
				}

				outCh <- addr
			case <-closeCh:
				return
			}
		}()
	}

	go func() {
		wg.Wait()
		close(closeCh)
	}()

	select {
	case out := <-outCh:
		return out, nil
	case err := <-errCh:
		return peer.AddrInfo{}, err
	case <-closeCh:
		return peer.AddrInfo{}, routing.ErrNotFound
	}
}

// PutValue
func (r *ComposableParallel) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	var wg sync.WaitGroup
	errCh := make(chan error)
	closeCh := make(chan bool)
	for _, r := range r.routers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				if ctx.Err() != nil && !r.IgnoreError {
					errCh <- ctx.Err()
				}
			case <-time.After(r.ExecuteAfter):
				ctx, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				err := r.Router.PutValue(ctx, key, val, opts...)
				if err != nil &&
					!errors.Is(err, routing.ErrNotSupported) &&
					!r.IgnoreError {
					errCh <- err
				}
			case <-closeCh:
				return
			}
		}()
	}

	go func() {
		wg.Wait()
		close(closeCh)
	}()

	var errOut error
	select {
	case err := <-errCh:
		errOut = multierror.Append(errOut, err)
	case <-closeCh:
	}

	return errOut
}

// GetValue
func (r *ComposableParallel) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	outCh := make(chan []byte)
	errCh := make(chan error)
	closeCh := make(chan bool)
	var wg sync.WaitGroup
	for _, r := range r.routers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				if ctx.Err() != nil && !r.IgnoreError {
					errCh <- ctx.Err()
				}
			case <-time.After(r.ExecuteAfter):
				ctx, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				val, err := r.Router.GetValue(ctx, key, opts...)
				if err != nil &&
					!errors.Is(err, routing.ErrNotSupported) &&
					!errors.Is(err, routing.ErrNotFound) &&
					!r.IgnoreError {
					errCh <- err
					return
				}

				if val == nil {
					return
				}

				outCh <- val
			case <-closeCh:
				return
			}
		}()
	}

	go func() {
		wg.Wait()
		close(closeCh)
	}()

	select {
	case out := <-outCh:
		return out, nil
	case err := <-errCh:
		return nil, err
	case <-closeCh:
		return nil, routing.ErrNotFound
	}
}

func (r *ComposableParallel) SearchValue(context.Context, string, ...routing.Option) (<-chan []byte, error) {
	return nil, nil
}

func (r *ComposableParallel) Bootstrap(ctx context.Context) error {
	return nil
}
