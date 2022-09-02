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

// NewComposableParallel creates a Router that will execute methods from provided Routers in parallel.
// On all methods, If IgnoreError flag is set, that Router will not stop the entire execution.
// On all methods, If ExecuteAfter is set, that Router will be executed after the timer.
// Router specific timeout will start counting AFTER the ExecuteAfter timer.
func NewComposableParallel(routers []*ParallelRouter) *ComposableParallel {
	return &ComposableParallel{
		routers: routers,
	}
}

// Provide will call all Routers in parallel.
func (r *ComposableParallel) Provide(ctx context.Context, cid cid.Cid, provide bool) error {
	var wg sync.WaitGroup
	errCh := make(chan error)
	for _, r := range r.routers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			tim := time.NewTimer(r.ExecuteAfter)
			defer tim.Stop()
			select {
			case <-ctx.Done():
				if !r.IgnoreError {
					errCh <- ctx.Err()
				}
			case <-tim.C:
				ctx, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				err := r.Router.Provide(ctx, cid, provide)
				if err != nil &&
					!r.IgnoreError {
					errCh <- err
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var errOut error
	for err := range errCh {
		errOut = multierror.Append(errOut, err)
	}

	return errOut
}

// FindProvidersAsync will execute all Routers in parallel, iterating results from them in unspecified oredr.
// If count is set, only that amount of elements will be returned without any specification about from what router is obtained.
// To gather providers from a set of Routers first, you can use the ExecuteAfter timer to delay some Router execution.
func (r *ComposableParallel) FindProvidersAsync(ctx context.Context, cid cid.Cid, count int) <-chan peer.AddrInfo {
	addrChanOut := make(chan peer.AddrInfo)
	var totalCount int64
	var wg sync.WaitGroup
	for _, r := range r.routers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			tim := time.NewTimer(r.ExecuteAfter)
			defer tim.Stop()
			select {
			case <-ctx.Done():
				return
			case <-tim.C:
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

						if atomic.AddInt64(&totalCount, 1) > int64(count) && count != 0 {
							return
						}

						select {
						case <-ctx.Done():
							return
						case addrChanOut <- addr:
						}

					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(addrChanOut)
	}()

	return addrChanOut
}

// FindPeer will execute all Routers in parallel, getting the first AddrInfo found and cancelling all other Router calls.
func (r *ComposableParallel) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	outCh := make(chan peer.AddrInfo)
	errCh := make(chan error)

	// global cancel context to stop early other router's execution.
	ctx, gcancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, r := range r.routers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			tim := time.NewTimer(r.ExecuteAfter)
			defer tim.Stop()
			select {
			case <-ctx.Done():
				if !r.IgnoreError {
					errCh <- ctx.Err()
				}
			case <-tim.C:
				ctx, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				addr, err := r.Router.FindPeer(ctx, id)
				if err != nil &&
					!errors.Is(err, routing.ErrNotFound) &&
					!r.IgnoreError {
					select {
					case <-ctx.Done():
					case errCh <- err:
					}
					return
				}
				if addr.ID == "" {
					return
				}
				select {
				case <-ctx.Done():
					return
				case outCh <- addr:
				}
			}
		}()
	}

	// goroutine closing everything when finishing execution
	go func() {
		wg.Wait()
		close(outCh)
		close(errCh)
	}()

	select {
	case out, ok := <-outCh:
		gcancel()
		if !ok {
			return peer.AddrInfo{}, routing.ErrNotFound
		}
		return out, nil
	case err, ok := <-errCh:
		gcancel()
		if !ok {
			return peer.AddrInfo{}, routing.ErrNotFound
		}
		return peer.AddrInfo{}, err
	case <-ctx.Done():
		gcancel()
		return peer.AddrInfo{}, ctx.Err()
	}
}

// PutValue will execute all Routers in parallel. If a Router fails and IgnoreError flag is not set, the whole execution will fail.
// Some Puts before the failure might be successful, even if we return an error.
func (r *ComposableParallel) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	var wg sync.WaitGroup
	errCh := make(chan error)
	for _, r := range r.routers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			tim := time.NewTimer(r.ExecuteAfter)
			defer tim.Stop()
			select {
			case <-ctx.Done():
				if !r.IgnoreError {
					errCh <- ctx.Err()
				}
			case <-tim.C:
				ctx, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				err := r.Router.PutValue(ctx, key, val, opts...)
				if err != nil &&
					!r.IgnoreError {
					errCh <- err
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var errOut error
	for err := range errCh {
		errOut = multierror.Append(errOut, err)
	}

	return errOut
}

// GetValue will execute all Routers in parallel. The first value found will be returned, cancelling all other executions.
func (r *ComposableParallel) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	outCh := make(chan []byte)
	errCh := make(chan error)

	// global cancel context to stop early other router's execution.
	ctx, gcancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, r := range r.routers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			tim := time.NewTimer(r.ExecuteAfter)
			defer tim.Stop()
			select {
			case <-ctx.Done():
				if !r.IgnoreError {
					errCh <- ctx.Err()
				}
			case <-tim.C:
				ctx, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				val, err := r.Router.GetValue(ctx, key, opts...)
				if err != nil &&
					!errors.Is(err, routing.ErrNotFound) &&
					!r.IgnoreError {
					select {
					case <-ctx.Done():
					case errCh <- err:
					}
					return
				}
				if len(val) == 0 {
					return
				}
				select {
				case <-ctx.Done():
					return
				case outCh <- val:
				}
			}
		}()
	}

	// goroutine closing everything when finishing execution
	go func() {
		wg.Wait()
		close(outCh)
		close(errCh)
	}()

	select {
	case out, ok := <-outCh:
		gcancel()
		if !ok {
			return nil, routing.ErrNotFound
		}
		return out, nil
	case err, ok := <-errCh:
		gcancel()
		if !ok {
			return nil, routing.ErrNotFound
		}
		return nil, err
	case <-ctx.Done():
		gcancel()
		return nil, ctx.Err()
	}
}

func (r *ComposableParallel) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	outCh := make(chan []byte)
	errCh := make(chan error)
	var wg sync.WaitGroup
	for _, r := range r.routers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			tim := time.NewTimer(r.ExecuteAfter)
			defer tim.Stop()
			select {
			case <-ctx.Done():
				return
			case <-tim.C:
				ctx, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				valueChan, err := r.Router.SearchValue(ctx, key, opts...)
				if err != nil && !r.IgnoreError {
					select {
					case <-ctx.Done():
					case errCh <- err:
					}
					return
				}
				for {
					select {
					case <-ctx.Done():
						return
					case val, ok := <-valueChan:
						if !ok {
							return
						}
						select {
						case <-ctx.Done():
							return
						case outCh <- val:
						}
					}
				}
			}
		}()
	}

	// goroutine closing everything when finishing execution
	go func() {
		wg.Wait()
		close(outCh)
		close(errCh)
	}()

	select {
	case err, ok := <-errCh:
		if !ok {
			return nil, routing.ErrNotFound
		}
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return outCh, nil
	}
}

func (r *ComposableParallel) Bootstrap(ctx context.Context) error {
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
