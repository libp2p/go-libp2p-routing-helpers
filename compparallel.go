package routinghelpers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jorropo/jsync"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"
	"go.uber.org/multierr"
)

var log = logging.Logger("routing/composable")

var _ routing.Routing = (*composableParallel)(nil)
var _ ProvideManyRouter = (*composableParallel)(nil)
var _ ReadyAbleRouter = (*composableParallel)(nil)
var _ ComposableRouter = (*composableParallel)(nil)

type composableParallel struct {
	routers []*ParallelRouter
}

// NewComposableParallel creates a Router that will execute methods from provided Routers in parallel.
// On all methods, If IgnoreError flag is set, that Router will not stop the entire execution.
// On all methods, If ExecuteAfter is set, that Router will be executed after the timer.
// Router specific timeout will start counting AFTER the ExecuteAfter timer.
func NewComposableParallel(routers []*ParallelRouter) *composableParallel {
	return &composableParallel{
		routers: routers,
	}
}

func (r *composableParallel) Routers() []routing.Routing {
	var routers []routing.Routing
	for _, pr := range r.routers {
		routers = append(routers, pr.Router)
	}

	return routers
}

// Provide will call all Routers in parallel.
func (r *composableParallel) Provide(ctx context.Context, cid cid.Cid, provide bool) error {
	return executeParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.Provide(ctx, cid, provide)
		},
	)
}

// ProvideMany will call all Routers in parallel, falling back to iterative
// single Provide call for routers which do not support [ProvideManyRouter].
func (r *composableParallel) ProvideMany(ctx context.Context, keys []multihash.Multihash) error {
	return executeParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			if pm, ok := r.(ProvideManyRouter); ok {
				return pm.ProvideMany(ctx, keys)
			}

			for _, k := range keys {
				if err := r.Provide(ctx, cid.NewCidV1(cid.Raw, k), true); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

// Ready will call all supported [ReadyAbleRouter] Routers SEQUENTIALLY.
// If some of them are not ready, this method will return false.
func (r *composableParallel) Ready() bool {
	for _, ro := range r.routers {
		pm, ok := ro.Router.(ReadyAbleRouter)
		if !ok {
			continue
		}

		if !pm.Ready() {
			return false
		}
	}

	return true
}

// FindProvidersAsync will execute all Routers in parallel, iterating results from them in unspecified order.
// If count is set, only that amount of elements will be returned without any specification about from what router is obtained.
// To gather providers from a set of Routers first, you can use the ExecuteAfter timer to delay some Router execution.
func (r *composableParallel) FindProvidersAsync(ctx context.Context, cid cid.Cid, count int) <-chan peer.AddrInfo {
	var totalCount int64
	ch, _ := getChannelOrErrorParallel(
		ctx,
		r.routers,
		func(ctx context.Context, r routing.Routing) (<-chan peer.AddrInfo, error) {
			return r.FindProvidersAsync(ctx, cid, count), nil
		},
		func() bool {
			if count == 0 {
				return false
			}
			return atomic.AddInt64(&totalCount, 1) >= int64(count)
		},
	)

	return ch
}

// FindPeer will execute all Routers in parallel, getting the first AddrInfo found and cancelling all other Router calls.
func (r *composableParallel) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	return getValueOrErrorParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (peer.AddrInfo, bool, error) {
			addr, err := r.FindPeer(ctx, id)
			return addr, addr.ID == "", err
		},
	)
}

// PutValue will execute all Routers in parallel. If a Router fails and IgnoreError flag is not set, the whole execution will fail.
// Some Puts before the failure might be successful, even if we return an error.
func (r *composableParallel) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	return executeParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.PutValue(ctx, key, val, opts...)
		},
	)
}

// GetValue will execute all Routers in parallel. The first value found will be returned, cancelling all other executions.
func (r *composableParallel) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	return getValueOrErrorParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) ([]byte, bool, error) {
			val, err := r.GetValue(ctx, key, opts...)
			return val, len(val) == 0, err
		})
}

func (r *composableParallel) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	return getChannelOrErrorParallel(
		ctx,
		r.routers,
		func(ctx context.Context, r routing.Routing) (<-chan []byte, error) {
			return r.SearchValue(ctx, key, opts...)
		},
		func() bool { return false },
	)
}

func (r *composableParallel) Bootstrap(ctx context.Context) error {
	return executeParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.Bootstrap(ctx)
		})
}

func withCancelAndOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout != 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return context.WithCancel(ctx)
}

func getValueOrErrorParallel[T any](
	ctx context.Context,
	routers []*ParallelRouter,
	f func(context.Context, routing.Routing) (T, bool, error),
) (value T, err error) {
	outCh := make(chan T)
	errCh := make(chan error)

	// global cancel context to stop early other router's execution.
	ctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	fwg := jsync.NewFWaitGroup(func() {
		close(outCh)
		close(errCh)
		log.Debug("getValueOrErrorParallel: finished executing all routers ", len(routers))
	}, uint64(len(routers)))
	for _, r := range routers {
		go func(r *ParallelRouter) {
			defer fwg.Done()
			log.Debug("getValueOrErrorParallel: starting execution for router ", r.Router,
				" with timeout ", r.Timeout,
				" and ignore errors ", r.IgnoreError,
			)
			tim := time.NewTimer(r.ExecuteAfter)
			defer tim.Stop()
			select {
			case <-ctx.Done():
			case <-tim.C:
				ctx, cancel := withCancelAndOptionalTimeout(ctx, r.Timeout)
				defer cancel()
				value, empty, err := f(ctx, r.Router)
				if err != nil &&
					!errors.Is(err, routing.ErrNotFound) &&
					!r.IgnoreError {
					log.Debug("getValueOrErrorParallel: error calling router function for router ", r.Router,
						" with timeout ", r.Timeout,
						" and ignore errors ", r.IgnoreError,
						" with error ", err,
					)
					select {
					case <-ctx.Done():
					case errCh <- err:
					}
					return
				}
				if empty {
					log.Debug("getValueOrErrorParallel: empty flag for router ", r.Router,
						" with timeout ", r.Timeout,
						" and ignore errors ", r.IgnoreError,
					)
					return
				}
				select {
				case <-ctx.Done():
					return
				case outCh <- value:
				}
			}
		}(r)
	}

	select {
	case out, ok := <-outCh:
		if !ok {
			return value, routing.ErrNotFound
		}

		log.Debug("getValueOrErrorParallel: value returned by channel")

		return out, nil
	case err, ok := <-errCh:
		if !ok {
			return value, routing.ErrNotFound
		}

		log.Debug("getValueOrErrorParallel: error returned by channel:", err)

		return value, err
	case <-ctx.Done():
		err := ctx.Err()
		log.Debug("getValueOrErrorParallel: error on context done:", err)
		return value, err
	}
}

func executeParallel(
	ctx context.Context,
	routers []*ParallelRouter,
	f func(context.Context, routing.Routing) error,
) error {
	var errsLk sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	wg.Add(len(routers))

	for _, r := range routers {
		go func(r *ParallelRouter, ctx context.Context) {
			defer wg.Done()

			if err := func() error {
				log.Debug("executeParallel: starting execution for router ", r.Router,
					" with timeout ", r.Timeout,
					" and ignore errors ", r.IgnoreError,
				)
				tim := time.NewTimer(r.ExecuteAfter)
				defer tim.Stop()
				select {
				case <-ctx.Done():
					if !r.IgnoreError {
						log.Debug("executeParallel: stopping execution on router on context done for router ", r.Router,
							" with timeout ", r.Timeout,
							" and ignore errors ", r.IgnoreError,
						)
						return ctx.Err()
					}
				case <-tim.C:
					ctx, cancel := withCancelAndOptionalTimeout(ctx, r.Timeout)
					defer cancel()

					log.Debug("executeParallel: calling router function for router ", r.Router,
						" with timeout ", r.Timeout,
						" and ignore errors ", r.IgnoreError,
					)
					if err := f(ctx, r.Router); err != nil && !r.IgnoreError {
						log.Debug("executeParallel: error calling router function for router ", r.Router,
							" with timeout ", r.Timeout,
							" and ignore errors ", r.IgnoreError,
							" with error ", err,
						)
						return err
					}
				}

				return nil
			}(); err != nil {
				errsLk.Lock()
				defer errsLk.Unlock()
				errs = append(errs, err)
			}
		}(r, ctx)
	}

	wg.Wait()
	errOut := multierr.Combine(errs...)

	if errOut != nil {
		log.Debug("executeParallel: finished executing all routers with error: ", errOut)
	}

	return errOut
}

func getChannelOrErrorParallel[T any](
	ctx context.Context,
	routers []*ParallelRouter,
	f func(context.Context, routing.Routing) (<-chan T, error),
	shouldStop func() bool,
) (chan T, error) {
	var ready sync.Mutex
	ready.Lock()
	var outCh chan T
	var errorsLk sync.Mutex
	// nil errors indicate sucess
	errors := []error{}

	ctx, cancelAll := context.WithCancel(ctx)
	fwg := jsync.NewFWaitGroup(func() {
		if outCh != nil {
			close(outCh)
		} else {
			ready.Unlock()
		}

		cancelAll()
	}, uint64(len(routers)))

	var blocking atomic.Uint64
	blocking.Add(1) // start at one so we don't cancel while dispatching
	var sent atomic.Bool

	for _, r := range routers {
		if !r.DoNotWaitForStreamingResponses {
			blocking.Add(1)
		}

		go func(r *ParallelRouter) {
			defer fwg.Done()
			defer func() {
				var remainingBlockers uint64
				if r.DoNotWaitForStreamingResponses {
					remainingBlockers = blocking.Load()
				} else {
					var minusOne uint64
					minusOne--
					remainingBlockers = blocking.Add(minusOne)
				}

				if remainingBlockers == 0 && sent.Load() {
					cancelAll()
				}
			}()

			if r.ExecuteAfter != 0 {
				tim := time.NewTimer(r.ExecuteAfter)
				defer tim.Stop()
				select {
				case <-ctx.Done():
					return
				case <-tim.C:
					// ready
				}
			}

			ctx, cancel := withCancelAndOptionalTimeout(ctx, r.Timeout)
			defer cancel()

			valueChan, err := f(ctx, r.Router)
			if err != nil {
				if r.IgnoreError {
					return
				}

				errorsLk.Lock()
				defer errorsLk.Unlock()
				if errors == nil {
					return
				}
				errors = append(errors, err)

				return
			}

			for first := true; true; first = false {
				select {
				case <-ctx.Done():
					return
				case val, ok := <-valueChan:
					if !ok {
						return
					}

					if first {
						errorsLk.Lock()
						if outCh == nil {
							outCh = make(chan T)
							errors = nil
							ready.Unlock()
						}
						errorsLk.Unlock()
					}

					select {
					case <-ctx.Done():
						return
					case outCh <- val:
						sent.Store(true)
					}

					if shouldStop() {
						cancelAll()
						return
					}
				}
			}
		}(r)
	}

	// remove the dispatch count and check if we should cancel
	var minusOne uint64
	minusOne--
	if blocking.Add(minusOne) == 0 && sent.Load() {
		cancelAll()
	}

	ready.Lock()
	if outCh != nil {
		return outCh, nil
	} else if len(errors) == 0 {
		// found nothing
		ch := make(chan T)
		close(ch)
		return ch, nil
	} else {
		return nil, multierr.Combine(errors...)
	}
}
