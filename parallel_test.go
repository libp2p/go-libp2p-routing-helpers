package routinghelpers

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/multierr"
)

// NOTE: While this test is primarily testing the Parallel combinator, it also
// mixes and matches other combiners for better coverage. Please don't simplify.

func TestParallelPutGet(t *testing.T) {
	d := Parallel{
		Routers: []routing.Routing{
			Parallel{
				Routers: []routing.Routing{
					&Compose{
						ValueStore: &LimitedValueStore{
							ValueStore: new(dummyValueStore),
							Namespaces: []string{"allow1", "allow2", "notsupported"},
						},
					},
				},
			},
			Tiered{
				Routers: []routing.Routing{
					&Compose{
						ValueStore: &LimitedValueStore{
							ValueStore: new(dummyValueStore),
							Namespaces: []string{"allow1", "allow2", "notsupported", "error"},
						},
					},
				},
			},
			&Compose{
				ValueStore: &LimitedValueStore{
					ValueStore: new(dummyValueStore),
					Namespaces: []string{"allow1", "error", "solo", "stall"},
				},
			},
			Parallel{
				Routers: []routing.Routing{&struct{ Compose }{}},
			},
			Tiered{
				Routers: []routing.Routing{
					&struct{ Compose }{},
				},
			},
			&struct{ Parallel }{},
			&struct{ Tiered }{},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := d.PutValue(ctx, "/allow1/hello", []byte("world")); err != nil {
		t.Fatal(err)
	}
	for _, di := range append([]routing.Routing{d}, d.Routers[:3]...) {
		v, err := di.GetValue(ctx, "/allow1/hello")
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != "world" {
			t.Fatal("got the wrong value")
		}
	}

	if err := d.PutValue(ctx, "/allow2/hello", []byte("world2")); err != nil {
		t.Fatal(err)
	}
	for _, di := range append([]routing.Routing{d}, d.Routers[:1]...) {
		v, err := di.GetValue(ctx, "/allow2/hello")
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != "world2" {
			t.Fatal("got the wrong value")
		}
	}
	if err := d.PutValue(ctx, "/forbidden/hello", []byte("world")); err != routing.ErrNotSupported {
		t.Fatalf("expected ErrNotSupported, got: %s", err)
	}
	for _, di := range append([]routing.Routing{d}, d.Routers...) {
		_, err := di.GetValue(ctx, "/forbidden/hello")
		if err != routing.ErrNotFound {
			t.Fatalf("expected ErrNotFound, got: %s", err)
		}
	}
	// Bypass the LimitedValueStore.
	if err := d.PutValue(ctx, "/notsupported/hello", []byte("world")); err != routing.ErrNotSupported {
		t.Fatalf("expected ErrNotSupported, got: %s", err)
	}
	if err := d.PutValue(ctx, "/error/myErr", []byte("world")); !errContains(err, "myErr") {
		t.Fatalf("expected error to contain myErr, got: %s", err)
	}
	if _, err := d.GetValue(ctx, "/error/myErr"); !errContains(err, "myErr") {
		t.Fatalf("expected error to contain myErr, got: %s", err)
	}
	if err := d.PutValue(ctx, "/solo/thing", []byte("value")); err != nil {
		t.Fatal(err)
	}
	v, err := d.GetValue(ctx, "/solo/thing")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "value" {
		t.Fatalf("expected 'value', got '%s'", string(v))
	}

	ctxt, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	if _, err := d.GetValue(ctxt, "/stall/bla"); err != context.DeadlineExceeded {
		t.Error(err)
	}
	cancel()

	ctxt, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
	if err := d.PutValue(ctxt, "/stall/bla", []byte("bla")); err != context.DeadlineExceeded {
		t.Error(err)
	}
	cancel()
}

func errContains(err error, substr string) bool {
	for _, e := range multierr.Errors(err) {
		if strings.Contains(e.Error(), substr) {
			return true
		}
	}
	return false
}

func TestParallelPutFailure(t *testing.T) {
	ctx := context.Background()
	router := Parallel{
		Routers: []routing.Routing{
			&Compose{
				ValueStore: new(failValueStore),
			},
			&Compose{
				ValueStore: new(dummyValueStore),
			},
		},
	}
	err := router.PutValue(ctx, "/some/thing", []byte("thing"))
	if err != errFailValue {
		t.Fatalf("exected put to fail with %q, got %q", errFailValue, err)
	}
}

func TestBasicParallelFindProviders(t *testing.T) {
	prefix := cid.NewPrefixV1(cid.Raw, mh.SHA2_256)
	c, _ := prefix.Sum([]byte("foo"))

	ctx := context.Background()

	d := Parallel{}
	if _, ok := <-d.FindProvidersAsync(ctx, c, 10); ok {
		t.Fatal("expected no results")
	}
	d = Parallel{
		Routers: []routing.Routing{
			&Compose{
				ContentRouting: &dummyProvider{},
			},
		},
	}
	if _, ok := <-d.FindProvidersAsync(ctx, c, 10); ok {
		t.Fatal("expected no results")
	}
}

func TestParallelFindProviders(t *testing.T) {
	prefix := cid.NewPrefixV1(cid.Raw, mh.SHA2_256)

	cid1, _ := prefix.Sum([]byte("foo"))
	cid2, _ := prefix.Sum([]byte("bar"))
	cid3, _ := prefix.Sum([]byte("baz"))
	cid4, _ := prefix.Sum([]byte("none"))
	cid5, _ := prefix.Sum([]byte("stall"))

	d := Parallel{
		Routers: []routing.Routing{
			Parallel{
				Routers: []routing.Routing{
					&Compose{},
				},
			},
			Tiered{
				Routers: []routing.Routing{
					&Compose{},
					&struct{ Compose }{},
				},
			},
			&struct{ Compose }{},
			Null{},
			Tiered{
				Routers: []routing.Routing{
					&Compose{
						ContentRouting: dummyProvider{
							cid1: []peer.ID{
								"first",
								"second",
								"third",
								"fourth",
								"fifth",
								"sixth",
							},
							cid2: []peer.ID{
								"fourth",
								"fifth",
								"sixth",
							},
							cid5: []peer.ID{
								"before",
								"stall",
								"after",
							},
						},
					},
				},
			},
			Parallel{
				Routers: []routing.Routing{
					Null{},
					&Compose{
						ContentRouting: dummyProvider{
							cid1: []peer.ID{
								"first",
								"second",
								"fifth",
								"sixth",
							},
							cid2: []peer.ID{
								"second",
								"fourth",
								"fifth",
							},
						},
					},
				},
			},
			&Compose{
				ValueStore: &LimitedValueStore{
					ValueStore: new(dummyValueStore),
					Namespaces: []string{"allow1"},
				},
				ContentRouting: dummyProvider{
					cid2: []peer.ID{
						"first",
					},
					cid3: []peer.ID{
						"second",
						"fourth",
						"fifth",
						"sixth",
					},
				},
			},
		},
	}

	ctx := context.Background()

	for i := 0; i < 2; i++ {

		for i, tc := range []struct {
			cid       cid.Cid
			providers []peer.ID
		}{
			{
				cid:       cid1,
				providers: []peer.ID{"first", "second", "third", "fourth", "fifth", "sixth"},
			},
			{
				cid:       cid2,
				providers: []peer.ID{"first", "second", "fourth", "fifth", "sixth"},
			},
			{
				cid:       cid3,
				providers: []peer.ID{"second", "fourth", "fifth", "sixth"},
			},
		} {
			expecting := make(map[peer.ID]struct{}, len(tc.providers))
			for _, p := range tc.providers {
				expecting[p] = struct{}{}
			}
			for p := range d.FindProvidersAsync(ctx, tc.cid, 10) {
				if _, ok := expecting[p.ID]; !ok {
					t.Errorf("not expecting provider %s for test case %d", string(p.ID), i)
				}
				delete(expecting, p.ID)
			}
			for p := range expecting {
				t.Errorf("failed to find expected provider %s for test case %d", string(p), i)
			}
		}
		expecting := []peer.ID{"second", "fourth", "fifth"}
		for p := range d.FindProvidersAsync(ctx, cid3, 3) {
			if len(expecting) == 0 {
				t.Errorf("not expecting any more providers, got %s", string(p.ID))
				continue
			}
			if expecting[0] != p.ID {
				t.Errorf("expecting peer %s, got peer %s", string(expecting[0]), string(p.ID))
			}
			expecting = expecting[1:]
		}
		for _, e := range expecting {
			t.Errorf("didn't find expected peer: %s", string(e))
		}
		if _, ok := <-d.FindProvidersAsync(ctx, cid4, 3); ok {
			t.Fatalf("shouldn't have found this CID")
		}
		count := 0
		for range d.FindProvidersAsync(ctx, cid1, 0) {
			count++
		}
		if count != 6 {
			t.Fatalf("should have found 6 peers, found %d", count)
		}

		ctxt, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		stallCh := d.FindProvidersAsync(ctxt, cid5, 5)
		if v := <-stallCh; v.ID != "before" {
			t.Fatal("expected peer 'before'")
		}
		if _, ok := <-stallCh; ok {
			t.Fatal("expected stall and close")
		}
		cancel()

		ctxt, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
		stallCh = d.FindProvidersAsync(ctxt, cid1, 10)
		time.Sleep(100 * time.Millisecond)
		if _, ok := <-stallCh; ok {
			t.Fatal("expected channel to have been closed")
		}
		cancel()

		// Now to test many content routers
		for i := 0; i < 30; i++ {
			d.Routers = append(d.Routers, &Compose{
				ContentRouting: &dummyProvider{},
			})
		}
	}
}

func TestParallelFindPeer(t *testing.T) {
	d := Parallel{
		Routers: []routing.Routing{
			Null{},
			Parallel{
				Routers: []routing.Routing{
					Null{},
					Null{},
				},
			},
			Tiered{
				Routers: []routing.Routing{
					Null{},
					Null{},
				},
			},
			&struct{ Compose }{},
			Parallel{
				Routers: []routing.Routing{
					&Compose{
						PeerRouting: dummyPeerRouter{
							"first":  struct{}{},
							"second": struct{}{},
						},
					},
				},
			},
			Tiered{
				Routers: []routing.Routing{
					&Compose{
						PeerRouting: dummyPeerRouter{
							"first": struct{}{},
							"third": struct{}{},
						},
					},
				},
			},
			&Compose{
				PeerRouting: dummyPeerRouter{
					"first": struct{}{},
					"fifth": struct{}{},
				},
			},
		},
	}

	ctx := context.Background()

	for _, di := range append([]routing.Routing{d}, d.Routers[4:]...) {
		if _, err := di.FindPeer(ctx, "first"); err != nil {
			t.Fatal(err)
		}
	}

	for _, p := range []peer.ID{
		"first",
		"second",
		"third",
		"fifth",
	} {
		if _, err := d.FindPeer(ctx, p); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := d.FindPeer(ctx, "fourth"); err != routing.ErrNotFound {
		t.Fatal(err)
	}
}

func TestParallelProvide(t *testing.T) {
	prefix := cid.NewPrefixV1(cid.Raw, mh.SHA2_256)

	d := Parallel{
		Routers: []routing.Routing{
			Parallel{
				Routers: []routing.Routing{
					&Compose{
						ContentRouting: cbProvider(func(c cid.Cid, local bool) error {
							return routing.ErrNotSupported
						}),
					},
					&Compose{
						ContentRouting: cbProvider(func(c cid.Cid, local bool) error {
							return errors.New(c.String())
						}),
					},
				},
			},
			Tiered{
				Routers: []routing.Routing{
					&struct{ Compose }{},
					&Compose{},
					&Compose{},
				},
			},
		},
	}

	ctx := context.Background()

	cid1, _ := prefix.Sum([]byte("foo"))

	if err := d.Provide(ctx, cid1, false); err.Error() != cid1.String() {
		t.Fatal(err)
	}
}

func TestParallelClose(t *testing.T) {
	closer := new(testCloser)
	d := Parallel{
		Routers: []routing.Routing{
			struct {
				*testCloser
				routing.Routing
			}{closer, Null{}},
		},
	}
	d.Close()
	if closer.closed != 1 {
		t.Fatalf("expected one close, got %d", closer.closed)
	}
}
