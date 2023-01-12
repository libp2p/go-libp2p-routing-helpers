package routinghelpers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/stretchr/testify/require"
)

func TestNoResults(t *testing.T) {
	require := require.New(t)
	rs := []*ParallelRouter{
		{
			Timeout:     time.Second,
			IgnoreError: true,
			Router:      Null{},
		},
		{
			Timeout:     time.Second,
			IgnoreError: true,
			Router: &Compose{
				ValueStore:     newDummyValueStore(t, []string{"a"}, []string{"av"}),
				PeerRouting:    Null{},
				ContentRouting: Null{},
			},
		},
	}

	cp := NewComposableParallel(rs)

	v, err := cp.GetValue(context.Background(), "a")
	require.NoError(err)
	require.Equal("av", string(v))

	require.Equal(2, len(cp.Routers()))
}

type getValueFixture struct {
	err            error
	key            string
	value          string
	searchValCount int
}

type putValueFixture struct {
	err   error
	key   string
	value string
}
type provideFixture struct {
	err error
}
type findPeerFixture struct {
	peerID string
	err    error
}
type searchValueFixture struct {
	ctx  context.Context
	err  error
	key  string
	vals []string
}

func TestComposableParallelFixtures(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	fixtures := []struct {
		Name        string
		routers     []*ParallelRouter
		GetValue    []getValueFixture
		PutValue    []putValueFixture
		Provide     []provideFixture
		FindPeer    []findPeerFixture
		SearchValue []searchValueFixture
	}{
		{
			Name: "simple two routers, one with delay",
			routers: []*ParallelRouter{
				{
					Timeout:     time.Second,
					IgnoreError: false,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"a", "b", "c"}, []string{"av", "bv", "cv"}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid2"}),
						ContentRouting: Null{},
					},
				},
				{
					Timeout:      time.Minute,
					IgnoreError:  false,
					ExecuteAfter: time.Second,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"a", "d"}, []string{"av2", "dv"}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid3"}),
						ContentRouting: Null{},
					},
				},
			},
			GetValue: []getValueFixture{
				{key: "d", value: "dv", searchValCount: 1},
				{key: "a", value: "av", searchValCount: 2},
			},
			PutValue: []putValueFixture{
				{err: errors.New("2 errors occurred:\n\t* a\n\t* a\n\n"), key: "/error/a", value: "a"},
				{key: "a", value: "a"},
			},
			Provide: []provideFixture{{
				err: errors.New("2 errors occurred:\n\t* routing: operation or key not supported\n\t* routing: operation or key not supported\n\n"),
			}},
			FindPeer:    []findPeerFixture{{peerID: "pid1"}, {peerID: "pid3"}},
			SearchValue: []searchValueFixture{{key: "a", vals: []string{"a", "a"}}},
		},
		{
			Name: "two routers with ignore errors",
			routers: []*ParallelRouter{
				{
					Timeout:     time.Second,
					IgnoreError: true,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{}, []string{}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid2"}),
						ContentRouting: Null{},
					},
				},
				{
					Timeout:      time.Minute,
					IgnoreError:  true,
					ExecuteAfter: time.Second,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"d"}, []string{"dv"}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid3"}),
						ContentRouting: Null{},
					},
				},
			},
			GetValue: []getValueFixture{
				{key: "d", value: "dv", searchValCount: 1},
				{err: routing.ErrNotFound, key: "a"}, // even ignoring errors, if the value is not found we return not found
			},
			PutValue: []putValueFixture{{key: "/error/x", value: "xv"}, {key: "/error/y", value: "yv"}},
			FindPeer: []findPeerFixture{
				{peerID: "pid1"},
				{err: routing.ErrNotFound, peerID: "pid4"}, // even ignoring errors, if the value is not found we return not found
			},
			SearchValue: []searchValueFixture{{key: "a", vals: nil}},
		},
		{
			Name: "two routers with ignore errors no delay",
			routers: []*ParallelRouter{
				{
					Timeout:     time.Second,
					IgnoreError: true,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"a"}, []string{"av"}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid2"}),
						ContentRouting: Null{},
					},
				},
				{
					Timeout:     time.Minute,
					IgnoreError: true,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"d", "e"}, []string{"dv", "ev"}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid3"}),
						ContentRouting: Null{},
					},
				},
			},
			GetValue: []getValueFixture{
				{key: "d", value: "dv", searchValCount: 1},
				{key: "a", value: "av", searchValCount: 1},
				{err: routing.ErrNotFound, key: "/error/z"},
				{err: routing.ErrNotFound, key: "/error/y"},
			},
			PutValue: []putValueFixture{
				{key: "/error/x", value: "xv"},
				{key: "/error/y", value: "yv"},
			},
			FindPeer: []findPeerFixture{
				{peerID: "pid1"},
				{peerID: "pid4", err: routing.ErrNotFound},
			},
		},
		{
			Name: "two routers one value store failing always",
			routers: []*ParallelRouter{
				{
					Timeout:     time.Second,
					IgnoreError: false,
					Router: &Compose{
						ValueStore:     failValueStore{},
						PeerRouting:    Null{},
						ContentRouting: Null{},
					},
				},
				{
					Timeout:      time.Minute,
					IgnoreError:  false,
					ExecuteAfter: time.Minute,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"d", "e"}, []string{"dv", "ev"}),
						PeerRouting:    Null{},
						ContentRouting: Null{},
					},
				},
			},
			GetValue: []getValueFixture{
				{err: errFailValue, key: "d", value: "dv"},
				{err: errFailValue, key: "a", value: "av"},
			},
		},
		{
			Name: "two routers one value store failing always but ignored",
			routers: []*ParallelRouter{
				{
					Timeout:     time.Second,
					IgnoreError: true,
					Router: &Compose{
						ValueStore:     failValueStore{},
						PeerRouting:    Null{},
						ContentRouting: Null{},
					},
				},
				{
					Timeout:     time.Second,
					IgnoreError: false,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"d", "e"}, []string{"dv", "ev"}),
						PeerRouting:    Null{},
						ContentRouting: Null{},
					},
				},
			},
			GetValue: []getValueFixture{
				{key: "d", value: "dv", searchValCount: 1},
				{err: routing.ErrNotFound, key: "a", value: "av"},
			},
		},
		{
			Name: "SearchValue returns an error",
			routers: []*ParallelRouter{{
				Timeout:     time.Second,
				IgnoreError: false,
				Router: &Compose{
					ValueStore:     newDummyValueStore(t, []string{"a", "a"}, []string{"b", "c"}),
					PeerRouting:    Null{},
					ContentRouting: Null{},
				},
			}},
			SearchValue: []searchValueFixture{{key: "a", ctx: canceledCtx, err: context.Canceled}},
		},
	}

	for _, f := range fixtures {
		f := f
		t.Run(f.Name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)
			cpr := NewComposableParallel(f.routers)
			for _, gvf := range f.GetValue {
				val, err := cpr.GetValue(context.Background(), gvf.key)
				if gvf.err != nil {
					require.ErrorContains(err, gvf.err.Error())
					continue
				}
				require.NoError(err)
				require.Equal(gvf.value, string(val))

				vals, err := cpr.SearchValue(context.Background(), gvf.key)
				if gvf.err != nil {
					require.ErrorContains(err, gvf.err.Error())
					continue
				}
				require.NoError(err)

				count := 0
				for range vals {
					count++
				}

				require.Equal(gvf.searchValCount, count)
			}

			for _, pvf := range f.PutValue {
				err := cpr.PutValue(context.Background(), pvf.key, []byte(pvf.value))
				if pvf.err != nil {
					require.ErrorContains(err, pvf.err.Error())
					continue
				}
				require.NoError(err)
			}

			for _, pf := range f.Provide {
				err := cpr.Provide(context.Background(), cid.Cid{}, true)
				if pf.err != nil {
					require.ErrorContains(err, pf.err.Error())
					continue
				}
				require.NoError(err)
			}

			for _, fpf := range f.FindPeer {
				addr, err := cpr.FindPeer(context.Background(), peer.ID(fpf.peerID))
				if fpf.err != nil {
					require.ErrorContains(err, fpf.err.Error())
					continue
				}
				require.NoError(err)
				require.Equal(fpf.peerID, string(addr.ID))
			}
			for _, svf := range f.SearchValue {
				ctx := context.Background()
				if svf.ctx != nil {
					ctx = svf.ctx
				}
				res, err := cpr.SearchValue(ctx, svf.key)
				if svf.err != nil {
					require.ErrorContains(err, svf.err.Error())

					// check that the result channel is responsive
					// ex if we accidentally return nil then result chan will hang when reading
					// it should either be closed or produce results
					timer := time.NewTimer(1 * time.Second)
					select {
					case <-res:
					case <-timer.C:
						t.Fatalf("result channel was unresponsive after an error occurred")
					}

					continue
				}
				require.NoError(err)

				var vals []string
				for v := range res {
					vals = append(vals, string(v))
				}
				require.Equal(svf.vals, vals)
			}
		})
	}
}

func newDummyPeerRouting(t testing.TB, ids []peer.ID) routing.PeerRouting {
	pr := dummyPeerRouter{}
	for _, id := range ids {
		pr[id] = struct{}{}
	}

	return pr
}

func newDummyValueStore(t testing.TB, keys []string, values []string) routing.ValueStore {
	t.Helper()

	if len(keys) != len(values) {
		t.Fatal("keys and values must be the same amount")
	}

	dvs := &dummyValueStore{}
	for i, k := range keys {
		v := values[i]
		err := dvs.PutValue(context.TODO(), k, []byte(v))
		if err != nil {
			t.Fatal(err)
		}
	}

	return dvs
}
