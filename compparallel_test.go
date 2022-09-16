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

func TestComposableParallelFixtures(t *testing.T) {
	fixtures := []struct {
		Name             string
		routers          []*ParallelRouter
		GetValueFixtures []struct {
			err            error
			key            string
			value          string
			searchValCount int
		}
		PutValueFixtures []struct {
			err   error
			key   string
			value string
		}
		ProvideFixtures []struct {
			err error
		}
		FindPeerFixtures []struct {
			peerID string
			err    error
		}
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
			GetValueFixtures: []struct {
				err            error
				key            string
				value          string
				searchValCount int
			}{
				{
					key:            "d",
					value:          "dv",
					searchValCount: 1,
				},
				{
					key:            "a",
					value:          "av",
					searchValCount: 2,
				},
			},
			PutValueFixtures: []struct {
				err   error
				key   string
				value string
			}{
				{
					err:   errors.New("2 errors occurred:\n\t* a\n\t* a\n\n"),
					key:   "/error/a",
					value: "a",
				},
				{
					key:   "a",
					value: "a",
				},
			},
			ProvideFixtures: []struct {
				err error
			}{
				{
					err: errors.New("2 errors occurred:\n\t* routing: operation or key not supported\n\t* routing: operation or key not supported\n\n"),
				},
			},
			FindPeerFixtures: []struct {
				peerID string
				err    error
			}{
				{
					peerID: "pid1",
				},
				{
					peerID: "pid3",
				},
			},
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
			GetValueFixtures: []struct {
				err            error
				key            string
				value          string
				searchValCount int
			}{
				{
					key:            "d",
					value:          "dv",
					searchValCount: 1,
				},
				{
					err: routing.ErrNotFound, // even ignoring errors, if the value is not found we return not found
					key: "a",
				},
			},
			PutValueFixtures: []struct {
				err   error
				key   string
				value string
			}{
				{
					key:   "/error/x",
					value: "xv",
				},
				{
					key:   "/error/y",
					value: "yv",
				},
			},
			FindPeerFixtures: []struct {
				peerID string
				err    error
			}{
				{
					peerID: "pid1",
				},
				{
					err:    routing.ErrNotFound, // even ignoring errors, if the value is not found we return not found
					peerID: "pid4",
				},
			},
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
			GetValueFixtures: []struct {
				err            error
				key            string
				value          string
				searchValCount int
			}{
				{
					key:            "d",
					value:          "dv",
					searchValCount: 1,
				},
				{
					key:            "a",
					value:          "av",
					searchValCount: 1,
				},
				{
					err: routing.ErrNotFound,
					key: "/error/z",
				},
				{
					err: routing.ErrNotFound,
					key: "/error/y",
				},
			},
			PutValueFixtures: []struct {
				err   error
				key   string
				value string
			}{
				{
					key:   "/error/x",
					value: "xv",
				},
				{
					key:   "/error/y",
					value: "yv",
				},
			},
			FindPeerFixtures: []struct {
				peerID string
				err    error
			}{
				{
					peerID: "pid1",
				},
				{
					peerID: "pid4",
					err:    routing.ErrNotFound,
				},
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
			GetValueFixtures: []struct {
				err            error
				key            string
				value          string
				searchValCount int
			}{
				{
					err:   errFailValue,
					key:   "d",
					value: "dv",
				},
				{
					err:   errFailValue,
					key:   "a",
					value: "av",
				},
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
			GetValueFixtures: []struct {
				err            error
				key            string
				value          string
				searchValCount int
			}{
				{
					key:            "d",
					value:          "dv",
					searchValCount: 1,
				},
				{
					err:   routing.ErrNotFound,
					key:   "a",
					value: "av",
				},
			},
		},
	}

	for _, f := range fixtures {
		f := f
		t.Run(f.Name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)
			cpr := NewComposableParallel(f.routers)
			for _, gvf := range f.GetValueFixtures {
				val, err := cpr.GetValue(context.Background(), gvf.key)
				if gvf.err != nil {
					require.ErrorContains(err, gvf.err.Error())
					continue
				} else {
					require.NoError(err)
				}

				require.Equal(gvf.value, string(val))

				vals, err := cpr.SearchValue(context.Background(), gvf.key)
				if gvf.err != nil {
					require.ErrorContains(err, gvf.err.Error())
					continue
				} else {
					require.NoError(err)
				}

				count := 0
				for range vals {
					count++
				}

				require.Equal(gvf.searchValCount, count)
			}

			for _, pvf := range f.PutValueFixtures {
				err := cpr.PutValue(context.Background(), pvf.key, []byte(pvf.value))
				if pvf.err != nil {
					require.ErrorContains(err, pvf.err.Error())
					continue
				} else {
					require.NoError(err)
				}
			}

			for _, pf := range f.ProvideFixtures {
				err := cpr.Provide(context.Background(), cid.Cid{}, true)
				if pf.err != nil {
					require.ErrorContains(err, pf.err.Error())
					continue
				} else {
					require.NoError(err)
				}
			}

			for _, fpf := range f.FindPeerFixtures {
				addr, err := cpr.FindPeer(context.Background(), peer.ID(fpf.peerID))
				if fpf.err != nil {
					require.ErrorContains(err, fpf.err.Error())
					continue
				} else {
					require.NoError(err)
				}

				require.Equal(fpf.peerID, string(addr.ID))
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
