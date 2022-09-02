package routinghelpers

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/stretchr/testify/require"
)

func TestComposableParallelFixtures(t *testing.T) {
	fixtures := []struct {
		Name             string
		routers          []*ParallelRouter
		GetValueFixtures []struct {
			isError bool
			key     string
			value   string
		}
		PutValueFixtures []struct {
			isError bool
			key     string
			value   string
		}
		FindPeerFixtures []struct {
			peerID  string
			isError bool
		}
	}{
		{
			Name: "simple two routers, one with delay",
			routers: []*ParallelRouter{
				{
					Timeout:     1 * time.Second,
					IgnoreError: false,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"a", "b", "c"}, []string{"av", "bv", "cv"}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid2"}),
						ContentRouting: Null{},
					},
				},
				{
					Timeout:      time.Hour,
					IgnoreError:  false,
					ExecuteAfter: 1 * time.Second,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"a", "d"}, []string{"av2", "dv"}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid3"}),
						ContentRouting: Null{},
					},
				},
			},
			GetValueFixtures: []struct {
				isError bool
				key     string
				value   string
			}{
				{
					isError: false,
					key:     "d",
					value:   "dv",
				},
				{
					isError: false,
					key:     "a",
					value:   "av",
				},
			},
			PutValueFixtures: []struct {
				isError bool
				key     string
				value   string
			}{
				{
					isError: true,
					key:     "/error/a",
					value:   "a",
				},
				{
					isError: false,
					key:     "a",
					value:   "a",
				},
			},
			FindPeerFixtures: []struct {
				peerID  string
				isError bool
			}{
				{
					peerID:  "pid1",
					isError: false,
				},
				{
					peerID:  "pid3",
					isError: false,
				},
			},
		},
		{
			Name: "two routers with ignore errors",
			routers: []*ParallelRouter{
				{
					Timeout:     0,
					IgnoreError: true,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{}, []string{}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid2"}),
						ContentRouting: Null{},
					},
				},
				{
					Timeout:      time.Hour,
					IgnoreError:  true,
					ExecuteAfter: 1 * time.Second,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"d"}, []string{"dv"}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid3"}),
						ContentRouting: Null{},
					},
				},
			},
			GetValueFixtures: []struct {
				isError bool
				key     string
				value   string
			}{
				{
					isError: false,
					key:     "d",
					value:   "dv",
				},
				{
					isError: true,
					key:     "a",
				},
			},
			PutValueFixtures: []struct {
				isError bool
				key     string
				value   string
			}{
				{
					isError: false,
					key:     "/error/x",
					value:   "xv",
				},
				{
					isError: false,
					key:     "/error/y",
					value:   "yv",
				},
			},
			FindPeerFixtures: []struct {
				peerID  string
				isError bool
			}{
				{
					peerID:  "pid1",
					isError: false,
				},
				{
					peerID:  "pid4",
					isError: true,
				},
			},
		},
		{
			Name: "two routers with ignore errors no delay",
			routers: []*ParallelRouter{
				{
					Timeout:     0,
					IgnoreError: true,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"a"}, []string{"av"}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid2"}),
						ContentRouting: Null{},
					},
				},
				{
					Timeout:     time.Hour,
					IgnoreError: true,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"d", "e"}, []string{"dv", "ev"}),
						PeerRouting:    newDummyPeerRouting(t, []peer.ID{"pid1", "pid3"}),
						ContentRouting: Null{},
					},
				},
			},
			GetValueFixtures: []struct {
				isError bool
				key     string
				value   string
			}{
				{
					isError: false,
					key:     "d",
					value:   "dv",
				},
				{
					isError: false,
					key:     "a",
					value:   "av",
				},
				{
					isError: true,
					key:     "/error/a",
				},
				{
					isError: true,
					key:     "/error/e",
				},
			},
			PutValueFixtures: []struct {
				isError bool
				key     string
				value   string
			}{
				{
					isError: false,
					key:     "/error/x",
					value:   "xv",
				},
				{
					isError: false,
					key:     "/error/y",
					value:   "yv",
				},
			},
			FindPeerFixtures: []struct {
				peerID  string
				isError bool
			}{
				{
					peerID:  "pid1",
					isError: false,
				},
				{
					peerID:  "pid4",
					isError: true,
				},
			},
		},
		{
			Name: "two routers one value store failing always",
			routers: []*ParallelRouter{
				{
					Timeout:     0,
					IgnoreError: false,
					Router: &Compose{
						ValueStore:     failValueStore{},
						PeerRouting:    Null{},
						ContentRouting: Null{},
					},
				},
				{
					Timeout:      time.Hour,
					IgnoreError:  false,
					ExecuteAfter: 1 * time.Hour,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"d", "e"}, []string{"dv", "ev"}),
						PeerRouting:    Null{},
						ContentRouting: Null{},
					},
				},
			},
			GetValueFixtures: []struct {
				isError bool
				key     string
				value   string
			}{
				{
					isError: true,
					key:     "d",
					value:   "dv",
				},
				{
					isError: true,
					key:     "a",
					value:   "av",
				},
			},
		},
		{
			Name: "two routers one value store failing always but ignored",
			routers: []*ParallelRouter{
				{
					Timeout:     0,
					IgnoreError: true,
					Router: &Compose{
						ValueStore:     failValueStore{},
						PeerRouting:    Null{},
						ContentRouting: Null{},
					},
				},
				{
					Timeout:     time.Hour,
					IgnoreError: false,
					Router: &Compose{
						ValueStore:     newDummyValueStore(t, []string{"d", "e"}, []string{"dv", "ev"}),
						PeerRouting:    Null{},
						ContentRouting: Null{},
					},
				},
			},
			GetValueFixtures: []struct {
				isError bool
				key     string
				value   string
			}{
				{
					isError: false,
					key:     "d",
					value:   "dv",
				},
				{
					isError: true,
					key:     "a",
					value:   "av",
				},
			},
		},
	}

	for _, f := range fixtures {
		t.Run(f.Name, func(t *testing.T) {
			require := require.New(t)
			cpr := NewComposableParallel(f.routers)
			for _, gvf := range f.GetValueFixtures {
				val, err := cpr.GetValue(context.Background(), gvf.key)
				if gvf.isError {
					require.Error(err)
					continue
				} else {
					require.NoError(err)
				}

				require.Equal(gvf.value, string(val))
			}

			for _, pvf := range f.PutValueFixtures {
				err := cpr.PutValue(context.Background(), pvf.key, []byte(pvf.value))
				if pvf.isError {
					require.Error(err)
					continue
				} else {
					require.NoError(err)
				}
			}

			for _, fpf := range f.FindPeerFixtures {
				addr, err := cpr.FindPeer(context.Background(), peer.ID(fpf.peerID))
				if fpf.isError {
					require.Error(err)
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
