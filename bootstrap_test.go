package routinghelpers

import (
	"context"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/routing"
)

type bootstrapRouter struct {
	Null
	bs func() error
}

func (bs *bootstrapRouter) Bootstrap(ctx context.Context) error {
	return bs.bs()
}

func TestBootstrap(t *testing.T) {
	t.Parallel()

	pings := make([]bool, 6)
	d := Parallel{
		Routers: []routing.Routing{
			Tiered{
				Routers: []routing.Routing{
					&bootstrapRouter{
						bs: func() error {
							pings[0] = true
							return nil
						},
					},
				},
			},
			Tiered{
				Routers: []routing.Routing{
					&bootstrapRouter{
						bs: func() error {
							pings[1] = true
							return nil
						},
					},
					&bootstrapRouter{
						bs: func() error {
							pings[2] = true
							return nil
						},
					},
				},
			},
			&Compose{
				ValueStore: &LimitedValueStore{
					ValueStore: &bootstrapRouter{
						bs: func() error {
							pings[3] = true
							return nil
						},
					},
					Namespaces: []string{"allow1", "allow2", "notsupported", "error"},
				},
			},
			&Compose{
				ValueStore: &LimitedValueStore{
					ValueStore: &dummyValueStore{},
				},
			},
			Null{},
			&Compose{},
			&Compose{
				ContentRouting: &bootstrapRouter{
					bs: func() error {
						pings[4] = true
						return nil
					},
				},
				PeerRouting: &bootstrapRouter{
					bs: func() error {
						pings[5] = true
						return nil
					},
				},
			},
		},
	}
	ctx := context.Background()
	if err := d.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	for i, p := range pings {
		if !p {
			t.Errorf("pings %d not seen", i)
		}
	}

}
func TestBootstrapErr(t *testing.T) {
	t.Parallel()

	d := Parallel{
		Routers: []routing.Routing{
			Tiered{
				Routers: []routing.Routing{
					&bootstrapRouter{
						bs: func() error {
							return errors.New("err1")
						},
					},
				},
			},
			Tiered{
				Routers: []routing.Routing{
					&bootstrapRouter{
						bs: func() error {
							return nil
						},
					},
					&bootstrapRouter{
						bs: func() error {
							return nil
						},
					},
				},
			},
			&Compose{
				ValueStore: &LimitedValueStore{
					ValueStore: &bootstrapRouter{
						bs: func() error {
							return errors.New("err2")
						},
					},
					Namespaces: []string{"allow1", "allow2", "notsupported", "error"},
				},
			},
			&Compose{
				ValueStore: &bootstrapRouter{
					bs: func() error {
						return errors.New("err3")
					},
				},
				ContentRouting: &bootstrapRouter{
					bs: func() error {
						return errors.New("err4")
					},
				},
			},
			Null{},
		},
	}
	ctx := context.Background()
	err := d.Bootstrap(ctx)
	t.Log(err)
	for _, s := range []string{"err1", "err2", "err3", "err4"} {
		if !errContains(err, s) {
			t.Errorf("expecting error to contain '%s'", s)
		}
	}
}
