package routinghelpers

import (
	"context"
	"testing"
)

type bootstrapRouter struct {
	Null
	bs func() error
}

func (bs *bootstrapRouter) Bootstrap(ctx context.Context) error {
	return bs.bs()
}

func TestBootstrap(t *testing.T) {
	pings := make([]bool, 6)
	d := Parallel{
		Serial{
			&bootstrapRouter{
				bs: func() error {
					pings[0] = true
					return nil
				},
			},
		},
		Serial{
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
