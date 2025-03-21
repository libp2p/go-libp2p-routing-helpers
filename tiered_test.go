package routinghelpers

import (
	"bytes"
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/routing"
)

type testValidator struct{}

func (testValidator) Validate(key string, value []byte) error {
	ns, k, err := record.SplitKey(key)
	if err != nil {
		return err
	}
	if ns != "namespace" {
		return record.ErrInvalidRecordType
	}
	if !bytes.Contains(value, []byte(k)) {
		return record.ErrInvalidRecordType
	}
	if bytes.Contains(value, []byte("invalid")) {
		return record.ErrInvalidRecordType
	}
	return nil

}

func (testValidator) Select(key string, vals [][]byte) (int, error) {
	if len(vals) == 0 {
		panic("selector with no values")
	}
	var best []byte
	idx := 0
	for i, val := range vals {
		if bytes.Compare(best, val) < 0 {
			best = val
			idx = i
		}
	}
	return idx, nil
}

func TestTieredSearch(t *testing.T) {
	t.Parallel()

	d := Tiered{
		Validator: testValidator{},
		Routers: []routing.Routing{
			Null{},
			&Compose{
				ValueStore:     new(dummyValueStore),
				ContentRouting: Null{},
				PeerRouting:    Null{},
			},
			&Compose{
				ValueStore:     new(dummyValueStore),
				ContentRouting: Null{},
				PeerRouting:    Null{},
			},
			Null{},
			&Compose{},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := d.Routers[1].PutValue(ctx, "/namespace/v1", []byte("v1 - 1")); err != nil {
		t.Fatal(err)
	}

	valch, err := d.SearchValue(ctx, "/namespace/v1")
	if err != nil {
		t.Fatal(err)
	}

	v, ok := <-valch
	if !ok {
		t.Fatal("expected to get a value")
	}
	if string(v) != "v1 - 1" {
		t.Fatalf("unexpected value: %s", string(v))
	}
	_, ok = <-valch
	if ok {
		t.Fatal("didn't expect a value")
	}

	if err := d.Routers[2].PutValue(ctx, "/namespace/v1", []byte("v1 - 2")); err != nil {
		t.Fatal(err)
	}
	valch, err = d.SearchValue(ctx, "/namespace/v1")
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := <-valch; !ok {
		t.Fatal("expected to get a value")
	}

	v, ok = <-valch
	if ok {
		if string(v) != "v1 - 2" {
			t.Fatalf("unexpected value: %s", string(v))
		}

		_, ok = <-valch
		if ok {
			t.Fatal("didn't expect a value")
		}
	}

}

func TestTieredGet(t *testing.T) {
	t.Parallel()

	d := Tiered{
		Routers: []routing.Routing{
			Null{},
			&Compose{
				ValueStore:     new(dummyValueStore),
				ContentRouting: Null{},
				PeerRouting:    Null{},
			},
			&Compose{
				ValueStore:     new(dummyValueStore),
				ContentRouting: Null{},
				PeerRouting:    Null{},
			},
			&Compose{
				ValueStore:     new(dummyValueStore),
				ContentRouting: Null{},
				PeerRouting:    Null{},
			},
			Null{},
			&Compose{},
		},
	}
	ctx := context.Background()
	if err := d.Routers[1].PutValue(ctx, "k1", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := d.Routers[2].PutValue(ctx, "k2", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := d.Routers[2].PutValue(ctx, "k1", []byte("v1shadow")); err != nil {
		t.Fatal(err)
	}
	if err := d.Routers[3].PutValue(ctx, "k3", []byte("v3")); err != nil {
		t.Fatal(err)
	}

	for k, v := range map[string]string{
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
	} {
		actual, err := d.GetValue(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		if string(actual) != v {
			t.Errorf("expected %s, got %s", v, string(actual))
		}
	}
	if _, err := d.GetValue(ctx, "missing"); err != routing.ErrNotFound {
		t.Fatal("wrong error: ", err)
	}

	if err := d.PutValue(ctx, "key", []byte("value")); err != nil {
		t.Fatal(err)
	}

	if _, err := d.GetValue(ctx, "/error/myErr"); !errContains(err, "myErr") {
		t.Fatalf("expected error to contain myErr, got: %s", err)
	}

	if _, err := (Tiered{Routers: []routing.Routing{d.Routers[1]}}).GetValue(ctx, "/error/myErr"); !errContains(err, "myErr") {
		t.Fatalf("expected error to contain myErr, got: %s", err)
	}

	for _, di := range append([]routing.Routing{d}, d.Routers[1:len(d.Routers)-2]...) {
		v, err := di.GetValue(ctx, "key")
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != "value" {
			t.Errorf("expected value, got %s", string(v))
		}
	}
}

func TestTieredNoSupport(t *testing.T) {
	t.Parallel()

	d := Tiered{Routers: []routing.Routing{Tiered{Routers: []routing.Routing{Null{}}}}}
	if _, ok := <-d.FindProvidersAsync(context.Background(), cid.Cid{}, 0); ok {
		t.Fatal("shouldn't have found a provider")
	}
}

func TestTieredClose(t *testing.T) {
	t.Parallel()

	closer := new(testCloser)
	d := Tiered{
		Routers: []routing.Routing{
			struct {
				*testCloser
				routing.Routing
			}{closer, Null{}},
		},
	}
	d.Close()
	if closer.closed != 1 {
		t.Fatal("expected one close")
	}
}
