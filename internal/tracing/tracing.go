// tracing provides high level method tracing for the [routing.Routing] API.
// Each method
package tracing

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("go-libp2p-routing-helpers").Start(ctx, name, opts...)
}

const base = multibase.Base64url

func bytesAsMultibase(b []byte) string {
	r, err := multibase.Encode(base, b)
	if err != nil {
		panic(fmt.Errorf("unreachable: %w", err))
	}
	return r
}

// keysAsMultibase avoids returning non utf8 which otel does not like.
func keysAsMultibase(name string, keys []multihash.Multihash) attribute.KeyValue {
	keysStr := make([]string, len(keys))
	for i, k := range keys {
		keysStr[i] = bytesAsMultibase(k)
	}
	return attribute.StringSlice(name, keysStr)
}

func Provide(routerName string, ctx context.Context, key cid.Cid, announce bool) (_ context.Context, end func(error)) {
	// outline so the concatenation can be folded at compile-time
	return provide(routerName+".Provide", ctx, key, announce)
}

func provide(traceName string, ctx context.Context, key cid.Cid, announce bool) (_ context.Context, end func(error)) {
	ctx, span := StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	span.SetAttributes(
		attribute.Stringer("key", key),
		attribute.Bool("announce", announce),
	)

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func ProvideMany(routerName string, ctx context.Context, keys []multihash.Multihash) (_ context.Context, end func(error)) {
	// outline so the concatenation can be folded at compile-time
	return provideMany(routerName+".ProvideMany", ctx, keys)
}

func provideMany(traceName string, ctx context.Context, keys []multihash.Multihash) (_ context.Context, end func(error)) {
	ctx, span := StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	span.SetAttributes(keysAsMultibase("keys", keys))

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func peerInfoToAttributes(p peer.AddrInfo) []attribute.KeyValue {
	strs := make([]string, len(p.Addrs))
	for i, v := range p.Addrs {
		strs[i] = v.String()
	}

	return []attribute.KeyValue{
		attribute.Stringer("id", p.ID),
		attribute.StringSlice("addrs", strs),
	}
}

func FindProvidersAsync(routerName string, ctx context.Context, key cid.Cid, count int) (_ context.Context, passthrough func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo) {
	// outline so the concatenation can be folded at compile-time
	return findProvidersAsync(routerName+".FindProvidersAsync", ctx, key, count)
}

func findProvidersAsync(traceName string, ctx context.Context, key cid.Cid, count int) (_ context.Context, passthrough func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo) {
	ctx, span := StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(c <-chan peer.AddrInfo, _ error) <-chan peer.AddrInfo { return c }
	}

	span.SetAttributes(
		attribute.Stringer("key", key),
		attribute.Int("count", count),
	)

	return ctx, func(in <-chan peer.AddrInfo, err error) <-chan peer.AddrInfo {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return in
		}

		span.AddEvent("started streaming")

		out := make(chan peer.AddrInfo)
		go func() {
			defer span.End()
			defer close(out)

			for v := range in {
				span.AddEvent("found provider", trace.WithAttributes(peerInfoToAttributes(v)...))
				out <- v
			}
		}()

		return out
	}
}

func FindPeer(routerName string, ctx context.Context, id peer.ID) (_ context.Context, end func(peer.AddrInfo, error)) {
	// outline so the concatenation can be folded at compile-time
	return findPeer(routerName+".FindPeer", ctx, id)
}

func findPeer(traceName string, ctx context.Context, id peer.ID) (_ context.Context, end func(peer.AddrInfo, error)) {
	ctx, span := StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(peer.AddrInfo, error) {}
	}

	span.SetAttributes(attribute.Stringer("key", id))

	return ctx, func(p peer.AddrInfo, err error) {
		defer span.End()

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return
		}

		span.AddEvent("found peer", trace.WithAttributes(peerInfoToAttributes(p)...))
	}
}

func PutValue(routerName string, ctx context.Context, key string, val []byte, opts ...routing.Option) (_ context.Context, end func(error)) {
	// outline so the concatenation can be folded at compile-time
	return putValue(routerName+".PutValue", ctx, key, val, opts...)
}

func putValue(traceName string, ctx context.Context, key string, val []byte, opts ...routing.Option) (_ context.Context, end func(error)) {
	ctx, span := StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	span.SetAttributes(
		attribute.String("key", bytesAsMultibase([]byte(key))),
		attribute.String("value", bytesAsMultibase(val)),
		attribute.Int("len(opts)", len(opts)),
	)

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func GetValue(routerName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, end func([]byte, error)) {
	// outline so the concatenation can be folded at compile-time
	return getValue(routerName+".GetValue", ctx, key, opts...)
}

func getValue(traceName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, end func([]byte, error)) {
	ctx, span := StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func([]byte, error) {}
	}

	span.SetAttributes(
		attribute.String("key", bytesAsMultibase([]byte(key))),
		attribute.Int("len(opts)", len(opts)),
	)

	return ctx, func(val []byte, err error) {
		defer span.End()

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return
		}

		span.AddEvent("found value", trace.WithAttributes(
			attribute.String("value", bytesAsMultibase(val))))
	}
}

func SearchValue(routerName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, passthrough func(<-chan []byte, error) (<-chan []byte, error)) {
	// outline so the concatenation can be folded at compile-time
	return searchValue(routerName+".SearchValue", ctx, key, opts...)
}

func searchValue(traceName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, passthrough func(<-chan []byte, error) (<-chan []byte, error)) {
	ctx, span := StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(c <-chan []byte, err error) (<-chan []byte, error) { return c, err }
	}

	span.SetAttributes(
		attribute.String("key", bytesAsMultibase([]byte(key))),
		attribute.Int("len(opts)", len(opts)),
	)

	return ctx, func(in <-chan []byte, err error) (<-chan []byte, error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return in, err
		}

		span.AddEvent("started streaming")

		out := make(chan []byte)
		go func() {
			defer span.End()
			defer close(out)

			for v := range in {
				span.AddEvent("found value", trace.WithAttributes(
					attribute.String("value", bytesAsMultibase(v))),
				)
				out <- v
			}
		}()

		return out, nil
	}
}

func Bootstrap(routerName string, ctx context.Context) (_ context.Context, end func(error)) {
	// outline so the concatenation can be folded at compile-time
	return bootstrap(routerName+".Bootstrap", ctx)
}

func bootstrap(traceName string, ctx context.Context) (_ context.Context, end func(error)) {
	ctx, span := StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}
