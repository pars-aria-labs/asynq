package rdb

import (
	"context"
	"sync/atomic"
)

type roundTripContextKey struct{}

type roundTripCounter struct {
	value  atomic.Int64
	parent *roundTripCounter
}

// WithRoundTripCounter returns a child context and a function that reports the
// number of instrumented Redis client execution attempts. A pipeline flush is
// counted once; retries and cluster fan-out performed inside go-redis are not
// visible and can involve more physical network round trips. It is internal so
// the public API can expose observations without leaking implementation details.
func WithRoundTripCounter(ctx context.Context) (context.Context, func() int) {
	counter := &roundTripCounter{}
	if parent, ok := ctx.Value(roundTripContextKey{}).(*roundTripCounter); ok {
		counter.parent = parent
	}
	return context.WithValue(ctx, roundTripContextKey{}, counter), func() int {
		return int(counter.value.Load())
	}
}

func countRoundTrip(ctx context.Context) {
	if counter, ok := ctx.Value(roundTripContextKey{}).(*roundTripCounter); ok {
		for current := counter; current != nil; current = current.parent {
			current.value.Add(1)
		}
	}
}
