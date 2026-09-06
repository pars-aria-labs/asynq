package rdb

import (
	"context"
	"testing"
)

func TestNestedRoundTripCounters(t *testing.T) {
	parent, parentTotal := WithRoundTripCounter(context.Background())
	child, childTotal := WithRoundTripCounter(parent)

	countRoundTrip(child)
	countRoundTrip(parent)

	if got := childTotal(); got != 1 {
		t.Fatalf("child count = %d, want 1", got)
	}
	if got := parentTotal(); got != 2 {
		t.Fatalf("parent count = %d, want 2", got)
	}
}
