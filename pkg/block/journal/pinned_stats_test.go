package journal

import (
	"context"
	"math"
	"testing"
)

// TestStatsPinnedSegmentsMatchesWhatEvictRefuses ties Stats.PinnedSegments to
// eviction's own behaviour rather than to a second copy of the synced-gate
// predicate. The counter exists to answer "how much local disk does an unsynced
// residue actually hold down", so the thing it must agree with is the set of
// segments an exhaustive evict pass leaves behind — not the arithmetic it was
// derived from.
func TestStatsPinnedSegmentsMatchesWhatEvictRefuses(t *testing.T) {
	s, _ := evictStore(t, Config{})
	ctx := context.Background()

	// Two sealed segments holding unsynced records, then more holding only
	// synced ones. Distinct FileIDs keep the offsets disjoint so no record
	// supersedes another and every segment stays live.
	fillUntilSealed(t, s, "dirty", false, 2)
	fillUntilSealed(t, s, "clean", true, 4)

	before := s.Stats()
	if before.PinnedSegments == 0 {
		t.Fatalf("no segment reported as pinned, so the assertion below would pass vacuously")
	}
	if before.PinnedBytes <= 0 {
		t.Fatalf("PinnedSegments=%d but PinnedBytes=%d", before.PinnedSegments, before.PinnedBytes)
	}

	// Drain everything eviction is willing to drop.
	if _, err := s.Evict(ctx, math.MaxInt64); err != nil {
		t.Fatalf("Evict: %v", err)
	}

	var survived int
	var survivedBytes int64
	for _, sh := range s.shards {
		sh.mu.Lock()
		for _, seg := range sh.sealed {
			survived++
			survivedBytes += seg.tail.Load()
		}
		sh.mu.Unlock()
	}

	if survived != before.PinnedSegments {
		t.Errorf("PinnedSegments = %d, but eviction left %d sealed segments behind",
			before.PinnedSegments, survived)
	}
	if survivedBytes != before.PinnedBytes {
		t.Errorf("PinnedBytes = %d, but the segments eviction left behind hold %d bytes",
			before.PinnedBytes, survivedBytes)
	}
}
