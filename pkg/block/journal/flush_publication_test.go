package journal

import (
	"context"
	"errors"
	"testing"
)

func TestHasDirtyIncludesManifestPublication(t *testing.T) {
	for _, outcome := range []string{"success", "error", "cancel", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			s := testStore(t, Config{ShardCount: 1})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := s.WriteAt(ctx, "f", 0, []byte("new bytes")); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("publication interrupted")
			var err error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				err = s.Flush(ctx, "f", FlushOptions{Force: true, AfterFile: func(ctx context.Context, id FileID) error {
					if s.UnsyncedBytes() != 0 {
						t.Fatal("records must already be marked synced before AfterFile")
					}
					if dirty, err := s.HasDirty(ctx, id); err != nil || !dirty {
						t.Fatalf("pending publication reported dirty=%v err=%v", dirty, err)
					}
					if dirty, err := s.HasDirty(ctx, "unrelated"); err != nil || dirty {
						t.Fatalf("another file on the shard reported dirty=%v err=%v", dirty, err)
					}
					switch outcome {
					case "error":
						return failure
					case "panic":
						panic(failure)
					}
					return nil
				}}, func(ctx context.Context, run Run) ([]Extent, error) {
					if outcome == "cancel" {
						cancel()
						return []Extent{run.Extent}, ctx.Err()
					}
					return []Extent{run.Extent}, nil
				})
			}()
			switch outcome {
			case "success":
				if err != nil || recovered != nil {
					t.Fatalf("Flush: err=%v panic=%v", err, recovered)
				}
			case "error":
				if !errors.Is(err, failure) {
					t.Fatalf("Flush: got %v, want %v", err, failure)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Flush: got %v, want cancellation", err)
				}
			case "panic":
				if recovered != failure {
					t.Fatalf("Flush panic: got %v, want %v", recovered, failure)
				}
			}
			if dirty, err := s.HasDirty(context.Background(), "f"); err != nil || dirty {
				t.Fatalf("publication scope survived Flush: dirty=%v err=%v", dirty, err)
			}
		})
	}
}
