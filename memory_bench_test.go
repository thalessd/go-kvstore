package kvstore

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"
)

// BenchmarkMemoryGet exists because the bound changes how reads lock: promoting
// on read needs the write lock, so the bounded path serializes readers while
// the unbounded path keeps the read lock it always had. The two subcases make
// that cost a number instead of an assertion.
func BenchmarkMemoryGet(b *testing.B) {
	const keys = 1024

	for _, tc := range []struct {
		name string
		opts []MemoryOption
	}{
		{name: "unbounded"},
		{name: "bounded", opts: []MemoryOption{WithMaxEntriesPerNamespace(keys)}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			store := NewMemory(tc.opts...)
			ctx := b.Context()
			for i := range keys {
				if err := store.Set(ctx, "bench", strconv.Itoa(i), json.RawMessage(`{"n":1}`), time.Time{}); err != nil {
					b.Fatalf("seed: %v", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if _, found, _ := store.Get(ctx, "bench", strconv.Itoa(i%keys)); !found {
						b.Fatal("seeded key missing")
					}
					i++
				}
			})
		})
	}
}
