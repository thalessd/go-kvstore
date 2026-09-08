package kvstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Cache is the typed layer over a Store: it binds a namespace and a default
// TTL, and marshals values to JSON so the Store stays codec-free.
type Cache[T any] struct {
	store     Store
	namespace string
	ttl       time.Duration

	// now is swapped by tests to backdate expiry instead of sleeping.
	now func() time.Time
}

// NewCache returns a cache on the given namespace. A ttl of zero or less
// means values never expire unless Set receives an explicit ttl.
func NewCache[T any](store Store, namespace string, ttl time.Duration) *Cache[T] {
	return &Cache[T]{store: store, namespace: namespace, ttl: ttl, now: time.Now}
}

func (c *Cache[T]) Get(ctx context.Context, key string) (T, bool, error) {
	var zero T
	raw, found, err := c.store.Get(ctx, c.namespace, key)
	if err != nil || !found {
		return zero, false, err
	}
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		return zero, false, fmt.Errorf("kvstore unmarshal %s/%s: %w", c.namespace, key, err)
	}
	return value, true, nil
}

// Set stores value under key. Without a ttl argument the cache default
// applies; an explicit ttl of zero or less means no expiry.
func (c *Cache[T]) Set(ctx context.Context, key string, value T, ttl ...time.Duration) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("kvstore marshal %s/%s: %w", c.namespace, key, err)
	}

	effective := c.ttl
	if len(ttl) > 0 {
		effective = ttl[0]
	}

	var expires time.Time
	if effective > 0 {
		expires = c.now().UTC().Add(effective)
	}

	if err := c.store.Set(ctx, c.namespace, key, raw, expires); err != nil {
		return fmt.Errorf("kvstore set %s/%s: %w", c.namespace, key, err)
	}
	return nil
}

// Delete is a best-effort invalidation; it is idempotent.
func (c *Cache[T]) Delete(ctx context.Context, keys ...string) error {
	if err := c.store.Delete(ctx, c.namespace, keys...); err != nil {
		return fmt.Errorf("kvstore delete %s: %w", c.namespace, err)
	}
	return nil
}
