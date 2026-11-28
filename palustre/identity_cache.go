package palustre

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/xrpc"
	lru "github.com/hashicorp/golang-lru"
)

// Cache only successful DID reads. Login and handle-to-DID lookups stay fresh.
type identityCache struct {
	once    sync.Once
	entries *lru.Cache
}
type cachedIdentity struct {
	doc     MiniDoc
	expires time.Time
}

func (c *identityCache) lru() *lru.Cache {
	c.once.Do(func() { c.entries, _ = lru.New(8192) })
	return c.entries
}

func (api *Session) CachedDID(ctx context.Context, did string) (MiniDoc, bool, error) {
	cache := api.identities.lru()
	if v, ok := cache.Get(did); ok {
		entry := v.(cachedIdentity)
		if time.Now().Before(entry.expires) {
			return entry.doc, true, nil
		}
	}
	doc, err := api.RefreshDID(ctx, did)
	return doc, false, err
}

func (api *Session) RefreshDID(ctx context.Context, did string) (MiniDoc, error) {
	cache := api.identities.lru()
	cache.Remove(did)
	doc, err := api.Resolve(ctx, did)
	if err == nil {
		cache.Add(did, cachedIdentity{doc, time.Now().Add(5 * time.Minute)})
	}
	return doc, err
}

// Missing repositories have different error codes across PDS implementations.
func MayHaveMoved(err error) bool {
	var response *xrpc.Error
	return errors.As(err, &response) && (response.StatusCode == 400 || response.StatusCode == 404 || response.StatusCode == 410)
}

// ConfirmPDS rechecks a cached route before accepting a missing or inactive
// record. A move leaves the job pending so the next read uses the new host.
func (api *Session) ConfirmPDS(ctx context.Context, did, host string) error {
	doc, err := api.RefreshDID(ctx, did)
	if err != nil {
		return err
	}
	if doc.PDS != host {
		return fmt.Errorf("PDS changed for %s", did)
	}
	return nil
}
