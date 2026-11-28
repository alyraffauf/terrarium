package cordata

import (
	"context"
	"sync"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/xrpc"
)

// Each post has one worker. It requests an inventory before discovering likers
// and finishes reconciling before requesting another. Only requests present at
// the start of a walk can use its records.
type ownerInventories struct {
	mu      sync.Mutex
	pending map[string]*inventoryRequest
	walking chan struct{}
}

type inventoryRequest struct {
	owner *ownerInventories
	list  string
	ready chan struct{}
	items inventory
	err   error
}

func (owner *ownerInventories) request(list string) *inventoryRequest {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.pending == nil {
		owner.pending = make(map[string]*inventoryRequest)
	}
	req := &inventoryRequest{owner: owner, list: list}
	owner.pending[list] = req
	return req
}

func (req *inventoryRequest) close() {
	owner := req.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.pending[req.list] == req {
		delete(owner.pending, req.list)
	}
}

func (req *inventoryRequest) read(ctx context.Context, client *xrpc.Client) (inventory, error) {
	owner := req.owner
	for {
		owner.mu.Lock()
		if ready := req.ready; ready != nil {
			owner.mu.Unlock()
			select {
			case <-ready:
				return req.items, req.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if walking := owner.walking; walking != nil {
			owner.mu.Unlock()
			select {
			case <-walking:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		done := make(chan struct{})
		group := owner.pending
		owner.pending = nil
		owner.walking = done
		for _, pending := range group {
			pending.ready = done
		}
		owner.mu.Unlock()

		err := palustre.WalkListItems(ctx, client, func(list, did, key string) error {
			if pending := group[list]; pending != nil {
				if pending.items == nil {
					pending.items = inventory{}
				}
				pending.items[did] = append(pending.items[did], key)
			}
			return nil
		})
		owner.mu.Lock()
		if err != nil {
			for _, pending := range group {
				pending.items = nil
				pending.err = err
			}
		}
		owner.walking = nil
		close(done)
		owner.mu.Unlock()
		return req.items, req.err
	}
}
