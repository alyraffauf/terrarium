package capensis

import (
	"context"

	"github.com/alyraffauf/terrarium/palustre"
)

func selectCandidate(_ context.Context, event palustre.Event) (bool, error) {
	switch event.Kind {
	case "identity", "account", "sync":
		return true, nil
	case "commit":
		return event.MalformedFollow, nil
	default:
		return false, nil
	}
}
