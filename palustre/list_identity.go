package palustre

import (
	"fmt"
	"slices"
	"strings"

	"github.com/bluesky-social/indigo/api/bsky"
)

type listIdentity struct {
	post                  bool
	previousNames         []string
	isPreviousDescription func(string) bool
}

// update returns nil for a matching list, an edited copy for a managed list,
// and ErrUnrelatedList when the stored record cannot be adopted.
func (identity listIdentity) update(list *bsky.GraphList, listURI, description string) (*bsky.GraphList, error) {
	if list == nil || list.Purpose == nil || *list.Purpose != moderationListPurpose {
		return nil, fmt.Errorf("%w: expected moderation list at %s", ErrUnrelatedList, listURI)
	}
	title := ListTitle(listURI)
	text := ""
	if list.Description != nil {
		text = *list.Description
	}
	lines := strings.Split(text, "\n")
	last := lines[len(lines)-1]
	legacy := list.Name == description || slices.Contains(identity.previousNames, list.Name)
	if identity.post {
		legacy = text == description
	}
	if list.Name == title && (last == description || (identity.post && lines[0] == description)) {
		return nil, nil
	}
	previousDescription := slices.Contains(identity.previousNames, last)
	if list.Name == title && identity.isPreviousDescription != nil && identity.isPreviousDescription(last) {
		previousDescription = true
	}
	if !legacy && !(list.Name == title && previousDescription && last != "") {
		return nil, fmt.Errorf("%w: refusing to adopt %s", ErrUnrelatedList, listURI)
	}
	updated := *list
	// Keep existing prose and its facets at the same byte offsets.
	if last == description {
		if identity.post && list.Name != title && list.Name != description {
			lines = append(lines, list.Name)
		}
	} else if last == "" || previousDescription {
		lastStart := int64(len(text) - len(last))
		updated.DescriptionFacets = slices.DeleteFunc(slices.Clone(list.DescriptionFacets), func(facet *bsky.RichtextFacet) bool {
			return facet == nil || facet.Index == nil || facet.Index.ByteEnd > lastStart
		})
		lines[len(lines)-1] = description
	} else {
		lines = append(lines, description)
	}
	text = strings.Join(lines, "\n")
	if err := validateListDescription(text); err != nil {
		return nil, fmt.Errorf("%w for %s: %w", ErrListUpdate, listURI, err)
	}
	updated.Name = title
	updated.Description = &text
	return &updated, nil
}
