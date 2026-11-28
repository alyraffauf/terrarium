package cordata

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
)

type gatedLikes struct {
	*testRepository
	started chan struct{}
	resume  chan struct{}
}

func (p *gatedLikes) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Query().Get("collection") == "app.bsky.feed.like" {
		select {
		case p.started <- struct{}{}:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		select {
		case <-p.resume:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	return p.testRepository.RoundTrip(req)
}

func TestBacklinksOverlapPDSReads(t *testing.T) {
	repo := &gatedLikes{testRepository: &testRepository{links: map[string]string{"did:example:a": post, "did:example:b": post}}, started: make(chan struct{}, 2), resume: make(chan struct{})}
	api := &ownerSession{Session: &palustre.Session{Config: palustre.Config{Slingshot: "https://resolver.test"}, HTTP: &http.Client{Transport: repo}}}
	scan := postScan{post: post}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	likers := map[string]bool{}
	done := make(chan error, 1)
	go func() {
		_, err := scan.backlinks(ctx, api, "https://index.test", likeCollection, likers, time.Now)
		done <- err
	}()
	for range 2 {
		select {
		case <-repo.started:
		case <-ctx.Done():
			t.Fatal("second public read did not start while first was blocked")
		case err := <-done:
			t.Fatalf("scan exited before both reads: %v", err)
		}
	}
	close(repo.resume)
	if err := <-done; err != nil || len(likers) != 2 {
		t.Fatalf("likers=%v err=%v", likers, err)
	}
}
