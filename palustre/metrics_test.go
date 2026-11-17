package palustre

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsReportCurrentQueueAndCounters(t *testing.T) {
	api := &Session{}
	pending := int64(17)
	handler := api.metricsHandler(func(context.Context) (int64, error) { return pending, nil })
	api.MembersAdded.Store(3)
	api.MembersRemoved.Store(2)
	api.ChecksCompleted.Store(9)
	api.CheckRetries.Store(4)
	for _, count := range []int64{17, 0} {
		pending = count
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if response.Code != http.StatusOK {
			t.Fatal(response.Code, response.Body.String())
		}
		for _, line := range []string{"terrarium_members_added_total 3", "terrarium_members_removed_total 2", "terrarium_checks_completed_total 9", "terrarium_check_retries_total 4"} {
			if !strings.Contains(response.Body.String(), line+"\n") {
				t.Fatalf("missing metric %q", line)
			}
		}
		expected := "terrarium_pending_accounts 17\n"
		if count == 0 {
			expected = "terrarium_pending_accounts 0\n"
		}
		if !strings.Contains(response.Body.String(), expected) {
			t.Fatal("queue gauge did not reflect current database count")
		}
	}
}

func TestMetricsFailScrapeWhenQueueCannotBeRead(t *testing.T) {
	api := &Session{}
	handler := api.metricsHandler(func(context.Context) (int64, error) { return 0, errors.New("database unavailable") })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("got status %d; must not report an empty healthy queue", response.Code)
	}
}
