package ingest_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestConcurrentDuplicateIngestDoesNotDoubleCount(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := post(t, srv.URL+"/webhooks/calls", body)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("got status %d, want 200", resp.StatusCode)
			}
		}()
	}
	wg.Wait()

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 143 {
		t.Fatalf("account_stats: got CallCount=%d TotalDurationSec=%d, want CallCount=1 TotalDurationSec=143",
			got.CallCount, got.TotalDurationSec)
	}
}

func TestMultipleEventsForSameCallDoesNotDoubleCount(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID1, callID, accountID := testutil.IDs(t, st)
	eventID2 := eventID1 + "_update"
	ctx := context.Background()

	body1 := eventJSON(eventID1, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body1); resp.StatusCode != http.StatusOK {
		t.Fatalf("event 1: got %d, want 200", resp.StatusCode)
	}

	body2 := eventJSON(eventID2, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body2); resp.StatusCode != http.StatusOK {
		t.Fatalf("event 2: got %d, want 200", resp.StatusCode)
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 143 {
		t.Fatalf("account_stats after 2 events for same call: got CallCount=%d TotalDurationSec=%d, want CallCount=1 TotalDurationSec=143",
			got.CallCount, got.TotalDurationSec)
	}
}

func TestRecordingProcessedInBackground(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// Give the background recording processing time to complete (recordingWork = 50ms)
	time.Sleep(100 * time.Millisecond)

	var processed bool
	row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan call: %v", err)
	}
	if !processed {
		t.Fatalf("expected recording_processed to be true, got false")
	}
}

func TestServiceCloseWaitsForInFlightRecordings(t *testing.T) {
	st := testutil.NewStore(t)
	c := stats.NewCache()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(st, c, nil, log)
	ctx := context.Background()

	eventID, callID, accountID := testutil.IDs(t, st)
	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  100,
		RecordingURL: "https://recordings.example.com/a.wav",
		OccurredAt:   time.Now(),
	}

	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Close() must block and wait for all in-flight recording processing tasks to drain before returning
	svc.Close()

	var processed bool
	row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan call: %v", err)
	}
	if !processed {
		t.Fatalf("expected recording_processed to be true after svc.Close(), got false")
	}
}

func TestAccountStatsFallbackToDatabaseOnColdCache(t *testing.T) {
	st := testutil.NewStore(t)
	c := stats.NewCache()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(st, c, nil, log)
	ctx := context.Background()

	_, _, accountID := testutil.IDs(t, st)
	if err := st.IncrementAccountStats(ctx, accountID, 300); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}

	// In-memory cache is empty. svc.Stats should query PostgreSQL database and return durable stats.
	got := svc.Stats(accountID)
	if got.CallCount != 1 || got.TotalDurationSec != 300 {
		t.Fatalf("cold cache fallback: got CallCount=%d TotalDurationSec=%d, want CallCount=1 TotalDurationSec=300",
			got.CallCount, got.TotalDurationSec)
	}
}

func TestIngestTxIsAtomicOnDuplicate(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID:     eventID,
		CallID:      callID,
		AccountID:   accountID,
		Status:      "completed",
		DurationSec: 120,
		Payload:     []byte(`{}`),
	}

	// First ingest — should insert everything
	inserted, isNewCall, err := s.IngestTx(ctx, evt)
	if err != nil {
		t.Fatalf("first IngestTx: %v", err)
	}
	if !inserted {
		t.Fatal("expected inserted=true on first call")
	}
	if !isNewCall {
		t.Fatal("expected isNewCall=true on first call")
	}

	// Second ingest — same event_id, must be a no-op
	inserted, _, err = s.IngestTx(ctx, evt)
	if err != nil {
		t.Fatalf("second IngestTx: %v", err)
	}
	if inserted {
		t.Fatal("expected inserted=false on duplicate")
	}

	// Stats must reflect exactly one call, not two
	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 120 {
		t.Fatalf("got CallCount=%d TotalDurationSec=%d, want CallCount=1 TotalDurationSec=120",
			got.CallCount, got.TotalDurationSec)
	}

	// events table must have exactly one row
	var n int
	row := s.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("events table has %d rows for event_id, want 1", n)
	}
}
