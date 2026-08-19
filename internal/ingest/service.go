// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
	wg    sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Close waits for all in-flight recording tasks to complete.
func (s *Service) Close() {
	s.wg.Wait()
}

// Stats returns the cached totals for an account, falling back to Postgres on cache miss.
func (s *Service) Stats(accountID string) stats.AccountStats {
	st := s.cache.Get(accountID)
	if st.CallCount > 0 || st.TotalDurationSec > 0 {
		return st
	}

	// Cache miss or empty cache: query database fallback
	dbStats, err := s.store.AccountStats(context.Background(), accountID)
	if err != nil || (dbStats.CallCount == 0 && dbStats.TotalDurationSec == 0) {
		return st
	}

	res := stats.AccountStats{
		CallCount:        dbStats.CallCount,
		TotalDurationSec: dbStats.TotalDurationSec,
	}
	s.cache.Set(accountID, res)
	return res
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, isNewCall, err := s.store.IngestTx(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	if isNewCall {
		s.cache.Record(rec.AccountID, rec.DurationSec)
	}

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		bgCtx := context.WithoutCancel(ctx)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.processRecording(bgCtx, rec); err != nil {
				s.log.Error("process recording failed", "call_id", rec.CallID, "err", err)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
