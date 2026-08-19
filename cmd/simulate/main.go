package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/httpapi"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println("  WEBHOOK INGESTION END-TO-END SIMULATION RUNNER  ")
	fmt.Println("==================================================")

	ctx := context.Background()
	cfg := config.Load()

	st, err := store.New(ctx, cfg.PostgresDSN, cfg.DBMaxConns)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer st.Close()

	rdb, err := redisclient.New(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("connect redis: %v", err)
	}
	defer func() { _ = rdb.Close() }()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache := stats.NewCache()
	svc := ingest.New(st, cache, rdb, logger)
	server := httptest.NewServer(httpapi.NewRouter(svc, logger))
	defer server.Close()

	accountID := "acc_sim_runner"

	// Cleanup existing simulation data
	_, _ = st.Pool().Exec(ctx, "DELETE FROM events WHERE account_id = $1", accountID)
	_, _ = st.Pool().Exec(ctx, "DELETE FROM calls WHERE account_id = $1", accountID)
	_, _ = st.Pool().Exec(ctx, "DELETE FROM account_stats WHERE account_id = $1", accountID)

	numCalls := 50
	webhooksPerCall := 3
	totalWebhooks := numCalls * webhooksPerCall
	durationPerCall := 100
	expectedTotalDuration := int64(numCalls * durationPerCall)

	fmt.Printf("\n[1/4] Firing %d concurrent webhooks (%d calls x %d redeliveries)...\n",
		totalWebhooks, numCalls, webhooksPerCall)

	var wg sync.WaitGroup
	start := time.Now()

	for i := 1; i <= numCalls; i++ {
		callID := fmt.Sprintf("call_sim_%d", i)
		eventID1 := fmt.Sprintf("evt_sim_%d_a", i)
		eventID2 := fmt.Sprintf("evt_sim_%d_b", i)

		payload1 := fmt.Sprintf(`{
			"event_id": %q, "call_id": %q, "account_id": %q,
			"status": "completed", "duration_sec": %d,
			"recording_url": "https://recordings.example.com/%s.wav",
			"occurred_at": "2026-08-19T10:00:00Z"
		}`, eventID1, callID, accountID, durationPerCall, callID)

		payload2 := fmt.Sprintf(`{
			"event_id": %q, "call_id": %q, "account_id": %q,
			"status": "completed", "duration_sec": %d,
			"recording_url": "https://recordings.example.com/%s.wav",
			"occurred_at": "2026-08-19T10:00:00Z"
		}`, eventID2, callID, accountID, durationPerCall, callID)

		// 1. Original delivery
		wg.Add(1)
		go func(body string) {
			defer wg.Done()
			post(server.URL+"/webhooks/calls", body)
		}(payload1)

		// 2. Immediate duplicate delivery (same event_id)
		wg.Add(1)
		go func(body string) {
			defer wg.Done()
			post(server.URL+"/webhooks/calls", body)
		}(payload1)

		// 3. Status update delivery (new event_id, same call_id)
		wg.Add(1)
		go func(body string) {
			defer wg.Done()
			post(server.URL+"/webhooks/calls", body)
		}(payload2)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("      Completed %d HTTP POST requests in %v 🟢\n", totalWebhooks, elapsed)

	// [2/4] Idempotent Stat Accuracy Check
	fmt.Println("\n[2/4] Verifying Idempotent Stat Accuracy in Database...")
	dbStats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		log.Fatalf("fetch account_stats: %v", err)
	}

	if dbStats.CallCount != int64(numCalls) || dbStats.TotalDurationSec != expectedTotalDuration {
		fmt.Printf("      FAILED ❌: got CallCount=%d TotalDurationSec=%d, want CallCount=%d TotalDurationSec=%d\n",
			dbStats.CallCount, dbStats.TotalDurationSec, numCalls, expectedTotalDuration)
		os.Exit(1)
	}
	fmt.Printf("      PASSED 🟢: Account stats strictly show CallCount=%d, TotalDurationSec=%d (zero overcounting!)\n",
		dbStats.CallCount, dbStats.TotalDurationSec)

	// [3/4] Background Recording Processing Check
	fmt.Println("\n[3/4] Verifying Background Recording Processing in Database...")
	svc.Close() // Drain all in-flight recording tasks

	var processedCount int
	err = st.Pool().QueryRow(ctx,
		"SELECT count(*) FROM calls WHERE account_id = $1 AND recording_processed = TRUE",
		accountID).Scan(&processedCount)
	if err != nil {
		log.Fatalf("count processed calls: %v", err)
	}

	if processedCount != numCalls {
		fmt.Printf("      FAILED ❌: %d/%d calls marked processed\n", processedCount, numCalls)
		os.Exit(1)
	}
	fmt.Printf("      PASSED 🟢: All %d/%d call recordings successfully marked processed (recording_processed = true)!\n",
		processedCount, numCalls)

	// [4/4] Cold Cache Database Fallback Check
	fmt.Println("\n[4/4] Verifying Cold Cache Database Fallback...")
	coldCache := stats.NewCache()
	coldSvc := ingest.New(st, coldCache, rdb, logger)
	coldStats := coldSvc.Stats(accountID)

	if coldStats.CallCount != int64(numCalls) || coldStats.TotalDurationSec != expectedTotalDuration {
		fmt.Printf("      FAILED ❌: cold cache returned CallCount=%d TotalDurationSec=%d\n",
			coldStats.CallCount, coldStats.TotalDurationSec)
		os.Exit(1)
	}
	fmt.Printf("      PASSED 🟢: Cold cache successfully queried PostgreSQL and returned CallCount=%d, TotalDurationSec=%d!\n",
		coldStats.CallCount, coldStats.TotalDurationSec)

	fmt.Println("\n==================================================")
	fmt.Println("  ALL SIMULATION VERIFICATIONS PASSED SUCCESSFULLY! 🟢 ")
	fmt.Println("==================================================")
}

func post(url, body string) {
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err == nil {
		_ = resp.Body.Close()
	}
}
