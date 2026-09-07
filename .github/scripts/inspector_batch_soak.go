//go:build ignore

// Run explicitly with `make soak-inspector-batch`; see the root README.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pars-aria-labs/asynq"
	"github.com/redis/go-redis/v9"
)

const soakRedisDB = 13

type soakConfig struct {
	redisAddr           string
	duration            time.Duration
	batchSize           int
	initialTasks        int
	producerInterval    time.Duration
	statsInterval       time.Duration
	scriptFlushInterval time.Duration
	operationTimeout    time.Duration
	drainTimeout        time.Duration
}

type soakCounters struct {
	enqueued      atomic.Int64
	processed     atomic.Int64
	batchCalls    atomic.Int64
	statsReads    atomic.Int64
	scriptFlushes atomic.Int64
}

type soakSummary struct {
	namespace     string
	queue         string
	elapsed       time.Duration
	enqueued      int64
	processed     int64
	batchCalls    int64
	statsReads    int64
	scriptFlushes int64
}

func main() {
	cfg, err := loadSoakConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	summary, err := runSoak(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf(
		"inspector batch soak passed: elapsed=%s namespace=%s queue=%s enqueued=%d processed=%d batch_calls=%d stats_reads=%d script_flushes=%d\n",
		summary.elapsed.Round(time.Millisecond),
		summary.namespace,
		summary.queue,
		summary.enqueued,
		summary.processed,
		summary.batchCalls,
		summary.statsReads,
		summary.scriptFlushes,
	)
}

func loadSoakConfig() (soakConfig, error) {
	redisAddr := strings.TrimSpace(os.Getenv("ASYNQ_TEST_REDIS_ADDR"))
	if redisAddr == "" {
		return soakConfig{}, errors.New(
			"ASYNQ_TEST_REDIS_ADDR is required; point it at a disposable Redis instance (for example 127.0.0.1:6379)",
		)
	}
	cfg := soakConfig{redisAddr: redisAddr}
	var err error
	if cfg.duration, err = positiveDurationEnv("ASYNQ_SOAK_DURATION", 30*time.Second); err != nil {
		return soakConfig{}, err
	}
	if cfg.duration < time.Second {
		return soakConfig{}, fmt.Errorf("ASYNQ_SOAK_DURATION must be at least 1s")
	}
	if cfg.batchSize, err = boundedIntEnv("ASYNQ_SOAK_BATCH_SIZE", 500, 1, 500); err != nil {
		return soakConfig{}, err
	}
	if cfg.initialTasks, err = boundedIntEnv("ASYNQ_SOAK_INITIAL_TASKS", 1000, 1, 1_000_000); err != nil {
		return soakConfig{}, err
	}
	if cfg.producerInterval, err = positiveDurationEnv("ASYNQ_SOAK_PRODUCER_INTERVAL", 2*time.Millisecond); err != nil {
		return soakConfig{}, err
	}
	if cfg.statsInterval, err = positiveDurationEnv("ASYNQ_SOAK_STATS_INTERVAL", 25*time.Millisecond); err != nil {
		return soakConfig{}, err
	}
	if cfg.scriptFlushInterval, err = positiveDurationEnv("ASYNQ_SOAK_SCRIPT_FLUSH_INTERVAL", 200*time.Millisecond); err != nil {
		return soakConfig{}, err
	}
	if cfg.operationTimeout, err = positiveDurationEnv("ASYNQ_SOAK_OPERATION_TIMEOUT", 5*time.Second); err != nil {
		return soakConfig{}, err
	}
	if cfg.drainTimeout, err = positiveDurationEnv("ASYNQ_SOAK_DRAIN_TIMEOUT", 30*time.Second); err != nil {
		return soakConfig{}, err
	}
	return cfg, nil
}

func positiveDurationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration, got %q", name, raw)
	}
	return value, nil
}

func boundedIntEnv(name string, fallback, minimum, maximum int) (int, error) {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d, got %q", name, minimum, maximum, raw)
	}
	return value, nil
}

func runSoak(cfg soakConfig) (summary soakSummary, returnErr error) {
	runID, err := newRunID()
	if err != nil {
		return summary, err
	}
	started := time.Now()
	summary.namespace = "inspector-soak-" + runID
	summary.queue = "soak-" + runID
	opt := asynq.RedisClientOpt{
		Addr:   cfg.redisAddr,
		DB:     soakRedisDB,
		Prefix: summary.namespace,
	}

	raw, ok := opt.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		return summary, fmt.Errorf("unexpected Redis client type for %T", opt)
	}
	defer raw.Close()
	client := asynq.NewClient(opt)
	defer client.Close()
	inspector := asynq.NewInspector(opt)
	defer inspector.Close()
	defer func() {
		if err := cleanupSoak(raw, inspector, summary.namespace, summary.queue, cfg.drainTimeout); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("soak cleanup: %w", err))
		}
	}()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), cfg.operationTimeout)
	err = raw.Ping(pingCtx).Err()
	pingCancel()
	if err != nil {
		return summary, fmt.Errorf("ping Redis %s: %w", cfg.redisAddr, err)
	}

	counters := new(soakCounters)
	if err := enqueueInitial(client, cfg, summary.queue, runID, counters); err != nil {
		return summary, err
	}
	if err := flushScripts(raw, cfg.operationTimeout); err != nil {
		return summary, fmt.Errorf("initial SCRIPT FLUSH: %w", err)
	}
	counters.scriptFlushes.Add(1)

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	runCtx, stopRun := context.WithTimeout(signalCtx, cfg.duration)
	defer stopRun()

	workerErrors := make(chan error, 4)
	// SCRIPT FLUSH is server-wide. Keep it between API calls so the next call
	// exercises NOSCRIPT recovery without invalidating an in-flight pipeline.
	var scriptGate sync.RWMutex
	var workers sync.WaitGroup
	startWorker := func(name string, work func(context.Context) error) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := work(runCtx); err != nil {
				select {
				case workerErrors <- fmt.Errorf("%s: %w", name, err):
				default:
				}
				stopRun()
			}
		}()
	}
	startWorker("producer", func(ctx context.Context) error {
		return produceScheduled(ctx, client, cfg, summary.queue, runID, counters, &scriptGate)
	})
	startWorker("batch processor", func(ctx context.Context) error {
		return processBatches(ctx, inspector, cfg, summary.queue, counters, &scriptGate)
	})
	startWorker("queue stats", func(ctx context.Context) error {
		return sampleQueueStats(ctx, inspector, cfg, summary.queue, counters, &scriptGate)
	})
	startWorker("script flusher", func(ctx context.Context) error {
		return periodicallyFlushScripts(ctx, raw, cfg, counters, &scriptGate)
	})

	<-runCtx.Done()
	workers.Wait()
	select {
	case err := <-workerErrors:
		return summary, err
	default:
	}
	if signalCtx.Err() != nil {
		return summary, fmt.Errorf("soak interrupted: %w", signalCtx.Err())
	}

	if err := drainAndVerify(inspector, cfg, summary.queue, counters); err != nil {
		return summary, err
	}
	summary.elapsed = time.Since(started)
	summary.enqueued = counters.enqueued.Load()
	summary.processed = counters.processed.Load()
	summary.batchCalls = counters.batchCalls.Load()
	summary.statsReads = counters.statsReads.Load()
	summary.scriptFlushes = counters.scriptFlushes.Load()
	if summary.statsReads == 0 || summary.scriptFlushes < 2 || summary.batchCalls == 0 {
		return summary, fmt.Errorf(
			"insufficient soak coverage: batch_calls=%d stats_reads=%d script_flushes=%d",
			summary.batchCalls, summary.statsReads, summary.scriptFlushes,
		)
	}
	return summary, nil
}

func newRunID() (string, error) {
	var random [6]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate run ID: %w", err)
	}
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(random[:])), nil
}

func enqueueInitial(client *asynq.Client, cfg soakConfig, queue, runID string, counters *soakCounters) error {
	for offset := 0; offset < cfg.initialTasks; {
		count := min(500, cfg.initialTasks-offset)
		tasks := make([]*asynq.Task, count)
		for j := range tasks {
			tasks[j] = asynq.NewTask("inspector:soak", []byte(runID))
		}
		ctx, cancel := context.WithTimeout(context.Background(), cfg.operationTimeout)
		results := client.BatchEnqueueContext(
			ctx,
			tasks,
			asynq.Queue(queue),
			asynq.ProcessIn(24*time.Hour),
		)
		cancel()
		for j, result := range results {
			if result.Err != nil {
				return fmt.Errorf("initial enqueue %d: %w", offset+j, result.Err)
			}
			counters.enqueued.Add(1)
		}
		offset += count
	}
	return nil
}

func produceScheduled(ctx context.Context, client *asynq.Client, cfg soakConfig, queue, runID string, counters *soakCounters, scriptGate *sync.RWMutex) error {
	ticker := time.NewTicker(cfg.producerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		opCtx, cancel := context.WithTimeout(context.Background(), cfg.operationTimeout)
		scriptGate.RLock()
		_, err := client.EnqueueContext(
			opCtx,
			asynq.NewTask("inspector:soak", []byte(runID)),
			asynq.Queue(queue),
			asynq.ProcessIn(24*time.Hour),
		)
		scriptGate.RUnlock()
		cancel()
		if err != nil {
			return err
		}
		counters.enqueued.Add(1)
	}
}

func processBatches(ctx context.Context, inspector *asynq.Inspector, cfg soakConfig, queue string, counters *soakCounters, scriptGate *sync.RWMutex) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		opCtx, cancel := context.WithTimeout(context.Background(), cfg.operationTimeout)
		scriptGate.RLock()
		processed, remaining, err := inspector.ProcessTaskBatch(
			opCtx, queue, "scheduled", "delete", "", cfg.batchSize,
		)
		scriptGate.RUnlock()
		cancel()
		counters.batchCalls.Add(1)
		if processed < 0 || processed > cfg.batchSize || remaining < 0 {
			return fmt.Errorf("invalid batch result: processed=%d remaining=%d limit=%d", processed, remaining, cfg.batchSize)
		}
		counters.processed.Add(int64(processed))
		if err != nil {
			return err
		}
		if processed == 0 && !waitForNext(ctx, time.Millisecond) {
			return nil
		}
	}
}

func sampleQueueStats(ctx context.Context, inspector *asynq.Inspector, cfg soakConfig, queue string, counters *soakCounters, scriptGate *sync.RWMutex) error {
	ticker := time.NewTicker(cfg.statsInterval)
	defer ticker.Stop()
	queues := []string{queue, queue, queue}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		opCtx, cancel := context.WithTimeout(context.Background(), cfg.operationTimeout)
		scriptGate.RLock()
		infos, err := inspector.GetQueueInfoBatch(opCtx, queues, 50*time.Millisecond)
		scriptGate.RUnlock()
		cancel()
		if err != nil {
			return err
		}
		if len(infos) != len(queues) {
			return fmt.Errorf("GetQueueInfoBatch returned %d entries, want %d", len(infos), len(queues))
		}
		for j, info := range infos {
			if info == nil || info.Queue != queue {
				return fmt.Errorf("queue info %d has wrong identity: %+v", j, info)
			}
			if info.Size < 0 || info.Pending < 0 || info.Scheduled < 0 || info.MemoryUsage < 0 {
				return fmt.Errorf("queue info %d contains a negative counter: %+v", j, info)
			}
			if info.Size != info.Scheduled || info.Pending != 0 || info.Active != 0 || info.Retry != 0 ||
				info.Archived != 0 || info.Completed != 0 || info.Aggregating != 0 || info.Groups != 0 {
				return fmt.Errorf("queue info %d violates scheduled-delete workload invariants: %+v", j, info)
			}
		}
		counters.statsReads.Add(1)
	}
}

func periodicallyFlushScripts(ctx context.Context, raw redis.UniversalClient, cfg soakConfig, counters *soakCounters, scriptGate *sync.RWMutex) error {
	ticker := time.NewTicker(cfg.scriptFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		scriptGate.Lock()
		if err := flushScripts(raw, cfg.operationTimeout); err != nil {
			scriptGate.Unlock()
			return err
		}
		scriptGate.Unlock()
		counters.scriptFlushes.Add(1)
	}
}

func flushScripts(raw redis.UniversalClient, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return raw.ScriptFlush(ctx).Err()
}

func waitForNext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func drainAndVerify(inspector *asynq.Inspector, cfg soakConfig, queue string, counters *soakCounters) error {
	drainCtx, cancel := context.WithTimeout(context.Background(), cfg.drainTimeout)
	defer cancel()
	for {
		processed, remaining, err := inspector.ProcessTaskBatch(
			drainCtx, queue, "scheduled", "delete", "", cfg.batchSize,
		)
		counters.batchCalls.Add(1)
		if processed < 0 || processed > cfg.batchSize || remaining < 0 {
			return fmt.Errorf("invalid drain result: processed=%d remaining=%d limit=%d", processed, remaining, cfg.batchSize)
		}
		counters.processed.Add(int64(processed))
		if err != nil {
			return fmt.Errorf("drain batch after %d confirmed tasks: %w", counters.processed.Load(), err)
		}
		if remaining == 0 {
			break
		}
		if processed == 0 {
			return fmt.Errorf("drain made no progress with %d scheduled tasks remaining", remaining)
		}
	}

	infos, err := inspector.GetQueueInfoBatch(drainCtx, []string{queue}, 0)
	if err != nil {
		return fmt.Errorf("final queue info: %w", err)
	}
	if len(infos) != 1 || infos[0] == nil {
		return fmt.Errorf("final queue info returned %d entries", len(infos))
	}
	enqueued, processed := counters.enqueued.Load(), counters.processed.Load()
	info := infos[0]
	if processed != enqueued {
		return fmt.Errorf("accounting mismatch: enqueued=%d processed=%d", enqueued, processed)
	}
	if info.Size != 0 || info.Pending != 0 || info.Scheduled != 0 {
		return fmt.Errorf(
			"unexpected final queue state: size=%d pending=%d scheduled=%d",
			info.Size, info.Pending, info.Scheduled,
		)
	}
	return nil
}

func cleanupSoak(raw redis.UniversalClient, inspector *asynq.Inspector, prefix, queue string, timeout time.Duration) error {
	var cleanupErrors []error
	if err := inspector.DeleteQueue(queue, false); err != nil && !errors.Is(err, asynq.ErrQueueNotFound) {
		cleanupErrors = append(cleanupErrors, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	namespace := strings.TrimRight(prefix, ":") + ":asynq:"
	var cursor uint64
	for {
		keys, next, err := raw.Scan(ctx, cursor, namespace+"*", 500).Result()
		if err != nil {
			cleanupErrors = append(cleanupErrors, err)
			break
		}
		safeKeys := keys[:0]
		for _, key := range keys {
			if !strings.HasPrefix(key, namespace) {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("refusing to delete key outside namespace: %q", key))
				continue
			}
			safeKeys = append(safeKeys, key)
		}
		if len(safeKeys) > 0 {
			_, err := raw.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for _, key := range safeKeys {
					pipe.Unlink(ctx, key)
				}
				return nil
			})
			if err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("unlink namespace batch: %w", err))
				break
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	cursor = 0
	for {
		keys, next, err := raw.Scan(ctx, cursor, namespace+"*", 100).Result()
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("verify namespace cleanup: %w", err))
			break
		}
		if len(keys) > 0 {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("namespace cleanup left key %q", keys[0]))
			break
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return errors.Join(cleanupErrors...)
}
