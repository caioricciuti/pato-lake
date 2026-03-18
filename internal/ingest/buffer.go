package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/caioricciuti/pato-lake/internal/duckdb"
)

// Event represents a single ingested event.
type Event struct {
	EventType string
	Data      map[string]interface{}
	ByteSize  int
}

// Buffer accumulates events per event type and flushes them to DuckDB.
type Buffer struct {
	db     *database.DB
	engine *duckdb.Engine
	writer *Writer

	mu      sync.Mutex
	buckets map[string][]Event // eventType → events

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBuffer creates a new ingestion buffer.
func NewBuffer(db *database.DB, engine *duckdb.Engine) *Buffer {
	return &Buffer{
		db:      db,
		engine:  engine,
		writer:  NewWriter(engine),
		buckets: make(map[string][]Event),
	}
}

// Start begins the background flush loop.
func (b *Buffer) Start() {
	b.ctx, b.cancel = context.WithCancel(context.Background())
	b.wg.Add(1)
	go b.flushLoop()
	slog.Info("Ingest buffer started")
}

// Stop drains remaining events and stops the buffer.
func (b *Buffer) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()
	// Final flush
	b.flushAll()
	slog.Info("Ingest buffer stopped")
}

// Enqueue adds events to the buffer. Returns the number accepted.
func (b *Buffer) Enqueue(eventType string, events []Event) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buckets[eventType] = append(b.buckets[eventType], events...)

	// Check if any bucket needs immediate flush (> threshold)
	source, _ := b.db.GetIngestSourceByEventType(eventType)
	threshold := 1000
	if source != nil && source.BufferSize > 0 {
		threshold = source.BufferSize
	}

	if len(b.buckets[eventType]) >= threshold {
		go b.flushEventType(eventType)
	}

	return len(events)
}

func (b *Buffer) flushLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.flushAll()
		}
	}
}

func (b *Buffer) flushAll() {
	b.mu.Lock()
	eventTypes := make([]string, 0, len(b.buckets))
	for et := range b.buckets {
		if len(b.buckets[et]) > 0 {
			eventTypes = append(eventTypes, et)
		}
	}
	b.mu.Unlock()

	for _, et := range eventTypes {
		b.flushEventType(et)
	}
}

func (b *Buffer) flushEventType(eventType string) {
	b.mu.Lock()
	events := b.buckets[eventType]
	if len(events) == 0 {
		b.mu.Unlock()
		return
	}
	b.buckets[eventType] = nil
	b.mu.Unlock()

	// Resolve source config
	source, _ := b.db.GetIngestSourceByEventType(eventType)
	schema := "main"
	table := "events_" + eventType
	sourceID := ""
	if source != nil {
		schema = source.TargetSchema
		table = source.TargetTable
		sourceID = source.ID
	}

	// Convert events to records
	records := make([]map[string]interface{}, len(events))
	totalBytes := 0
	for i, e := range events {
		records[i] = e.Data
		totalBytes += e.ByteSize
	}

	// Write batch
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	written, err := b.writer.WriteBatch(ctx, schema, table, records)
	if err != nil {
		slog.Error("Ingest flush failed", "event_type", eventType, "count", len(records), "error", err)
		if sourceID != "" {
			b.db.IncrementIngestStats(sourceID, len(records), 0, totalBytes, len(records), err.Error())
		}
		return
	}

	slog.Info("Ingest flush completed", "event_type", eventType, "written", written)
	if sourceID != "" {
		b.db.IncrementIngestStats(sourceID, len(records), written, totalBytes, 0, "")
	}
}
