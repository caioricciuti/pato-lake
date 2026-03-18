package governance

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/caioricciuti/pato-lake/internal/duckdb"
)

const (
	syncTickInterval = 5 * time.Minute
	staleDuration    = 10 * time.Minute
)

// Syncer orchestrates DuckDB -> SQLite governance synchronisation.
// It runs periodic background syncs and supports on-demand sync.
type Syncer struct {
	store       *Store
	db          *database.DB
	engine      *duckdb.Engine
	activeSyncs sync.Map // syncType -> bool (prevents concurrent syncs)
	stopCh      chan struct{}
}

// NewSyncer creates a new governance Syncer.
func NewSyncer(store *Store, db *database.DB, engine *duckdb.Engine) *Syncer {
	return &Syncer{
		store:  store,
		db:     db,
		engine: engine,
		stopCh: make(chan struct{}),
	}
}

// GetStore returns the underlying governance store.
func (s *Syncer) GetStore() *Store {
	return s.store
}

// StartBackground launches the background goroutine that ticks every 5 minutes
// to sync governance data from the embedded DuckDB engine.
func (s *Syncer) StartBackground() {
	go func() {
		slog.Info("Governance syncer started", "interval", syncTickInterval)
		ticker := time.NewTicker(syncTickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.stopCh:
				slog.Info("Governance syncer stopped")
				return
			case <-ticker.C:
				s.backgroundTick()
			}
		}
	}()
}

// Stop signals the background goroutine to stop.
func (s *Syncer) Stop() {
	close(s.stopCh)
}

// SyncAll runs the governance sync (metadata only).
func (s *Syncer) SyncAll(ctx context.Context) (*SyncResult, error) {
	result := &SyncResult{}

	// Phase 1: Metadata
	metaResult, err := s.syncMetadata(ctx)
	if err != nil {
		result.MetadataError = err.Error()
		slog.Error("Metadata sync failed", "error", err)
	} else {
		result.MetadataResult = metaResult
	}

	return result, nil
}

// SyncSingle runs a single sync phase.
func (s *Syncer) SyncSingle(ctx context.Context, syncType SyncType) error {
	switch syncType {
	case SyncMetadata:
		_, err := s.syncMetadata(ctx)
		return err
	default:
		return fmt.Errorf("unknown sync type: %s", syncType)
	}
}

// backgroundTick checks staleness and triggers a full sync if needed.
func (s *Syncer) backgroundTick() {
	// Prevent concurrent background syncs
	if _, loaded := s.activeSyncs.LoadOrStore("background", true); loaded {
		return
	}
	defer s.activeSyncs.Delete("background")

	if !s.isSyncStale() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := s.SyncAll(ctx)
	if err != nil {
		slog.Error("Governance background sync failed", "error", err)
		return
	}

	slog.Info("Governance background sync completed",
		"metadata", result.MetadataResult != nil,
	)
}

// isSyncStale returns true if the metadata sync is older than staleDuration.
func (s *Syncer) isSyncStale() bool {
	state, err := s.store.GetSyncState(string(SyncMetadata))
	if err != nil || state == nil {
		return true // no state yet -> needs sync
	}
	if state.LastSyncedAt == nil {
		return true
	}
	lastSync, err := time.Parse(time.RFC3339, *state.LastSyncedAt)
	if err != nil {
		return true
	}
	return time.Since(lastSync) > staleDuration
}

// executeQuery sends a SQL query to the DuckDB engine and returns rows as maps.
func (s *Syncer) executeQuery(ctx context.Context, sql string) ([]map[string]interface{}, error) {
	result, err := s.engine.Execute(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	if result == nil || len(result.Data) == 0 {
		return nil, nil
	}

	// Convert [][]interface{} to []map[string]interface{} using meta column names
	rows := make([]map[string]interface{}, 0, len(result.Data))
	for _, row := range result.Data {
		m := make(map[string]interface{}, len(result.Meta))
		for i, col := range result.Meta {
			if i < len(row) {
				m[col.Name] = row[i]
			}
		}
		rows = append(rows, m)
	}

	return rows, nil
}
