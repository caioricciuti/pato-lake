package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// ColumnMeta describes a single result column.
type ColumnMeta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// QueryStats holds execution statistics.
type QueryStats struct {
	Elapsed   float64 `json:"elapsed"`
	RowsRead  int64   `json:"rows_read"`
	BytesRead int64   `json:"bytes_read"`
}

// QueryResult is the standard result of a query execution.
type QueryResult struct {
	Meta       []ColumnMeta    `json:"meta"`
	Data       [][]interface{} `json:"data"`
	Statistics *QueryStats     `json:"statistics,omitempty"`
	RowCount   int64           `json:"rows"`
}

// StreamMeta is sent once at the start of a streaming query.
type StreamMeta struct {
	Type string       `json:"type"`
	Meta []ColumnMeta `json:"meta"`
}

// StreamChunk is a batch of rows during streaming.
type StreamChunk struct {
	Type string          `json:"type"`
	Data [][]interface{} `json:"data"`
	Seq  int             `json:"seq"`
}

// StreamDone signals query completion.
type StreamDone struct {
	Type       string      `json:"type"`
	Statistics *QueryStats `json:"statistics,omitempty"`
	TotalRows  int64       `json:"total_rows"`
}

// StreamError signals a query error.
type StreamError struct {
	Type  string `json:"type"`
	Error string `json:"error"`
}

// Config holds DuckDB engine configuration.
type Config struct {
	Path        string // empty = in-memory
	MemoryLimit string // e.g. "4GB"
	Threads     int    // 0 = auto
	Extensions  []string
}

// Engine wraps a DuckDB database for query execution.
type Engine struct {
	connector *duckdb.Connector
	db        *sql.DB
	mu        sync.RWMutex
	closed    bool
}

// NewEngine creates a new DuckDB engine.
func NewEngine(cfg Config) (*Engine, error) {
	dsn := cfg.Path
	if dsn == "" {
		dsn = "" // in-memory
	}

	// Build initialization function for settings and extensions
	initFn := func(execer driver.ExecerContext) error {
		ctx := context.Background()

		if cfg.MemoryLimit != "" {
			if _, err := execer.ExecContext(ctx, fmt.Sprintf("SET memory_limit = '%s'", cfg.MemoryLimit), nil); err != nil {
				slog.Warn("Failed to set memory_limit", "value", cfg.MemoryLimit, "error", err)
			}
		}
		if cfg.Threads > 0 {
			if _, err := execer.ExecContext(ctx, fmt.Sprintf("SET threads = %d", cfg.Threads), nil); err != nil {
				slog.Warn("Failed to set threads", "value", cfg.Threads, "error", err)
			}
		}

		// Load extensions
		for _, ext := range cfg.Extensions {
			if _, err := execer.ExecContext(ctx, fmt.Sprintf("INSTALL '%s'", ext), nil); err != nil {
				slog.Warn("Failed to install extension", "extension", ext, "error", err)
			}
			if _, err := execer.ExecContext(ctx, fmt.Sprintf("LOAD '%s'", ext), nil); err != nil {
				slog.Warn("Failed to load extension", "extension", ext, "error", err)
			}
		}

		return nil
	}

	connector, err := duckdb.NewConnector(dsn, initFn)
	if err != nil {
		return nil, fmt.Errorf("duckdb connector: %w", err)
	}

	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)

	// Verify connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		connector.Close()
		return nil, fmt.Errorf("duckdb ping: %w", err)
	}

	slog.Info("DuckDB engine initialized", "path", cfg.Path)

	return &Engine{
		connector: connector,
		db:        db,
	}, nil
}

// DB returns the underlying *sql.DB for advanced usage.
func (e *Engine) DB() *sql.DB {
	return e.db
}

// Close shuts down the engine.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	if err := e.db.Close(); err != nil {
		return err
	}
	return e.connector.Close()
}

// Execute runs a query and returns structured results.
func (e *Engine) Execute(ctx context.Context, query string) (*QueryResult, error) {
	start := time.Now()

	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("column types: %w", err)
	}

	meta := make([]ColumnMeta, len(colTypes))
	for i, ct := range colTypes {
		meta[i] = ColumnMeta{
			Name: ct.Name(),
			Type: ct.DatabaseTypeName(),
		}
	}

	var data [][]interface{}
	var rowCount int64

	for rows.Next() {
		vals := make([]interface{}, len(colTypes))
		ptrs := make([]interface{}, len(colTypes))
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		// Normalize values for JSON serialization
		row := make([]interface{}, len(vals))
		for i, v := range vals {
			row[i] = normalizeValue(v)
		}
		data = append(data, row)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	elapsed := time.Since(start).Seconds()

	return &QueryResult{
		Meta: meta,
		Data: data,
		Statistics: &QueryStats{
			Elapsed:  elapsed,
			RowsRead: rowCount,
		},
		RowCount: rowCount,
	}, nil
}

// Exec runs a statement that doesn't return rows (DDL, INSERT, etc.).
func (e *Engine) Exec(ctx context.Context, query string) (sql.Result, error) {
	return e.db.ExecContext(ctx, query)
}

// ExecuteStream runs a query and streams results in chunks.
func (e *Engine) ExecuteStream(
	ctx context.Context,
	query string,
	chunkSize int,
	onMeta func([]ColumnMeta),
	onChunk func([][]interface{}, int),
	onDone func(*QueryStats, int64),
	onError func(error),
) {
	if chunkSize <= 0 {
		chunkSize = 5000
	}

	start := time.Now()

	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		onError(err)
		return
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		onError(fmt.Errorf("column types: %w", err))
		return
	}

	meta := make([]ColumnMeta, len(colTypes))
	for i, ct := range colTypes {
		meta[i] = ColumnMeta{
			Name: ct.Name(),
			Type: ct.DatabaseTypeName(),
		}
	}
	onMeta(meta)

	var totalRows int64
	seq := 0
	chunk := make([][]interface{}, 0, chunkSize)

	for rows.Next() {
		if ctx.Err() != nil {
			onError(ctx.Err())
			return
		}

		vals := make([]interface{}, len(colTypes))
		ptrs := make([]interface{}, len(colTypes))
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			onError(fmt.Errorf("scan row: %w", err))
			return
		}

		row := make([]interface{}, len(vals))
		for i, v := range vals {
			row[i] = normalizeValue(v)
		}
		chunk = append(chunk, row)
		totalRows++

		if len(chunk) >= chunkSize {
			onChunk(chunk, seq)
			seq++
			chunk = make([][]interface{}, 0, chunkSize)
		}
	}

	if err := rows.Err(); err != nil {
		onError(err)
		return
	}

	// Flush remaining
	if len(chunk) > 0 {
		onChunk(chunk, seq)
	}

	elapsed := time.Since(start).Seconds()
	onDone(&QueryStats{
		Elapsed:  elapsed,
		RowsRead: totalRows,
	}, totalRows)
}

// Version returns the DuckDB version string.
func (e *Engine) Version(ctx context.Context) (string, error) {
	var ver string
	err := e.db.QueryRowContext(ctx, "SELECT version()").Scan(&ver)
	return ver, err
}

// normalizeValue converts DuckDB driver types to JSON-friendly Go types.
func normalizeValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		// Try to parse as JSON first
		var js json.RawMessage
		if json.Unmarshal(val, &js) == nil {
			return js
		}
		return string(val)
	case time.Time:
		if val.Hour() == 0 && val.Minute() == 0 && val.Second() == 0 && val.Nanosecond() == 0 {
			return val.Format("2006-01-02")
		}
		return val.Format(time.RFC3339Nano)
	case int64, int32, int16, int8, float64, float32, bool, string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// AttachExistingDatabases scans dataDir for .duckdb files and attaches them.
// This ensures user-created databases persist across server restarts.
func (e *Engine) AttachExistingDatabases(dataDir, mainDBFile string) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		slog.Warn("Failed to scan data directory for databases", "dir", dataDir, "error", err)
		return
	}
	mainBase := filepath.Base(mainDBFile)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".duckdb") {
			continue
		}
		if entry.Name() == mainBase {
			continue
		}
		stem := strings.TrimSuffix(entry.Name(), ".duckdb")
		fullPath := filepath.Join(dataDir, entry.Name())
		escaped := strings.ReplaceAll(fullPath, "'", "''")
		attachSQL := fmt.Sprintf(`ATTACH IF NOT EXISTS '%s' AS "%s"`, escaped, strings.ReplaceAll(stem, `"`, `""`))
		if _, err := e.db.ExecContext(context.Background(), attachSQL); err != nil {
			slog.Warn("Failed to reattach database", "file", entry.Name(), "error", err)
		} else {
			slog.Info("Reattached database", "name", stem, "path", fullPath)
		}
	}
}
