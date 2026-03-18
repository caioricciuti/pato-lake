package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/caioricciuti/pato-lake/internal/duckdb"
)

// Writer handles DuckDB table creation, schema evolution, and batch inserts for ingested events.
type Writer struct {
	engine *duckdb.Engine
}

// NewWriter creates a new ingest writer.
func NewWriter(engine *duckdb.Engine) *Writer {
	return &Writer{engine: engine}
}

// WriteBatch writes a batch of records to the target table, creating/evolving it as needed.
func (w *Writer) WriteBatch(ctx context.Context, schema, table string, records []map[string]interface{}) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	// Add _ingested_at timestamp to every record
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, rec := range records {
		rec["_ingested_at"] = now
	}

	// Ensure table exists (from first record's schema)
	if err := w.ensureTable(ctx, schema, table, records[0]); err != nil {
		return 0, fmt.Errorf("ensure table: %w", err)
	}

	// Evolve schema if new columns appear
	if err := w.evolveSchema(ctx, schema, table, records); err != nil {
		slog.Warn("Schema evolution warning", "schema", schema, "table", table, "error", err)
	}

	// Get all unique column names across all records
	colSet := make(map[string]bool)
	for _, rec := range records {
		for k := range rec {
			colSet[k] = true
		}
	}
	colNames := make([]string, 0, len(colSet))
	for k := range colSet {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)

	// Build INSERT statement
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`INSERT INTO "%s"."%s" (`, schema, table))
	for i, col := range colNames {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf(`"%s"`, col))
	}
	sb.WriteString(") VALUES ")

	for ri, rec := range records {
		if ri > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for ci, col := range colNames {
			if ci > 0 {
				sb.WriteString(", ")
			}
			val := rec[col]
			sb.WriteString(duckdb.SQLValue(val))
		}
		sb.WriteByte(')')
	}

	_, err := w.engine.Exec(ctx, sb.String())
	if err != nil {
		return 0, fmt.Errorf("execute insert: %w", err)
	}

	return len(records), nil
}

// ensureTable creates the target table if it doesn't exist.
func (w *Writer) ensureTable(ctx context.Context, schema, table string, sample map[string]interface{}) error {
	colNames := make([]string, 0, len(sample))
	for k := range sample {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)

	var cols []string
	for _, name := range colNames {
		duckType := duckdb.InferDuckDBType(sample[name])
		// _ingested_at should always be TIMESTAMP
		if name == "_ingested_at" {
			duckType = "TIMESTAMP"
		}
		cols = append(cols, fmt.Sprintf(`"%s" %s`, name, duckType))
	}

	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (%s)`, schema, table, strings.Join(cols, ", "))
	_, err := w.engine.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

// evolveSchema adds new columns found in records that don't exist in the table yet.
func (w *Writer) evolveSchema(ctx context.Context, schema, table string, records []map[string]interface{}) error {
	// Collect all keys
	allKeys := make(map[string]interface{})
	for _, rec := range records {
		for k, v := range rec {
			if _, exists := allKeys[k]; !exists {
				allKeys[k] = v
			}
		}
	}

	for colName, sampleVal := range allKeys {
		duckType := duckdb.InferDuckDBType(sampleVal)
		if colName == "_ingested_at" {
			duckType = "TIMESTAMP"
		}

		ddl := fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN "%s" %s`, schema, table, colName, duckType)
		_, err := w.engine.Exec(ctx, ddl)
		if err != nil {
			// Ignore "already exists" errors — this is expected during concurrent evolution
			errStr := err.Error()
			if strings.Contains(errStr, "already exists") || strings.Contains(errStr, "Duplicate") {
				continue
			}
			return fmt.Errorf("add column %s: %w", colName, err)
		}
		slog.Info("Schema evolution: added column", "schema", schema, "table", table, "column", colName, "type", duckType)
	}

	return nil
}
