package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/caioricciuti/pato-lake/internal/duckdb"
)

// DuckDBSink writes batches to a DuckDB table.
type DuckDBSink struct {
	engine    *duckdb.Engine
	tableOnce sync.Once
	tableErr  error
}

// NewDuckDBSink creates a new DuckDB sink connector.
func NewDuckDBSink(engine *duckdb.Engine) *DuckDBSink {
	return &DuckDBSink{engine: engine}
}

func (s *DuckDBSink) Type() string { return "sink_duckdb" }

// Validate checks the sink configuration.
func (s *DuckDBSink) Validate(cfg ConnectorConfig) error {
	table, _ := cfg.Fields["table"].(string)
	if table == "" {
		return fmt.Errorf("table is required")
	}
	return nil
}

// WriteBatch inserts a batch of records into the DuckDB table.
func (s *DuckDBSink) WriteBatch(ctx context.Context, cfg ConnectorConfig, batch Batch) (int, error) {
	if len(batch.Records) == 0 {
		return 0, nil
	}

	// Auto-create table on first batch if configured
	if boolField(cfg.Fields, "create_table", false) {
		s.tableOnce.Do(func() {
			s.tableErr = s.ensureTable(ctx, cfg, batch)
		})
		if s.tableErr != nil {
			return 0, fmt.Errorf("ensure table: %w", s.tableErr)
		}
	}

	schema := stringField(cfg.Fields, "schema", "main")
	table, _ := cfg.Fields["table"].(string)

	// Build INSERT ... VALUES statement
	if len(batch.Records) == 0 {
		return 0, nil
	}

	// Get column names from first record
	first := batch.Records[0].Data
	if len(first) == 0 && len(batch.Records[0].RawJSON) > 0 {
		if err := json.Unmarshal(batch.Records[0].RawJSON, &first); err != nil {
			return 0, fmt.Errorf("unmarshal record: %w", err)
		}
	}

	colNames := make([]string, 0, len(first))
	for k := range first {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)

	// Build VALUES rows
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`INSERT INTO "%s"."%s" (`, schema, table))
	for i, col := range colNames {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf(`"%s"`, col))
	}
	sb.WriteString(") VALUES ")

	for ri, rec := range batch.Records {
		data := rec.Data
		if len(data) == 0 && len(rec.RawJSON) > 0 {
			_ = json.Unmarshal(rec.RawJSON, &data)
		}
		if ri > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for ci, col := range colNames {
			if ci > 0 {
				sb.WriteString(", ")
			}
			val := data[col]
			sb.WriteString(sqlValue(val))
		}
		sb.WriteByte(')')
	}

	_, err := s.engine.Exec(ctx, sb.String())
	if err != nil {
		return 0, fmt.Errorf("execute insert: %w", err)
	}

	return len(batch.Records), nil
}

// ensureTable creates the target table if it doesn't exist.
func (s *DuckDBSink) ensureTable(ctx context.Context, cfg ConnectorConfig, batch Batch) error {
	schema := stringField(cfg.Fields, "schema", "main")
	table := stringField(cfg.Fields, "table", "")
	if table == "" {
		return fmt.Errorf("table name is required for auto-creation")
	}

	if len(batch.Records) == 0 {
		return fmt.Errorf("cannot infer schema from empty batch")
	}

	data := batch.Records[0].Data
	if len(data) == 0 && len(batch.Records[0].RawJSON) > 0 {
		_ = json.Unmarshal(batch.Records[0].RawJSON, &data)
	}
	if len(data) == 0 {
		return fmt.Errorf("cannot infer schema from empty record")
	}

	colNames := make([]string, 0, len(data))
	for k := range data {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)

	var cols []string
	for _, name := range colNames {
		duckType := inferDuckDBType(data[name])
		cols = append(cols, fmt.Sprintf(`"%s" %s`, name, duckType))
	}

	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (%s)`, schema, table, strings.Join(cols, ", "))

	_, err := s.engine.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("execute CREATE TABLE: %w", err)
	}

	slog.Info("Auto-created DuckDB table", "schema", schema, "table", table, "columns", len(cols))
	return nil
}

// inferDuckDBType maps a Go/JSON value to a DuckDB column type.
// Delegates to the shared duckdb.InferDuckDBType helper.
func inferDuckDBType(v interface{}) string {
	return duckdb.InferDuckDBType(v)
}

// sqlValue formats a Go value as a SQL literal.
// Delegates to the shared duckdb.SQLValue helper.
func sqlValue(v interface{}) string {
	return duckdb.SQLValue(v)
}
