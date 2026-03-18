package duckdb

import (
	"context"
	"fmt"
)

// CatalogDatabase represents a database in DuckDB.
type CatalogDatabase struct {
	Name string `json:"name"`
}

// CatalogTable represents a table or view.
type CatalogTable struct {
	Name      string `json:"name"`
	Type      string `json:"type"` // BASE TABLE, VIEW, etc.
	Schema    string `json:"schema,omitempty"`
	Estimated int64  `json:"estimated_rows,omitempty"`
}

// CatalogColumn represents a column.
type CatalogColumn struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	IsNullable   bool    `json:"is_nullable"`
	DefaultValue *string `json:"default_value,omitempty"`
	Comment      *string `json:"comment,omitempty"`
}

// ListDatabases returns all attached databases.
func (e *Engine) ListDatabases(ctx context.Context) ([]CatalogDatabase, error) {
	rows, err := e.db.QueryContext(ctx, `
		SELECT catalog_name
		FROM information_schema.schemata
		GROUP BY catalog_name
		ORDER BY catalog_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dbs []CatalogDatabase
	seen := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if !seen[name] {
			seen[name] = true
			dbs = append(dbs, CatalogDatabase{Name: name})
		}
	}
	return dbs, rows.Err()
}

// ListTables returns tables/views in a database.
func (e *Engine) ListTables(ctx context.Context, database string) ([]CatalogTable, error) {
	query := `
		SELECT table_name, table_type
		FROM information_schema.tables
		WHERE table_catalog = ?
		  AND table_schema = 'main'
		ORDER BY table_name
	`
	if database == "" {
		database = "memory"
	}

	rows, err := e.db.QueryContext(ctx, query, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []CatalogTable
	for rows.Next() {
		var t CatalogTable
		if err := rows.Scan(&t.Name, &t.Type); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

// ListColumns returns columns for a table.
func (e *Engine) ListColumns(ctx context.Context, database, table string) ([]CatalogColumn, error) {
	if database == "" {
		database = "memory"
	}

	rows, err := e.db.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_catalog = ?
		  AND table_name = ?
		ORDER BY ordinal_position
	`, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []CatalogColumn
	for rows.Next() {
		var c CatalogColumn
		var nullable string
		var defVal *string
		if err := rows.Scan(&c.Name, &c.Type, &nullable, &defVal); err != nil {
			return nil, err
		}
		c.IsNullable = nullable == "YES"
		c.DefaultValue = defVal
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// ListFunctions returns built-in DuckDB functions for autocomplete.
func (e *Engine) ListFunctions(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, `
		SELECT DISTINCT function_name
		FROM duckdb_functions()
		ORDER BY function_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var funcs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		funcs = append(funcs, name)
	}
	return funcs, rows.Err()
}

// TableRowCount returns the approximate row count for a table.
func (e *Engine) TableRowCount(ctx context.Context, database, table string) (int64, error) {
	q := fmt.Sprintf(`SELECT COUNT(*) FROM "%s".main."%s"`, database, table)
	var count int64
	err := e.db.QueryRowContext(ctx, q).Scan(&count)
	return count, err
}

// GetDatabaseInfo returns summary info about a database.
func (e *Engine) GetDatabaseInfo(ctx context.Context, database string) (map[string]interface{}, error) {
	if database == "" {
		database = "memory"
	}

	var tableCount int
	err := e.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_catalog = ?
		  AND table_schema = 'main'
	`, database).Scan(&tableCount)
	if err != nil {
		return nil, err
	}

	var dbSize string
	_ = e.db.QueryRowContext(ctx, `SELECT database_size FROM pragma_database_size() WHERE database_name = ?`, database).Scan(&dbSize)

	return map[string]interface{}{
		"name":        database,
		"table_count": tableCount,
		"size":        dbSize,
	}, nil
}
