package duckdb

import (
	"fmt"
	"strings"
)

// InferDuckDBType maps a Go/JSON value to a DuckDB column type.
func InferDuckDBType(v interface{}) string {
	switch v.(type) {
	case string:
		return "VARCHAR"
	case float64:
		return "DOUBLE"
	case bool:
		return "BOOLEAN"
	case nil:
		return "VARCHAR"
	default:
		return "VARCHAR"
	}
}

// SQLValue formats a Go value as a SQL literal.
func SQLValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case float64:
		return fmt.Sprintf("%v", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		s := fmt.Sprintf("%v", val)
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
}
