package governance

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// -- Query helper functions ---------------------------------------------------

// ClassifyQuery returns a classification string for the query type.
func ClassifyQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	for strings.HasPrefix(trimmed, "--") {
		if idx := strings.Index(trimmed, "\n"); idx >= 0 {
			trimmed = strings.TrimSpace(trimmed[idx+1:])
		} else {
			break
		}
	}

	upper := strings.ToUpper(trimmed)
	switch {
	case strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "WITH"):
		return "Select"
	case strings.HasPrefix(upper, "INSERT"):
		return "Insert"
	case strings.HasPrefix(upper, "CREATE"):
		return "Create"
	case strings.HasPrefix(upper, "ALTER"):
		return "Alter"
	case strings.HasPrefix(upper, "DROP"):
		return "Drop"
	default:
		return "Other"
	}
}

var (
	stringLiteralRe = regexp.MustCompile(`'[^']*'`)
	numberLiteralRe = regexp.MustCompile(`\b\d+\.?\d*\b`)
	multiSpaceRe    = regexp.MustCompile(`\s+`)
)

func normalizeQuery(query string) string {
	normalized := stringLiteralRe.ReplaceAllString(query, "'?'")
	normalized = numberLiteralRe.ReplaceAllString(normalized, "?")
	normalized = multiSpaceRe.ReplaceAllString(normalized, " ")
	normalized = strings.TrimSpace(normalized)
	return strings.ToUpper(normalized)
}

func hashNormalized(normalized string) string {
	h := sha256.Sum256([]byte(normalized))
	return fmt.Sprintf("%x", h)[:32]
}

// ExtractTablesFromQuery returns a JSON array string of table references
// found in the given SQL text. It is used by the API-layer query logger.
func ExtractTablesFromQuery(queryText string) string {
	sources := extractSourceTables(queryText)
	if target := extractTarget(queryText); target != nil {
		sources = append(sources, *target)
	}
	if len(sources) == 0 {
		return "[]"
	}
	tables := make([]string, 0, len(sources))
	seen := make(map[string]bool, len(sources))
	for _, ref := range sources {
		var key string
		if ref.Database != "" {
			key = ref.Database + "." + ref.Table
		} else {
			key = ref.Table
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		tables = append(tables, key)
	}
	b, _ := json.Marshal(tables)
	return string(b)
}

func extractTablesJSON(v interface{}) string {
	if v == nil {
		return "[]"
	}

	switch val := v.(type) {
	case string:
		if strings.HasPrefix(val, "[") {
			return val
		}
		if val == "" {
			return "[]"
		}
		b, _ := json.Marshal([]string{val})
		return string(b)
	case []interface{}:
		strs := make([]string, 0, len(val))
		for _, item := range val {
			strs = append(strs, fmt.Sprintf("%v", item))
		}
		b, _ := json.Marshal(strs)
		return string(b)
	case []string:
		b, _ := json.Marshal(val)
		return string(b)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "[]"
		}
		return string(b)
	}
}
