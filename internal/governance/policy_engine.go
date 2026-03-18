package governance

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ── Public API ──────────────────────────────────────────────────────────────

// EvaluatePolicies checks a query log entry against all provided policies and
// returns any violations. It uses the Patolake role hierarchy to check access.
func EvaluatePolicies(entry QueryLogEntry, policies []Policy, userRole string) []PolicyViolation {
	// Parse the tables_used JSON field.
	tablesUsed := parseTablesUsed(entry.TablesUsed)

	now := time.Now().UTC().Format(time.RFC3339)

	var violations []PolicyViolation
	for _, policy := range policies {
		if !policy.Enabled {
			continue
		}
		if normalizePolicyEnforcementMode(policy.EnforcementMode) == "block" {
			continue
		}

		// Check whether the query touches the object protected by this policy.
		if !queryTouchesObject(tablesUsed, entry.QueryText, policy) {
			continue
		}

		// Check whether the user's role satisfies the required role.
		if roleSatisfiesRequirement(userRole, policy.RequiredRole) {
			continue
		}

		// No required role found → create a violation.
		detail := fmt.Sprintf(
			"User %q executed a query touching %s without required role %q",
			entry.User,
			describePolicyObject(policy),
			policy.RequiredRole,
		)

		violations = append(violations, PolicyViolation{
			ID:              uuid.New().String(),
			PolicyID:        policy.ID,
			QueryLogID:      entry.ID,
			User:            entry.User,
			ViolationDetail: detail,
			Severity:        policy.Severity,
			DetectedAt:      now,
			CreatedAt:       now,
			PolicyName:      policy.Name,
		})
	}

	return violations
}

// roleSatisfiesRequirement checks whether the user's role meets the required
// role using a simple hierarchy: admin=3, analyst=2, viewer=1.
func roleSatisfiesRequirement(userRole, requiredRole string) bool {
	hierarchy := map[string]int{"admin": 3, "analyst": 2, "viewer": 1}
	return hierarchy[strings.ToLower(userRole)] >= hierarchy[strings.ToLower(requiredRole)]
}

// ── Internal helpers ────────────────────────────────────────────────────────

// parseTablesUsed deserialises the JSON array stored in QueryLogEntry.TablesUsed.
// It returns an empty slice on error or empty input.
func parseTablesUsed(raw string) []string {
	if raw == "" || raw == "[]" {
		return nil
	}
	var tables []string
	if err := json.Unmarshal([]byte(raw), &tables); err != nil {
		return nil
	}
	return tables
}

// queryTouchesObject determines whether a query (identified by its list of
// tables used and the raw SQL text) accesses the object described by a policy.
func queryTouchesObject(tablesUsed []string, queryText string, policy Policy) bool {
	switch strings.ToLower(policy.ObjectType) {
	case "database":
		return touchesDatabase(tablesUsed, deref(policy.ObjectDatabase))

	case "table":
		return touchesTable(tablesUsed, deref(policy.ObjectDatabase), deref(policy.ObjectTable))

	case "column":
		if !touchesTable(tablesUsed, deref(policy.ObjectDatabase), deref(policy.ObjectTable)) {
			return false
		}
		// For column-level policies, check if the column name appears in the
		// query text.  This is a heuristic — a full parser would be needed
		// for perfect accuracy.
		col := deref(policy.ObjectColumn)
		if col == "" {
			return false
		}
		return columnMentioned(queryText, col)

	default:
		return false
	}
}

// touchesDatabase returns true if any entry in tablesUsed belongs to the
// given database.  tablesUsed entries are expected in "db.table" format.
func touchesDatabase(tablesUsed []string, database string) bool {
	if database == "" {
		return false
	}
	lowerDB := strings.ToLower(database)
	for _, t := range tablesUsed {
		parts := strings.SplitN(t, ".", 2)
		if len(parts) == 2 && strings.ToLower(parts[0]) == lowerDB {
			return true
		}
	}
	return false
}

// touchesTable returns true if the specific db.table combination appears in
// tablesUsed.
func touchesTable(tablesUsed []string, database, table string) bool {
	if table == "" {
		return false
	}
	lowerDB := strings.ToLower(database)
	lowerTbl := strings.ToLower(table)

	for _, t := range tablesUsed {
		parts := strings.SplitN(t, ".", 2)
		switch {
		case len(parts) == 2:
			if strings.ToLower(parts[0]) == lowerDB && strings.ToLower(parts[1]) == lowerTbl {
				return true
			}
		case len(parts) == 1:
			// No database qualifier in tablesUsed — match on table name alone
			// only when the policy also has no database.
			if lowerDB == "" && strings.ToLower(parts[0]) == lowerTbl {
				return true
			}
		}
	}
	return false
}

// columnMentioned does a case-insensitive check for the column identifier in
// the query text.  It looks for the column name as a whole word (surrounded
// by non-identifier characters or string boundaries).
func columnMentioned(queryText, column string) bool {
	lower := strings.ToLower(queryText)
	col := strings.ToLower(column)
	idx := 0
	for {
		pos := strings.Index(lower[idx:], col)
		if pos < 0 {
			return false
		}
		pos += idx
		// Check word boundaries.
		startOK := pos == 0 || !isIdentChar(lower[pos-1])
		endPos := pos + len(col)
		endOK := endPos >= len(lower) || !isIdentChar(lower[endPos])
		if startOK && endOK {
			return true
		}
		idx = pos + 1
	}
}

// isIdentChar returns true for characters that can appear in a SQL identifier.
func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '_'
}

// describePolicyObject returns a human-readable description of the object a
// policy protects, for use in violation messages.
func describePolicyObject(p Policy) string {
	switch strings.ToLower(p.ObjectType) {
	case "database":
		return fmt.Sprintf("database %q", deref(p.ObjectDatabase))
	case "table":
		db := deref(p.ObjectDatabase)
		tbl := deref(p.ObjectTable)
		if db != "" {
			return fmt.Sprintf("table %q.%q", db, tbl)
		}
		return fmt.Sprintf("table %q", tbl)
	case "column":
		db := deref(p.ObjectDatabase)
		tbl := deref(p.ObjectTable)
		col := deref(p.ObjectColumn)
		if db != "" {
			return fmt.Sprintf("column %q.%q.%q", db, tbl, col)
		}
		return fmt.Sprintf("column %q.%q", tbl, col)
	default:
		return p.ObjectType
	}
}

// deref safely dereferences a string pointer, returning the empty string for nil.
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
