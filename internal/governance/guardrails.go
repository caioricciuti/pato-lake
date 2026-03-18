package governance

import (
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"

	"github.com/caioricciuti/pato-lake/internal/alerts"
	"github.com/caioricciuti/pato-lake/internal/database"
)

var showTablesFromRe = regexp.MustCompile(`(?i)\bSHOW\s+TABLES\s+(?:FROM|IN)\s+` + tableRefPattern)

type guardrailStore interface {
	GetEnabledPolicies() ([]Policy, error)
	CreateViolation(policyID, queryLogID, user, detail, severity, detectionPhase, requestEndpoint string) (string, error)
	UpsertIncidentFromViolation(sourceRef, policyName, user, severity, detail string) (string, bool, error)
}

type alertEventWriter interface {
	CreateAlertEvent(eventType, severity, title, message string, payload interface{}, fingerprint, sourceRef string) (string, error)
}

type GuardrailService struct {
	store  guardrailStore
	alerts alertEventWriter
}

type GuardrailDecision struct {
	Allowed bool
	Block   *GuardrailBlock
}

type GuardrailBlock struct {
	PolicyID        string
	PolicyName      string
	Severity        string
	EnforcementMode string
	ViolationID     string
	Detail          string
}

func NewGuardrailService(store *Store, db *database.DB) *GuardrailService {
	return &GuardrailService{
		store:  store,
		alerts: db,
	}
}

func (s *GuardrailService) EvaluateQuery(user, userRole, queryText, requestEndpoint string) (GuardrailDecision, error) {
	tablesUsed := extractPolicyTablesFromQuery(queryText)
	return s.evaluate(user, userRole, queryText, tablesUsed, requestEndpoint)
}

func (s *GuardrailService) EvaluateTable(user, userRole, databaseName, tableName, requestEndpoint string) (GuardrailDecision, error) {
	db := strings.TrimSpace(databaseName)
	tbl := strings.TrimSpace(tableName)
	if db == "" || tbl == "" {
		return GuardrailDecision{Allowed: true}, nil
	}
	queryText := fmt.Sprintf(`SELECT * FROM "%s"."%s"`, db, tbl)
	tablesUsed := []string{db + "." + tbl}
	return s.evaluate(user, userRole, queryText, tablesUsed, requestEndpoint)
}

func (s *GuardrailService) evaluate(user, userRole, queryText string, tablesUsed []string, requestEndpoint string) (GuardrailDecision, error) {
	policies, err := s.store.GetEnabledPolicies()
	if err != nil {
		return GuardrailDecision{}, fmt.Errorf("load enabled policies: %w", err)
	}
	if len(policies) == 0 {
		return GuardrailDecision{Allowed: true}, nil
	}

	blockingPolicies := make([]Policy, 0)

	for _, policy := range policies {
		if !policy.Enabled {
			continue
		}
		if !queryTouchesObject(tablesUsed, queryText, policy) {
			continue
		}
		if roleSatisfiesRequirement(userRole, policy.RequiredRole) {
			continue
		}
		if normalizePolicyEnforcementMode(policy.EnforcementMode) == "block" {
			blockingPolicies = append(blockingPolicies, policy)
		}
	}

	if len(blockingPolicies) == 0 {
		return GuardrailDecision{Allowed: true}, nil
	}

	selected := pickBlockingPolicy(blockingPolicies)
	detail := fmt.Sprintf(
		"Query blocked before execution: user %q touched %s without required role %q",
		user,
		describePolicyObject(selected),
		selected.RequiredRole,
	)
	severity := normalizeGuardrailSeverity(selected.Severity)

	violationID := ""
	createdViolationID, err := s.store.CreateViolation(
		selected.ID,
		"",
		user,
		detail,
		severity,
		"pre_exec_block",
		requestEndpoint,
	)
	if err != nil {
		slog.Warn("Failed to persist pre-exec guardrail violation", "policy_id", selected.ID, "error", err)
	} else {
		violationID = createdViolationID
		if _, _, err := s.store.UpsertIncidentFromViolation(violationID, selected.Name, user, severity, detail); err != nil {
			slog.Warn("Failed to upsert incident from pre-exec guardrail violation", "violation_id", violationID, "error", err)
		}
	}

	if s.alerts != nil {
		fingerprint := fmt.Sprintf("policy:%s:user:%s:hash:%s", selected.ID, user, hashNormalized(normalizeQuery(queryText)))
		payload := map[string]interface{}{
			"guardrail_status":   "blocked",
			"policy_id":          selected.ID,
			"policy_name":        selected.Name,
			"query_hash":         hashNormalized(normalizeQuery(queryText)),
			"request_endpoint":   requestEndpoint,
			"violation_id":       violationID,
			"violation_severity": severity,
			"detection_phase":    "pre_exec_block",
			"enforcement_mode":   "block",
			"blocked_user":       user,
		}
		sourceRef := violationID
		if _, err := s.alerts.CreateAlertEvent(
			alerts.EventTypePolicyViolation,
			severity,
			fmt.Sprintf("Policy blocked query: %s", strings.TrimSpace(selected.Name)),
			detail,
			payload,
			fingerprint,
			sourceRef,
		); err != nil {
			slog.Warn("Failed to create blocked guardrail alert event", "policy_id", selected.ID, "error", err)
		}
	}

	return GuardrailDecision{
		Allowed: false,
		Block: &GuardrailBlock{
			PolicyID:        selected.ID,
			PolicyName:      selected.Name,
			Severity:        severity,
			EnforcementMode: "block",
			ViolationID:     violationID,
			Detail:          detail,
		},
	}, nil
}

func pickBlockingPolicy(policies []Policy) Policy {
	ordered := make([]Policy, len(policies))
	copy(ordered, policies)
	sort.SliceStable(ordered, func(i, j int) bool {
		left := ordered[i]
		right := ordered[j]
		lp := guardrailSeverityPriority(left.Severity)
		rp := guardrailSeverityPriority(right.Severity)
		if lp != rp {
			return lp > rp
		}
		ln := strings.ToLower(strings.TrimSpace(left.Name))
		rn := strings.ToLower(strings.TrimSpace(right.Name))
		if ln != rn {
			return ln < rn
		}
		return strings.ToLower(strings.TrimSpace(left.ID)) < strings.ToLower(strings.TrimSpace(right.ID))
	})
	return ordered[0]
}

func guardrailSeverityPriority(v string) int {
	switch normalizeGuardrailSeverity(v) {
	case "critical":
		return 4
	case "error":
		return 3
	case "warn":
		return 2
	case "info":
		return 1
	default:
		return 0
	}
}

func normalizeGuardrailSeverity(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "critical":
		return "critical"
	case "error":
		return "error"
	case "info":
		return "info"
	default:
		return "warn"
	}
}

func extractPolicyTablesFromQuery(queryText string) []string {
	query := normaliseWhitespace(queryText)
	seen := make(map[string]bool, 16)
	out := make([]string, 0, 8)
	isShowTablesQuery := showTablesFromRe.MatchString(query)
	addTable := func(dbName, tableName string) {
		dbName = strings.TrimSpace(dbName)
		tableName = strings.TrimSpace(tableName)
		if tableName == "" {
			return
		}
		key := tableName
		val := tableName
		if dbName != "" {
			key = strings.ToLower(dbName + "." + tableName)
			val = dbName + "." + tableName
		} else {
			key = strings.ToLower(tableName)
		}
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, val)
	}
	addDatabase := func(dbName string) {
		dbName = strings.TrimSpace(dbName)
		if dbName == "" {
			return
		}
		key := strings.ToLower(dbName + ".__all_tables__")
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, dbName+".__all_tables__")
	}

	if !isShowTablesQuery {
		for _, src := range extractSourceTables(query) {
			addTable(src.Database, src.Table)
		}
	}
	if target := extractTarget(query); target != nil {
		addTable(target.Database, target.Table)
	}
	for _, match := range showTablesFromRe.FindAllStringSubmatch(query, -1) {
		if len(match) < 2 {
			continue
		}
		raw := stripBackticks(strings.TrimSpace(match[1]))
		if raw == "" {
			continue
		}
		parts := strings.SplitN(raw, ".", 2)
		if len(parts) == 2 {
			addDatabase(stripBackticks(parts[0]))
			continue
		}
		addDatabase(stripBackticks(raw))
	}
	return out
}
