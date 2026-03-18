package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/caioricciuti/pato-lake/internal/config"
	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/caioricciuti/pato-lake/internal/duckdb"
	"github.com/caioricciuti/pato-lake/internal/governance"
	"github.com/caioricciuti/pato-lake/internal/server/middleware"
	"github.com/go-chi/chi/v5"
)

const maxQueryTimeout = 5 * time.Minute

// QueryHandler handles SQL query execution and schema exploration endpoints.
type QueryHandler struct {
	DB         *database.DB
	Engine     *duckdb.Engine
	Config     *config.Config
	Guardrails *governance.GuardrailService
	GovStore   *governance.Store
}

// Routes registers all query-related routes on the given chi.Router.
func (h *QueryHandler) Routes(r chi.Router) {
	r.Post("/", h.ExecuteQuery)
	r.Post("/run", h.ExecuteQuery)
	r.Post("/stream", h.StreamQuery)
	r.Post("/explorer-data", h.ExplorerData)
	r.Post("/format", h.FormatSQL)
	r.Post("/explain", h.ExplainQuery)
	r.Post("/plan", h.QueryPlan)
	r.Get("/databases", h.ListDatabases)
	r.Get("/tables", h.ListTables)
	r.Get("/columns", h.ListColumns)
	r.Get("/data-types", h.ListDataTypes)
	r.Get("/host-info", h.GetHostInfo)
	r.Get("/completions", h.ListCompletions)
	r.Get("/history", h.QueryHistory)
	r.Post("/schema/database", h.CreateDatabase)
	r.Post("/schema/database/drop", h.DropDatabase)
	r.Post("/schema/table", h.CreateTable)
	r.Post("/schema/table/drop", h.DropTable)
	r.Post("/upload/discover", h.DiscoverUploadSchema)
	r.Post("/upload/ingest", h.IngestUpload)
}

// --- Request / Response types ---

type executeQueryRequest struct {
	Query   string `json:"query"`
	Timeout int    `json:"timeout"` // seconds
}

type formatRequest struct {
	Query string `json:"query"`
}

type formatResponse struct {
	Formatted string `json:"formatted"`
}

type explainRequest struct {
	Query string `json:"query"`
}

type planNode struct {
	ID       string  `json:"id"`
	ParentID *string `json:"parent_id,omitempty"`
	Level    int     `json:"level"`
	Label    string  `json:"label"`
}

type createDatabaseRequest struct {
	Name        string `json:"name"`
	IfNotExists *bool  `json:"if_not_exists"`
}

type dropDatabaseRequest struct {
	Name     string `json:"name"`
	IfExists *bool  `json:"if_exists"`
}

type createTableColumn struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	DefaultExpression string `json:"default_expression"`
	Comment           string `json:"comment"`
}

type createTableRequest struct {
	Database    string              `json:"database"`
	Name        string              `json:"name"`
	IfNotExists *bool               `json:"if_not_exists"`
	Columns     []createTableColumn `json:"columns"`
	Comment     string              `json:"comment"`
}

type dropTableRequest struct {
	Database string `json:"database"`
	Name     string `json:"name"`
	IfExists *bool  `json:"if_exists"`
}

// --- Handlers ---

// ExecuteQuery handles POST / and POST /run.
func (h *QueryHandler) ExecuteQuery(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	var req executeQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		writeError(w, http.StatusBadRequest, "Query is required")
		return
	}
	if !h.enforceGuardrailsForQuery(w, r, query, r.URL.Path) {
		return
	}

	timeout := 30 * time.Second
	if req.Timeout > 0 {
		timeout = time.Duration(req.Timeout) * time.Second
	}
	if timeout > maxQueryTimeout {
		timeout = maxQueryTimeout
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	start := time.Now()
	result, err := h.Engine.Execute(ctx, query)
	elapsed := time.Since(start).Milliseconds()
	eventTime := start.UTC().Format(time.RFC3339)

	if err != nil {
		slog.Warn("Query execution failed", "error", err, "user", session.Username)
		// Log failed query to governance
		h.logQueryToGovernance(session.Username, query, eventTime, elapsed, 0, true, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Log successful query to governance
	h.logQueryToGovernance(session.Username, query, eventTime, elapsed, result.RowCount, false, "")

	// Audit log
	preview := query
	if len(preview) > 100 {
		preview = preview[:100] + "..."
	}
	go func() {
		h.DB.CreateAuditLog(database.AuditLogParams{
			Action:    "query.execute",
			Username:  strPtr(session.Username),
			Details:   strPtr(preview),
			IPAddress: strPtr(r.RemoteAddr),
		})
	}()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"meta":       result.Meta,
		"data":       result.Data,
		"statistics": result.Statistics,
		"rows":       result.RowCount,
		"elapsed_ms": elapsed,
	})
}

// StreamQuery handles POST /stream — streaming query execution via NDJSON chunked response.
func (h *QueryHandler) StreamQuery(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	var req executeQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		writeError(w, http.StatusBadRequest, "Query is required")
		return
	}
	if !h.enforceGuardrailsForQuery(w, r, query, r.URL.Path) {
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "Streaming not supported")
		return
	}

	timeout := 30 * time.Second
	if req.Timeout > 0 {
		timeout = time.Duration(req.Timeout) * time.Second
	}
	if timeout > maxQueryTimeout {
		timeout = maxQueryTimeout
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(w)
	start := time.Now()
	var streamTotalRows int64
	var streamErr error

	h.Engine.ExecuteStream(
		ctx,
		query,
		5000, // chunk size
		func(meta []duckdb.ColumnMeta) {
			enc.Encode(map[string]interface{}{"type": "meta", "meta": meta})
			flusher.Flush()
		},
		func(data [][]interface{}, seq int) {
			enc.Encode(map[string]interface{}{"type": "chunk", "data": data, "seq": seq})
			flusher.Flush()
		},
		func(stats *duckdb.QueryStats, totalRows int64) {
			streamTotalRows = totalRows
			enc.Encode(map[string]interface{}{
				"type":       "done",
				"statistics": stats,
				"total_rows": totalRows,
			})
			flusher.Flush()
		},
		func(err error) {
			streamErr = err
			enc.Encode(map[string]interface{}{"type": "error", "error": err.Error()})
			flusher.Flush()
		},
	)

	elapsed := time.Since(start).Milliseconds()
	eventTime := start.UTC().Format(time.RFC3339)
	if streamErr != nil {
		h.logQueryToGovernance(session.Username, query, eventTime, elapsed, 0, true, streamErr.Error())
	} else {
		h.logQueryToGovernance(session.Username, query, eventTime, elapsed, streamTotalRows, false, "")
	}

	// Audit log
	preview := query
	if len(preview) > 100 {
		preview = preview[:100] + "..."
	}
	go func() {
		h.DB.CreateAuditLog(database.AuditLogParams{
			Action:    "query.stream",
			Username:  strPtr(session.Username),
			Details:   strPtr(preview),
			IPAddress: strPtr(r.RemoteAddr),
		})
	}()
}

// FormatSQL handles POST /format.
func (h *QueryHandler) FormatSQL(w http.ResponseWriter, r *http.Request) {
	var req formatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		writeError(w, http.StatusBadRequest, "Query is required")
		return
	}

	formatted := formatSQL(query)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(formatResponse{Formatted: formatted})
}

// ExplainQuery handles POST /explain.
func (h *QueryHandler) ExplainQuery(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	var req explainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		writeError(w, http.StatusBadRequest, "Query is required")
		return
	}
	if !h.enforceGuardrailsForQuery(w, r, query, r.URL.Path) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	explainSQL := "EXPLAIN " + query
	result, err := h.Engine.Execute(ctx, explainSQL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"meta":    result.Meta,
		"data":    result.Data,
	})
}

// QueryPlan handles POST /plan and returns a parsed plan tree for visualization.
func (h *QueryHandler) QueryPlan(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	var req explainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		writeError(w, http.StatusBadRequest, "Query is required")
		return
	}
	if !h.enforceGuardrailsForQuery(w, r, query, r.URL.Path) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()

	// DuckDB supports EXPLAIN ANALYZE for detailed plan info
	candidates := []struct {
		source string
		sql    string
	}{
		{source: "analyze", sql: "EXPLAIN ANALYZE " + query},
		{source: "explain", sql: "EXPLAIN " + query},
	}

	var lastErr error
	for _, candidate := range candidates {
		result, err := h.Engine.Execute(ctx, candidate.sql)
		if err != nil {
			lastErr = err
			continue
		}

		lines := extractExplainLinesFromResult(result)
		if len(lines) == 0 {
			continue
		}

		nodes := buildPlanTree(lines)

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"source":  candidate.source,
			"lines":   lines,
			"nodes":   nodes,
		})
		return
	}

	if lastErr != nil {
		writeError(w, http.StatusInternalServerError, lastErr.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "No plan information returned")
}

// ExplorerData handles POST /explorer-data — server-side paginated data browsing.
func (h *QueryHandler) ExplorerData(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	var req struct {
		Database   string `json:"database"`
		Table      string `json:"table"`
		Page       int    `json:"page"`
		PageSize   int    `json:"page_size"`
		SortColumn string `json:"sort_column"`
		SortDir    string `json:"sort_dir"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Database == "" || req.Table == "" {
		writeError(w, http.StatusBadRequest, "database and table are required")
		return
	}
	if !h.enforceGuardrailsForTable(w, r, req.Database, req.Table, r.URL.Path) {
		return
	}
	if req.PageSize <= 0 || req.PageSize > 1000 {
		req.PageSize = 100
	}
	if req.Page < 0 {
		req.Page = 0
	}

	sortDir := "ASC"
	if strings.EqualFold(req.SortDir, "desc") {
		sortDir = "DESC"
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	offset := req.Page * req.PageSize

	// Build data query
	dataSQL := fmt.Sprintf(
		"SELECT * FROM %s.%s",
		escapeIdentifier(req.Database),
		escapeIdentifier(req.Table),
	)
	if req.SortColumn != "" {
		dataSQL += fmt.Sprintf(" ORDER BY %s %s", escapeIdentifier(req.SortColumn), sortDir)
	}
	dataSQL += fmt.Sprintf(" LIMIT %d OFFSET %d", req.PageSize, offset)

	result, err := h.Engine.Execute(ctx, dataSQL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Get total row count
	totalRows, countErr := h.Engine.TableRowCount(ctx, req.Database, req.Table)
	if countErr != nil {
		slog.Warn("Failed to count rows for explorer", "error", countErr)
		totalRows = result.RowCount
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"meta":       result.Meta,
		"data":       result.Data,
		"rows":       result.RowCount,
		"total_rows": totalRows,
		"page":       req.Page,
		"page_size":  req.PageSize,
	})
}

// ListDatabases handles GET /databases.
func (h *QueryHandler) ListDatabases(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	dbs, err := h.Engine.ListDatabases(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"databases": dbs,
	})
}

// ListTables handles GET /tables?database=X.
func (h *QueryHandler) ListTables(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	dbName := r.URL.Query().Get("database")
	if dbName == "" {
		writeError(w, http.StatusBadRequest, "database query parameter is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	tables, err := h.Engine.ListTables(ctx, dbName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"tables":  tables,
	})
}

// ListColumns handles GET /columns?database=X&table=Y.
func (h *QueryHandler) ListColumns(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	dbName := r.URL.Query().Get("database")
	table := r.URL.Query().Get("table")
	if dbName == "" || table == "" {
		writeError(w, http.StatusBadRequest, "database and table query parameters are required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	columns, err := h.Engine.ListColumns(ctx, dbName, table)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"columns": columns,
	})
}

// ListDataTypes handles GET /data-types.
func (h *QueryHandler) ListDataTypes(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	result, err := h.Engine.Execute(ctx, `
		SELECT DISTINCT type_name
		FROM duckdb_types()
		ORDER BY type_name
	`)
	if err != nil {
		slog.Warn("Failed to list DuckDB data types", "error", err)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":    true,
			"data_types": []string{},
		})
		return
	}

	types := make([]string, 0, len(result.Data))
	for _, row := range result.Data {
		if len(row) > 0 {
			if s, ok := row[0].(string); ok && s != "" {
				types = append(types, s)
			}
		}
	}
	sort.Strings(types)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"data_types": types,
	})
}

// GetHostInfo handles GET /host-info — returns DuckDB version and settings.
func (h *QueryHandler) GetHostInfo(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	version, err := h.Engine.Version(ctx)
	if err != nil {
		slog.Warn("Failed to get DuckDB version", "error", err)
		version = "unknown"
	}

	info := map[string]interface{}{
		"engine":  "DuckDB",
		"version": version,
	}

	// Fetch memory limit and threads from current settings
	settingsResult, err := h.Engine.Execute(ctx, `
		SELECT name, value
		FROM duckdb_settings()
		WHERE name IN ('memory_limit', 'threads', 'worker_threads', 'default_order')
	`)
	if err == nil {
		settings := make(map[string]interface{})
		for _, row := range settingsResult.Data {
			if len(row) >= 2 {
				if key, ok := row[0].(string); ok {
					settings[key] = row[1]
				}
			}
		}
		info["settings"] = settings
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"host_info": info,
	})
}

// ListCompletions handles GET /completions — returns DuckDB functions and keywords for autocomplete.
func (h *QueryHandler) ListCompletions(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	functions, err := h.Engine.ListFunctions(ctx)
	if err != nil {
		slog.Warn("Failed to list DuckDB functions for completions", "error", err)
		functions = []string{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"functions": functions,
		"keywords":  duckdbKeywords,
	})
}

// QueryHistory handles GET /history — returns the current user's recent query log.
func (h *QueryHandler) QueryHistory(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return
	}

	// Parse query params
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")
	search := r.URL.Query().Get("search")

	limit := 50
	if limitStr != "" {
		if v, err := strconv.Atoi(limitStr); err == nil && v > 0 {
			limit = v
		}
	}
	if limit > 200 {
		limit = 200
	}

	offset := 0
	if offsetStr != "" {
		if v, err := strconv.Atoi(offsetStr); err == nil && v >= 0 {
			offset = v
		}
	}

	db := h.DB.Conn()

	// Build WHERE clause
	where := "WHERE username = ?"
	args := []interface{}{session.Username}
	if search != "" {
		where += " AND query_text LIKE ?"
		args = append(args, "%"+search+"%")
	}

	// Count total matching rows
	var total int
	countSQL := "SELECT COUNT(*) FROM gov_query_log " + where
	if err := db.QueryRowContext(r.Context(), countSQL, args...).Scan(&total); err != nil {
		slog.Warn("Failed to count query history", "error", err)
		writeError(w, http.StatusInternalServerError, "Failed to retrieve query history")
		return
	}

	// Fetch rows
	dataSQL := "SELECT id, query_id, username, query_text, query_kind, event_time, duration_ms, result_rows, tables_used, is_error, error_message, created_at FROM gov_query_log " + where + " ORDER BY event_time DESC LIMIT ? OFFSET ?"
	dataArgs := append(args, limit, offset)

	rows, err := db.QueryContext(r.Context(), dataSQL, dataArgs...)
	if err != nil {
		slog.Warn("Failed to query history", "error", err)
		writeError(w, http.StatusInternalServerError, "Failed to retrieve query history")
		return
	}
	defer rows.Close()

	type historyEntry struct {
		ID           string  `json:"id"`
		QueryID      string  `json:"query_id"`
		Username     string  `json:"username"`
		QueryText    string  `json:"query_text"`
		QueryKind    *string `json:"query_kind"`
		EventTime    string  `json:"event_time"`
		DurationMs   int64   `json:"duration_ms"`
		ResultRows   int64   `json:"result_rows"`
		TablesUsed   *string `json:"tables_used"`
		IsError      bool    `json:"is_error"`
		ErrorMessage *string `json:"error_message"`
		CreatedAt    string  `json:"created_at"`
	}

	history := make([]historyEntry, 0)
	for rows.Next() {
		var e historyEntry
		var isErr int
		if err := rows.Scan(&e.ID, &e.QueryID, &e.Username, &e.QueryText, &e.QueryKind, &e.EventTime, &e.DurationMs, &e.ResultRows, &e.TablesUsed, &isErr, &e.ErrorMessage, &e.CreatedAt); err != nil {
			slog.Warn("Failed to scan query history row", "error", err)
			continue
		}
		e.IsError = isErr != 0
		history = append(history, e)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"history": history,
		"total":   total,
	})
}

// CreateDatabase handles POST /schema/database.
func (h *QueryHandler) CreateDatabase(w http.ResponseWriter, r *http.Request) {
	session := h.requireAuthenticated(w, r)
	if session == nil {
		return
	}

	var req createDatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	name := strings.TrimSpace(req.Name)
	if err := validateSimpleObjectName(name, "database"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if isSystemDatabaseName(name) {
		writeError(w, http.StatusBadRequest, "Cannot create reserved system database")
		return
	}

	ifNotExists := req.IfNotExists == nil || *req.IfNotExists

	// DuckDB uses ATTACH to create databases (each database is a file)
	dataDir := filepath.Dir(h.Config.DuckDBPath)
	dbFilePath := filepath.Join(dataDir, name+".duckdb")

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("ATTACH ")
	if ifNotExists {
		sqlBuilder.WriteString("IF NOT EXISTS ")
	}
	sqlBuilder.WriteString("'" + strings.ReplaceAll(dbFilePath, "'", "''") + "'")
	sqlBuilder.WriteString(" AS ")
	sqlBuilder.WriteString(escapeIdentifier(name))

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	ddl := sqlBuilder.String()
	if _, err := h.Engine.Exec(ctx, ddl); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("%s\n\nCommand:\n%s", err.Error(), ddl))
		return
	}

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:    "schema.database.create",
		Username:  strPtr(session.Username),
		Details:   strPtr(fmt.Sprintf("database=%s, path=%s", name, dbFilePath)),
		IPAddress: strPtr(r.RemoteAddr),
	})

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success":  true,
		"database": name,
	})
}

// DropDatabase handles POST /schema/database/drop.
func (h *QueryHandler) DropDatabase(w http.ResponseWriter, r *http.Request) {
	session := h.requireAuthenticated(w, r)
	if session == nil {
		return
	}

	var req dropDatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	name := strings.TrimSpace(req.Name)
	if err := validateSimpleObjectName(name, "database"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if isSystemDatabaseName(name) {
		writeError(w, http.StatusBadRequest, "Cannot drop system database")
		return
	}

	ifExists := req.IfExists == nil || *req.IfExists

	// DuckDB uses DETACH to remove databases.
	// DuckDB 1.4.x does not support DETACH IF EXISTS, so we always run plain
	// DETACH and swallow "not found" errors when ifExists is true.
	ddl := "DETACH " + escapeIdentifier(name)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if _, err := h.Engine.Exec(ctx, ddl); err != nil {
		if ifExists && strings.Contains(err.Error(), "not found") {
			// IF EXISTS semantics: silently succeed
		} else {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	// Best-effort cleanup of the database file
	dataDir := filepath.Dir(h.Config.DuckDBPath)
	dbFilePath := filepath.Join(dataDir, name+".duckdb")
	_ = os.Remove(dbFilePath)
	_ = os.Remove(dbFilePath + ".wal")

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:    "schema.database.drop",
		Username:  strPtr(session.Username),
		Details:   strPtr(fmt.Sprintf("database=%s", name)),
		IPAddress: strPtr(r.RemoteAddr),
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"database": name,
	})
}

// CreateTable handles POST /schema/table.
func (h *QueryHandler) CreateTable(w http.ResponseWriter, r *http.Request) {
	session := h.requireAuthenticated(w, r)
	if session == nil {
		return
	}

	var req createTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	dbName := strings.TrimSpace(req.Database)
	tableName := strings.TrimSpace(req.Name)
	if err := validateSimpleObjectName(dbName, "database"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateSimpleObjectName(tableName, "table"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if isSystemDatabaseName(dbName) {
		writeError(w, http.StatusBadRequest, "Cannot create tables in system databases")
		return
	}

	if len(req.Columns) == 0 {
		writeError(w, http.StatusBadRequest, "At least one column is required")
		return
	}

	columnsSQL := make([]string, 0, len(req.Columns))
	for i, col := range req.Columns {
		colName := strings.TrimSpace(col.Name)
		colType := strings.TrimSpace(col.Type)
		if err := validateSimpleObjectName(colName, fmt.Sprintf("column #%d", i+1)); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if colType == "" || isUnsafeSQLFragment(colType) {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid type for column %q", colName))
			return
		}

		part := escapeIdentifier(colName) + " " + colType
		if def := strings.TrimSpace(col.DefaultExpression); def != "" {
			if isUnsafeSQLFragment(def) {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid default expression for column %q", colName))
				return
			}
			part += " DEFAULT " + def
		}
		columnsSQL = append(columnsSQL, part)
	}

	ifNotExists := req.IfNotExists == nil || *req.IfNotExists

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("CREATE TABLE ")
	if ifNotExists {
		sqlBuilder.WriteString("IF NOT EXISTS ")
	}
	sqlBuilder.WriteString(escapeIdentifier(dbName))
	sqlBuilder.WriteString(".")
	sqlBuilder.WriteString(escapeIdentifier(tableName))
	sqlBuilder.WriteString(" (\n  ")
	sqlBuilder.WriteString(strings.Join(columnsSQL, ",\n  "))
	sqlBuilder.WriteString("\n)")

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	ddl := sqlBuilder.String()
	if _, err := h.Engine.Exec(ctx, ddl); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:    "schema.table.create",
		Username:  strPtr(session.Username),
		Details:   strPtr(fmt.Sprintf("table=%s.%s", dbName, tableName)),
		IPAddress: strPtr(r.RemoteAddr),
	})

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success":  true,
		"database": dbName,
		"table":    tableName,
		"command":  ddl,
	})
}

// DropTable handles POST /schema/table/drop.
func (h *QueryHandler) DropTable(w http.ResponseWriter, r *http.Request) {
	session := h.requireAuthenticated(w, r)
	if session == nil {
		return
	}

	var req dropTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	dbName := strings.TrimSpace(req.Database)
	tableName := strings.TrimSpace(req.Name)
	if err := validateSimpleObjectName(dbName, "database"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateSimpleObjectName(tableName, "table"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if isSystemDatabaseName(dbName) {
		writeError(w, http.StatusBadRequest, "Cannot drop tables from system databases")
		return
	}

	ifExists := req.IfExists == nil || *req.IfExists

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("DROP TABLE ")
	if ifExists {
		sqlBuilder.WriteString("IF EXISTS ")
	}
	sqlBuilder.WriteString(escapeIdentifier(dbName))
	sqlBuilder.WriteString(".")
	sqlBuilder.WriteString(escapeIdentifier(tableName))

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	ddl := sqlBuilder.String()
	if _, err := h.Engine.Exec(ctx, ddl); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:    "schema.table.drop",
		Username:  strPtr(session.Username),
		Details:   strPtr(fmt.Sprintf("table=%s.%s", dbName, tableName)),
		IPAddress: strPtr(r.RemoteAddr),
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"database": dbName,
		"table":    tableName,
	})
}

// --- Helpers ---

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   message,
	})
}

func (h *QueryHandler) logQueryToGovernance(username, queryText, eventTime string, durationMs, resultRows int64, isError bool, errorMessage string) {
	if h.GovStore == nil {
		return
	}
	go func() {
		queryKind := governance.ClassifyQuery(queryText)
		tablesUsed := governance.ExtractTablesFromQuery(queryText)
		if _, err := h.GovStore.LogQuery(username, queryText, queryKind, eventTime, durationMs, resultRows, tablesUsed, isError, errorMessage); err != nil {
			slog.Warn("Failed to log query to governance", "error", err)
		}
	}()
}

func (h *QueryHandler) guardrailsEnabled() bool {
	if h.Guardrails == nil {
		return false
	}
	if h.Config == nil {
		return true
	}
	return h.Config.IsPro()
}

func (h *QueryHandler) enforceGuardrailsForQuery(w http.ResponseWriter, r *http.Request, queryText, requestEndpoint string) bool {
	if !h.guardrailsEnabled() {
		return true
	}
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return false
	}

	decision, err := h.Guardrails.EvaluateQuery(session.Username, session.UserRole, queryText, requestEndpoint)
	if err != nil {
		slog.Error("Guardrail pre-exec evaluation failed", "endpoint", requestEndpoint, "error", err)
		writeError(w, http.StatusInternalServerError, "Failed to evaluate governance guardrails")
		return false
	}
	if decision.Allowed {
		return true
	}
	h.writePolicyBlocked(w, decision.Block)
	return false
}

func (h *QueryHandler) enforceGuardrailsForTable(w http.ResponseWriter, r *http.Request, databaseName, tableName, requestEndpoint string) bool {
	if !h.guardrailsEnabled() {
		return true
	}
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return false
	}

	decision, err := h.Guardrails.EvaluateTable(session.Username, session.UserRole, databaseName, tableName, requestEndpoint)
	if err != nil {
		slog.Error("Guardrail table pre-exec evaluation failed",
			"database", databaseName, "table", tableName,
			"endpoint", requestEndpoint, "error", err)
		writeError(w, http.StatusInternalServerError, "Failed to evaluate governance guardrails")
		return false
	}
	if decision.Allowed {
		return true
	}
	h.writePolicyBlocked(w, decision.Block)
	return false
}

func (h *QueryHandler) writePolicyBlocked(w http.ResponseWriter, block *governance.GuardrailBlock) {
	if block == nil {
		writeJSON(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"error":   "Query blocked by governance policy",
			"code":    "policy_blocked",
		})
		return
	}

	writeJSON(w, http.StatusForbidden, map[string]interface{}{
		"success":          false,
		"error":            block.Detail,
		"code":             "policy_blocked",
		"policy_id":        block.PolicyID,
		"policy_name":      block.PolicyName,
		"severity":         block.Severity,
		"enforcement_mode": block.EnforcementMode,
		"violation_id":     block.ViolationID,
	})
}

// escapeIdentifier wraps a SQL identifier in double quotes and escapes any inner double quotes.
func escapeIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

// escapeLiteral escapes single quotes for SQL string literals.
func escapeLiteral(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "\\", "\\\\"), "'", "''")
}

func (h *QueryHandler) requireAuthenticated(w http.ResponseWriter, r *http.Request) *middleware.SessionInfo {
	session := middleware.GetSession(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "Not authenticated")
		return nil
	}
	return session
}

func validateSimpleObjectName(name string, label string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%s name is required", label)
	}
	if strings.Contains(name, ".") {
		return fmt.Errorf("%s name cannot contain '.'", label)
	}
	if strings.ContainsAny(name, "\x00\r\n\t") {
		return fmt.Errorf("%s name contains invalid control characters", label)
	}
	return nil
}

func isUnsafeSQLFragment(value string) bool {
	v := strings.TrimSpace(strings.ToLower(value))
	if v == "" {
		return false
	}
	return strings.Contains(v, ";") ||
		strings.Contains(v, "--") ||
		strings.Contains(v, "/*") ||
		strings.Contains(v, "*/") ||
		strings.ContainsAny(v, "\x00\r\n")
}

func isSystemDatabaseName(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "system", "information_schema", "pg_catalog":
		return true
	default:
		return false
	}
}

// strPtr returns a pointer to the given string.
func strPtr(s string) *string {
	return &s
}

// extractExplainLinesFromResult extracts text lines from a DuckDB EXPLAIN result.
func extractExplainLinesFromResult(result *duckdb.QueryResult) []string {
	if result == nil || len(result.Data) == 0 {
		return nil
	}
	lines := make([]string, 0, len(result.Data))
	for _, row := range result.Data {
		if len(row) == 0 {
			continue
		}
		line := fmt.Sprint(row[0])
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func buildPlanTree(lines []string) []planNode {
	nodes := make([]planNode, 0, len(lines))
	stack := make([]string, 0, 16)

	for i, line := range lines {
		level := planLineLevel(line)
		label := cleanPlanLabel(line)
		if label == "" {
			continue
		}
		if level < 0 {
			level = 0
		}
		if level > len(stack) {
			level = len(stack)
		}

		id := fmt.Sprintf("n%d", i+1)
		var parentID *string
		if level > 0 && level-1 < len(stack) {
			parent := stack[level-1]
			parentID = &parent
		}

		if level == len(stack) {
			stack = append(stack, id)
		} else {
			stack[level] = id
			stack = stack[:level+1]
		}

		nodes = append(nodes, planNode{
			ID:       id,
			ParentID: parentID,
			Level:    level,
			Label:    label,
		})
	}
	return nodes
}

func planLineLevel(line string) int {
	level := 0
	runes := []rune(line)
	for i := 0; i < len(runes); {
		if i+1 < len(runes) && runes[i] == ' ' && runes[i+1] == ' ' {
			level++
			i += 2
			continue
		}
		if i+1 < len(runes) && runes[i] == '\u2502' && runes[i+1] == ' ' {
			level++
			i += 2
			continue
		}
		if runes[i] == ' ' || runes[i] == '\u2502' {
			i++
			continue
		}
		break
	}
	return level
}

func cleanPlanLabel(line string) string {
	label := strings.TrimSpace(line)
	label = strings.TrimLeft(label, "\u2502 ")
	label = strings.TrimPrefix(label, "\u2514\u2500")
	label = strings.TrimPrefix(label, "\u251c\u2500")
	label = strings.TrimPrefix(label, "\u2500")
	return strings.TrimSpace(label)
}

// formatSQL performs basic SQL formatting: uppercases keywords and adds newlines
// before major clauses.
func formatSQL(sql string) string {
	keywords := []string{
		"SELECT", "FROM", "WHERE", "AND", "OR", "ORDER BY", "GROUP BY",
		"HAVING", "LIMIT", "OFFSET", "JOIN", "LEFT JOIN", "RIGHT JOIN",
		"INNER JOIN", "OUTER JOIN", "FULL JOIN", "CROSS JOIN",
		"ON", "AS", "IN", "NOT", "NULL", "IS", "BETWEEN", "LIKE", "ILIKE",
		"INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
		"CREATE", "TABLE", "ALTER", "DROP", "INDEX",
		"DISTINCT", "UNION", "ALL", "EXISTS", "CASE", "WHEN", "THEN",
		"ELSE", "END", "ASC", "DESC", "WITH",
		"PIVOT", "UNPIVOT", "QUALIFY", "SAMPLE",
		"COPY", "ATTACH", "DETACH", "USE",
	}

	result := sql

	for _, kw := range keywords {
		pattern := `(?i)\b` + regexp.QuoteMeta(kw) + `\b`
		re := regexp.MustCompile(pattern)
		result = re.ReplaceAllString(result, kw)
	}

	clauses := []string{
		"SELECT", "FROM", "WHERE", "ORDER BY", "GROUP BY", "HAVING",
		"LIMIT", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "OUTER JOIN",
		"FULL JOIN", "CROSS JOIN", "JOIN", "UNION",
	}
	for _, clause := range clauses {
		pattern := `(?m)\s+` + regexp.QuoteMeta(clause) + `\b`
		re := regexp.MustCompile(pattern)
		result = re.ReplaceAllString(result, "\n"+clause)
	}

	return strings.TrimSpace(result)
}

// --- DuckDB SQL keywords for completions ---

var duckdbKeywords = []string{
	"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "OFFSET",
	"JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN", "INNER JOIN",
	"ON", "USING", "AS", "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN",
	"LIKE", "ILIKE", "IS NULL", "IS NOT NULL", "CASE", "WHEN", "THEN", "ELSE", "END",
	"UNION", "UNION ALL", "INTERSECT", "EXCEPT",
	"INSERT INTO", "VALUES", "UPDATE", "SET", "DELETE FROM",
	"CREATE TABLE", "CREATE VIEW", "CREATE SCHEMA", "CREATE SEQUENCE",
	"DROP TABLE", "DROP VIEW", "DROP SCHEMA",
	"ALTER TABLE", "ADD COLUMN", "DROP COLUMN", "RENAME TO",
	"WITH", "RECURSIVE", "HAVING", "DISTINCT", "ALL",
	"ASC", "DESC", "NULLS FIRST", "NULLS LAST",
	"COPY", "ATTACH", "DETACH", "USE",
	"PIVOT", "UNPIVOT", "QUALIFY", "SAMPLE", "TABLESAMPLE",
	"DESCRIBE", "SHOW", "SUMMARIZE", "EXPLAIN", "EXPLAIN ANALYZE",
	"read_parquet", "read_csv", "read_csv_auto", "read_json", "read_json_auto",
	"INSTALL", "LOAD", "PRAGMA",
}
