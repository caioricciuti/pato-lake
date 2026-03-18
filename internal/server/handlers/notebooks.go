package handlers

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/caioricciuti/pato-lake/internal/config"
	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/caioricciuti/pato-lake/internal/duckdb"
	"github.com/caioricciuti/pato-lake/internal/server/middleware"
	"github.com/go-chi/chi/v5"
)

// NotebooksHandler handles SQL notebook endpoints.
type NotebooksHandler struct {
	DB     *database.DB
	Engine *duckdb.Engine
	Config *config.Config
}

// Routes registers notebook routes in the protected group.
func (h *NotebooksHandler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Get("/", h.List)
	r.Post("/", h.Create)
	r.Get("/{id}", h.Get)
	r.Put("/{id}", h.Update)
	r.Delete("/{id}", h.Delete)
	r.Put("/{id}/cells", h.SaveCells)
	r.Post("/{id}/cells/{cellId}/run", h.RunCell)
	r.Post("/{id}/share", h.Share)
	r.Delete("/{id}/share", h.RevokeShare)
	return r
}

// SharedRoutes registers public routes for shared notebooks.
func (h *NotebooksHandler) SharedRoutes(r chi.Router) {
	r.Get("/notebooks/shared/{token}", h.GetShared)
}

// List returns all notebooks for the current user.
func (h *NotebooksHandler) List(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	// Admins see all, others see their own
	userFilter := session.Username
	if session.UserRole == "admin" {
		userFilter = ""
	}

	notebooks, err := h.DB.ListNotebooks(userFilter)
	if err != nil {
		slog.Error("Failed to list notebooks", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to list notebooks"})
		return
	}
	if notebooks == nil {
		notebooks = []database.Notebook{}
	}
	writeJSON(w, http.StatusOK, notebooks)
}

// Create creates a new notebook.
func (h *NotebooksHandler) Create(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	var body struct {
		Title       string `json:"title"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	title := strings.TrimSpace(body.Title)
	if title == "" {
		title = "Untitled Notebook"
	}

	notebook, err := h.DB.CreateNotebook(database.CreateNotebookParams{
		Title:       title,
		Description: strings.TrimSpace(body.Description),
		CreatedBy:   session.Username,
	})
	if err != nil {
		slog.Error("Failed to create notebook", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to create notebook"})
		return
	}

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:   "notebook.create",
		Username: strPtr(session.Username),
		Details:  strPtr("Created notebook: " + title),
	})

	writeJSON(w, http.StatusCreated, notebook)
}

// Get retrieves a notebook with its cells.
func (h *NotebooksHandler) Get(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")
	notebook, err := h.DB.GetNotebook(id)
	if err != nil {
		slog.Error("Failed to get notebook", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to get notebook"})
		return
	}
	if notebook == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Notebook not found"})
		return
	}

	cells, err := h.DB.GetNotebookCells(id)
	if err != nil {
		slog.Error("Failed to get notebook cells", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to get cells"})
		return
	}
	if cells == nil {
		cells = []database.NotebookCell{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"notebook": notebook,
		"cells":    cells,
	})
}

// Update modifies a notebook's title and description.
func (h *NotebooksHandler) Update(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")

	var body struct {
		Title       string `json:"title"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	title := strings.TrimSpace(body.Title)
	if title == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Title is required"})
		return
	}

	if err := h.DB.UpdateNotebook(id, title, strings.TrimSpace(body.Description)); err != nil {
		slog.Error("Failed to update notebook", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to update notebook"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Delete removes a notebook.
func (h *NotebooksHandler) Delete(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")
	if err := h.DB.DeleteNotebook(id); err != nil {
		slog.Error("Failed to delete notebook", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to delete notebook"})
		return
	}

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:   "notebook.delete",
		Username: strPtr(session.Username),
		Details:  strPtr("Deleted notebook: " + id),
	})

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// SaveCells bulk-saves all cells for a notebook.
func (h *NotebooksHandler) SaveCells(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")

	var body struct {
		Cells []database.NotebookCell `json:"cells"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	if err := h.DB.BulkSaveCells(id, body.Cells); err != nil {
		slog.Error("Failed to save cells", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to save cells"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// RunCell executes a SQL cell and returns results.
func (h *NotebooksHandler) RunCell(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	var body struct {
		SQL             string `json:"sql"`
		PrecedingCells  []struct {
			Position int    `json:"position"`
			SQL      string `json:"sql"`
		} `json:"preceding_cells"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	sqlText := strings.TrimSpace(body.SQL)
	if sqlText == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "SQL is required"})
		return
	}

	// Replace $cell[N] references with temp table names
	sqlText = replaceCellRefs(sqlText)

	// Get a dedicated connection for temp table isolation
	conn, err := h.Engine.DB().Conn(r.Context())
	if err != nil {
		slog.Error("Failed to get DuckDB connection", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to get connection"})
		return
	}
	defer conn.Close()

	// Create temp tables for preceding cells
	for _, pc := range body.PrecedingCells {
		pcSQL := replaceCellRefs(strings.TrimSpace(pc.SQL))
		if pcSQL == "" {
			continue
		}
		tempDDL := fmt.Sprintf("CREATE OR REPLACE TEMP TABLE _cell_%d AS (%s)", pc.Position, pcSQL)
		if _, err := conn.ExecContext(r.Context(), tempDDL); err != nil {
			slog.Warn("Failed to create preceding cell temp table", "position", pc.Position, "error", err)
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error":    fmt.Sprintf("Error in cell %d: %s", pc.Position, err.Error()),
				"position": pc.Position,
			})
			return
		}
	}

	// Execute the target cell
	rows, err := conn.QueryContext(r.Context(), sqlText)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"error": err.Error(),
			"meta":  []interface{}{},
			"data":  []interface{}{},
		})
		return
	}
	defer rows.Close()

	// Get column metadata
	columns, err := rows.Columns()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to get columns"})
		return
	}

	colTypes, _ := rows.ColumnTypes()
	meta := make([]map[string]string, len(columns))
	for i, col := range columns {
		typeName := "VARCHAR"
		if i < len(colTypes) && colTypes[i].DatabaseTypeName() != "" {
			typeName = colTypes[i].DatabaseTypeName()
		}
		meta[i] = map[string]string{"name": col, "type": typeName}
	}

	// Read all rows (limit 10000 to prevent OOM)
	var data [][]interface{}
	maxRows := 10000
	for rows.Next() && len(data) < maxRows {
		values := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to scan row"})
			return
		}
		// Normalize values for JSON
		for i, v := range values {
			switch val := v.(type) {
			case []byte:
				values[i] = string(val)
			}
		}
		data = append(data, values)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"meta":      meta,
		"data":      data,
		"row_count": len(data),
		"truncated": len(data) >= maxRows,
	})
}

// replaceCellRefs replaces $cell[N] with _cell_N in SQL text.
func replaceCellRefs(sql string) string {
	// Simple replacement: $cell[0] → _cell_0, $cell[1] → _cell_1, etc.
	result := sql
	for i := 0; i < 100; i++ {
		ref := fmt.Sprintf("$cell[%d]", i)
		replacement := fmt.Sprintf("_cell_%d", i)
		result = strings.ReplaceAll(result, ref, replacement)
	}
	return result
}

// Share generates a share token for a notebook.
func (h *NotebooksHandler) Share(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")
	token, err := h.DB.GenerateShareToken(id)
	if err != nil {
		slog.Error("Failed to generate share token", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to generate share token"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"share_token": token})
}

// RevokeShare removes the share token from a notebook.
func (h *NotebooksHandler) RevokeShare(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")
	if err := h.DB.RevokeShareToken(id); err != nil {
		slog.Error("Failed to revoke share token", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to revoke share token"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GetShared returns a public notebook by its share token.
func (h *NotebooksHandler) GetShared(w http.ResponseWriter, r *http.Request) {
	token := chi.URLParam(r, "token")

	notebook, err := h.DB.GetNotebookByShareToken(token)
	if err != nil {
		slog.Error("Failed to get shared notebook", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to get notebook"})
		return
	}
	if notebook == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Notebook not found or not shared"})
		return
	}

	cells, err := h.DB.GetNotebookCells(notebook.ID)
	if err != nil {
		slog.Error("Failed to get shared notebook cells", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to get cells"})
		return
	}
	if cells == nil {
		cells = []database.NotebookCell{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"notebook": notebook,
		"cells":    cells,
	})
}
