package handlers

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/caioricciuti/pato-lake/internal/config"
	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/caioricciuti/pato-lake/internal/ingest"
	"github.com/caioricciuti/pato-lake/internal/server/middleware"
	"github.com/go-chi/chi/v5"
)

// IngestHandler handles event ingestion endpoints.
type IngestHandler struct {
	DB     *database.DB
	Config *config.Config
	Buffer *ingest.Buffer
}

// PublicRoutes registers the ingestion endpoint (API key auth only).
func (h *IngestHandler) PublicRoutes(r chi.Router) {
	r.Post("/{eventType}", h.IngestEvents)
}

// ManagementRoutes registers source management endpoints (session-protected).
func (h *IngestHandler) ManagementRoutes(r chi.Router) {
	r.Get("/sources", h.ListSources)
	r.Post("/sources", h.CreateSource)
	r.Put("/sources/{id}", h.UpdateSource)
	r.Delete("/sources/{id}", h.DeleteSource)
	r.Get("/stats/overview", h.GetStatsOverview)
}

// IngestEvents accepts JSON events and enqueues them for writing.
func (h *IngestHandler) IngestEvents(w http.ResponseWriter, r *http.Request) {
	eventType := chi.URLParam(r, "eventType")
	if eventType == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Event type is required"})
		return
	}

	// Check API key scope
	if !middleware.HasAPIKeyScope(r, "ingest") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Ingest scope required"})
		return
	}

	// Check source exists and is active
	source, err := h.DB.GetIngestSourceByEventType(eventType)
	if err != nil {
		slog.Error("Failed to lookup ingest source", "event_type", eventType, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Source lookup failed"})
		return
	}
	if source == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Unknown event type: " + eventType})
		return
	}
	if !source.IsActive {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "Ingest source is disabled"})
		return
	}

	// Read body (10MB limit)
	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Failed to read body"})
		return
	}
	if len(body) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Empty body"})
		return
	}

	// Parse JSON — accept object or array
	events, err := parseIngestBody(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON: " + err.Error()})
		return
	}

	if len(events) == 0 {
		writeJSON(w, http.StatusOK, map[string]interface{}{"accepted": 0})
		return
	}

	// Convert to ingest events
	byteSize := len(body) / len(events) // approximate per-event size
	ingestEvents := make([]ingest.Event, len(events))
	for i, data := range events {
		ingestEvents[i] = ingest.Event{
			EventType: eventType,
			Data:      data,
			ByteSize:  byteSize,
		}
	}

	accepted := h.Buffer.Enqueue(eventType, ingestEvents)

	// Track API key usage for ingest
	session := middleware.GetSession(r)
	if session != nil && strings.HasPrefix(session.ID, "apikey:") {
		apiKeyID := strings.TrimPrefix(session.ID, "apikey:")
		go h.DB.IncrementAPIKeyUsage(apiKeyID, 0, 0, accepted, len(body))
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"accepted": accepted})
}

func parseIngestBody(body []byte) ([]map[string]interface{}, error) {
	// Try array first
	var arr []map[string]interface{}
	if err := json.Unmarshal(body, &arr); err == nil {
		return arr, nil
	}

	// Try single object
	var obj map[string]interface{}
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, err
	}
	return []map[string]interface{}{obj}, nil
}

// ListSources returns all ingest sources.
func (h *IngestHandler) ListSources(w http.ResponseWriter, r *http.Request) {
	sources, err := h.DB.ListIngestSources()
	if err != nil {
		slog.Error("Failed to list ingest sources", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to list sources"})
		return
	}
	if sources == nil {
		sources = []database.IngestSource{}
	}
	writeJSON(w, http.StatusOK, sources)
}

// CreateSource creates a new ingest source.
func (h *IngestHandler) CreateSource(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	var body struct {
		Name            string `json:"name"`
		EventType       string `json:"event_type"`
		TargetSchema    string `json:"target_schema"`
		TargetTable     string `json:"target_table"`
		BufferSize      int    `json:"buffer_size"`
		FlushIntervalMs int    `json:"flush_interval_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	name := strings.TrimSpace(body.Name)
	eventType := strings.TrimSpace(body.EventType)
	if name == "" || eventType == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Name and event_type are required"})
		return
	}

	source, err := h.DB.CreateIngestSource(database.CreateIngestSourceParams{
		Name:            name,
		EventType:       eventType,
		TargetSchema:    body.TargetSchema,
		TargetTable:     body.TargetTable,
		BufferSize:      body.BufferSize,
		FlushIntervalMs: body.FlushIntervalMs,
		CreatedBy:       session.Username,
	})
	if err != nil {
		slog.Error("Failed to create ingest source", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to create source"})
		return
	}

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:   "ingest.source.create",
		Username: strPtr(session.Username),
		Details:  strPtr("Created ingest source: " + name),
	})

	writeJSON(w, http.StatusCreated, source)
}

// UpdateSource updates an ingest source.
func (h *IngestHandler) UpdateSource(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")

	var body struct {
		Name            string `json:"name"`
		IsActive        *bool  `json:"is_active"`
		BufferSize      int    `json:"buffer_size"`
		FlushIntervalMs int    `json:"flush_interval_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	name := strings.TrimSpace(body.Name)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Name is required"})
		return
	}

	isActive := true
	if body.IsActive != nil {
		isActive = *body.IsActive
	}
	bufferSize := body.BufferSize
	if bufferSize <= 0 {
		bufferSize = 1000
	}
	flushInterval := body.FlushIntervalMs
	if flushInterval <= 0 {
		flushInterval = 5000
	}

	if err := h.DB.UpdateIngestSource(id, name, isActive, bufferSize, flushInterval); err != nil {
		slog.Error("Failed to update ingest source", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to update source"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// DeleteSource removes an ingest source.
func (h *IngestHandler) DeleteSource(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")
	if err := h.DB.DeleteIngestSource(id); err != nil {
		slog.Error("Failed to delete ingest source", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to delete source"})
		return
	}

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:   "ingest.source.delete",
		Username: strPtr(session.Username),
		Details:  strPtr("Deleted ingest source: " + id),
	})

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GetStatsOverview returns ingestion stats.
func (h *IngestHandler) GetStatsOverview(w http.ResponseWriter, r *http.Request) {
	stats, err := h.DB.GetIngestStatsOverview(30)
	if err != nil {
		slog.Error("Failed to get ingest stats", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to get stats"})
		return
	}
	if stats == nil {
		stats = []database.IngestStats{}
	}
	writeJSON(w, http.StatusOK, stats)
}
