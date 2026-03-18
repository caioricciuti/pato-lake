package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/caioricciuti/pato-lake/internal/config"
	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/caioricciuti/pato-lake/internal/server/middleware"
	"github.com/go-chi/chi/v5"
)

// APIKeysHandler handles API key management endpoints.
type APIKeysHandler struct {
	DB     *database.DB
	Config *config.Config
}

// Routes registers the API key management routes.
func (h *APIKeysHandler) Routes(r chi.Router) {
	r.Get("/", h.List)
	r.Post("/", h.Create)
	r.Put("/{id}", h.Update)
	r.Delete("/{id}", h.Delete)
	r.Get("/{id}/usage", h.GetUsage)
}

// AdminRoutes registers admin-only API key routes.
func (h *APIKeysHandler) AdminRoutes(r chi.Router) {
	r.Get("/", h.ListAll)
}

// List returns the current user's API keys.
func (h *APIKeysHandler) List(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	keys, err := h.DB.ListAPIKeysByUser(session.UserID)
	if err != nil {
		slog.Error("Failed to list API keys", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to list API keys"})
		return
	}
	if keys == nil {
		keys = []database.APIKey{}
	}
	writeJSON(w, http.StatusOK, keys)
}

// ListAll returns all API keys (admin only).
func (h *APIKeysHandler) ListAll(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil || session.UserRole != "admin" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Admin access required"})
		return
	}

	keys, err := h.DB.ListAllAPIKeys()
	if err != nil {
		slog.Error("Failed to list all API keys", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to list API keys"})
		return
	}
	if keys == nil {
		keys = []database.APIKey{}
	}
	writeJSON(w, http.StatusOK, keys)
}

// Create generates a new API key.
func (h *APIKeysHandler) Create(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	var body struct {
		Name         string `json:"name"`
		Role         string `json:"role"`
		Scopes       string `json:"scopes"`
		RateLimitRPM int    `json:"rate_limit_rpm"`
		ExpiresAt    string `json:"expires_at"`
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

	result, err := h.DB.CreateAPIKey(database.CreateAPIKeyParams{
		UserID:       session.UserID,
		Name:         name,
		Role:         body.Role,
		Scopes:       body.Scopes,
		RateLimitRPM: body.RateLimitRPM,
		ExpiresAt:    body.ExpiresAt,
	})
	if err != nil {
		slog.Error("Failed to create API key", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to create API key"})
		return
	}

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:   "api_key.create",
		Username: strPtr(session.Username),
		Details:  strPtr("Created API key: " + name),
	})

	writeJSON(w, http.StatusCreated, result)
}

// Update modifies an API key.
func (h *APIKeysHandler) Update(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")

	var body struct {
		Name         string `json:"name"`
		IsActive     *bool  `json:"is_active"`
		RateLimitRPM int    `json:"rate_limit_rpm"`
		Scopes       string `json:"scopes"`
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
	rateLimitRPM := body.RateLimitRPM
	if rateLimitRPM <= 0 {
		rateLimitRPM = 60
	}
	scopes := body.Scopes
	if scopes == "" {
		scopes = "*"
	}

	if err := h.DB.UpdateAPIKey(id, name, isActive, rateLimitRPM, scopes); err != nil {
		slog.Error("Failed to update API key", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to update API key"})
		return
	}

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:   "api_key.update",
		Username: strPtr(session.Username),
		Details:  strPtr("Updated API key: " + id),
	})

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Delete removes an API key.
func (h *APIKeysHandler) Delete(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")

	if err := h.DB.DeleteAPIKey(id); err != nil {
		slog.Error("Failed to delete API key", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to delete API key"})
		return
	}

	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:   "api_key.delete",
		Username: strPtr(session.Username),
		Details:  strPtr("Deleted API key: " + id),
	})

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GetUsage returns usage stats for an API key.
func (h *APIKeysHandler) GetUsage(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)
	if session == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
		return
	}

	id := chi.URLParam(r, "id")
	daysStr := r.URL.Query().Get("days")
	days := 30
	if daysStr != "" {
		if d, err := strconv.Atoi(daysStr); err == nil && d > 0 {
			days = d
		}
	}

	usage, err := h.DB.GetAPIKeyUsage(id, days)
	if err != nil {
		slog.Error("Failed to get API key usage", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to get usage stats"})
		return
	}
	if usage == nil {
		usage = []database.APIKeyUsage{}
	}
	writeJSON(w, http.StatusOK, usage)
}
