package handlers

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/caioricciuti/pato-lake/internal/config"
	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/caioricciuti/pato-lake/internal/duckdb"
	"github.com/caioricciuti/pato-lake/internal/server/middleware"
)

// AdminHandler handles admin-only routes for DuckDB management.
// All routes require the admin role, enforced by middleware.RequireAdmin.
type AdminHandler struct {
	DB     *database.DB
	Engine *duckdb.Engine
	Config *config.Config
}

// Routes registers all admin routes on the given chi.Router.
func (h *AdminHandler) Routes(r chi.Router) {
	r.Use(middleware.RequireAdmin(h.DB))

	r.Get("/users", h.GetUsers)
	r.Post("/users", h.CreateUser)
	r.Delete("/users/{id}", h.DeleteUser)
	r.Put("/user-roles/{username}", h.SetUserRole)
	r.Get("/stats", h.GetStats)

	// Brain admin management
	r.Get("/brain/providers", h.ListBrainProviders)
	r.Post("/brain/providers", h.CreateBrainProvider)
	r.Put("/brain/providers/{id}", h.UpdateBrainProvider)
	r.Delete("/brain/providers/{id}", h.DeleteBrainProvider)
	r.Post("/brain/providers/{id}/sync-models", h.SyncBrainProviderModels)
	r.Get("/brain/models", h.ListBrainModels)
	r.Put("/brain/models/{id}", h.UpdateBrainModel)
	r.Post("/brain/models/bulk", h.BulkUpdateBrainModels)
	r.Get("/brain/skills", h.ListBrainSkills)
	r.Post("/brain/skills", h.CreateBrainSkill)
	r.Put("/brain/skills/{id}", h.UpdateBrainSkill)
}

// ---------- GET /users ----------

func (h *AdminHandler) GetUsers(w http.ResponseWriter, r *http.Request) {
	users, err := h.DB.ListUsers()
	if err != nil {
		slog.Error("Failed to get users", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to retrieve users"})
		return
	}

	type responseUser struct {
		ID        string `json:"id"`
		Username  string `json:"username"`
		Role      string `json:"role"`
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
	}

	result := make([]responseUser, 0, len(users))
	for _, u := range users {
		result = append(result, responseUser{
			ID:        u.ID,
			Username:  u.Username,
			Role:      u.Role,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"users": result,
	})
}

// ---------- POST /users ----------

func (h *AdminHandler) CreateUser(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)

	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	body.Username = strings.TrimSpace(body.Username)
	body.Role = strings.ToLower(strings.TrimSpace(body.Role))
	if body.Username == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Username is required"})
		return
	}
	if len(body.Password) < 6 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Password must be at least 6 characters"})
		return
	}

	validRoles := map[string]bool{"admin": true, "analyst": true, "viewer": true}
	if body.Role == "" {
		body.Role = "viewer"
	}
	if !validRoles[body.Role] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Role must be one of: admin, analyst, viewer"})
		return
	}

	user, err := h.DB.CreateUser(body.Username, body.Password, body.Role)
	if err != nil {
		slog.Error("Failed to create user", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to create user"})
		return
	}

	var actorName *string
	if session != nil {
		actorName = strPtr(session.Username)
	}
	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:    "user.created",
		Username:  actorName,
		Details:   strPtr(fmt.Sprintf("Created user %q with role %s", body.Username, body.Role)),
		IPAddress: strPtr(r.RemoteAddr),
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"user": map[string]string{
			"id":       user.ID,
			"username": user.Username,
			"role":     user.Role,
		},
	})
}

// ---------- DELETE /users/{id} ----------

func (h *AdminHandler) DeleteUser(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)

	userID := chi.URLParam(r, "id")
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "User ID is required"})
		return
	}

	// Prevent self-deletion
	if session != nil && session.UserID == userID {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Cannot delete your own account"})
		return
	}

	// Check if target user is the last admin
	target, err := h.DB.GetUserByID(userID)
	if err != nil {
		slog.Error("Failed to find user", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to find user"})
		return
	}
	if target == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "User not found"})
		return
	}
	if target.Role == "admin" {
		adminCount, err := h.DB.CountUsersWithRole("admin")
		if err != nil {
			slog.Error("Failed counting admins", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to validate admin safety rule"})
			return
		}
		if adminCount <= 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Cannot delete the last admin"})
			return
		}
	}

	if err := h.DB.DeleteUser(userID); err != nil {
		slog.Error("Failed to delete user", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to delete user"})
		return
	}

	var actorName *string
	if session != nil {
		actorName = strPtr(session.Username)
	}
	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:    "user.deleted",
		Username:  actorName,
		Details:   strPtr(fmt.Sprintf("Deleted user %q (ID: %s)", target.Username, userID)),
		IPAddress: strPtr(r.RemoteAddr),
	})

	writeJSON(w, http.StatusOK, map[string]string{"message": "User deleted"})
}

// ---------- PUT /user-roles/{username} ----------

func (h *AdminHandler) SetUserRole(w http.ResponseWriter, r *http.Request) {
	session := middleware.GetSession(r)

	username := chi.URLParam(r, "username")
	if username == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Username is required"})
		return
	}

	var body struct {
		Role string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}
	body.Role = strings.ToLower(strings.TrimSpace(body.Role))

	validRoles := map[string]bool{"admin": true, "analyst": true, "viewer": true}
	if !validRoles[body.Role] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Role must be one of: admin, analyst, viewer"})
		return
	}

	isTargetAdmin, err := h.DB.IsUserRole(username, "admin")
	if err != nil {
		slog.Error("Failed checking current role assignment", "error", err, "user", username)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to validate current role"})
		return
	}
	if isTargetAdmin && body.Role != "admin" {
		adminCount, err := h.DB.CountUsersWithRole("admin")
		if err != nil {
			slog.Error("Failed counting admins", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to validate admin safety rule"})
			return
		}
		if adminCount <= 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "Cannot remove the last admin. Assign another admin first.",
			})
			return
		}
	}

	if err := h.DB.SetUserRole(username, body.Role); err != nil {
		slog.Error("Failed to set user role", "error", err, "user", username)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to set user role"})
		return
	}


	var actorName *string
	if session != nil {
		actorName = strPtr(session.Username)
	}
	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:    "user_role.set",
		Username:  actorName,
		Details:   strPtr(fmt.Sprintf("Set role for %q to %s", username, body.Role)),
		IPAddress: strPtr(r.RemoteAddr),
	})

	writeJSON(w, http.StatusOK, map[string]string{
		"message":  "User role updated",
		"username": username,
		"role":     body.Role,
	})
}

// ---------- GET /stats ----------

func (h *AdminHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	users, err := h.DB.ListUsers()
	if err != nil {
		slog.Error("Failed to get users for stats", "error", err)
		users = []database.User{}
	}

	auditLogs, err := h.DB.GetAuditLogs(1000)
	if err != nil {
		slog.Error("Failed to get audit logs for stats", "error", err)
		auditLogs = []database.AuditLog{}
	}

	loginCount := 0
	queryCount := 0
	for _, log := range auditLogs {
		switch log.Action {
		case "user.login":
			loginCount++
		case "query.execute":
			queryCount++
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"users_count": len(users),
		"login_count": loginCount,
		"query_count": queryCount,
	})
}


// ---------- Helpers ----------

func escapeString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

