package middleware

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/caioricciuti/pato-lake/internal/database"
)

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// Session returns a middleware that validates the duckui_session cookie.
func Session(db *database.DB) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cookie, err := r.Cookie("duckui_session")
			if err != nil || cookie.Value == "" {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Not authenticated"})
				return
			}

			session, err := db.GetSessionByToken(cookie.Value)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Session lookup failed"})
				return
			}
			if session == nil {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Session expired or invalid"})
				return
			}

			role := session.UserRole
			if role == "" {
				userRole, err := db.GetUserRole(session.Username)
				if err != nil {
					slog.Warn("Failed to resolve user role", "user", session.Username, "error", err)
					role = "viewer"
				} else if userRole != "" {
					role = userRole
				} else {
					role = "viewer"
				}
			}

			info := &SessionInfo{
				ID:       session.Token,
				UserID:   session.UserID,
				Username: session.Username,
				UserRole: role,
			}

			ctx := SetSession(r.Context(), info)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireAdmin returns a middleware that requires admin role.
func RequireAdmin(db *database.DB) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			session := GetSession(r)
			if session == nil {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "Admin access required"})
				return
			}

			isAdmin, err := db.IsUserRole(session.Username, "admin")
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Role check failed"})
				return
			}
			if !isAdmin {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "Admin access required"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
