package handlers

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/caioricciuti/pato-lake/internal/config"
	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/caioricciuti/pato-lake/internal/server/middleware"
	"github.com/caioricciuti/pato-lake/internal/version"
)

// Session and rate-limit constants.
const (
	SessionCookie      = "duckui_session"
	SessionDuration    = 7 * 24 * time.Hour
	RateLimitWindow    = 15 * time.Minute
	MaxAttemptsPerIP   = 5
	MaxAttemptsPerUser = 3
)

// AuthHandler implements the authentication HTTP endpoints.
type AuthHandler struct {
	DB          *database.DB
	RateLimiter *middleware.RateLimiter
	Config      *config.Config
}

// Routes returns a chi.Router with all auth routes mounted.
func (h *AuthHandler) Routes(r chi.Router) {
	r.Post("/login", h.Login)
	r.Post("/logout", h.Logout)
	r.Get("/session", h.Session)
}

// ---------- request / response types ----------

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type userResponse struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Role     string `json:"role"`
}

// ---------- helpers ----------

func normalizeUsername(username string) string {
	return strings.ToLower(strings.TrimSpace(username))
}

func userRateLimitKey(username string) string {
	return fmt.Sprintf("user:%s", normalizeUsername(username))
}

func shouldUseSecureCookie(r *http.Request, cfg *config.Config) bool {
	if r != nil && r.TLS != nil {
		return true
	}
	if r != nil && strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")), "https") {
		return true
	}
	if cfg != nil && strings.TrimSpace(cfg.AppURL) != "" {
		if parsed, err := url.Parse(cfg.AppURL); err == nil {
			return strings.EqualFold(parsed.Scheme, "https")
		}
	}
	return false
}

func getClientIP(r *http.Request) string {
	if r.Header.Get("X-Forwarded-Proto") != "" || r.TLS != nil {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.SplitN(xff, ",", 2)
			ip := strings.TrimSpace(parts[0])
			if ip != "" {
				return ip
			}
		}
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			return strings.TrimSpace(xri)
		}
	}
	addr := r.RemoteAddr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// ---------- POST /api/auth/login ----------

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Username is required"})
		return
	}
	if req.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Password is required"})
		return
	}

	// --- Rate limiting (per-IP) ---
	clientIP := getClientIP(r)
	ipKey := fmt.Sprintf("ip:%s", clientIP)

	ipResult := h.RateLimiter.CheckAuthRateLimit(ipKey, "ip", MaxAttemptsPerIP, RateLimitWindow)
	if !ipResult.Allowed {
		retrySeconds := int(ipResult.RetryAfter.Seconds())
		slog.Warn("IP rate limited", "ip", clientIP, "retryAfter", retrySeconds)
		writeJSON(w, http.StatusTooManyRequests, map[string]interface{}{
			"error":      "Too many login attempts from this IP",
			"retryAfter": retrySeconds,
		})
		return
	}

	// --- Rate limiting (per-username) ---
	userKey := userRateLimitKey(req.Username)
	userResult := h.RateLimiter.CheckAuthRateLimit(userKey, "user", MaxAttemptsPerUser, RateLimitWindow)
	if !userResult.Allowed {
		retrySeconds := int(userResult.RetryAfter.Seconds())
		slog.Warn("User rate limited", "user", req.Username, "retryAfter", retrySeconds)
		writeJSON(w, http.StatusTooManyRequests, map[string]interface{}{
			"error":      "Too many login attempts for this user",
			"retryAfter": retrySeconds,
		})
		return
	}

	// --- Look up user in SQLite ---
	user, err := h.DB.GetUserByUsername(req.Username)
	if err != nil {
		slog.Error("Failed to look up user", "username", req.Username, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Internal server error"})
		return
	}
	if user == nil {
		h.RateLimiter.RecordAttempt(ipKey, "ip")
		h.RateLimiter.RecordAttempt(userKey, "user")
		slog.Info("Login failed: user not found", "username", req.Username)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Invalid credentials"})
		return
	}

	// --- Verify password with bcrypt ---
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		h.RateLimiter.RecordAttempt(ipKey, "ip")
		h.RateLimiter.RecordAttempt(userKey, "user")
		slog.Info("Login failed: invalid password", "username", req.Username)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Invalid credentials"})
		return
	}

	// --- Create session ---
	token := uuid.NewString()
	expiresAt := time.Now().UTC().Add(SessionDuration).Format(time.RFC3339)

	if err := h.DB.CreateSession(token, user.ID, user.Username, user.Role, expiresAt); err != nil {
		slog.Error("Failed to create session", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to create session"})
		return
	}

	// --- Set cookie ---
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookie,
		Value:    token,
		Path:     "/",
		MaxAge:   int(SessionDuration.Seconds()),
		HttpOnly: true,
		Secure:   shouldUseSecureCookie(r, h.Config),
		SameSite: http.SameSiteLaxMode,
	})

	// --- Reset rate limits on success ---
	h.RateLimiter.ResetLimit(ipKey)
	h.RateLimiter.ResetLimit(userKey)

	// --- Audit log ---
	h.DB.CreateAuditLog(database.AuditLogParams{
		Action:    "user.login",
		Username:  strPtr(user.Username),
		Details:   strPtr(fmt.Sprintf("Login from %s (role: %s)", clientIP, user.Role)),
		IPAddress: strPtr(clientIP),
	})

	slog.Info("User logged in", "user", user.Username, "role", user.Role)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"user": userResponse{
			ID:       user.ID,
			Username: user.Username,
			Role:     user.Role,
		},
	})
}

// ---------- POST /api/auth/logout ----------

func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(SessionCookie)
	if err != nil || cookie.Value == "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
		return
	}

	// Fetch session for audit log before deleting.
	session, _ := h.DB.GetSessionByToken(cookie.Value)

	if err := h.DB.DeleteSession(cookie.Value); err != nil {
		slog.Error("Failed to delete session", "error", err)
	}

	// Clear cookie.
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookie,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   shouldUseSecureCookie(r, h.Config),
		SameSite: http.SameSiteLaxMode,
	})

	if session != nil {
		clientIP := getClientIP(r)
		h.DB.CreateAuditLog(database.AuditLogParams{
			Action:    "user.logout",
			Username:  strPtr(session.Username),
			Details:   strPtr(fmt.Sprintf("Logout from %s", clientIP)),
			IPAddress: strPtr(clientIP),
		})
		slog.Info("User logged out", "user", session.Username)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// ---------- GET /api/auth/session ----------

func (h *AuthHandler) Session(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(SessionCookie)
	if err != nil || cookie.Value == "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"authenticated": false,
		})
		return
	}

	session, err := h.DB.GetSessionByToken(cookie.Value)
	if err != nil {
		slog.Error("Failed to get session", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Session lookup failed"})
		return
	}
	if session == nil {
		// Session expired or invalid — clear the stale cookie.
		http.SetCookie(w, &http.Cookie{
			Name:     SessionCookie,
			Value:    "",
			Path:     "/",
			MaxAge:   -1,
			HttpOnly: true,
			Secure:   shouldUseSecureCookie(r, h.Config),
			SameSite: http.SameSiteLaxMode,
		})
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"authenticated": false,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"authenticated": true,
		"user": userResponse{
			ID:       session.UserID,
			Username: session.Username,
			Role:     session.UserRole,
		},
		"version": version.Version,
	})
}
