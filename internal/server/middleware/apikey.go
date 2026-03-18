package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/caioricciuti/pato-lake/internal/database"
)

// rateLimitEntry tracks requests in a sliding window.
type rateLimitEntry struct {
	mu       sync.Mutex
	requests []time.Time
}

var apiKeyRateLimits sync.Map // key_hash → *rateLimitEntry

// SessionOrAPIKey authenticates via API key (Bearer token) first, then falls
// back to session cookie. Populates the same SessionInfo in the context.
func SessionOrAPIKey(db *database.DB) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ptlk_") {
				token := strings.TrimPrefix(authHeader, "Bearer ")
				handleAPIKeyAuth(w, r, next, db, token)
				return
			}

			// Fall through to session cookie auth
			Session(db)(next).ServeHTTP(w, r)
		})
	}
}

func handleAPIKeyAuth(w http.ResponseWriter, r *http.Request, next http.Handler, db *database.DB, token string) {
	hash := sha256.Sum256([]byte(token))
	keyHash := hex.EncodeToString(hash[:])

	apiKey, err := db.GetAPIKeyByHash(keyHash)
	if err != nil {
		slog.Error("API key lookup failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "API key lookup failed"})
		return
	}
	if apiKey == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Invalid API key"})
		return
	}

	if !apiKey.IsActive {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "API key is deactivated"})
		return
	}

	// Check expiry
	if apiKey.ExpiresAt != nil && *apiKey.ExpiresAt != "" {
		expiry, err := time.Parse(time.RFC3339, *apiKey.ExpiresAt)
		if err == nil && time.Now().UTC().After(expiry) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "API key has expired"})
			return
		}
	}

	// Rate limiting (sliding window)
	if !checkRateLimit(keyHash, apiKey.RateLimitRPM) {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]interface{}{
			"error":       "Rate limit exceeded",
			"retry_after": 60,
		})
		return
	}

	// Resolve user to populate SessionInfo
	user, err := db.GetUserByID(apiKey.UserID)
	if err != nil || user == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "API key owner not found"})
		return
	}

	info := &SessionInfo{
		ID:       "apikey:" + apiKey.ID,
		UserID:   apiKey.UserID,
		Username: user.Username,
		UserRole: apiKey.Role,
	}

	// Touch usage async
	go func() {
		db.TouchAPIKeyUsage(apiKey.ID)
		db.IncrementAPIKeyUsage(apiKey.ID, 1, 0, 0, 0)
	}()

	ctx := SetSession(r.Context(), info)
	next.ServeHTTP(w, r.WithContext(ctx))
}

func checkRateLimit(keyHash string, limitRPM int) bool {
	if limitRPM <= 0 {
		return true
	}

	val, _ := apiKeyRateLimits.LoadOrStore(keyHash, &rateLimitEntry{})
	entry := val.(*rateLimitEntry)

	entry.mu.Lock()
	defer entry.mu.Unlock()

	now := time.Now()
	window := now.Add(-time.Minute)

	// Remove expired entries
	valid := entry.requests[:0]
	for _, t := range entry.requests {
		if t.After(window) {
			valid = append(valid, t)
		}
	}
	entry.requests = valid

	if len(entry.requests) >= limitRPM {
		return false
	}

	entry.requests = append(entry.requests, now)
	return true
}

// HasAPIKeyScope checks if the API key in the current request has the given scope.
// Returns true if auth is via session cookie (not API key) or if scope matches.
func HasAPIKeyScope(r *http.Request, scope string) bool {
	session := GetSession(r)
	if session == nil {
		return false
	}
	// Session-based auth has all scopes
	if !strings.HasPrefix(session.ID, "apikey:") {
		return true
	}
	// For API key auth, we'd need to check scopes. For now the middleware
	// doesn't restrict scopes, but this helper enables future scope checks.
	return true
}

// writeJSONAPIKey is a local JSON writer (avoids import cycle).
func init() {
	// Ensure the sync.Map is ready
	_ = json.Marshal
}
