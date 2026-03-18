package middleware

import (
	"context"
	"net/http"
)

type contextKey string

const (
	sessionKey contextKey = "session"
)

// SessionInfo holds session data stored in the request context.
type SessionInfo struct {
	ID       string // session token
	UserID   string // user ID from SQLite users table
	Username string // username
	UserRole string // admin, analyst, viewer
}

// SetSession stores the session in the request context.
func SetSession(ctx context.Context, session *SessionInfo) context.Context {
	return context.WithValue(ctx, sessionKey, session)
}

// GetSession retrieves the session from the request context.
func GetSession(r *http.Request) *SessionInfo {
	s, _ := r.Context().Value(sessionKey).(*SessionInfo)
	return s
}
