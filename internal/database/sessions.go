package database

import (
	"database/sql"
	"fmt"
	"time"
)

// Session represents an authenticated session.
type Session struct {
	ID        int    `json:"id"`
	Token     string `json:"token"`
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	UserRole  string `json:"user_role"`
	CreatedAt string `json:"created_at"`
	ExpiresAt string `json:"expires_at"`
}

// GetSessionByToken retrieves a session by token. Deletes and returns nil if expired.
func (db *DB) GetSessionByToken(token string) (*Session, error) {
	row := db.conn.QueryRow(
		"SELECT id, token, user_id, username, user_role, created_at, expires_at FROM sessions WHERE token = ?",
		token,
	)

	var s Session
	err := row.Scan(&s.ID, &s.Token, &s.UserID, &s.Username, &s.UserRole, &s.CreatedAt, &s.ExpiresAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get session: %w", err)
	}

	expiresAt, err := time.Parse(time.RFC3339, s.ExpiresAt)
	if err != nil {
		expiresAt, err = time.Parse("2006-01-02T15:04:05.000Z", s.ExpiresAt)
		if err != nil {
			db.conn.Exec("DELETE FROM sessions WHERE id = ?", s.ID)
			return nil, nil
		}
	}

	if time.Now().UTC().After(expiresAt) {
		db.conn.Exec("DELETE FROM sessions WHERE id = ?", s.ID)
		return nil, nil
	}

	return &s, nil
}

// CreateSession creates a new session.
func (db *DB) CreateSession(token, userID, username, role, expiresAt string) error {
	if role == "" {
		role = "viewer"
	}
	_, err := db.conn.Exec(
		`INSERT INTO sessions (token, user_id, username, user_role, expires_at) VALUES (?, ?, ?, ?, ?)`,
		token, userID, username, role, expiresAt,
	)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	return nil
}

// DeleteSession deletes a session by its token.
func (db *DB) DeleteSession(token string) error {
	_, err := db.conn.Exec("DELETE FROM sessions WHERE token = ?", token)
	if err != nil {
		return fmt.Errorf("delete session: %w", err)
	}
	return nil
}

// CleanExpiredSessions removes all expired sessions.
func (db *DB) CleanExpiredSessions() error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.conn.Exec("DELETE FROM sessions WHERE expires_at < ?", now)
	return err
}
