package database

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// APIKey represents a stored API key.
type APIKey struct {
	ID           string  `json:"id"`
	UserID       string  `json:"user_id"`
	Name         string  `json:"name"`
	KeyPrefix    string  `json:"key_prefix"`
	Role         string  `json:"role"`
	Scopes       string  `json:"scopes"`
	RateLimitRPM int     `json:"rate_limit_rpm"`
	IsActive     bool    `json:"is_active"`
	LastUsedAt   *string `json:"last_used_at"`
	ExpiresAt    *string `json:"expires_at"`
	CreatedAt    string  `json:"created_at"`
	UpdatedAt    string  `json:"updated_at"`
}

// APIKeyWithHash includes the hash for internal lookups.
type APIKeyWithHash struct {
	APIKey
	KeyHash string `json:"-"`
}

// APIKeyUsage represents daily usage stats for an API key.
type APIKeyUsage struct {
	ID           string `json:"id"`
	APIKeyID     string `json:"api_key_id"`
	Date         string `json:"date"`
	RequestCount int    `json:"request_count"`
	QueryCount   int    `json:"query_count"`
	IngestEvents int    `json:"ingest_events"`
	IngestBytes  int    `json:"ingest_bytes"`
	CreatedAt    string `json:"created_at"`
}

// CreateAPIKeyParams holds the parameters for creating a new API key.
type CreateAPIKeyParams struct {
	UserID       string
	Name         string
	Role         string
	Scopes       string
	RateLimitRPM int
	ExpiresAt    string
}

// CreateAPIKeyResult is returned after creating an API key.
type CreateAPIKeyResult struct {
	Key    string `json:"key"`     // raw key, shown once
	APIKey APIKey `json:"api_key"` // stored metadata
}

// CreateAPIKey generates a new API key and stores its SHA-256 hash.
func (db *DB) CreateAPIKey(params CreateAPIKeyParams) (*CreateAPIKeyResult, error) {
	rawBytes := make([]byte, 20)
	if _, err := rand.Read(rawBytes); err != nil {
		return nil, fmt.Errorf("generate random key: %w", err)
	}
	rawHex := hex.EncodeToString(rawBytes)
	fullKey := "ptlk_" + rawHex

	prefix := fullKey[:9] // "ptlk_" + 4 hex chars

	hash := sha256.Sum256([]byte(fullKey))
	keyHash := hex.EncodeToString(hash[:])

	id := uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339)

	role := params.Role
	if role == "" {
		role = "viewer"
	}
	scopes := params.Scopes
	if scopes == "" {
		scopes = "*"
	}
	rateLimitRPM := params.RateLimitRPM
	if rateLimitRPM <= 0 {
		rateLimitRPM = 60
	}

	var expiresAt interface{}
	if params.ExpiresAt != "" {
		expiresAt = params.ExpiresAt
	}

	_, err := db.conn.Exec(`INSERT INTO api_keys (id, user_id, name, key_prefix, key_hash, role, scopes, rate_limit_rpm, is_active, expires_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
		id, params.UserID, params.Name, prefix, keyHash, role, scopes, rateLimitRPM, expiresAt, now, now,
	)
	if err != nil {
		return nil, fmt.Errorf("insert api key: %w", err)
	}

	apiKey := APIKey{
		ID:           id,
		UserID:       params.UserID,
		Name:         params.Name,
		KeyPrefix:    prefix,
		Role:         role,
		Scopes:       scopes,
		RateLimitRPM: rateLimitRPM,
		IsActive:     true,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if params.ExpiresAt != "" {
		apiKey.ExpiresAt = &params.ExpiresAt
	}

	return &CreateAPIKeyResult{Key: fullKey, APIKey: apiKey}, nil
}

// GetAPIKeyByHash looks up an API key by its SHA-256 hash.
func (db *DB) GetAPIKeyByHash(keyHash string) (*APIKeyWithHash, error) {
	row := db.conn.QueryRow(`SELECT id, user_id, name, key_prefix, key_hash, role, scopes, rate_limit_rpm, is_active, last_used_at, expires_at, created_at, updated_at
		FROM api_keys WHERE key_hash = ?`, keyHash)

	var k APIKeyWithHash
	var isActive int
	var lastUsedAt, expiresAt sql.NullString
	err := row.Scan(&k.ID, &k.UserID, &k.Name, &k.KeyPrefix, &k.KeyHash, &k.Role, &k.Scopes, &k.RateLimitRPM, &isActive, &lastUsedAt, &expiresAt, &k.CreatedAt, &k.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get api key by hash: %w", err)
	}
	k.IsActive = isActive == 1
	k.LastUsedAt = nullStringToPtr(lastUsedAt)
	k.ExpiresAt = nullStringToPtr(expiresAt)
	return &k, nil
}

// ListAPIKeysByUser returns all API keys for a given user.
func (db *DB) ListAPIKeysByUser(userID string) ([]APIKey, error) {
	rows, err := db.conn.Query(`SELECT id, user_id, name, key_prefix, role, scopes, rate_limit_rpm, is_active, last_used_at, expires_at, created_at, updated_at
		FROM api_keys WHERE user_id = ? ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		var k APIKey
		var isActive int
		var lastUsedAt, expiresAt sql.NullString
		if err := rows.Scan(&k.ID, &k.UserID, &k.Name, &k.KeyPrefix, &k.Role, &k.Scopes, &k.RateLimitRPM, &isActive, &lastUsedAt, &expiresAt, &k.CreatedAt, &k.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan api key: %w", err)
		}
		k.IsActive = isActive == 1
		k.LastUsedAt = nullStringToPtr(lastUsedAt)
		k.ExpiresAt = nullStringToPtr(expiresAt)
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// ListAllAPIKeys returns all API keys (admin use).
func (db *DB) ListAllAPIKeys() ([]APIKey, error) {
	rows, err := db.conn.Query(`SELECT id, user_id, name, key_prefix, role, scopes, rate_limit_rpm, is_active, last_used_at, expires_at, created_at, updated_at
		FROM api_keys ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list all api keys: %w", err)
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		var k APIKey
		var isActive int
		var lastUsedAt, expiresAt sql.NullString
		if err := rows.Scan(&k.ID, &k.UserID, &k.Name, &k.KeyPrefix, &k.Role, &k.Scopes, &k.RateLimitRPM, &isActive, &lastUsedAt, &expiresAt, &k.CreatedAt, &k.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan api key: %w", err)
		}
		k.IsActive = isActive == 1
		k.LastUsedAt = nullStringToPtr(lastUsedAt)
		k.ExpiresAt = nullStringToPtr(expiresAt)
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// UpdateAPIKey updates a key's mutable fields.
func (db *DB) UpdateAPIKey(id string, name string, isActive bool, rateLimitRPM int, scopes string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	active := 0
	if isActive {
		active = 1
	}
	_, err := db.conn.Exec(`UPDATE api_keys SET name = ?, is_active = ?, rate_limit_rpm = ?, scopes = ?, updated_at = ? WHERE id = ?`,
		name, active, rateLimitRPM, scopes, now, id)
	if err != nil {
		return fmt.Errorf("update api key: %w", err)
	}
	return nil
}

// DeleteAPIKey removes an API key.
func (db *DB) DeleteAPIKey(id string) error {
	_, err := db.conn.Exec("DELETE FROM api_keys WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete api key: %w", err)
	}
	return nil
}

// TouchAPIKeyUsage updates the last_used_at timestamp.
func (db *DB) TouchAPIKeyUsage(id string) {
	now := time.Now().UTC().Format(time.RFC3339)
	db.conn.Exec("UPDATE api_keys SET last_used_at = ? WHERE id = ?", now, id)
}

// IncrementAPIKeyUsage increments daily usage counters.
func (db *DB) IncrementAPIKeyUsage(apiKeyID string, requests, queries, ingestEvents, ingestBytes int) {
	today := time.Now().UTC().Format("2006-01-02")
	_, err := db.conn.Exec(`INSERT INTO api_key_usage (id, api_key_id, date, request_count, query_count, ingest_events, ingest_bytes)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(api_key_id, date) DO UPDATE SET
			request_count = request_count + excluded.request_count,
			query_count = query_count + excluded.query_count,
			ingest_events = ingest_events + excluded.ingest_events,
			ingest_bytes = ingest_bytes + excluded.ingest_bytes`,
		uuid.NewString(), apiKeyID, today, requests, queries, ingestEvents, ingestBytes)
	if err != nil {
		// Non-critical — just log
		fmt.Printf("warn: increment api key usage: %v\n", err)
	}
}

// GetAPIKeyUsage returns usage stats for an API key within the last N days.
func (db *DB) GetAPIKeyUsage(apiKeyID string, days int) ([]APIKeyUsage, error) {
	since := time.Now().UTC().AddDate(0, 0, -days).Format("2006-01-02")
	rows, err := db.conn.Query(`SELECT id, api_key_id, date, request_count, query_count, ingest_events, ingest_bytes, created_at
		FROM api_key_usage WHERE api_key_id = ? AND date >= ? ORDER BY date DESC`, apiKeyID, since)
	if err != nil {
		return nil, fmt.Errorf("get api key usage: %w", err)
	}
	defer rows.Close()

	var usage []APIKeyUsage
	for rows.Next() {
		var u APIKeyUsage
		if err := rows.Scan(&u.ID, &u.APIKeyID, &u.Date, &u.RequestCount, &u.QueryCount, &u.IngestEvents, &u.IngestBytes, &u.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan api key usage: %w", err)
		}
		usage = append(usage, u)
	}
	return usage, rows.Err()
}
