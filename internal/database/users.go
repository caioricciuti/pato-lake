package database

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// User represents a local user account.
type User struct {
	ID           string `json:"id"`
	Username     string `json:"username"`
	PasswordHash string `json:"-"`
	Role         string `json:"role"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

// GetUserByUsername retrieves a user by username.
func (db *DB) GetUserByUsername(username string) (*User, error) {
	row := db.conn.QueryRow(
		"SELECT id, username, password_hash, role, created_at, updated_at FROM users WHERE username = ?",
		username,
	)

	var u User
	err := row.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}
	return &u, nil
}

// GetUserByID retrieves a user by ID.
func (db *DB) GetUserByID(id string) (*User, error) {
	row := db.conn.QueryRow(
		"SELECT id, username, password_hash, role, created_at, updated_at FROM users WHERE id = ?",
		id,
	)

	var u User
	err := row.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get user by id: %w", err)
	}
	return &u, nil
}

// CreateUser creates a new user with a bcrypt-hashed password.
func (db *DB) CreateUser(username, password, role string) (*User, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	id := uuid.NewString()
	if role == "" {
		role = "viewer"
	}

	_, err = db.conn.Exec(
		`INSERT INTO users (id, username, password_hash, role) VALUES (?, ?, ?, ?)`,
		id, username, string(hash), role,
	)
	if err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}

	return &User{ID: id, Username: username, Role: role}, nil
}

// ListUsers returns all users (without password hashes).
func (db *DB) ListUsers() ([]User, error) {
	rows, err := db.conn.Query(
		"SELECT id, username, password_hash, role, created_at, updated_at FROM users ORDER BY created_at",
	)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// UpdateUserPassword changes a user's password.
func (db *DB) UpdateUserPassword(userID, newPassword string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}

	_, err = db.conn.Exec(
		"UPDATE users SET password_hash = ?, updated_at = datetime('now') WHERE id = ?",
		string(hash), userID,
	)
	return err
}

// UpdateUserRole changes a user's role.
func (db *DB) UpdateUserRole(userID, role string) error {
	_, err := db.conn.Exec(
		"UPDATE users SET role = ?, updated_at = datetime('now') WHERE id = ?",
		role, userID,
	)
	return err
}

// DeleteUser deletes a user and all their sessions.
func (db *DB) DeleteUser(userID string) error {
	_, err := db.conn.Exec("DELETE FROM sessions WHERE user_id = ?", userID)
	if err != nil {
		return fmt.Errorf("delete user sessions: %w", err)
	}
	_, err = db.conn.Exec("DELETE FROM users WHERE id = ?", userID)
	if err != nil {
		return fmt.Errorf("delete user: %w", err)
	}
	return nil
}

// BootstrapAdmin ensures an admin user exists. Creates one if the users table is empty.
func (db *DB) BootstrapAdmin(username, password string) error {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		return fmt.Errorf("count users: %w", err)
	}

	if count > 0 {
		return nil // Users already exist
	}

	if username == "" {
		username = "admin"
	}
	if password == "" {
		password = "admin" // Default password — user should change it
		slog.Warn("No ADMIN_PASSWORD set; created admin user with default password 'admin'. Change it immediately!")
	}

	_, err = db.CreateUser(username, password, "admin")
	if err != nil {
		return fmt.Errorf("bootstrap admin: %w", err)
	}

	slog.Info("Admin user created", "username", username)
	return nil
}

// HasUsers returns true if any users exist.
func (db *DB) HasUsers() (bool, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	return count > 0, err
}
