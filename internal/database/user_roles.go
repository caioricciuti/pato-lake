package database

import (
	"database/sql"
	"fmt"
)

// GetUserRole retrieves the role for a user from the users table.
// Returns empty string if the user is not found.
func (db *DB) GetUserRole(username string) (string, error) {
	var role string
	err := db.conn.QueryRow("SELECT role FROM users WHERE username = ?", username).Scan(&role)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("get user role: %w", err)
	}
	return role, nil
}

// SetUserRole updates the role for a user in the users table.
func (db *DB) SetUserRole(username, role string) error {
	result, err := db.conn.Exec(
		"UPDATE users SET role = ?, updated_at = datetime('now') WHERE username = ?",
		role, username,
	)
	if err != nil {
		return fmt.Errorf("set user role: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set user role (rows affected): %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("set user role: user %q not found", username)
	}
	return nil
}

// CountUsersWithRole returns the number of users with a given role.
func (db *DB) CountUsersWithRole(role string) (int, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM users WHERE role = ?", role).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count users with role: %w", err)
	}
	return count, nil
}

// IsUserRole returns true if the user currently has the given role.
func (db *DB) IsUserRole(username, role string) (bool, error) {
	var exists int
	err := db.conn.QueryRow(
		"SELECT 1 FROM users WHERE username = ? AND role = ? LIMIT 1",
		username, role,
	).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("is user role: %w", err)
	}
	return exists == 1, nil
}
