package database

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Notebook represents a SQL notebook.
type Notebook struct {
	ID          string  `json:"id"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	CreatedBy   string  `json:"created_by"`
	IsPublic    bool    `json:"is_public"`
	ShareToken  *string `json:"share_token"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
}

// NotebookCell represents a cell in a notebook.
type NotebookCell struct {
	ID         string `json:"id"`
	NotebookID string `json:"notebook_id"`
	CellType   string `json:"cell_type"`
	Content    string `json:"content"`
	Position   int    `json:"position"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

// CreateNotebookParams holds params for creating a notebook.
type CreateNotebookParams struct {
	Title       string
	Description string
	CreatedBy   string
}

// CreateNotebook creates a new notebook.
func (db *DB) CreateNotebook(params CreateNotebookParams) (*Notebook, error) {
	id := uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339)

	_, err := db.conn.Exec(`INSERT INTO notebooks (id, title, description, created_by, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		id, params.Title, params.Description, params.CreatedBy, now, now)
	if err != nil {
		return nil, fmt.Errorf("create notebook: %w", err)
	}

	return &Notebook{
		ID:          id,
		Title:       params.Title,
		Description: params.Description,
		CreatedBy:   params.CreatedBy,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

// GetNotebook retrieves a notebook by ID.
func (db *DB) GetNotebook(id string) (*Notebook, error) {
	row := db.conn.QueryRow(`SELECT id, title, description, created_by, is_public, share_token, created_at, updated_at
		FROM notebooks WHERE id = ?`, id)

	var n Notebook
	var isPublic int
	var shareToken sql.NullString
	err := row.Scan(&n.ID, &n.Title, &n.Description, &n.CreatedBy, &isPublic, &shareToken, &n.CreatedAt, &n.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get notebook: %w", err)
	}
	n.IsPublic = isPublic == 1
	n.ShareToken = nullStringToPtr(shareToken)
	return &n, nil
}

// ListNotebooks returns all notebooks, optionally filtered by user.
func (db *DB) ListNotebooks(userID string) ([]Notebook, error) {
	query := `SELECT id, title, description, created_by, is_public, share_token, created_at, updated_at FROM notebooks`
	var args []interface{}
	if userID != "" {
		query += " WHERE created_by = ?"
		args = append(args, userID)
	}
	query += " ORDER BY updated_at DESC"

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("list notebooks: %w", err)
	}
	defer rows.Close()

	var notebooks []Notebook
	for rows.Next() {
		var n Notebook
		var isPublic int
		var shareToken sql.NullString
		if err := rows.Scan(&n.ID, &n.Title, &n.Description, &n.CreatedBy, &isPublic, &shareToken, &n.CreatedAt, &n.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan notebook: %w", err)
		}
		n.IsPublic = isPublic == 1
		n.ShareToken = nullStringToPtr(shareToken)
		notebooks = append(notebooks, n)
	}
	return notebooks, rows.Err()
}

// UpdateNotebook updates a notebook's title and description.
func (db *DB) UpdateNotebook(id, title, description string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.conn.Exec(`UPDATE notebooks SET title = ?, description = ?, updated_at = ? WHERE id = ?`,
		title, description, now, id)
	if err != nil {
		return fmt.Errorf("update notebook: %w", err)
	}
	return nil
}

// DeleteNotebook removes a notebook and its cells (cascade).
func (db *DB) DeleteNotebook(id string) error {
	_, err := db.conn.Exec("DELETE FROM notebooks WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete notebook: %w", err)
	}
	return nil
}

// GetNotebookCells returns all cells for a notebook, ordered by position.
func (db *DB) GetNotebookCells(notebookID string) ([]NotebookCell, error) {
	rows, err := db.conn.Query(`SELECT id, notebook_id, cell_type, content, position, created_at, updated_at
		FROM notebook_cells WHERE notebook_id = ? ORDER BY position`, notebookID)
	if err != nil {
		return nil, fmt.Errorf("get notebook cells: %w", err)
	}
	defer rows.Close()

	var cells []NotebookCell
	for rows.Next() {
		var c NotebookCell
		if err := rows.Scan(&c.ID, &c.NotebookID, &c.CellType, &c.Content, &c.Position, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan notebook cell: %w", err)
		}
		cells = append(cells, c)
	}
	return cells, rows.Err()
}

// UpsertCell creates or updates a cell.
func (db *DB) UpsertCell(notebookID string, cell NotebookCell) error {
	now := time.Now().UTC().Format(time.RFC3339)

	if cell.ID == "" {
		cell.ID = uuid.NewString()
	}

	_, err := db.conn.Exec(`INSERT INTO notebook_cells (id, notebook_id, cell_type, content, position, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET cell_type = excluded.cell_type, content = excluded.content, position = excluded.position, updated_at = excluded.updated_at`,
		cell.ID, notebookID, cell.CellType, cell.Content, cell.Position, now, now)
	if err != nil {
		return fmt.Errorf("upsert cell: %w", err)
	}

	// Also update notebook's updated_at
	db.conn.Exec("UPDATE notebooks SET updated_at = ? WHERE id = ?", now, notebookID)
	return nil
}

// DeleteCell removes a cell.
func (db *DB) DeleteCell(cellID string) error {
	_, err := db.conn.Exec("DELETE FROM notebook_cells WHERE id = ?", cellID)
	if err != nil {
		return fmt.Errorf("delete cell: %w", err)
	}
	return nil
}

// BulkSaveCells replaces all cells for a notebook.
func (db *DB) BulkSaveCells(notebookID string, cells []NotebookCell) error {
	now := time.Now().UTC().Format(time.RFC3339)

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Delete existing cells
	if _, err := tx.Exec("DELETE FROM notebook_cells WHERE notebook_id = ?", notebookID); err != nil {
		return fmt.Errorf("delete existing cells: %w", err)
	}

	// Insert new cells
	for _, cell := range cells {
		cellID := cell.ID
		if cellID == "" {
			cellID = uuid.NewString()
		}
		if _, err := tx.Exec(`INSERT INTO notebook_cells (id, notebook_id, cell_type, content, position, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			cellID, notebookID, cell.CellType, cell.Content, cell.Position, now, now); err != nil {
			return fmt.Errorf("insert cell: %w", err)
		}
	}

	// Update notebook timestamp
	if _, err := tx.Exec("UPDATE notebooks SET updated_at = ? WHERE id = ?", now, notebookID); err != nil {
		return fmt.Errorf("update notebook timestamp: %w", err)
	}

	return tx.Commit()
}

// GenerateShareToken generates and stores a share token for a notebook.
func (db *DB) GenerateShareToken(notebookID string) (string, error) {
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("generate share token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)
	now := time.Now().UTC().Format(time.RFC3339)

	_, err := db.conn.Exec(`UPDATE notebooks SET share_token = ?, is_public = 1, updated_at = ? WHERE id = ?`,
		token, now, notebookID)
	if err != nil {
		return "", fmt.Errorf("store share token: %w", err)
	}
	return token, nil
}

// RevokeShareToken removes the share token from a notebook.
func (db *DB) RevokeShareToken(notebookID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.conn.Exec(`UPDATE notebooks SET share_token = NULL, is_public = 0, updated_at = ? WHERE id = ?`,
		now, notebookID)
	if err != nil {
		return fmt.Errorf("revoke share token: %w", err)
	}
	return nil
}

// GetNotebookByShareToken retrieves a public notebook by its share token.
func (db *DB) GetNotebookByShareToken(token string) (*Notebook, error) {
	row := db.conn.QueryRow(`SELECT id, title, description, created_by, is_public, share_token, created_at, updated_at
		FROM notebooks WHERE share_token = ? AND is_public = 1`, token)

	var n Notebook
	var isPublic int
	var shareToken sql.NullString
	err := row.Scan(&n.ID, &n.Title, &n.Description, &n.CreatedBy, &isPublic, &shareToken, &n.CreatedAt, &n.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get notebook by share token: %w", err)
	}
	n.IsPublic = isPublic == 1
	n.ShareToken = nullStringToPtr(shareToken)
	return &n, nil
}
