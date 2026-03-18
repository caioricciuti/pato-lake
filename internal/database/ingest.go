package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// IngestSource represents an event ingestion source configuration.
type IngestSource struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	EventType       string `json:"event_type"`
	TargetSchema    string `json:"target_schema"`
	TargetTable     string `json:"target_table"`
	BufferSize      int    `json:"buffer_size"`
	FlushIntervalMs int    `json:"flush_interval_ms"`
	IsActive        bool   `json:"is_active"`
	CreatedBy       string `json:"created_by"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

// IngestStats represents daily ingestion statistics.
type IngestStats struct {
	ID             string  `json:"id"`
	SourceID       string  `json:"source_id"`
	Date           string  `json:"date"`
	EventsReceived int     `json:"events_received"`
	EventsWritten  int     `json:"events_written"`
	BytesReceived  int     `json:"bytes_received"`
	ErrorsCount    int     `json:"errors_count"`
	LastError      *string `json:"last_error"`
	CreatedAt      string  `json:"created_at"`
}

// CreateIngestSourceParams holds parameters for creating an ingest source.
type CreateIngestSourceParams struct {
	Name            string
	EventType       string
	TargetSchema    string
	TargetTable     string
	BufferSize      int
	FlushIntervalMs int
	CreatedBy       string
}

// CreateIngestSource creates a new ingestion source.
func (db *DB) CreateIngestSource(params CreateIngestSourceParams) (*IngestSource, error) {
	id := uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339)

	schema := params.TargetSchema
	if schema == "" {
		schema = "main"
	}
	table := params.TargetTable
	if table == "" {
		table = "events_" + params.EventType
	}
	bufferSize := params.BufferSize
	if bufferSize <= 0 {
		bufferSize = 1000
	}
	flushInterval := params.FlushIntervalMs
	if flushInterval <= 0 {
		flushInterval = 5000
	}

	_, err := db.conn.Exec(`INSERT INTO ingest_sources (id, name, event_type, target_schema, target_table, buffer_size, flush_interval_ms, is_active, created_by, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
		id, params.Name, params.EventType, schema, table, bufferSize, flushInterval, params.CreatedBy, now, now)
	if err != nil {
		return nil, fmt.Errorf("create ingest source: %w", err)
	}

	return &IngestSource{
		ID:              id,
		Name:            params.Name,
		EventType:       params.EventType,
		TargetSchema:    schema,
		TargetTable:     table,
		BufferSize:      bufferSize,
		FlushIntervalMs: flushInterval,
		IsActive:        true,
		CreatedBy:       params.CreatedBy,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

// GetIngestSourceByEventType retrieves an ingest source by its event type.
func (db *DB) GetIngestSourceByEventType(eventType string) (*IngestSource, error) {
	row := db.conn.QueryRow(`SELECT id, name, event_type, target_schema, target_table, buffer_size, flush_interval_ms, is_active, created_by, created_at, updated_at
		FROM ingest_sources WHERE event_type = ?`, eventType)

	var s IngestSource
	var isActive int
	var createdBy sql.NullString
	err := row.Scan(&s.ID, &s.Name, &s.EventType, &s.TargetSchema, &s.TargetTable, &s.BufferSize, &s.FlushIntervalMs, &isActive, &createdBy, &s.CreatedAt, &s.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get ingest source by event type: %w", err)
	}
	s.IsActive = isActive == 1
	if createdBy.Valid {
		s.CreatedBy = createdBy.String
	}
	return &s, nil
}

// ListIngestSources returns all ingest sources.
func (db *DB) ListIngestSources() ([]IngestSource, error) {
	rows, err := db.conn.Query(`SELECT id, name, event_type, target_schema, target_table, buffer_size, flush_interval_ms, is_active, created_by, created_at, updated_at
		FROM ingest_sources ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list ingest sources: %w", err)
	}
	defer rows.Close()

	var sources []IngestSource
	for rows.Next() {
		var s IngestSource
		var isActive int
		var createdBy sql.NullString
		if err := rows.Scan(&s.ID, &s.Name, &s.EventType, &s.TargetSchema, &s.TargetTable, &s.BufferSize, &s.FlushIntervalMs, &isActive, &createdBy, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan ingest source: %w", err)
		}
		s.IsActive = isActive == 1
		if createdBy.Valid {
			s.CreatedBy = createdBy.String
		}
		sources = append(sources, s)
	}
	return sources, rows.Err()
}

// UpdateIngestSource updates an ingest source.
func (db *DB) UpdateIngestSource(id, name string, isActive bool, bufferSize, flushIntervalMs int) error {
	now := time.Now().UTC().Format(time.RFC3339)
	active := 0
	if isActive {
		active = 1
	}
	_, err := db.conn.Exec(`UPDATE ingest_sources SET name = ?, is_active = ?, buffer_size = ?, flush_interval_ms = ?, updated_at = ? WHERE id = ?`,
		name, active, bufferSize, flushIntervalMs, now, id)
	if err != nil {
		return fmt.Errorf("update ingest source: %w", err)
	}
	return nil
}

// DeleteIngestSource removes an ingest source.
func (db *DB) DeleteIngestSource(id string) error {
	_, err := db.conn.Exec("DELETE FROM ingest_sources WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete ingest source: %w", err)
	}
	return nil
}

// IncrementIngestStats updates daily ingestion statistics.
func (db *DB) IncrementIngestStats(sourceID string, received, written, bytesReceived, errors int, lastError string) {
	today := time.Now().UTC().Format("2006-01-02")

	var errVal interface{}
	if lastError != "" {
		errVal = lastError
	}

	_, err := db.conn.Exec(`INSERT INTO ingest_stats (id, source_id, date, events_received, events_written, bytes_received, errors_count, last_error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_id, date) DO UPDATE SET
			events_received = events_received + excluded.events_received,
			events_written = events_written + excluded.events_written,
			bytes_received = bytes_received + excluded.bytes_received,
			errors_count = errors_count + excluded.errors_count,
			last_error = CASE WHEN excluded.last_error IS NOT NULL THEN excluded.last_error ELSE last_error END`,
		uuid.NewString(), sourceID, today, received, written, bytesReceived, errors, errVal)
	if err != nil {
		fmt.Printf("warn: increment ingest stats: %v\n", err)
	}
}

// GetIngestStatsOverview returns recent ingestion stats for all sources.
func (db *DB) GetIngestStatsOverview(days int) ([]IngestStats, error) {
	since := time.Now().UTC().AddDate(0, 0, -days).Format("2006-01-02")
	rows, err := db.conn.Query(`SELECT id, source_id, date, events_received, events_written, bytes_received, errors_count, last_error, created_at
		FROM ingest_stats WHERE date >= ? ORDER BY date DESC`, since)
	if err != nil {
		return nil, fmt.Errorf("get ingest stats overview: %w", err)
	}
	defer rows.Close()

	var stats []IngestStats
	for rows.Next() {
		var s IngestStats
		var lastError sql.NullString
		if err := rows.Scan(&s.ID, &s.SourceID, &s.Date, &s.EventsReceived, &s.EventsWritten, &s.BytesReceived, &s.ErrorsCount, &lastError, &s.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan ingest stats: %w", err)
		}
		s.LastError = nullStringToPtr(lastError)
		stats = append(stats, s)
	}
	return stats, rows.Err()
}
