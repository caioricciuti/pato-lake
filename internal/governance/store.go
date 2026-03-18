package governance

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/google/uuid"
)

// nullStringToPtr converts a sql.NullString to a *string (nil if not valid).
func nullStringToPtr(ns sql.NullString) *string {
	if ns.Valid {
		return &ns.String
	}
	return nil
}

// nullIntToPtr converts a sql.NullInt64 to an *int (nil if not valid).
func nullIntToPtr(ni sql.NullInt64) *int {
	if ni.Valid {
		v := int(ni.Int64)
		return &v
	}
	return nil
}

// ptrToNullString converts a *string to a sql.NullString.
func ptrToNullString(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}

// Store provides all governance CRUD operations against SQLite.
type Store struct {
	db *database.DB
}

// NewStore creates a new governance Store.
func NewStore(db *database.DB) *Store {
	return &Store{db: db}
}

// conn returns the underlying *sql.DB for running queries.
func (s *Store) conn() *sql.DB {
	return s.db.Conn()
}

// ── Sync State ───────────────────────────────────────────────────────────────

// GetSyncStates returns all sync states.
func (s *Store) GetSyncStates() ([]SyncState, error) {
	rows, err := s.conn().Query(
		`SELECT id, sync_type, last_synced_at, watermark, status, last_error, row_count, created_at, updated_at
		 FROM gov_sync_state ORDER BY sync_type`,
	)
	if err != nil {
		return nil, fmt.Errorf("get sync states: %w", err)
	}
	defer rows.Close()

	var results []SyncState
	for rows.Next() {
		var ss SyncState
		var lastSynced, watermark, lastError sql.NullString
		if err := rows.Scan(&ss.ID, &ss.SyncType, &lastSynced, &watermark, &ss.Status, &lastError, &ss.RowCount, &ss.CreatedAt, &ss.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan sync state: %w", err)
		}
		ss.LastSyncedAt = nullStringToPtr(lastSynced)
		ss.Watermark = nullStringToPtr(watermark)
		ss.LastError = nullStringToPtr(lastError)
		results = append(results, ss)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sync state rows: %w", err)
	}
	return results, nil
}

// GetSyncState returns a specific sync state for a sync type.
func (s *Store) GetSyncState(syncType string) (*SyncState, error) {
	row := s.conn().QueryRow(
		`SELECT id, sync_type, last_synced_at, watermark, status, last_error, row_count, created_at, updated_at
		 FROM gov_sync_state WHERE sync_type = ?`, syncType,
	)

	var ss SyncState
	var lastSynced, watermark, lastError sql.NullString
	err := row.Scan(&ss.ID, &ss.SyncType, &lastSynced, &watermark, &ss.Status, &lastError, &ss.RowCount, &ss.CreatedAt, &ss.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get sync state: %w", err)
	}
	ss.LastSyncedAt = nullStringToPtr(lastSynced)
	ss.Watermark = nullStringToPtr(watermark)
	ss.LastError = nullStringToPtr(lastError)
	return &ss, nil
}

// UpsertSyncState inserts or updates a sync state record.
func (s *Store) UpsertSyncState(syncType string, status string, watermark *string, lastError *string, rowCount int) error {
	now := time.Now().UTC().Format(time.RFC3339)
	id := uuid.NewString()

	_, err := s.conn().Exec(
		`INSERT INTO gov_sync_state (id, sync_type, last_synced_at, watermark, status, last_error, row_count, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(sync_type) DO UPDATE SET
		   last_synced_at = excluded.last_synced_at,
		   watermark = COALESCE(excluded.watermark, gov_sync_state.watermark),
		   status = excluded.status,
		   last_error = excluded.last_error,
		   row_count = excluded.row_count,
		   updated_at = excluded.updated_at`,
		id, syncType, now, ptrToNullString(watermark), status, ptrToNullString(lastError), rowCount, now, now,
	)
	if err != nil {
		return fmt.Errorf("upsert sync state: %w", err)
	}
	return nil
}

// UpdateSyncWatermark updates only the watermark for a specific sync state.
func (s *Store) UpdateSyncWatermark(syncType string, watermark string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.conn().Exec(
		`UPDATE gov_sync_state SET watermark = ?, updated_at = ? WHERE sync_type = ?`,
		watermark, now, syncType,
	)
	if err != nil {
		return fmt.Errorf("update sync watermark: %w", err)
	}
	return nil
}

// ── Databases ────────────────────────────────────────────────────────────────

// GetDatabases returns all databases.
func (s *Store) GetDatabases() ([]GovDatabase, error) {
	rows, err := s.conn().Query(
		`SELECT id, name, engine, first_seen, last_updated, is_deleted
		 FROM gov_databases ORDER BY name`,
	)
	if err != nil {
		return nil, fmt.Errorf("get databases: %w", err)
	}
	defer rows.Close()

	var results []GovDatabase
	for rows.Next() {
		var d GovDatabase
		if err := rows.Scan(&d.ID, &d.Name, &d.Engine, &d.FirstSeen, &d.LastUpdated, &d.IsDeleted); err != nil {
			return nil, fmt.Errorf("scan database: %w", err)
		}
		results = append(results, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate database rows: %w", err)
	}
	return results, nil
}

// UpsertDatabase inserts or updates a database record from a GovDatabase struct.
func (s *Store) UpsertDatabase(d GovDatabase) error {
	isDeleted := 0
	if d.IsDeleted {
		isDeleted = 1
	}

	_, err := s.conn().Exec(
		`INSERT INTO gov_databases (id, name, engine, first_seen, last_updated, is_deleted)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(name) DO UPDATE SET
		   engine = excluded.engine,
		   last_updated = excluded.last_updated,
		   is_deleted = excluded.is_deleted`,
		d.ID, d.Name, d.Engine, d.FirstSeen, d.LastUpdated, isDeleted,
	)
	if err != nil {
		return fmt.Errorf("upsert database: %w", err)
	}
	return nil
}

// MarkDatabaseDeleted soft-deletes a database record.
func (s *Store) MarkDatabaseDeleted(name string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.conn().Exec(
		`UPDATE gov_databases SET is_deleted = 1, last_updated = ? WHERE name = ?`,
		now, name,
	)
	if err != nil {
		return fmt.Errorf("mark database deleted: %w", err)
	}
	return nil
}

// ── Tables ───────────────────────────────────────────────────────────────────

// GetTables returns all non-deleted tables.
func (s *Store) GetTables() ([]GovTable, error) {
	rows, err := s.conn().Query(
		`SELECT id, database_name, table_name, engine, total_rows, total_bytes, first_seen, last_updated, is_deleted
		 FROM gov_tables WHERE is_deleted = 0 ORDER BY database_name, table_name`,
	)
	if err != nil {
		return nil, fmt.Errorf("get tables: %w", err)
	}
	defer rows.Close()

	return scanTables(rows)
}

// GetTablesByDatabase returns all non-deleted tables for a specific database.
func (s *Store) GetTablesByDatabase(databaseName string) ([]GovTable, error) {
	rows, err := s.conn().Query(
		`SELECT id, database_name, table_name, engine, total_rows, total_bytes, first_seen, last_updated, is_deleted
		 FROM gov_tables WHERE database_name = ? AND is_deleted = 0 ORDER BY table_name`,
		databaseName,
	)
	if err != nil {
		return nil, fmt.Errorf("get tables by database: %w", err)
	}
	defer rows.Close()

	return scanTables(rows)
}

// GetTableByName returns a single table by database and table name.
func (s *Store) GetTableByName(dbName, tableName string) (*GovTable, error) {
	row := s.conn().QueryRow(
		`SELECT id, database_name, table_name, engine, total_rows, total_bytes, first_seen, last_updated, is_deleted
		 FROM gov_tables WHERE database_name = ? AND table_name = ?`,
		dbName, tableName,
	)

	var t GovTable
	err := row.Scan(&t.ID, &t.DatabaseName, &t.TableName, &t.Engine, &t.TotalRows, &t.TotalBytes, &t.FirstSeen, &t.LastUpdated, &t.IsDeleted)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get table by name: %w", err)
	}
	return &t, nil
}

// UpsertTable inserts or updates a table record from a GovTable struct.
func (s *Store) UpsertTable(t GovTable) error {
	isDeleted := 0
	if t.IsDeleted {
		isDeleted = 1
	}

	_, err := s.conn().Exec(
		`INSERT INTO gov_tables (id, database_name, table_name, engine, total_rows, total_bytes, first_seen, last_updated, is_deleted)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(database_name, table_name) DO UPDATE SET
		   engine = excluded.engine,
		   total_rows = excluded.total_rows,
		   total_bytes = excluded.total_bytes,
		   last_updated = excluded.last_updated,
		   is_deleted = excluded.is_deleted`,
		t.ID, t.DatabaseName, t.TableName, t.Engine,
		t.TotalRows, t.TotalBytes, t.FirstSeen, t.LastUpdated, isDeleted,
	)
	if err != nil {
		return fmt.Errorf("upsert table: %w", err)
	}
	return nil
}

// MarkTableDeleted soft-deletes a table record.
func (s *Store) MarkTableDeleted(dbName, tableName string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.conn().Exec(
		`UPDATE gov_tables SET is_deleted = 1, last_updated = ? WHERE database_name = ? AND table_name = ?`,
		now, dbName, tableName,
	)
	if err != nil {
		return fmt.Errorf("mark table deleted: %w", err)
	}
	return nil
}

func scanTables(rows *sql.Rows) ([]GovTable, error) {
	var results []GovTable
	for rows.Next() {
		var t GovTable
		if err := rows.Scan(&t.ID, &t.DatabaseName, &t.TableName, &t.Engine, &t.TotalRows, &t.TotalBytes, &t.FirstSeen, &t.LastUpdated, &t.IsDeleted); err != nil {
			return nil, fmt.Errorf("scan table: %w", err)
		}
		results = append(results, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table rows: %w", err)
	}
	return results, nil
}

// ── Columns ──────────────────────────────────────────────────────────────────

// GetColumns returns columns, optionally filtered by database and table.
func (s *Store) GetColumns(dbName, tableName string) ([]GovColumn, error) {
	var query string
	var args []interface{}

	if dbName == "" && tableName == "" {
		query = `SELECT id, database_name, table_name, column_name, column_type, column_position, default_kind, default_expression, comment, first_seen, last_updated, is_deleted
			 FROM gov_columns WHERE is_deleted = 0 ORDER BY database_name, table_name, column_position`
	} else {
		query = `SELECT id, database_name, table_name, column_name, column_type, column_position, default_kind, default_expression, comment, first_seen, last_updated, is_deleted
			 FROM gov_columns WHERE database_name = ? AND table_name = ? AND is_deleted = 0 ORDER BY column_position`
		args = []interface{}{dbName, tableName}
	}

	rows, err := s.conn().Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}
	defer rows.Close()

	var results []GovColumn
	for rows.Next() {
		var c GovColumn
		var defaultKind, defaultExpr, comment sql.NullString
		if err := rows.Scan(&c.ID, &c.DatabaseName, &c.TableName, &c.ColumnName, &c.ColumnType, &c.ColumnPosition, &defaultKind, &defaultExpr, &comment, &c.FirstSeen, &c.LastUpdated, &c.IsDeleted); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		c.DefaultKind = nullStringToPtr(defaultKind)
		c.DefaultExpression = nullStringToPtr(defaultExpr)
		c.Comment = nullStringToPtr(comment)
		results = append(results, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate column rows: %w", err)
	}
	return results, nil
}

// UpsertColumn inserts or updates a column record from a GovColumn struct.
func (s *Store) UpsertColumn(c GovColumn) error {
	isDeleted := 0
	if c.IsDeleted {
		isDeleted = 1
	}

	_, err := s.conn().Exec(
		`INSERT INTO gov_columns (id, database_name, table_name, column_name, column_type, column_position, default_kind, default_expression, comment, first_seen, last_updated, is_deleted)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(database_name, table_name, column_name) DO UPDATE SET
		   column_type = excluded.column_type,
		   column_position = excluded.column_position,
		   default_kind = excluded.default_kind,
		   default_expression = excluded.default_expression,
		   comment = excluded.comment,
		   last_updated = excluded.last_updated,
		   is_deleted = excluded.is_deleted`,
		c.ID, c.DatabaseName, c.TableName, c.ColumnName, c.ColumnType, c.ColumnPosition,
		ptrToNullString(c.DefaultKind), ptrToNullString(c.DefaultExpression), ptrToNullString(c.Comment),
		c.FirstSeen, c.LastUpdated, isDeleted,
	)
	if err != nil {
		return fmt.Errorf("upsert column: %w", err)
	}
	return nil
}

// MarkColumnDeleted soft-deletes a column record.
func (s *Store) MarkColumnDeleted(dbName, tableName, colName string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.conn().Exec(
		`UPDATE gov_columns SET is_deleted = 1, last_updated = ? WHERE database_name = ? AND table_name = ? AND column_name = ?`,
		now, dbName, tableName, colName,
	)
	if err != nil {
		return fmt.Errorf("mark column deleted: %w", err)
	}
	return nil
}

// ── Schema Changes ───────────────────────────────────────────────────────────

// InsertSchemaChange inserts a schema change record from a SchemaChange struct.
func (s *Store) InsertSchemaChange(sc SchemaChange) error {
	_, err := s.conn().Exec(
		`INSERT INTO gov_schema_changes (id, change_type, database_name, table_name, column_name, old_value, new_value, detected_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		sc.ID, sc.ChangeType, sc.DatabaseName, sc.TableName, sc.ColumnName, sc.OldValue, sc.NewValue, sc.DetectedAt, sc.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert schema change: %w", err)
	}
	return nil
}

// CreateSchemaChange creates a new schema change record with auto-generated ID and timestamps.
func (s *Store) CreateSchemaChange(changeType SchemaChangeType, dbName, tableName, colName, oldVal, newVal string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	id := uuid.NewString()

	_, err := s.conn().Exec(
		`INSERT INTO gov_schema_changes (id, change_type, database_name, table_name, column_name, old_value, new_value, detected_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, string(changeType), dbName, tableName, colName, oldVal, newVal, now, now,
	)
	if err != nil {
		return fmt.Errorf("create schema change: %w", err)
	}
	return nil
}

// GetSchemaChanges returns recent schema changes.
func (s *Store) GetSchemaChanges(limit int) ([]SchemaChange, error) {
	rows, err := s.conn().Query(
		`SELECT id, change_type, database_name, table_name, column_name, old_value, new_value, detected_at, created_at
		 FROM gov_schema_changes ORDER BY detected_at DESC LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("get schema changes: %w", err)
	}
	defer rows.Close()

	var results []SchemaChange
	for rows.Next() {
		var sc SchemaChange
		if err := rows.Scan(&sc.ID, &sc.ChangeType, &sc.DatabaseName, &sc.TableName, &sc.ColumnName, &sc.OldValue, &sc.NewValue, &sc.DetectedAt, &sc.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan schema change: %w", err)
		}
		results = append(results, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema change rows: %w", err)
	}
	return results, nil
}

// ── Query Log ────────────────────────────────────────────────────────────────

// LogQuery inserts a single query log entry from the API layer.
func (s *Store) LogQuery(username, queryText, queryKind, eventTime string, durationMs, resultRows int64, tablesUsed string, isError bool, errorMessage string) (string, error) {
	id := uuid.NewString()
	queryID := uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339)
	errMsg := sql.NullString{}
	if errorMessage != "" {
		errMsg = sql.NullString{String: errorMessage, Valid: true}
	}
	isErr := 0
	if isError {
		isErr = 1
	}
	_, err := s.conn().Exec(
		`INSERT INTO gov_query_log (id, query_id, username, query_text, query_kind, event_time, duration_ms, result_rows, tables_used, is_error, error_message, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, queryID, username, queryText, queryKind, eventTime, durationMs, resultRows, tablesUsed, isErr, errMsg, now,
	)
	if err != nil {
		return "", fmt.Errorf("log query: %w", err)
	}
	return id, nil
}

// BatchInsertQueryLog inserts a batch of query log entries using INSERT OR IGNORE.
func (s *Store) BatchInsertQueryLog(entries []QueryLogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	tx, err := s.conn().Begin()
	if err != nil {
		return fmt.Errorf("begin query log batch: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(
		`INSERT OR IGNORE INTO gov_query_log (id, query_id, username, query_text, query_kind, event_time, duration_ms, result_rows, tables_used, is_error, error_message, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return fmt.Errorf("prepare query log insert: %w", err)
	}
	defer stmt.Close()

	for _, e := range entries {
		isError := 0
		if e.IsError {
			isError = 1
		}
		_, err := stmt.Exec(
			e.ID, e.QueryID, e.User, e.QueryText, e.QueryKind,
			e.EventTime, e.DurationMs, e.ResultRows,
			e.TablesUsed, isError, ptrToNullString(e.ErrorMessage), e.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("insert query log entry: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit query log batch: %w", err)
	}
	return nil
}

// InsertQueryLogBatch is an alias for BatchInsertQueryLog that also returns inserted count.
func (s *Store) InsertQueryLogBatch(entries []QueryLogEntry) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}

	tx, err := s.conn().Begin()
	if err != nil {
		return 0, fmt.Errorf("begin query log batch: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(
		`INSERT OR IGNORE INTO gov_query_log (id, query_id, username, query_text, query_kind, event_time, duration_ms, result_rows, tables_used, is_error, error_message, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return 0, fmt.Errorf("prepare query log insert: %w", err)
	}
	defer stmt.Close()

	now := time.Now().UTC().Format(time.RFC3339)
	inserted := 0

	for _, e := range entries {
		id := e.ID
		if id == "" {
			id = uuid.NewString()
		}
		isError := 0
		if e.IsError {
			isError = 1
		}
		createdAt := e.CreatedAt
		if createdAt == "" {
			createdAt = now
		}
		result, err := stmt.Exec(
			id, e.QueryID, e.User, e.QueryText, e.QueryKind,
			e.EventTime, e.DurationMs, e.ResultRows,
			e.TablesUsed, isError, ptrToNullString(e.ErrorMessage), createdAt,
		)
		if err != nil {
			return 0, fmt.Errorf("insert query log entry: %w", err)
		}
		affected, _ := result.RowsAffected()
		inserted += int(affected)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit query log batch: %w", err)
	}
	return inserted, nil
}

// GetQueryLog returns paginated query log entries with optional user/table filters.
func (s *Store) GetQueryLog(limit, offset int, user, table string) ([]QueryLogEntry, int, error) {
	where := "1=1"
	var args []interface{}

	if user != "" {
		where += " AND username = ?"
		args = append(args, user)
	}
	if table != "" {
		where += " AND tables_used LIKE ?"
		args = append(args, "%"+table+"%")
	}

	// Get total count
	var total int
	countArgs := make([]interface{}, len(args))
	copy(countArgs, args)
	err := s.conn().QueryRow("SELECT COUNT(*) FROM gov_query_log WHERE "+where, countArgs...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("count query log: %w", err)
	}

	// Get page
	query := fmt.Sprintf(
		`SELECT id, query_id, username, query_text, query_kind, event_time, duration_ms, result_rows, tables_used, is_error, error_message, created_at
		 FROM gov_query_log WHERE %s ORDER BY event_time DESC LIMIT ? OFFSET ?`, where,
	)
	args = append(args, limit, offset)

	rows, err := s.conn().Query(query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("get query log: %w", err)
	}
	defer rows.Close()

	var results []QueryLogEntry
	for rows.Next() {
		var e QueryLogEntry
		var errorMsg sql.NullString
		if err := rows.Scan(&e.ID, &e.QueryID, &e.User, &e.QueryText, &e.QueryKind, &e.EventTime, &e.DurationMs, &e.ResultRows, &e.TablesUsed, &e.IsError, &errorMsg, &e.CreatedAt); err != nil {
			return nil, 0, fmt.Errorf("scan query log entry: %w", err)
		}
		e.ErrorMessage = nullStringToPtr(errorMsg)
		results = append(results, e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate query log rows: %w", err)
	}
	return results, total, nil
}

// GetTopQueries returns the top queries grouped by query_kind.
func (s *Store) GetTopQueries(limit int) ([]map[string]interface{}, error) {
	rows, err := s.conn().Query(
		`SELECT
			query_kind,
			COUNT(*) AS cnt,
			ROUND(AVG(duration_ms), 2) AS avg_duration_ms,
			MIN(query_text) AS sample_query,
			MAX(event_time) AS last_seen
		 FROM gov_query_log
		 WHERE query_kind != ''
		 GROUP BY query_kind
		 ORDER BY cnt DESC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("get top queries: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var queryKind, sampleQuery, lastSeen string
		var cnt int
		var avgDurationMs float64
		if err := rows.Scan(&queryKind, &cnt, &avgDurationMs, &sampleQuery, &lastSeen); err != nil {
			return nil, fmt.Errorf("scan top query: %w", err)
		}
		results = append(results, map[string]interface{}{
			"query_kind":      queryKind,
			"count":           cnt,
			"avg_duration_ms": avgDurationMs,
			"sample_query":    sampleQuery,
			"last_seen":       lastSeen,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate top query rows: %w", err)
	}
	return results, nil
}

// ── Lineage ──────────────────────────────────────────────────────────────────

// InsertLineageEdge inserts a lineage edge using INSERT OR IGNORE.
func (s *Store) InsertLineageEdge(edge LineageEdge) error {
	now := time.Now().UTC().Format(time.RFC3339)

	_, err := s.conn().Exec(
		`INSERT OR IGNORE INTO gov_lineage_edges (id, source_database, source_table, target_database, target_table, query_id, username, edge_type, detected_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		edge.ID, edge.SourceDatabase, edge.SourceTable, edge.TargetDatabase, edge.TargetTable,
		edge.QueryID, edge.User, edge.EdgeType, edge.DetectedAt, now,
	)
	if err != nil {
		return fmt.Errorf("insert lineage edge: %w", err)
	}
	return nil
}

// UpsertLineageEdge is an alias for InsertLineageEdge (INSERT OR IGNORE is idempotent).
func (s *Store) UpsertLineageEdge(edge LineageEdge) error {
	return s.InsertLineageEdge(edge)
}

// GetLineageForTable returns upstream and downstream edges for a specific table.
func (s *Store) GetLineageForTable(dbName, tableName string) ([]LineageEdge, []LineageEdge, error) {
	upstreamRows, err := s.conn().Query(
		`SELECT id, source_database, source_table, target_database, target_table, query_id, username, edge_type, detected_at
		 FROM gov_lineage_edges WHERE target_database = ? AND target_table = ?`,
		dbName, tableName,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("get upstream lineage: %w", err)
	}
	defer upstreamRows.Close()

	upstream, err := scanLineageEdges(upstreamRows)
	if err != nil {
		return nil, nil, err
	}

	downstreamRows, err := s.conn().Query(
		`SELECT id, source_database, source_table, target_database, target_table, query_id, username, edge_type, detected_at
		 FROM gov_lineage_edges WHERE source_database = ? AND source_table = ?`,
		dbName, tableName,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("get downstream lineage: %w", err)
	}
	defer downstreamRows.Close()

	downstream, err := scanLineageEdges(downstreamRows)
	if err != nil {
		return nil, nil, err
	}

	return upstream, downstream, nil
}

// GetFullLineageGraph returns all lineage edges.
func (s *Store) GetFullLineageGraph() ([]LineageEdge, error) {
	rows, err := s.conn().Query(
		`SELECT id, source_database, source_table, target_database, target_table, query_id, username, edge_type, detected_at
		 FROM gov_lineage_edges ORDER BY detected_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("get full lineage graph: %w", err)
	}
	defer rows.Close()

	return scanLineageEdges(rows)
}

func scanLineageEdges(rows *sql.Rows) ([]LineageEdge, error) {
	var results []LineageEdge
	for rows.Next() {
		var e LineageEdge
		if err := rows.Scan(&e.ID, &e.SourceDatabase, &e.SourceTable, &e.TargetDatabase, &e.TargetTable, &e.QueryID, &e.User, &e.EdgeType, &e.DetectedAt); err != nil {
			return nil, fmt.Errorf("scan lineage edge: %w", err)
		}
		results = append(results, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate lineage edge rows: %w", err)
	}
	return results, nil
}

// ── Tags ─────────────────────────────────────────────────────────────────────

// GetTags returns all tags.
func (s *Store) GetTags() ([]TagEntry, error) {
	rows, err := s.conn().Query(
		`SELECT id, object_type, database_name, table_name, column_name, tag, tagged_by, created_at
		 FROM gov_tags ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("get tags: %w", err)
	}
	defer rows.Close()

	return scanTags(rows)
}

// GetTagsForTable returns all tags for a specific table.
func (s *Store) GetTagsForTable(dbName, tableName string) ([]TagEntry, error) {
	rows, err := s.conn().Query(
		`SELECT id, object_type, database_name, table_name, column_name, tag, tagged_by, created_at
		 FROM gov_tags WHERE database_name = ? AND table_name = ? ORDER BY created_at DESC`,
		dbName, tableName,
	)
	if err != nil {
		return nil, fmt.Errorf("get tags for table: %w", err)
	}
	defer rows.Close()

	return scanTags(rows)
}

// GetTagsForColumn returns all tags for a specific column.
func (s *Store) GetTagsForColumn(dbName, tableName, colName string) ([]TagEntry, error) {
	rows, err := s.conn().Query(
		`SELECT id, object_type, database_name, table_name, column_name, tag, tagged_by, created_at
		 FROM gov_tags WHERE database_name = ? AND table_name = ? AND column_name = ? ORDER BY created_at DESC`,
		dbName, tableName, colName,
	)
	if err != nil {
		return nil, fmt.Errorf("get tags for column: %w", err)
	}
	defer rows.Close()

	return scanTags(rows)
}

// CreateTag creates a new tag entry and returns its ID.
func (s *Store) CreateTag(objectType, dbName, tableName, colName string, tag SensitivityTag, taggedBy string) (string, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	id := uuid.NewString()

	_, err := s.conn().Exec(
		`INSERT INTO gov_tags (id, object_type, database_name, table_name, column_name, tag, tagged_by, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id, objectType, dbName, tableName, colName, string(tag), taggedBy, now,
	)
	if err != nil {
		return "", fmt.Errorf("create tag: %w", err)
	}
	return id, nil
}

// DeleteTag deletes a tag by ID.
func (s *Store) DeleteTag(id string) error {
	_, err := s.conn().Exec("DELETE FROM gov_tags WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete tag: %w", err)
	}
	return nil
}

// GetTaggedTableCount returns the count of distinct tables that have at least one tag.
func (s *Store) GetTaggedTableCount() (int, error) {
	var count int
	err := s.conn().QueryRow(
		`SELECT COUNT(DISTINCT database_name || '.' || table_name) FROM gov_tags`,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("get tagged table count: %w", err)
	}
	return count, nil
}

func scanTags(rows *sql.Rows) ([]TagEntry, error) {
	var results []TagEntry
	for rows.Next() {
		var t TagEntry
		if err := rows.Scan(&t.ID, &t.ObjectType, &t.DatabaseName, &t.TableName, &t.ColumnName, &t.Tag, &t.TaggedBy, &t.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan tag: %w", err)
		}
		results = append(results, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tag rows: %w", err)
	}
	return results, nil
}

// ── Policies ─────────────────────────────────────────────────────────────────

// GetPolicies returns all policies.
func (s *Store) GetPolicies() ([]Policy, error) {
	return s.scanPolicies(
		`SELECT id, name, description, object_type, object_database, object_table, object_column, required_role, severity, enforcement_mode, enabled, created_by, created_at, updated_at
		 FROM gov_policies ORDER BY name`,
	)
}

// GetEnabledPolicies returns all enabled policies.
func (s *Store) GetEnabledPolicies() ([]Policy, error) {
	return s.scanPolicies(
		`SELECT id, name, description, object_type, object_database, object_table, object_column, required_role, severity, enforcement_mode, enabled, created_by, created_at, updated_at
		 FROM gov_policies WHERE enabled = 1 ORDER BY name`,
	)
}

func (s *Store) scanPolicies(query string, args ...interface{}) ([]Policy, error) {
	rows, err := s.conn().Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("get policies: %w", err)
	}
	defer rows.Close()

	var results []Policy
	for rows.Next() {
		var p Policy
		var desc, objDB, objTable, objCol, createdBy, enforcementMode sql.NullString
		if err := rows.Scan(&p.ID, &p.Name, &desc, &p.ObjectType, &objDB, &objTable, &objCol, &p.RequiredRole, &p.Severity, &enforcementMode, &p.Enabled, &createdBy, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan policy: %w", err)
		}
		p.Description = nullStringToPtr(desc)
		p.ObjectDatabase = nullStringToPtr(objDB)
		p.ObjectTable = nullStringToPtr(objTable)
		p.ObjectColumn = nullStringToPtr(objCol)
		p.EnforcementMode = normalizePolicyEnforcementMode(enforcementMode.String)
		p.CreatedBy = nullStringToPtr(createdBy)
		results = append(results, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate policy rows: %w", err)
	}
	return results, nil
}

// GetPolicyByID returns a single policy by ID.
func (s *Store) GetPolicyByID(id string) (*Policy, error) {
	row := s.conn().QueryRow(
		`SELECT id, name, description, object_type, object_database, object_table, object_column, required_role, severity, enforcement_mode, enabled, created_by, created_at, updated_at
		 FROM gov_policies WHERE id = ?`, id,
	)

	var p Policy
	var desc, objDB, objTable, objCol, createdBy, enforcementMode sql.NullString
	err := row.Scan(&p.ID, &p.Name, &desc, &p.ObjectType, &objDB, &objTable, &objCol, &p.RequiredRole, &p.Severity, &enforcementMode, &p.Enabled, &createdBy, &p.CreatedAt, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get policy by id: %w", err)
	}
	p.Description = nullStringToPtr(desc)
	p.ObjectDatabase = nullStringToPtr(objDB)
	p.ObjectTable = nullStringToPtr(objTable)
	p.ObjectColumn = nullStringToPtr(objCol)
	p.EnforcementMode = normalizePolicyEnforcementMode(enforcementMode.String)
	p.CreatedBy = nullStringToPtr(createdBy)
	return &p, nil
}

// CreatePolicy creates a new policy and returns its ID.
func (s *Store) CreatePolicy(name, description, objectType, objectDB, objectTable, objectCol, requiredRole, severity, enforcementMode, createdBy string) (string, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	id := uuid.NewString()

	var desc, oDB, oTable, oCol, cBy interface{}
	if description != "" {
		desc = description
	}
	if objectDB != "" {
		oDB = objectDB
	}
	if objectTable != "" {
		oTable = objectTable
	}
	if objectCol != "" {
		oCol = objectCol
	}
	if createdBy != "" {
		cBy = createdBy
	}

	_, err := s.conn().Exec(
		`INSERT INTO gov_policies (id, name, description, object_type, object_database, object_table, object_column, required_role, severity, enforcement_mode, enabled, created_by, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
		id, name, desc, objectType, oDB, oTable, oCol, requiredRole, severity, normalizePolicyEnforcementMode(enforcementMode), cBy, now, now,
	)
	if err != nil {
		return "", fmt.Errorf("create policy: %w", err)
	}
	return id, nil
}

// UpdatePolicy updates an existing policy.
func (s *Store) UpdatePolicy(id, name, description, requiredRole, severity, enforcementMode string, enabled bool) error {
	now := time.Now().UTC().Format(time.RFC3339)

	var desc interface{}
	if description != "" {
		desc = description
	}

	enabledInt := 0
	if enabled {
		enabledInt = 1
	}

	_, err := s.conn().Exec(
		`UPDATE gov_policies SET name = ?, description = ?, required_role = ?, severity = ?, enforcement_mode = ?, enabled = ?, updated_at = ? WHERE id = ?`,
		name, desc, requiredRole, severity, normalizePolicyEnforcementMode(enforcementMode), enabledInt, now, id,
	)
	if err != nil {
		return fmt.Errorf("update policy: %w", err)
	}
	return nil
}

// DeletePolicy deletes a policy by ID (cascades to violations).
func (s *Store) DeletePolicy(id string) error {
	_, err := s.conn().Exec("DELETE FROM gov_policies WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete policy: %w", err)
	}
	return nil
}

// ── Violations ───────────────────────────────────────────────────────────────

// InsertPolicyViolation inserts a policy violation from a PolicyViolation struct.
func (s *Store) InsertPolicyViolation(v PolicyViolation) error {
	_, err := s.conn().Exec(
		`INSERT INTO gov_policy_violations (id, policy_id, query_log_id, username, violation_detail, severity, detection_phase, request_endpoint, detected_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		v.ID, v.PolicyID, nullableValue(v.QueryLogID), v.User, v.ViolationDetail, v.Severity, normalizeDetectionPhase(v.DetectionPhase), nullableValue(deref(v.RequestEndpoint)), v.DetectedAt, v.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert policy violation: %w", err)
	}
	return nil
}

// CreateViolation creates a new policy violation and returns its ID.
func (s *Store) CreateViolation(policyID, queryLogID, user, detail, severity, detectionPhase, requestEndpoint string) (string, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	id := uuid.NewString()

	_, err := s.conn().Exec(
		`INSERT INTO gov_policy_violations (id, policy_id, query_log_id, username, violation_detail, severity, detection_phase, request_endpoint, detected_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, policyID, nullableValue(queryLogID), user, detail, severity, normalizeDetectionPhase(detectionPhase), nullableValue(requestEndpoint), now, now,
	)
	if err != nil {
		return "", fmt.Errorf("create violation: %w", err)
	}
	return id, nil
}

// GetViolations returns violations with optional policyID filter.
func (s *Store) GetViolations(limit int, policyID string) ([]PolicyViolation, error) {
	where := "1=1"
	var args []interface{}

	if policyID != "" {
		where += " AND v.policy_id = ?"
		args = append(args, policyID)
	}

	args = append(args, limit)

	query := fmt.Sprintf(
		`SELECT v.id, v.policy_id, v.query_log_id, v.username, v.violation_detail, v.severity, v.detection_phase, v.request_endpoint, v.detected_at, v.created_at, COALESCE(p.name, '')
		 FROM gov_policy_violations v
		 LEFT JOIN gov_policies p ON p.id = v.policy_id
		 WHERE %s
		 ORDER BY v.detected_at DESC
		 LIMIT ?`, where,
	)

	rows, err := s.conn().Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("get violations: %w", err)
	}
	defer rows.Close()

	var results []PolicyViolation
	for rows.Next() {
		var v PolicyViolation
		var queryLogID, requestEndpoint sql.NullString
		if err := rows.Scan(&v.ID, &v.PolicyID, &queryLogID, &v.User, &v.ViolationDetail, &v.Severity, &v.DetectionPhase, &requestEndpoint, &v.DetectedAt, &v.CreatedAt, &v.PolicyName); err != nil {
			return nil, fmt.Errorf("scan violation: %w", err)
		}
		v.QueryLogID = queryLogID.String
		v.RequestEndpoint = nullStringToPtr(requestEndpoint)
		v.DetectionPhase = normalizeDetectionPhase(v.DetectionPhase)
		results = append(results, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate violation rows: %w", err)
	}
	return results, nil
}

func normalizePolicyEnforcementMode(v string) string {
	mode := strings.ToLower(strings.TrimSpace(v))
	switch mode {
	case "block":
		return "block"
	default:
		return "warn"
	}
}

func normalizeDetectionPhase(v string) string {
	phase := strings.ToLower(strings.TrimSpace(v))
	switch phase {
	case "pre_exec_block":
		return "pre_exec_block"
	default:
		return "post_exec"
	}
}

// ── Overview ─────────────────────────────────────────────────────────────────

// GetOverview returns aggregate counts from all governance tables.
func (s *Store) GetOverview() (*GovernanceOverview, error) {
	o := &GovernanceOverview{}

	s.conn().QueryRow("SELECT COUNT(*) FROM gov_databases WHERE is_deleted = 0").Scan(&o.DatabaseCount)
	s.conn().QueryRow("SELECT COUNT(*) FROM gov_tables WHERE is_deleted = 0").Scan(&o.TableCount)
	s.conn().QueryRow("SELECT COUNT(*) FROM gov_columns WHERE is_deleted = 0").Scan(&o.ColumnCount)

	tagCount, err := s.GetTaggedTableCount()
	if err == nil {
		o.TaggedTableCount = tagCount
	}

	cutoff24h := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	s.conn().QueryRow("SELECT COUNT(*) FROM gov_query_log WHERE event_time > ?", cutoff24h).Scan(&o.QueryCount24h)

	s.conn().QueryRow("SELECT COUNT(*) FROM gov_lineage_edges").Scan(&o.LineageEdgeCount)
	s.conn().QueryRow("SELECT COUNT(*) FROM gov_policies").Scan(&o.PolicyCount)
	s.conn().QueryRow("SELECT COUNT(*) FROM gov_policy_violations").Scan(&o.ViolationCount)
	s.conn().QueryRow("SELECT COUNT(*) FROM gov_incidents WHERE status IN ('open', 'triaged', 'in_progress')").Scan(&o.IncidentCount)
	s.conn().QueryRow("SELECT COUNT(*) FROM gov_schema_changes").Scan(&o.SchemaChangeCount)

	syncStates, err := s.GetSyncStates()
	if err == nil {
		o.SyncStates = syncStates
	}

	recentChanges, err := s.GetSchemaChanges(10)
	if err == nil {
		o.RecentChanges = recentChanges
	}

	recentViolations, err := s.GetViolations(10, "")
	if err == nil {
		o.RecentViolations = recentViolations
	}

	return o, nil
}

// ── Cleanup ──────────────────────────────────────────────────────────────────

// CleanupOldQueryLogs deletes query logs older than the given timestamp.
func (s *Store) CleanupOldQueryLogs(before string) (int64, error) {
	result, err := s.conn().Exec(
		"DELETE FROM gov_query_log WHERE event_time < ?",
		before,
	)
	if err != nil {
		return 0, fmt.Errorf("cleanup old query logs: %w", err)
	}
	return result.RowsAffected()
}

// CleanupOldViolations deletes violations older than the given timestamp.
func (s *Store) CleanupOldViolations(before string) (int64, error) {
	result, err := s.conn().Exec(
		"DELETE FROM gov_policy_violations WHERE detected_at < ?",
		before,
	)
	if err != nil {
		return 0, fmt.Errorf("cleanup old violations: %w", err)
	}
	return result.RowsAffected()
}
