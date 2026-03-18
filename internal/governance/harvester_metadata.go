package governance

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// syncMetadata harvests database/table/column metadata from DuckDB information_schema,
// diffs against existing SQLite state, and records schema changes.
func (s *Syncer) syncMetadata(ctx context.Context) (*MetadataSyncResult, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	// Update sync state to running
	if err := s.store.UpsertSyncState(string(SyncMetadata), "running", nil, nil, 0); err != nil {
		slog.Error("Failed to update sync state", "error", err)
	}

	result := &MetadataSyncResult{}
	var syncErr error

	defer func() {
		status := "idle"
		var errMsg *string
		if syncErr != nil {
			status = "error"
			e := syncErr.Error()
			errMsg = &e
		}
		rowCount := result.DatabasesSynced + result.TablesSynced + result.ColumnsSynced
		if err := s.store.UpsertSyncState(string(SyncMetadata), status, &now, errMsg, rowCount); err != nil {
			slog.Error("Failed to update sync state after metadata sync", "error", err)
		}
	}()

	// -- Phase 1: Databases ------------------------------------------------
	dbRows, err := s.executeQuery(ctx,
		`SELECT DISTINCT catalog_name AS name
		 FROM information_schema.schemata
		 WHERE catalog_name NOT IN ('system', 'temp')
		 ORDER BY catalog_name`)
	if err != nil {
		slog.Warn("Metadata sync: failed to query databases", "error", err)
		syncErr = fmt.Errorf("databases query failed: %w", err)
		return result, syncErr
	}

	existingDBs, err := s.store.GetDatabases()
	if err != nil {
		syncErr = fmt.Errorf("failed to load existing databases: %w", err)
		return result, syncErr
	}
	existingDBMap := make(map[string]*GovDatabase, len(existingDBs))
	for i := range existingDBs {
		existingDBMap[existingDBs[i].Name] = &existingDBs[i]
	}

	seenDBs := make(map[string]bool)
	for _, row := range dbRows {
		name := fmt.Sprintf("%v", row["name"])
		seenDBs[name] = true

		_, found := existingDBMap[name]
		if err := s.store.UpsertDatabase(GovDatabase{
			ID:          uuid.NewString(),
			Name:        name,
			Engine:      "DuckDB",
			FirstSeen:   now,
			LastUpdated: now,
		}); err != nil {
			slog.Error("Failed to upsert database", "name", name, "error", err)
			continue
		}

		if !found {
			s.store.CreateSchemaChange(ChangeDatabaseAdded, name, "", "", "", name)
			result.SchemaChanges++
		}
		result.DatabasesSynced++
	}

	// Mark removed databases
	for name, existing := range existingDBMap {
		if !seenDBs[name] && !existing.IsDeleted {
			if err := s.store.MarkDatabaseDeleted(name); err != nil {
				slog.Error("Failed to mark database deleted", "name", name, "error", err)
			}
			s.store.CreateSchemaChange(ChangeDatabaseRemoved, name, "", "", name, "")
			result.SchemaChanges++
		}
	}

	// -- Phase 2: Tables ---------------------------------------------------
	tableRows, err := s.executeQuery(ctx,
		`SELECT
			table_catalog AS database_name,
			table_name,
			table_type AS engine
		 FROM information_schema.tables
		 WHERE table_schema = 'main'
		   AND table_catalog NOT IN ('system', 'temp')
		 ORDER BY table_catalog, table_name`)
	if err != nil {
		slog.Warn("Metadata sync: failed to query tables", "error", err)
		// Continue -- tables query failure is non-fatal
	} else {
		existingTables, err := s.store.GetTables()
		if err != nil {
			slog.Error("Failed to load existing tables", "error", err)
		}
		existingTableMap := make(map[string]*GovTable)
		for i := range existingTables {
			key := existingTables[i].DatabaseName + "." + existingTables[i].TableName
			existingTableMap[key] = &existingTables[i]
		}

		seenTables := make(map[string]bool)
		for _, row := range tableRows {
			dbName := fmt.Sprintf("%v", row["database_name"])
			tableName := fmt.Sprintf("%v", row["table_name"])
			engine := fmt.Sprintf("%v", row["engine"])
			key := dbName + "." + tableName
			seenTables[key] = true

			_, found := existingTableMap[key]
			if err := s.store.UpsertTable(GovTable{
				ID:           uuid.NewString(),
				DatabaseName: dbName,
				TableName:    tableName,
				Engine:       engine,
				FirstSeen:    now,
				LastUpdated:  now,
			}); err != nil {
				slog.Error("Failed to upsert table", "table", key, "error", err)
				continue
			}

			if !found {
				s.store.CreateSchemaChange(ChangeTableAdded, dbName, tableName, "", "", tableName)
				result.SchemaChanges++
			}
			result.TablesSynced++
		}

		// Mark removed tables
		for key, existing := range existingTableMap {
			if !seenTables[key] && !existing.IsDeleted {
				if err := s.store.MarkTableDeleted(existing.DatabaseName, existing.TableName); err != nil {
					slog.Error("Failed to mark table deleted", "table", key, "error", err)
				}
				s.store.CreateSchemaChange(ChangeTableRemoved, existing.DatabaseName, existing.TableName, "", existing.TableName, "")
				result.SchemaChanges++
			}
		}
	}

	// -- Phase 3: Columns --------------------------------------------------
	colRows, err := s.executeQuery(ctx,
		`SELECT
			table_catalog AS database_name,
			table_name,
			column_name,
			data_type AS column_type,
			ordinal_position AS column_position,
			column_default AS default_expression,
			is_nullable
		 FROM information_schema.columns
		 WHERE table_schema = 'main'
		   AND table_catalog NOT IN ('system', 'temp')
		 ORDER BY table_catalog, table_name, ordinal_position`)
	if err != nil {
		slog.Warn("Metadata sync: failed to query columns", "error", err)
	} else {
		existingColMap := make(map[string]*GovColumn)
		tables, tblErr := s.store.GetTables()
		if tblErr == nil {
			for _, tbl := range tables {
				cols, colErr := s.store.GetColumns(tbl.DatabaseName, tbl.TableName)
				if colErr != nil {
					continue
				}
				for i := range cols {
					key := cols[i].DatabaseName + "." + cols[i].TableName + "." + cols[i].ColumnName
					existingColMap[key] = &cols[i]
				}
			}
		}

		seenCols := make(map[string]bool)
		for _, row := range colRows {
			dbName := fmt.Sprintf("%v", row["database_name"])
			tableName := fmt.Sprintf("%v", row["table_name"])
			colName := fmt.Sprintf("%v", row["column_name"])
			colType := fmt.Sprintf("%v", row["column_type"])
			position := int(toInt64(row["column_position"]))
			key := dbName + "." + tableName + "." + colName
			seenCols[key] = true

			defaultExpr := toStringPtr(row["default_expression"])

			existing, found := existingColMap[key]
			if err := s.store.UpsertColumn(GovColumn{
				ID:                uuid.NewString(),
				DatabaseName:      dbName,
				TableName:         tableName,
				ColumnName:        colName,
				ColumnType:        colType,
				ColumnPosition:    position,
				DefaultExpression: defaultExpr,
				FirstSeen:         now,
				LastUpdated:       now,
			}); err != nil {
				slog.Error("Failed to upsert column", "column", key, "error", err)
				continue
			}

			if !found {
				s.store.CreateSchemaChange(ChangeColumnAdded, dbName, tableName, colName, "", colName)
				result.SchemaChanges++
			} else if existing.ColumnType != colType {
				s.store.CreateSchemaChange(ChangeColumnTypeChanged, dbName, tableName, colName, existing.ColumnType, colType)
				result.SchemaChanges++
			}
			result.ColumnsSynced++
		}

		// Mark removed columns
		for key, existing := range existingColMap {
			if !seenCols[key] && !existing.IsDeleted {
				if err := s.store.MarkColumnDeleted(existing.DatabaseName, existing.TableName, existing.ColumnName); err != nil {
					slog.Error("Failed to mark column deleted", "column", key, "error", err)
				}
				s.store.CreateSchemaChange(ChangeColumnRemoved, existing.DatabaseName, existing.TableName, existing.ColumnName, existing.ColumnName, "")
				result.SchemaChanges++
			}
		}
	}

	slog.Info("Metadata sync completed",
		"databases", result.DatabasesSynced,
		"tables", result.TablesSynced,
		"columns", result.ColumnsSynced,
		"changes", result.SchemaChanges,
	)

	return result, nil
}

// toInt64 converts interface{} values (float64, int64, string, etc.) to int64.
func toInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case int32:
		return int64(val)
	case int:
		return int64(val)
	case string:
		n, _ := strconv.ParseInt(val, 10, 64)
		return n
	default:
		s := fmt.Sprintf("%v", v)
		n, _ := strconv.ParseInt(s, 10, 64)
		return n
	}
}

// toStringPtr converts interface{} to *string. Returns nil for nil or empty strings.
func toStringPtr(v interface{}) *string {
	if v == nil {
		return nil
	}
	s := fmt.Sprintf("%v", v)
	if s == "" || s == "<nil>" {
		return nil
	}
	return &s
}
