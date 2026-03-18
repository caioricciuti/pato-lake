// ── Sync ────────────────────────────────────────────────────────

export interface SyncState {
  id: string
  sync_type: 'metadata' | 'query_log'
  last_synced_at: string | null
  watermark: string | null
  status: 'idle' | 'running' | 'error'
  last_error: string | null
  row_count: number
  created_at: string
  updated_at: string
}

export interface SyncResult {
  metadata?: { databases_synced: number; tables_synced: number; columns_synced: number; schema_changes: number }
  metadata_error?: string
  query_log?: { queries_ingested: number; lineage_edges_found: number; violations_found: number; new_watermark: string }
  query_log_error?: string
}

// ── Overview ────────────────────────────────────────────────────

export interface GovernanceOverview {
  database_count: number
  table_count: number
  column_count: number
  tagged_table_count: number
  query_count_24h: number
  lineage_edge_count: number
  policy_count: number
  violation_count: number
  incident_count: number
  schema_change_count: number
  sync_states: SyncState[]
  recent_changes: SchemaChange[]
  recent_violations: PolicyViolation[]
}

// ── Metadata ────────────────────────────────────────────────────

export interface GovDatabase {
  id: string
  name: string
  engine: string
  first_seen: string
  last_updated: string
  is_deleted: boolean
}

export interface GovTable {
  id: string
  database_name: string
  table_name: string
  engine: string
  total_rows: number
  total_bytes: number
  first_seen: string
  last_updated: string
  is_deleted: boolean
  tags?: string[]
}

export interface GovColumn {
  id: string
  database_name: string
  table_name: string
  column_name: string
  column_type: string
  column_position: number
  default_kind: string | null
  default_expression: string | null
  comment: string | null
  first_seen: string
  last_updated: string
  is_deleted: boolean
  tags?: string[]
}

export interface SchemaChange {
  id: string
  change_type: string
  database_name: string
  table_name: string
  column_name: string
  old_value: string
  new_value: string
  detected_at: string
  created_at: string
}

// ── Query Log ───────────────────────────────────────────────────

export interface QueryLogEntry {
  id: string
  query_id: string
  username: string
  query_text: string
  query_kind: string
  event_time: string
  duration_ms: number
  result_rows: number
  tables_used: string
  is_error: boolean
  error_message: string | null
}

export interface TopQuery {
  normalized_hash: string
  count: number
  avg_duration_ms: number
  total_read_rows: number
  sample_query: string
  last_seen: string
}

// ── Lineage ─────────────────────────────────────────────────────

export interface LineageEdge {
  id: string
  source_database: string
  source_table: string
  target_database: string
  target_table: string
  edge_type: string
  username: string
  detected_at: string
}

export interface LineageNode {
  id: string
  database: string
  table: string
  type: 'source' | 'target' | 'current'
}

export interface LineageGraph {
  nodes: LineageNode[]
  edges: LineageEdge[]
}

// ── Tags ────────────────────────────────────────────────────────

export interface TagEntry {
  id: string
  object_type: 'table' | 'column'
  database_name: string
  table_name: string
  column_name: string
  tag: string
  tagged_by: string
  created_at: string
}

// ── Policies ────────────────────────────────────────────────────

export interface Policy {
  id: string
  name: string
  description: string | null
  object_type: 'database' | 'table' | 'column'
  object_database: string | null
  object_table: string | null
  object_column: string | null
  required_role: string
  severity: string
  enforcement_mode: 'warn' | 'block'
  enabled: boolean
  created_by: string | null
  created_at: string
  updated_at: string
}

export interface PolicyViolation {
  id: string
  policy_id: string
  query_log_id: string
  username: string
  violation_detail: string
  severity: string
  detection_phase?: 'post_exec' | 'pre_exec_block' | string
  request_endpoint?: string | null
  detected_at: string
  created_at: string
  policy_name?: string
}

export interface GovernanceObjectComment {
  id: string
  object_type: 'table' | 'column' | string
  database_name: string
  table_name: string
  column_name: string
  comment_text: string
  created_by?: string | null
  created_at: string
  updated_at: string
}

export interface GovernanceIncident {
  id: string
  source_type: 'manual' | 'violation' | 'over_permission' | string
  source_ref?: string | null
  dedupe_key?: string | null
  title: string
  severity: 'info' | 'warn' | 'error' | 'critical' | string
  status: 'open' | 'triaged' | 'in_progress' | 'resolved' | 'dismissed' | string
  assignee?: string | null
  details?: string | null
  resolution_note?: string | null
  occurrence_count: number
  first_seen_at: string
  last_seen_at: string
  resolved_at?: string | null
  created_by?: string | null
  created_at: string
  updated_at: string
}

export interface GovernanceIncidentComment {
  id: string
  incident_id: string
  comment_text: string
  created_by?: string | null
  created_at: string
}
