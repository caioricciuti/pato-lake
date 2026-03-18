import { apiGet, apiPost } from './client'
import type {
  LegacyQueryResult,
  ExplorerDataResponse,
  QueryPlanResult,
} from '../types/query'
import type { Column } from '../types/schema'

interface RunQueryParams {
  query: string
  timeout?: number
}

function escapeLiteral(value: string): string {
  // Reject null bytes which can truncate strings in some SQL engines
  if (value.includes('\0')) throw new Error('Invalid character in identifier')
  return value.replace(/\\/g, '\\\\').replace(/'/g, "\\'")
}

function escapeIdentifier(value: string): string {
  if (value.includes('\0')) throw new Error('Invalid character in identifier')
  return '"' + value.replace(/"/g, '""') + '"'
}

/** Execute a query (legacy JSON format) */
export function runQuery(params: RunQueryParams): Promise<LegacyQueryResult> {
  return apiPost<LegacyQueryResult>('/api/query/run', params)
}

/** Format a SQL query */
export async function formatSQL(query: string): Promise<string> {
  const res = await apiPost<{ formatted: string }>('/api/query/format', { query })
  return res.formatted
}

/** Get EXPLAIN output for a query */
export function explainQuery(query: string): Promise<LegacyQueryResult> {
  return apiPost<LegacyQueryResult>('/api/query/explain', { query })
}

/** Get parsed query plan (tree + raw lines) */
export function fetchQueryPlan(query: string): Promise<QueryPlanResult> {
  return apiPost<QueryPlanResult>('/api/query/plan', { query })
}

/** Fetch paginated explorer data */
export function fetchExplorerData(params: {
  database: string
  table: string
  page?: number
  page_size?: number
  sort_column?: string
  sort_dir?: string
}): Promise<ExplorerDataResponse> {
  return apiPost<ExplorerDataResponse>('/api/query/explorer-data', {
    database: params.database,
    table: params.table,
    page: params.page ?? 0,
    page_size: params.page_size ?? 100,
    sort_column: params.sort_column ?? '',
    sort_dir: params.sort_dir ?? 'asc',
  })
}

/** List databases */
export async function listDatabases(): Promise<string[]> {
  const res = await apiGet<{ databases: Array<{ name: string }> }>('/api/query/databases')
  return (res.databases ?? []).map(d => d.name)
}

/** Fetch autocomplete data (functions + keywords) */
export async function fetchCompletions(): Promise<{ functions: string[]; keywords: string[] }> {
  const res = await apiGet<{ functions: string[]; keywords: string[] }>('/api/query/completions')
  return { functions: res.functions ?? [], keywords: res.keywords ?? [] }
}

/** List tables in a database */
export async function listTables(database: string): Promise<string[]> {
  const res = await apiGet<{ tables: Array<{ name: string; type: string }> }>(`/api/query/tables?database=${encodeURIComponent(database)}`)
  return (res.tables ?? []).map(t => t.name)
}

/** List columns for a table */
export async function listColumns(database: string, table: string): Promise<Column[]> {
  const res = await apiGet<{ columns: Column[] }>(
    `/api/query/columns?database=${encodeURIComponent(database)}&table=${encodeURIComponent(table)}`,
  )
  return res.columns ?? []
}

/** Fetch table metadata from duckdb_tables() */
export async function fetchTableInfo(database: string, table: string): Promise<Record<string, any>> {
  const db = escapeLiteral(database)
  const tbl = escapeLiteral(table)
  const query = `SELECT database_name AS database, table_name AS name, estimated_size, column_count, sql AS create_table_query FROM duckdb_tables() WHERE database_name = '${db}' AND table_name = '${tbl}'`
  const res = await runQuery({ query })
  if (res.data?.length > 0) {
    const row = res.data[0]
    if (Array.isArray(row)) {
      const obj: Record<string, any> = {}
      res.meta.forEach((col: any, i: number) => { obj[col.name] = row[i] })
      return obj
    }
    return row as Record<string, any>
  }
  return {}
}

/** Fetch table schema via DESCRIBE */
export async function fetchTableSchema(database: string, table: string): Promise<LegacyQueryResult> {
  return runQuery({ query: `DESCRIBE ${escapeIdentifier(database)}.${escapeIdentifier(table)}` })
}

/** Fetch database metadata and aggregate stats */
export async function fetchDatabaseInfo(database: string): Promise<Record<string, any>> {
  const db = escapeLiteral(database)
  const query = `SELECT d.database_name AS name, d.path AS data_path, count(t.table_name) AS table_count FROM duckdb_databases() d LEFT JOIN duckdb_tables() t ON t.database_name = d.database_name WHERE d.database_name = '${db}' GROUP BY d.database_name, d.path`
  const res = await runQuery({ query })
  if (res.data?.length > 0) {
    const row = res.data[0]
    if (Array.isArray(row)) {
      const obj: Record<string, any> = {}
      res.meta.forEach((col: any, i: number) => { obj[col.name] = row[i] })
      return obj
    }
    return row as Record<string, any>
  }
  return {}
}

/** Fetch tables list and stats for a database */
export async function fetchDatabaseTables(database: string): Promise<LegacyQueryResult> {
  const db = escapeLiteral(database)
  const query = `SELECT table_name AS name, estimated_size, column_count FROM duckdb_tables() WHERE database_name = '${db}' ORDER BY table_name`
  return runQuery({ query })
}

/** Query history entry returned by the backend */
export interface QueryHistoryEntry {
  id: string
  query_id: string
  username: string
  query_text: string
  query_kind: string | null
  event_time: string
  duration_ms: number
  result_rows: number
  tables_used: string | null
  is_error: boolean
  error_message: string | null
  created_at: string
}

/** Fetch the current user's query history */
export async function fetchQueryHistory(params?: {
  limit?: number
  offset?: number
  search?: string
}): Promise<{ history: QueryHistoryEntry[]; total: number }> {
  const searchParams = new URLSearchParams()
  if (params?.limit) searchParams.set('limit', String(params.limit))
  if (params?.offset) searchParams.set('offset', String(params.offset))
  if (params?.search) searchParams.set('search', params.search)
  const qs = searchParams.toString()
  const url = `/api/query/history${qs ? `?${qs}` : ''}`
  const res = await apiGet<{ history: QueryHistoryEntry[]; total: number }>(url)
  return { history: res.history ?? [], total: res.total ?? 0 }
}
