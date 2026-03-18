import { apiGet, apiPost, apiPut, apiDel } from './client'

export interface IngestSource {
  id: string
  name: string
  event_type: string
  target_schema: string
  target_table: string
  buffer_size: number
  flush_interval_ms: number
  is_active: boolean
  created_by: string
  created_at: string
  updated_at: string
}

export interface IngestStats {
  id: string
  source_id: string
  date: string
  events_received: number
  events_written: number
  bytes_received: number
  errors_count: number
  last_error: string | null
  created_at: string
}

export interface CreateIngestSourceParams {
  name: string
  event_type: string
  target_schema?: string
  target_table?: string
  buffer_size?: number
  flush_interval_ms?: number
}

export interface UpdateIngestSourceParams {
  name: string
  is_active?: boolean
  buffer_size?: number
  flush_interval_ms?: number
}

export function listIngestSources(): Promise<IngestSource[]> {
  return apiGet<IngestSource[]>('/api/ingest/sources')
}

export function createIngestSource(params: CreateIngestSourceParams): Promise<IngestSource> {
  return apiPost<IngestSource>('/api/ingest/sources', params)
}

export function updateIngestSource(id: string, params: UpdateIngestSourceParams): Promise<void> {
  return apiPut(`/api/ingest/sources/${id}`, params)
}

export function deleteIngestSource(id: string): Promise<void> {
  return apiDel(`/api/ingest/sources/${id}`)
}

export function getIngestStats(): Promise<IngestStats[]> {
  return apiGet<IngestStats[]>('/api/ingest/stats/overview')
}
