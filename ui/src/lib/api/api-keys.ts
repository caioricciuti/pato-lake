import { apiGet, apiPost, apiPut, apiDel } from './client'

export interface APIKey {
  id: string
  user_id: string
  name: string
  key_prefix: string
  role: string
  scopes: string
  rate_limit_rpm: number
  is_active: boolean
  last_used_at: string | null
  expires_at: string | null
  created_at: string
  updated_at: string
}

export interface APIKeyCreateResult {
  key: string
  api_key: APIKey
}

export interface APIKeyUsage {
  id: string
  api_key_id: string
  date: string
  request_count: number
  query_count: number
  ingest_events: number
  ingest_bytes: number
  created_at: string
}

export interface CreateAPIKeyParams {
  name: string
  role?: string
  scopes?: string
  rate_limit_rpm?: number
  expires_at?: string
}

export interface UpdateAPIKeyParams {
  name: string
  is_active?: boolean
  rate_limit_rpm?: number
  scopes?: string
}

export function listAPIKeys(): Promise<APIKey[]> {
  return apiGet<APIKey[]>('/api/keys')
}

export function createAPIKey(params: CreateAPIKeyParams): Promise<APIKeyCreateResult> {
  return apiPost<APIKeyCreateResult>('/api/keys', params)
}

export function updateAPIKey(id: string, params: UpdateAPIKeyParams): Promise<void> {
  return apiPut(`/api/keys/${id}`, params)
}

export function deleteAPIKey(id: string): Promise<void> {
  return apiDel(`/api/keys/${id}`)
}

export function getAPIKeyUsage(id: string, days = 30): Promise<APIKeyUsage[]> {
  return apiGet<APIKeyUsage[]>(`/api/keys/${id}/usage?days=${days}`)
}

export function listAllAPIKeys(): Promise<APIKey[]> {
  return apiGet<APIKey[]>('/api/admin/keys')
}
