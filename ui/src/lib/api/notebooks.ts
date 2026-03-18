import { apiGet, apiPost, apiPut, apiDel } from './client'

export interface Notebook {
  id: string
  title: string
  description: string
  created_by: string
  is_public: boolean
  share_token: string | null
  created_at: string
  updated_at: string
}

export interface NotebookCell {
  id: string
  notebook_id: string
  cell_type: 'sql' | 'markdown'
  content: string
  position: number
  created_at: string
  updated_at: string
}

export interface NotebookWithCells {
  notebook: Notebook
  cells: NotebookCell[]
}

export interface CellRunResult {
  meta: { name: string; type: string }[]
  data: unknown[][]
  row_count: number
  truncated: boolean
  error?: string
}

export interface CreateNotebookParams {
  title?: string
  description?: string
}

export function listNotebooks(): Promise<Notebook[]> {
  return apiGet<Notebook[]>('/api/notebooks')
}

export function createNotebook(params: CreateNotebookParams = {}): Promise<Notebook> {
  return apiPost<Notebook>('/api/notebooks', params)
}

export function getNotebook(id: string): Promise<NotebookWithCells> {
  return apiGet<NotebookWithCells>(`/api/notebooks/${id}`)
}

export function updateNotebook(id: string, title: string, description: string): Promise<void> {
  return apiPut(`/api/notebooks/${id}`, { title, description })
}

export function deleteNotebook(id: string): Promise<void> {
  return apiDel(`/api/notebooks/${id}`)
}

export function saveCells(notebookId: string, cells: Partial<NotebookCell>[]): Promise<void> {
  return apiPut(`/api/notebooks/${notebookId}/cells`, { cells })
}

export function runCell(
  notebookId: string,
  cellId: string,
  sql: string,
  precedingCells: { position: number; sql: string }[] = [],
): Promise<CellRunResult> {
  return apiPost<CellRunResult>(`/api/notebooks/${notebookId}/cells/${cellId}/run`, {
    sql,
    preceding_cells: precedingCells,
  })
}

export function shareNotebook(id: string): Promise<{ share_token: string }> {
  return apiPost<{ share_token: string }>(`/api/notebooks/${id}/share`)
}

export function revokeNotebookShare(id: string): Promise<void> {
  return apiDel(`/api/notebooks/${id}/share`)
}

export function getSharedNotebook(token: string): Promise<NotebookWithCells> {
  return apiGet<NotebookWithCells>(`/api/notebooks/shared/${token}`)
}
