<script lang="ts">
  import { onMount } from 'svelte'
  import {
    listIngestSources,
    createIngestSource,
    updateIngestSource,
    deleteIngestSource,
    getIngestStats,
    type IngestSource,
    type IngestStats,
    type CreateIngestSourceParams,
  } from '../lib/api/ingest'
  import { listAPIKeys, type APIKey } from '../lib/api/api-keys'
  import { success as toastSuccess, error as toastError } from '../lib/stores/toast.svelte'
  import Spinner from '../lib/components/common/Spinner.svelte'
  import Skeleton from '../lib/components/common/Skeleton.svelte'
  import Sheet from '../lib/components/common/Sheet.svelte'
  import ConfirmDialog from '../lib/components/common/ConfirmDialog.svelte'
  import { ArrowDownToLine, Plus, Trash2, Copy, Check } from 'lucide-svelte'

  let sources = $state<IngestSource[]>([])
  let stats = $state<IngestStats[]>([])
  let apiKeys = $state<APIKey[]>([])
  let loading = $state(true)
  let createSheetOpen = $state(false)
  let snippetSourceId = $state<string | null>(null)
  let deletingSource = $state<IngestSource | null>(null)
  let copiedSnippet = $state('')

  let createForm = $state<CreateIngestSourceParams>({
    name: '',
    event_type: '',
    target_schema: 'main',
    target_table: '',
    buffer_size: 1000,
    flush_interval_ms: 5000,
  })

  onMount(() => {
    loadData()
  })

  async function loadData() {
    loading = true
    try {
      const [s, st, k] = await Promise.all([
        listIngestSources(),
        getIngestStats(),
        listAPIKeys().catch(() => [] as APIKey[]),
      ])
      sources = s
      stats = st
      apiKeys = k
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to load data')
    } finally {
      loading = false
    }
  }

  async function handleCreate() {
    const name = createForm.name.trim()
    const eventType = createForm.event_type.trim()
    if (!name || !eventType) {
      toastError('Name and event type are required')
      return
    }
    try {
      await createIngestSource(createForm)
      toastSuccess('Ingest source created')
      createForm = { name: '', event_type: '', target_schema: 'main', target_table: '', buffer_size: 1000, flush_interval_ms: 5000 }
      createSheetOpen = false
      await loadData()
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to create source')
    }
  }

  async function toggleActive(source: IngestSource) {
    try {
      await updateIngestSource(source.id, {
        name: source.name,
        is_active: !source.is_active,
        buffer_size: source.buffer_size,
        flush_interval_ms: source.flush_interval_ms,
      })
      toastSuccess(source.is_active ? 'Source disabled' : 'Source enabled')
      await loadData()
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to update source')
    }
  }

  async function confirmDelete() {
    if (!deletingSource) return
    const s = deletingSource
    deletingSource = null
    try {
      await deleteIngestSource(s.id)
      toastSuccess('Source deleted')
      await loadData()
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to delete source')
    }
  }

  function getStatsForSource(sourceId: string): { received: number; written: number; errors: number } {
    const sourceStats = stats.filter(s => s.source_id === sourceId)
    return {
      received: sourceStats.reduce((sum, s) => sum + s.events_received, 0),
      written: sourceStats.reduce((sum, s) => sum + s.events_written, 0),
      errors: sourceStats.reduce((sum, s) => sum + s.errors_count, 0),
    }
  }

  function totalStats(): { received: number; written: number; errors: number } {
    return {
      received: stats.reduce((sum, s) => sum + s.events_received, 0),
      written: stats.reduce((sum, s) => sum + s.events_written, 0),
      errors: stats.reduce((sum, s) => sum + s.errors_count, 0),
    }
  }

  function snippetSource(): IngestSource | undefined {
    return sources.find(s => s.id === snippetSourceId)
  }

  function firstKey(): string {
    const active = apiKeys.find(k => k.is_active)
    return active ? active.key_prefix + '...' : 'ptlk_YOUR_API_KEY'
  }

  function curlSnippet(source: IngestSource): string {
    return `curl -X POST \\
  ${window.location.origin}/api/ingest/${source.event_type} \\
  -H "Authorization: Bearer ${firstKey()}" \\
  -H "Content-Type: application/json" \\
  -d '{"user_id": "123", "action": "click", "page": "/home"}'`
  }

  function jsSnippet(source: IngestSource): string {
    return `await fetch("${window.location.origin}/api/ingest/${source.event_type}", {
  method: "POST",
  headers: {
    "Authorization": "Bearer ${firstKey()}",
    "Content-Type": "application/json",
  },
  body: JSON.stringify({
    user_id: "123",
    action: "click",
    page: "/home",
  }),
})`
  }

  function pySnippet(source: IngestSource): string {
    return `import requests

requests.post(
    "${window.location.origin}/api/ingest/${source.event_type}",
    headers={"Authorization": "Bearer ${firstKey()}"},
    json={"user_id": "123", "action": "click", "page": "/home"},
)`
  }

  async function copyToClipboard(text: string, label: string) {
    try {
      await navigator.clipboard.writeText(text)
      copiedSnippet = label
      setTimeout(() => { copiedSnippet = '' }, 2000)
    } catch {
      toastError('Failed to copy')
    }
  }
</script>

<div class="flex flex-col h-full">
  <div class="border-b border-gray-200 dark:border-gray-800">
    <div class="flex items-center justify-between px-4 py-3">
      <div class="flex items-center gap-3">
        <ArrowDownToLine size={18} class="text-ch-blue" />
        <h1 class="ds-page-title">Event Ingestion</h1>
      </div>
      <button class="ds-btn-primary" onclick={() => createSheetOpen = true}>
        <Plus size={14} />
        New Source
      </button>
    </div>
  </div>

  <Sheet
    open={createSheetOpen}
    title="Create Ingest Source"
    size="md"
    onclose={() => createSheetOpen = false}
  >
    <form
      class="space-y-4"
      onsubmit={(e) => { e.preventDefault(); void handleCreate() }}
    >
      <label class="block space-y-1">
        <span class="text-xs text-gray-500">Source Name</span>
        <input class="ds-input-sm" placeholder="Pageviews" bind:value={createForm.name} required />
      </label>
      <label class="block space-y-1">
        <span class="text-xs text-gray-500">Event Type (URL slug)</span>
        <input class="ds-input-sm" placeholder="pageview" bind:value={createForm.event_type} required />
        <span class="text-[11px] text-gray-400">Used in URL: /api/ingest/{createForm.event_type || 'event_type'}</span>
      </label>
      <div class="grid grid-cols-2 gap-3">
        <label class="block space-y-1">
          <span class="text-xs text-gray-500">Target Schema</span>
          <input class="ds-input-sm" bind:value={createForm.target_schema} />
        </label>
        <label class="block space-y-1">
          <span class="text-xs text-gray-500">Target Table</span>
          <input class="ds-input-sm" placeholder="events_{createForm.event_type || 'type'}" bind:value={createForm.target_table} />
        </label>
      </div>
      <div class="grid grid-cols-2 gap-3">
        <label class="block space-y-1">
          <span class="text-xs text-gray-500">Buffer Size</span>
          <input class="ds-input-sm" type="number" bind:value={createForm.buffer_size} />
        </label>
        <label class="block space-y-1">
          <span class="text-xs text-gray-500">Flush Interval (ms)</span>
          <input class="ds-input-sm" type="number" bind:value={createForm.flush_interval_ms} />
        </label>
      </div>
      <div class="flex items-center justify-end gap-2 pt-2 border-t border-gray-200 dark:border-gray-800">
        <button type="button" class="ds-btn-outline" onclick={() => createSheetOpen = false}>Cancel</button>
        <button type="submit" class="ds-btn-primary" disabled={!createForm.name.trim() || !createForm.event_type.trim()}>Create Source</button>
      </div>
    </form>
  </Sheet>

  <ConfirmDialog
    open={deletingSource !== null}
    title="Delete ingest source?"
    description={deletingSource ? `Delete "${deletingSource.name}"? This will not delete the DuckDB table.` : ''}
    confirmLabel="Delete Source"
    destructive
    onconfirm={confirmDelete}
    oncancel={() => deletingSource = null}
  />

  <div class="flex-1 overflow-auto p-4">
    {#if loading}
      <div class="grid grid-cols-3 gap-4 mb-6">
        {#each { length: 3 } as _}
          <div class="ds-stat-card space-y-2">
            <Skeleton width="120px" height="0.75rem" rounded="rounded" />
            <Skeleton width="80px" height="1.75rem" rounded="rounded" />
          </div>
        {/each}
      </div>
      <div class="ds-table-wrap">
        <div class="space-y-3 p-4">
          {#each { length: 3 } as _}
            <div class="flex items-center gap-4">
              <Skeleton width="120px" height="1rem" rounded="rounded" />
              <Skeleton width="100px" height="1rem" rounded="rounded" />
              <Skeleton width="150px" height="1rem" rounded="rounded" />
              <Skeleton width="60px" height="1rem" rounded="rounded-full" />
              <Skeleton width="80px" height="1rem" rounded="rounded" />
            </div>
          {/each}
        </div>
      </div>
    {:else}
      <!-- Stats overview -->
      <div class="grid grid-cols-3 gap-4 mb-6">
        <div class="ds-stat-card">
          <div class="text-xs text-gray-500 mb-1">Total Received (30d)</div>
          <div class="text-2xl font-bold text-gray-900 dark:text-gray-100">{totalStats().received.toLocaleString()}</div>
        </div>
        <div class="ds-stat-card">
          <div class="text-xs text-gray-500 mb-1">Total Written (30d)</div>
          <div class="text-2xl font-bold text-gray-900 dark:text-gray-100">{totalStats().written.toLocaleString()}</div>
        </div>
        <div class="ds-stat-card">
          <div class="text-xs text-gray-500 mb-1">Errors (30d)</div>
          <div class="text-2xl font-bold {totalStats().errors > 0 ? 'text-red-500' : 'text-gray-900 dark:text-gray-100'}">{totalStats().errors.toLocaleString()}</div>
        </div>
      </div>

      {#if sources.length === 0}
        <div class="ds-empty">
          <p class="text-sm text-gray-500 mb-2">No ingest sources yet. Create one to start collecting events.</p>
          <button class="ds-btn-primary" onclick={() => createSheetOpen = true}>Create First Source</button>
        </div>
      {:else}
        <div class="ds-table-wrap">
          <table class="ds-table">
            <thead>
              <tr class="ds-table-head-row">
                <th class="ds-table-th">Name</th>
                <th class="ds-table-th">Event Type</th>
                <th class="ds-table-th">Target Table</th>
                <th class="ds-table-th">Status</th>
                <th class="ds-table-th">Events (30d)</th>
                <th class="ds-table-th-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {#each sources as source}
                {@const sourceStats = getStatsForSource(source.id)}
                <tr class="ds-table-row">
                  <td class="ds-td-strong">{source.name}</td>
                  <td class="ds-td-mono">{source.event_type}</td>
                  <td class="ds-td-mono">{source.target_schema}.{source.target_table}</td>
                  <td class="ds-td">
                    <button
                      class="ds-badge {source.is_active ? 'ds-badge-success' : 'ds-badge-neutral'}"
                      onclick={() => toggleActive(source)}
                    >
                      {source.is_active ? 'Active' : 'Disabled'}
                    </button>
                  </td>
                  <td class="ds-td-mono">{sourceStats.received.toLocaleString()}</td>
                  <td class="ds-td-right">
                    <div class="flex justify-end gap-2">
                      <button class="ds-btn-outline text-xs" onclick={() => snippetSourceId = source.id}>Snippets</button>
                      <button class="text-xs text-red-500 hover:text-red-700" onclick={() => deletingSource = source}>
                        <Trash2 size={14} />
                      </button>
                    </div>
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}

      <!-- Code snippets panel -->
      {#if snippetSourceId && snippetSource()}
        {@const src = snippetSource()!}
        <div class="mt-6 ds-card p-4">
          <div class="flex items-center justify-between mb-3">
            <h3 class="text-sm font-semibold text-gray-800 dark:text-gray-200">Code Snippets — {src.name}</h3>
            <button class="text-xs text-gray-500 hover:text-gray-700" onclick={() => snippetSourceId = null}>Close</button>
          </div>
          <div class="space-y-4">
            {#each [['curl', curlSnippet(src)], ['JavaScript', jsSnippet(src)], ['Python', pySnippet(src)]] as [label, code]}
              <div>
                <div class="flex items-center justify-between mb-1">
                  <span class="text-xs font-medium text-gray-500">{label}</span>
                  <button
                    class="flex items-center gap-1 text-xs text-gray-500 hover:text-ch-blue"
                    onclick={() => copyToClipboard(code, label)}
                  >
                    {#if copiedSnippet === label}
                      <Check size={12} /> Copied
                    {:else}
                      <Copy size={12} /> Copy
                    {/if}
                  </button>
                </div>
                <pre class="text-[11px] leading-relaxed whitespace-pre-wrap bg-gray-50 dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded p-3 overflow-x-auto">{code}</pre>
              </div>
            {/each}
          </div>
        </div>
      {/if}
    {/if}
  </div>
</div>
