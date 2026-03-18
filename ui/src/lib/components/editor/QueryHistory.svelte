<script lang="ts">
  import { onMount } from 'svelte'
  import { fetchQueryHistory, type QueryHistoryEntry } from '../../api/query'
  import { error as toastError } from '../../stores/toast.svelte'
  import Sheet from '../common/Sheet.svelte'
  import Spinner from '../common/Spinner.svelte'
  import { Search, X, AlertCircle, Clock, Rows3 } from 'lucide-svelte'

  interface Props {
    open: boolean
    onclose: () => void
    onopenquery: (sql: string) => void
  }

  let { open, onclose, onopenquery }: Props = $props()

  let history = $state<QueryHistoryEntry[]>([])
  let total = $state(0)
  let loading = $state(false)
  let searchTerm = $state('')
  let offset = $state(0)
  const limit = 50

  let searchTimeout: ReturnType<typeof setTimeout> | undefined

  $effect(() => {
    if (open) {
      offset = 0
      history = []
      loadHistory()
    }
  })

  function handleSearchInput(value: string) {
    searchTerm = value
    clearTimeout(searchTimeout)
    searchTimeout = setTimeout(() => {
      offset = 0
      history = []
      loadHistory()
    }, 300)
  }

  async function loadHistory() {
    loading = true
    try {
      const res = await fetchQueryHistory({
        limit,
        offset,
        search: searchTerm.trim() || undefined,
      })
      if (offset === 0) {
        history = res.history
      } else {
        history = [...history, ...res.history]
      }
      total = res.total
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to load query history')
    } finally {
      loading = false
    }
  }

  function loadMore() {
    offset += limit
    loadHistory()
  }

  function handleClick(entry: QueryHistoryEntry) {
    onopenquery(entry.query_text)
    onclose()
  }

  function formatRelative(value: string): string {
    const t = Date.parse(value)
    if (!Number.isFinite(t)) return 'unknown'
    const delta = Math.max(0, Date.now() - t)
    const minutes = Math.floor(delta / 60000)
    if (minutes < 1) return 'just now'
    if (minutes < 60) return `${minutes}m ago`
    const hours = Math.floor(minutes / 60)
    if (hours < 24) return `${hours}h ago`
    const days = Math.floor(hours / 24)
    if (days < 30) return `${days}d ago`
    return new Date(t).toLocaleDateString()
  }

  function truncateSQL(sql: string, maxLen = 120): string {
    const oneLine = sql.replace(/\s+/g, ' ').trim()
    if (oneLine.length <= maxLen) return oneLine
    return oneLine.slice(0, maxLen) + '...'
  }

  function formatDuration(ms: number): string {
    if (ms < 1000) return `${ms}ms`
    return `${(ms / 1000).toFixed(1)}s`
  }
</script>

<Sheet {open} title="Query History" size="lg" onclose={onclose}>
  <div class="flex flex-col gap-3 h-full">
    <!-- Search -->
    <div class="flex items-center gap-2 rounded-lg border border-gray-300/80 dark:border-gray-700/80 bg-gray-100/60 dark:bg-gray-900/60 px-2.5">
      <Search size={14} class="text-gray-500 shrink-0" />
      <input
        type="text"
        class="w-full h-9 bg-transparent text-[13px] outline-none text-gray-800 dark:text-gray-200 placeholder:text-gray-500"
        placeholder="Search query text..."
        value={searchTerm}
        oninput={(e) => handleSearchInput((e.target as HTMLInputElement).value)}
      />
      {#if searchTerm}
        <button
          class="rounded p-1 text-gray-500 hover:text-gray-700 dark:hover:text-gray-300"
          onclick={() => { handleSearchInput('') }}
        >
          <X size={13} />
        </button>
      {/if}
    </div>

    <div class="text-[11px] text-gray-500">
      {total} {total === 1 ? 'query' : 'queries'} found
    </div>

    <!-- History list -->
    <div class="flex flex-col gap-1.5 flex-1 overflow-auto">
      {#if loading && history.length === 0}
        <div class="flex items-center justify-center py-12"><Spinner /></div>
      {:else if history.length === 0}
        <div class="text-center py-12 text-gray-500 text-sm">
          {searchTerm ? 'No queries match your search.' : 'No query history yet.'}
        </div>
      {:else}
        {#each history as entry (entry.id)}
          <button
            class="text-left w-full px-3 py-2.5 rounded-lg border border-gray-200 dark:border-gray-800 hover:border-gray-300 dark:hover:border-gray-700 hover:bg-gray-100/50 dark:hover:bg-gray-800/50 transition-colors cursor-pointer"
            onclick={() => handleClick(entry)}
          >
            <div class="flex items-start gap-2">
              <pre class="flex-1 text-[12px] font-mono text-gray-800 dark:text-gray-200 whitespace-pre-wrap break-all leading-relaxed">{truncateSQL(entry.query_text, 200)}</pre>
              {#if entry.is_error}
                <span class="shrink-0 mt-0.5">
                  <AlertCircle size={13} class="text-red-400" />
                </span>
              {/if}
            </div>
            <div class="flex items-center gap-3 mt-1.5 text-[11px] text-gray-500">
              <span class="inline-flex items-center gap-1"><Clock size={10} /> {formatRelative(entry.event_time)}</span>
              <span>{formatDuration(entry.duration_ms)}</span>
              {#if !entry.is_error}
                <span class="inline-flex items-center gap-1"><Rows3 size={10} /> {entry.result_rows.toLocaleString()} rows</span>
              {/if}
              {#if entry.query_kind}
                <span class="px-1.5 py-0.5 rounded bg-gray-200 dark:bg-gray-800 text-[10px] font-medium">{entry.query_kind}</span>
              {/if}
            </div>
            {#if entry.is_error && entry.error_message}
              <div class="mt-1.5 text-[11px] text-red-400 truncate">{entry.error_message}</div>
            {/if}
          </button>
        {/each}

        {#if history.length < total}
          <button
            class="self-center px-4 py-2 text-xs text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 hover:bg-gray-200/50 dark:hover:bg-gray-800/50 rounded-lg transition-colors"
            onclick={loadMore}
            disabled={loading}
          >
            {loading ? 'Loading...' : 'Load more'}
          </button>
        {/if}
      {/if}
    </div>
  </div>
</Sheet>
