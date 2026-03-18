<script lang="ts">
  import { onMount } from 'svelte'
  import {
    listNotebooks,
    createNotebook,
    getNotebook,
    updateNotebook,
    deleteNotebook,
    saveCells,
    runCell,
    shareNotebook,
    revokeNotebookShare,
    type Notebook,
    type NotebookCell,
    type NotebookWithCells,
    type CellRunResult,
  } from '../lib/api/notebooks'
  import { success as toastSuccess, error as toastError } from '../lib/stores/toast.svelte'
  import Spinner from '../lib/components/common/Spinner.svelte'
  import Skeleton from '../lib/components/common/Skeleton.svelte'
  import ConfirmDialog from '../lib/components/common/ConfirmDialog.svelte'
  import { BookOpen, Plus, Trash2, Play, Share2, Save, ChevronUp, ChevronDown, FileText, Code, Copy, Check, X } from 'lucide-svelte'
  import { marked } from 'marked'
  import NotebookSqlCell from '../lib/components/notebooks/NotebookSqlCell.svelte'

  // List view state
  let notebooks = $state<Notebook[]>([])
  let loading = $state(true)
  let deletingNotebook = $state<Notebook | null>(null)

  // Detail view state
  let activeNotebook = $state<Notebook | null>(null)
  let cells = $state<NotebookCell[]>([])
  let cellResults = $state<Map<string, CellRunResult>>(new Map())
  let runningCells = $state<Set<string>>(new Set())
  let saving = $state(false)
  let editingTitle = $state(false)
  let titleInput = $state('')
  let copiedShare = $state(false)
  let mdEditingCells = $state<Set<string>>(new Set())

  onMount(() => {
    loadNotebooks()

    function handleKeydown(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 's') {
        if (activeNotebook) {
          e.preventDefault()
          handleSave()
        }
      }
    }
    window.addEventListener('keydown', handleKeydown)
    return () => window.removeEventListener('keydown', handleKeydown)
  })

  async function loadNotebooks() {
    loading = true
    try {
      notebooks = await listNotebooks()
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to load notebooks')
    } finally {
      loading = false
    }
  }

  async function handleCreate() {
    try {
      const nb = await createNotebook({ title: 'Untitled Notebook' })
      toastSuccess('Notebook created')
      await openNotebook(nb.id)
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to create notebook')
    }
  }

  async function openNotebook(id: string) {
    try {
      const data: NotebookWithCells = await getNotebook(id)
      activeNotebook = data.notebook
      cells = data.cells
      cellResults = new Map()
      titleInput = data.notebook.title
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to open notebook')
    }
  }

  function goBack() {
    activeNotebook = null
    cells = []
    cellResults = new Map()
    loadNotebooks()
  }

  async function confirmDelete() {
    if (!deletingNotebook) return
    const nb = deletingNotebook
    deletingNotebook = null
    try {
      await deleteNotebook(nb.id)
      toastSuccess('Notebook deleted')
      if (activeNotebook?.id === nb.id) {
        goBack()
      } else {
        await loadNotebooks()
      }
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to delete notebook')
    }
  }

  // Cell operations
  function addCell(type: 'sql' | 'markdown', afterIndex = -1) {
    const position = afterIndex >= 0 ? afterIndex + 1 : cells.length
    const newCell: NotebookCell = {
      id: crypto.randomUUID(),
      notebook_id: activeNotebook?.id ?? '',
      cell_type: type,
      content: '',
      position,
      created_at: '',
      updated_at: '',
    }
    // Shift positions of cells after insertion point
    const updated = cells.map(c => c.position >= position ? { ...c, position: c.position + 1 } : c)
    cells = [...updated.slice(0, position), newCell, ...updated.slice(position)]
    // Renormalize positions
    cells = cells.map((c, i) => ({ ...c, position: i }))
  }

  function removeCell(cellId: string) {
    cells = cells.filter(c => c.id !== cellId).map((c, i) => ({ ...c, position: i }))
    const updated = new Map(cellResults)
    updated.delete(cellId)
    cellResults = updated
  }

  function moveCell(cellId: string, direction: -1 | 1) {
    const idx = cells.findIndex(c => c.id === cellId)
    if (idx < 0) return
    const newIdx = idx + direction
    if (newIdx < 0 || newIdx >= cells.length) return
    const updated = [...cells]
    const temp = updated[idx]
    updated[idx] = updated[newIdx]
    updated[newIdx] = temp
    cells = updated.map((c, i) => ({ ...c, position: i }))
  }

  function updateCellContent(cellId: string, content: string) {
    cells = cells.map(c => c.id === cellId ? { ...c, content } : c)
  }

  function toggleCellType(cellId: string) {
    cells = cells.map(c => {
      if (c.id !== cellId) return c
      return { ...c, cell_type: c.cell_type === 'sql' ? 'markdown' : 'sql' }
    })
  }

  async function handleRunCell(cell: NotebookCell) {
    if (!activeNotebook || cell.cell_type !== 'sql') return

    const updated = new Set(runningCells)
    updated.add(cell.id)
    runningCells = updated

    try {
      // Gather preceding SQL cells
      const preceding = cells
        .filter(c => c.position < cell.position && c.cell_type === 'sql' && c.content.trim())
        .map(c => ({ position: c.position, sql: c.content }))

      const result = await runCell(activeNotebook.id, cell.id, cell.content, preceding)
      const resultMap = new Map(cellResults)
      resultMap.set(cell.id, result)
      cellResults = resultMap
    } catch (e: unknown) {
      const resultMap = new Map(cellResults)
      resultMap.set(cell.id, {
        meta: [],
        data: [],
        row_count: 0,
        truncated: false,
        error: e instanceof Error ? e.message : 'Execution failed',
      })
      cellResults = resultMap
    } finally {
      const s = new Set(runningCells)
      s.delete(cell.id)
      runningCells = s
    }
  }

  async function handleRunAll() {
    if (!activeNotebook) return
    for (const cell of cells) {
      if (cell.cell_type === 'sql' && cell.content.trim()) {
        await handleRunCell(cell)
      }
    }
  }

  async function handleSave() {
    if (!activeNotebook) return
    saving = true
    try {
      await saveCells(activeNotebook.id, cells)
      if (titleInput !== activeNotebook.title) {
        await updateNotebook(activeNotebook.id, titleInput, activeNotebook.description)
        activeNotebook = { ...activeNotebook, title: titleInput }
      }
      toastSuccess('Notebook saved')
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to save')
    } finally {
      saving = false
    }
  }

  async function handleShare() {
    if (!activeNotebook) return
    try {
      if (activeNotebook.share_token) {
        await revokeNotebookShare(activeNotebook.id)
        activeNotebook = { ...activeNotebook, share_token: null, is_public: false }
        toastSuccess('Share link revoked')
      } else {
        const result = await shareNotebook(activeNotebook.id)
        activeNotebook = { ...activeNotebook, share_token: result.share_token, is_public: true }
        toastSuccess('Share link generated')
      }
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to toggle sharing')
    }
  }

  async function copyShareLink() {
    if (!activeNotebook?.share_token) return
    const url = `${window.location.origin}/api/notebooks/shared/${activeNotebook.share_token}`
    try {
      await navigator.clipboard.writeText(url)
      copiedShare = true
      setTimeout(() => { copiedShare = false }, 2000)
    } catch {
      toastError('Failed to copy')
    }
  }

  function formatDate(ts: string): string {
    try { return new Date(ts).toLocaleDateString() } catch { return ts }
  }

  function renderMarkdown(content: string): string {
    try { return marked(content) as string } catch { return content }
  }
</script>

<div class="flex flex-col h-full">
  {#if !activeNotebook}
    <!-- List view -->
    <div class="border-b border-gray-200 dark:border-gray-800">
      <div class="flex items-center justify-between px-4 py-3">
        <div class="flex items-center gap-3">
          <BookOpen size={18} class="text-ch-blue" />
          <h1 class="ds-page-title">SQL Notebooks</h1>
        </div>
        <button class="ds-btn-primary" onclick={handleCreate}>
          <Plus size={14} />
          New Notebook
        </button>
      </div>
    </div>

    <ConfirmDialog
      open={deletingNotebook !== null}
      title="Delete notebook?"
      description={deletingNotebook ? `Delete "${deletingNotebook.title}"? This cannot be undone.` : ''}
      confirmLabel="Delete Notebook"
      destructive
      onconfirm={confirmDelete}
      oncancel={() => deletingNotebook = null}
    />

    <div class="flex-1 overflow-auto p-4">
      {#if loading}
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {#each { length: 6 } as _}
            <div class="ds-card p-4 space-y-3">
              <Skeleton width="60%" height="1rem" rounded="rounded" />
              <Skeleton width="80%" height="0.75rem" rounded="rounded" />
              <div class="flex items-center gap-3">
                <Skeleton width="50px" height="0.6rem" rounded="rounded" />
                <Skeleton width="70px" height="0.6rem" rounded="rounded" />
              </div>
            </div>
          {/each}
        </div>
      {:else if notebooks.length === 0}
        <div class="ds-empty">
          <p class="text-sm text-gray-500 mb-2">No notebooks yet. Create one to get started.</p>
          <button class="ds-btn-primary" onclick={handleCreate}>Create First Notebook</button>
        </div>
      {:else}
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {#each notebooks as nb}
            <!-- svelte-ignore a11y_no_static_element_interactions -->
            <!-- svelte-ignore a11y_click_events_have_key_events -->
            <div
              class="ds-card p-4 text-left hover:border-ch-blue/50 transition-colors group cursor-pointer"
              onclick={() => openNotebook(nb.id)}
            >
              <div class="flex items-start justify-between mb-2">
                <h3 class="text-sm font-semibold text-gray-900 dark:text-gray-100 group-hover:text-ch-blue truncate">{nb.title}</h3>
                <button
                  class="text-gray-400 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-opacity"
                  onclick={(e) => { e.stopPropagation(); deletingNotebook = nb }}
                >
                  <Trash2 size={14} />
                </button>
              </div>
              {#if nb.description}
                <p class="text-xs text-gray-500 mb-2 line-clamp-2">{nb.description}</p>
              {/if}
              <div class="flex items-center gap-3 text-[11px] text-gray-400">
                <span>{nb.created_by}</span>
                <span>{formatDate(nb.updated_at)}</span>
                {#if nb.is_public}
                  <span class="ds-badge ds-badge-success text-[10px]">Shared</span>
                {/if}
              </div>
            </div>
          {/each}
        </div>
      {/if}
    </div>

  {:else}
    <!-- Detail/editor view -->
    <div class="border-b border-gray-200 dark:border-gray-800">
      <div class="flex items-center gap-3 px-4 py-2">
        <button class="text-xs text-gray-500 hover:text-ch-blue" onclick={goBack}>&larr; Back</button>
        {#if editingTitle}
          <input
            class="ds-input-sm text-sm font-semibold flex-1 max-w-xs"
            bind:value={titleInput}
            onblur={() => editingTitle = false}
            onkeydown={(e) => { if (e.key === 'Enter') editingTitle = false }}
          />
        {:else}
          <button class="text-sm font-semibold text-gray-900 dark:text-gray-100 hover:text-ch-blue" onclick={() => editingTitle = true}>
            {activeNotebook.title}
          </button>
        {/if}
        <div class="flex items-center gap-2 ml-auto">
          {#if activeNotebook.share_token}
            <button class="ds-btn-outline text-xs" onclick={copyShareLink}>
              {#if copiedShare}
                <Check size={12} /> Copied
              {:else}
                <Copy size={12} /> Copy Link
              {/if}
            </button>
          {/if}
          <button class="ds-btn-outline" onclick={handleShare} title={activeNotebook.share_token ? 'Revoke share' : 'Share'}>
            <Share2 size={14} />
          </button>
          <button class="ds-btn-outline" onclick={handleRunAll} title="Run all cells">
            <Play size={14} />
            Run All
          </button>
          <button class="ds-btn-primary" onclick={handleSave} disabled={saving} title="Save (Cmd+S)">
            <Save size={14} />
            {saving ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>
    </div>

    <ConfirmDialog
      open={deletingNotebook !== null}
      title="Delete notebook?"
      description={deletingNotebook ? `Delete "${deletingNotebook.title}"? This cannot be undone.` : ''}
      confirmLabel="Delete Notebook"
      destructive
      onconfirm={confirmDelete}
      oncancel={() => deletingNotebook = null}
    />

    <div class="flex-1 overflow-auto p-4">
      <div class="max-w-4xl mx-auto space-y-3">
        {#each cells as cell, idx (cell.id)}
          <div class="ds-card group relative">
            <!-- Cell toolbar -->
            <div class="flex items-center gap-1 px-3 py-1.5 border-b border-gray-200 dark:border-gray-800 bg-gray-50 dark:bg-gray-900">
              <span class="text-[10px] text-gray-400 font-mono mr-2">[{idx}]</span>
              <button
                class="text-[11px] px-1.5 py-0.5 rounded {cell.cell_type === 'sql' ? 'bg-blue-100 dark:bg-blue-500/15 text-ch-blue' : 'bg-purple-100 dark:bg-purple-500/15 text-purple-600 dark:text-purple-400'}"
                onclick={() => toggleCellType(cell.id)}
                title="Toggle cell type"
              >
                {#if cell.cell_type === 'sql'}
                  <Code size={12} class="inline" /> SQL
                {:else}
                  <FileText size={12} class="inline" /> MD
                {/if}
              </button>
              {#if cell.cell_type === 'sql'}
                <button
                  class="text-xs text-gray-500 hover:text-ch-blue px-1.5"
                  onclick={() => handleRunCell(cell)}
                  disabled={runningCells.has(cell.id)}
                  title="Run cell"
                >
                  {#if runningCells.has(cell.id)}
                    <Spinner />
                  {:else}
                    <Play size={12} />
                  {/if}
                </button>
              {/if}
              <div class="ml-auto flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                <button class="p-1 text-gray-400 hover:text-gray-600" onclick={() => moveCell(cell.id, -1)} title="Move up" disabled={idx === 0}>
                  <ChevronUp size={12} />
                </button>
                <button class="p-1 text-gray-400 hover:text-gray-600" onclick={() => moveCell(cell.id, 1)} title="Move down" disabled={idx === cells.length - 1}>
                  <ChevronDown size={12} />
                </button>
                <button class="p-1 text-gray-400 hover:text-red-500" onclick={() => removeCell(cell.id)} title="Delete cell">
                  <X size={12} />
                </button>
              </div>
            </div>

            <!-- Cell content -->
            <div>
              {#if cell.cell_type === 'sql'}
                {#key cell.id}
                  <NotebookSqlCell
                    value={cell.content}
                    onrun={(sql) => handleRunCell(cell)}
                    onchange={(content) => updateCellContent(cell.id, content)}
                  />
                {/key}
              {:else}
                {@const editing = !cell.content.trim() || mdEditingCells.has(cell.id)}
                {#if editing}
                  <div class="p-3">
                    <textarea
                      class="w-full min-h-[60px] font-mono text-[12px] leading-relaxed bg-transparent border-0 outline-none resize-y text-gray-800 dark:text-gray-200"
                      placeholder="Write markdown..."
                      value={cell.content}
                      oninput={(e) => updateCellContent(cell.id, (e.target as HTMLTextAreaElement).value)}
                      onblur={() => { if (cell.content.trim()) { const s = new Set(mdEditingCells); s.delete(cell.id); mdEditingCells = s } }}
                    ></textarea>
                  </div>
                {:else}
                  <!-- svelte-ignore a11y_click_events_have_key_events -->
                  <!-- svelte-ignore a11y_no_static_element_interactions -->
                  <div
                    class="prose prose-sm dark:prose-invert max-w-none cursor-text p-3"
                    onclick={() => { mdEditingCells = new Set([...mdEditingCells, cell.id]) }}
                  >
                    {@html renderMarkdown(cell.content)}
                  </div>
                {/if}
              {/if}
            </div>

            <!-- Cell result (SQL cells only) -->
            {#if cell.cell_type === 'sql' && cellResults.has(cell.id)}
              {@const result = cellResults.get(cell.id)!}
              <div class="border-t border-gray-200 dark:border-gray-800 p-3">
                {#if result.error}
                  <div class="text-xs text-red-500 font-mono">{result.error}</div>
                {:else if result.data.length > 0}
                  <div class="overflow-auto max-h-[300px]">
                    <table class="ds-table text-[11px]">
                      <thead>
                        <tr class="ds-table-head-row">
                          {#each result.meta as col}
                            <th class="ds-table-th">{col.name}</th>
                          {/each}
                        </tr>
                      </thead>
                      <tbody>
                        {#each result.data as row}
                          <tr class="ds-table-row">
                            {#each row as val}
                              <td class="ds-td-mono">{val ?? 'NULL'}</td>
                            {/each}
                          </tr>
                        {/each}
                      </tbody>
                    </table>
                  </div>
                  <div class="text-[10px] text-gray-400 mt-1">
                    {result.row_count} row{result.row_count !== 1 ? 's' : ''}{result.truncated ? ' (truncated)' : ''}
                  </div>
                {:else}
                  <div class="text-xs text-gray-400">Query executed successfully (no rows returned)</div>
                {/if}
              </div>
            {/if}
          </div>
        {/each}

        <!-- Add cell buttons -->
        <div class="flex items-center justify-center gap-2 py-4">
          <button class="ds-btn-outline text-xs" onclick={() => addCell('sql')}>
            <Code size={12} /> Add SQL Cell
          </button>
          <button class="ds-btn-outline text-xs" onclick={() => addCell('markdown')}>
            <FileText size={12} /> Add Markdown Cell
          </button>
        </div>
      </div>
    </div>
  {/if}
</div>
