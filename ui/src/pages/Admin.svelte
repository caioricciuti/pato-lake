<script lang="ts">
  import { onMount } from 'svelte'
  import type { AdminStats } from '../lib/types/api'
  import { apiGet, apiPut, apiDel, apiPost } from '../lib/api/client'
  import type { BrainModelOption, BrainProviderAdmin, BrainSkill } from '../lib/types/brain'
  import {
    adminBulkUpdateBrainModels,
    adminCreateBrainProvider,
    adminCreateBrainSkill,
    adminDeleteBrainProvider,
    adminListBrainModels,
    adminListBrainProviders,
    adminListBrainSkills,
    adminSyncBrainProviderModels,
    adminUpdateBrainModel,
    adminUpdateBrainProvider,
    adminUpdateBrainSkill,
  } from '../lib/api/brain'
  import { success as toastSuccess, error as toastError } from '../lib/stores/toast.svelte'
  import Spinner from '../lib/components/common/Spinner.svelte'
  import Skeleton from '../lib/components/common/Skeleton.svelte'
  import Combobox from '../lib/components/common/Combobox.svelte'
  import type { ComboboxOption } from '../lib/components/common/Combobox.svelte'
  import Sheet from '../lib/components/common/Sheet.svelte'
  import HelpTip from '../lib/components/common/HelpTip.svelte'
  import ConfirmDialog from '../lib/components/common/ConfirmDialog.svelte'
  import {
    listAPIKeys,
    createAPIKey,
    updateAPIKey,
    deleteAPIKey,
    type APIKey,
    type APIKeyCreateResult,
  } from '../lib/api/api-keys'
  import { Shield, RefreshCw, Users, Database, Activity, LogIn, ChevronDown, ChevronRight, Brain, Trash2, UserPlus, Key, Copy, Check, Plus } from 'lucide-svelte'

  // Tab state
  type AdminTab = 'overview' | 'users' | 'brain' | 'api-keys'
  const adminTabIds: AdminTab[] = ['overview', 'users', 'brain', 'api-keys']
  let activeTab = $state<AdminTab>('overview')

  // Overview
  let stats = $state<AdminStats | null>(null)
  let statsLoading = $state(true)

  // Users
  let users = $state<any[]>([])
  let usersLoading = $state(true)
  let roleSavingUser = $state<string | null>(null)
  let createUserSheetOpen = $state(false)
  let createUserForm = $state({ username: '', password: '', role: 'viewer' })
  let creatingUser = $state(false)
  let deletingUser = $state<{ id: string; username: string } | null>(null)

  // Brain admin
  let brainLoading = $state(false)
  let brainProviders = $state<BrainProviderAdmin[]>([])
  let brainModels = $state<BrainModelOption[]>([])
  let brainSkills = $state<BrainSkill[]>([])
  let modelProviderFilter = $state('')
  let modelSearch = $state('')
  let modelShowOnlyActive = $state(false)
  let providerSheetOpen = $state(false)
  let skillSheetOpen = $state(false)
  let deletingProvider = $state<BrainProviderAdmin | null>(null)

  // API keys
  let apiKeysLoading = $state(false)
  let apiKeys = $state<APIKey[]>([])
  let createKeySheetOpen = $state(false)
  let newKeyResult = $state<APIKeyCreateResult | null>(null)
  let createKeyForm = $state({ name: '', role: 'viewer', scopes: '*', rate_limit_rpm: 60, expires_at: '' })
  let deletingKey = $state<APIKey | null>(null)
  let copiedKey = $state(false)

  const roleOptions: ComboboxOption[] = [
    { value: 'admin', label: 'admin' },
    { value: 'analyst', label: 'analyst' },
    { value: 'viewer', label: 'viewer' },
  ]

  const providerKindOptions: ComboboxOption[] = [
    { value: 'openai', label: 'openai' },
    { value: 'openai_compatible', label: 'openai_compatible' },
    { value: 'ollama', label: 'ollama' },
  ]
  const providerBaseUrls: Record<string, string> = {
    openai: 'https://api.openai.com/v1',
    openai_compatible: '',
    ollama: 'http://localhost:11434/v1',
  }
  let providerForm = $state({
    name: '',
    kind: 'openai',
    baseUrl: '',
    apiKey: '',
    isActive: true,
    isDefault: false,
  })
  let skillForm = $state({
    name: 'Default Brain Skill',
    content: '',
    isActive: true,
    isDefault: true,
  })

  function normalizeAdminTab(value: string | null | undefined): AdminTab {
    const raw = (value ?? '').trim().toLowerCase()
    if ((adminTabIds as string[]).includes(raw)) return raw as AdminTab
    return 'overview'
  }

  function syncAdminTabParam(tab: AdminTab) {
    if (typeof window === 'undefined') return
    const url = new URL(window.location.href)
    if (url.searchParams.get('tab') === tab) return
    url.searchParams.set('tab', tab)
    history.replaceState(null, '', `${url.pathname}?${url.searchParams.toString()}`)
  }

  onMount(() => {
    loadStats()
    const initialTab = normalizeAdminTab(
      typeof window === 'undefined' ? null : new URLSearchParams(window.location.search).get('tab'),
    )
    switchTab(initialTab, true)
  })

  async function loadStats() {
    statsLoading = true
    try {
      stats = await apiGet<AdminStats>('/api/admin/stats')
    } catch (e: any) {
      toastError(e.message)
    } finally {
      statsLoading = false
    }
  }

  async function loadUsers() {
    usersLoading = true
    try {
      const usersResponse = await apiGet<any>('/api/admin/users')
      if (Array.isArray(usersResponse)) {
        users = usersResponse
      } else {
        users = usersResponse?.users ?? []
      }
    } catch (e: any) {
      toastError(e.message)
    } finally {
      usersLoading = false
    }
  }

  async function refreshUsersTab() {
    await loadUsers()
  }

  function switchTab(tab: AdminTab, syncUrl = true) {
    activeTab = tab
    if (syncUrl) syncAdminTabParam(tab)
    if (tab === 'users' && users.length === 0) {
      refreshUsersTab()
    }
    if (tab === 'brain' && !brainLoading && brainProviders.length === 0 && brainSkills.length === 0) {
      loadBrainAdmin()
    }
    if (tab === 'api-keys' && !apiKeysLoading && apiKeys.length === 0) {
      loadApiKeys()
    }
  }

  async function loadApiKeys() {
    apiKeysLoading = true
    try {
      apiKeys = await listAPIKeys()
    } catch (e: any) {
      toastError(e.message)
    } finally {
      apiKeysLoading = false
    }
  }

  async function handleCreateKey() {
    const name = createKeyForm.name.trim()
    if (!name) { toastError('Name is required'); return }
    try {
      const result = await createAPIKey({
        name,
        role: createKeyForm.role,
        scopes: createKeyForm.scopes,
        rate_limit_rpm: createKeyForm.rate_limit_rpm,
        expires_at: createKeyForm.expires_at || undefined,
      })
      newKeyResult = result
      toastSuccess('API key created')
      createKeyForm = { name: '', role: 'viewer', scopes: '*', rate_limit_rpm: 60, expires_at: '' }
      await loadApiKeys()
    } catch (e: any) {
      toastError(e.message)
    }
  }

  async function toggleKeyActive(key: APIKey) {
    try {
      await updateAPIKey(key.id, { name: key.name, is_active: !key.is_active, rate_limit_rpm: key.rate_limit_rpm, scopes: key.scopes })
      toastSuccess(key.is_active ? 'Key deactivated' : 'Key activated')
      await loadApiKeys()
    } catch (e: any) {
      toastError(e.message)
    }
  }

  async function confirmDeleteKey() {
    if (!deletingKey) return
    const k = deletingKey
    deletingKey = null
    try {
      await deleteAPIKey(k.id)
      toastSuccess('API key deleted')
      await loadApiKeys()
    } catch (e: any) {
      toastError(e.message)
    }
  }

  async function copyKey(key: string) {
    try {
      await navigator.clipboard.writeText(key)
      copiedKey = true
      setTimeout(() => { copiedKey = false }, 2000)
    } catch {
      toastError('Failed to copy')
    }
  }

  async function loadBrainAdmin() {
    brainLoading = true
    try {
      const [providers, models, skills] = await Promise.all([
        adminListBrainProviders(),
        adminListBrainModels(),
        adminListBrainSkills(),
      ])
      brainProviders = providers
      brainModels = models
      brainSkills = skills
      if (!modelProviderFilter && providers.length > 0) {
        modelProviderFilter = providers[0].id
      } else if (modelProviderFilter && !providers.some(p => p.id === modelProviderFilter)) {
        modelProviderFilter = providers[0]?.id ?? ''
      }
      if (skills.length > 0 && !skillForm.content) {
        const active = skills.find(s => s.is_active) ?? skills[0]
        skillForm = {
          name: active.name,
          content: active.content,
          isActive: active.is_active,
          isDefault: active.is_default,
        }
      }
    } catch (e: any) {
      toastError(e.message)
    } finally {
      brainLoading = false
    }
  }


  async function createProvider() {
    try {
      await adminCreateBrainProvider(providerForm)
      toastSuccess('Brain provider created')
      providerForm = { ...providerForm, name: '', apiKey: '', isDefault: false }
      providerSheetOpen = false
      await loadBrainAdmin()
    } catch (e: any) {
      toastError(e.message)
    }
  }

  async function toggleProvider(provider: BrainProviderAdmin, key: 'is_active' | 'is_default', value: boolean) {
    try {
      await adminUpdateBrainProvider(provider.id, {
        isActive: key === 'is_active' ? value : provider.is_active,
        isDefault: key === 'is_default' ? value : provider.is_default,
      })
      toastSuccess('Provider updated')
      await loadBrainAdmin()
    } catch (e: any) {
      toastError(e.message)
    }
  }

  async function syncProviderModels(provider: BrainProviderAdmin) {
    try {
      await adminSyncBrainProviderModels(provider.id)
      toastSuccess(`Synced models for ${provider.name}. Recommended model auto-selected.`)
      await loadBrainAdmin()
    } catch (e: any) {
      toastError(e.message)
    }
  }

  function deleteProvider(provider: BrainProviderAdmin) {
    deletingProvider = provider
  }

  async function confirmDeleteProvider() {
    if (!deletingProvider) return
    const provider = deletingProvider
    deletingProvider = null
    try {
      await adminDeleteBrainProvider(provider.id)
      toastSuccess('Provider deleted')
      await loadBrainAdmin()
    } catch (e: any) {
      toastError(e.message)
    }
  }

  async function updateModel(model: BrainModelOption, key: 'is_active' | 'is_default', value: boolean) {
    try {
      await adminUpdateBrainModel(model.id, {
        displayName: model.display_name || model.name,
        isActive: key === 'is_active' ? value : model.is_active,
        isDefault: key === 'is_default' ? value : model.is_default,
      })
      toastSuccess('Model updated')
      await loadBrainAdmin()
    } catch (e: any) {
      toastError(e.message)
    }
  }

  function filteredBrainModels(): BrainModelOption[] {
    const providerID = modelProviderFilter.trim()
    const term = modelSearch.trim().toLowerCase()
    return brainModels.filter(model => {
      if (providerID && model.provider_id !== providerID) return false
      if (modelShowOnlyActive && !model.is_active) return false
      if (!term) return true
      const candidate = `${model.display_name || ''} ${model.name} ${model.provider_name}`.toLowerCase()
      return candidate.includes(term)
    })
  }

  function modelsForProvider(providerId: string): BrainModelOption[] {
    const term = modelSearch.trim().toLowerCase()
    return brainModels.filter(model => {
      if (model.provider_id !== providerId) return false
      if (modelShowOnlyActive && !model.is_active) return false
      if (!term) return true
      const candidate = `${model.display_name || ''} ${model.name} ${model.provider_name}`.toLowerCase()
      return candidate.includes(term)
    })
  }

  function providerFilterOptions(): ComboboxOption[] {
    return [
      { value: '', label: 'All providers' },
      ...brainProviders.map(provider => ({ value: provider.id, label: provider.name, hint: provider.kind })),
    ]
  }

  function visibleProvidersForModels(): BrainProviderAdmin[] {
    if (!modelProviderFilter) return brainProviders
    return brainProviders.filter(p => p.id === modelProviderFilter)
  }

  async function runModelBulkAction(action: 'deactivate_all' | 'activate_all' | 'activate_recommended') {
    if (!modelProviderFilter) {
      toastError('Select a provider first')
      return
    }
    try {
      await adminBulkUpdateBrainModels({ providerId: modelProviderFilter, action })
      if (action === 'deactivate_all') toastSuccess('All models deactivated for selected provider')
      if (action === 'activate_all') toastSuccess('All models activated for selected provider')
      if (action === 'activate_recommended') toastSuccess('Recommended model activated and set as default')
      await loadBrainAdmin()
    } catch (e: any) {
      toastError(e.message)
    }
  }

  async function saveSkill() {
    try {
      if (!skillForm.content.trim() || !skillForm.name.trim()) {
        toastError('Skill name and content are required')
        return
      }
      const active = brainSkills.find(s => s.is_active)
      if (active) {
        await adminUpdateBrainSkill(active.id, {
          name: skillForm.name,
          content: skillForm.content,
          isActive: skillForm.isActive,
          isDefault: skillForm.isDefault,
        })
      } else {
        await adminCreateBrainSkill({
          name: skillForm.name,
          content: skillForm.content,
          isActive: skillForm.isActive,
          isDefault: skillForm.isDefault,
        })
      }
      toastSuccess('Brain skill saved')
      skillSheetOpen = false
      await loadBrainAdmin()
    } catch (e: any) {
      toastError(e.message)
    }
  }

  function openSkillSheet() {
    const active = brainSkills.find(s => s.is_active) ?? brainSkills[0]
    if (active) {
      skillForm = {
        name: active.name,
        content: active.content,
        isActive: active.is_active,
        isDefault: active.is_default,
      }
    }
    skillSheetOpen = true
  }

  async function setRole(username: string, role: string) {
    if (!username || roleSavingUser === username) return
    roleSavingUser = username
    try {
      await apiPut(`/api/admin/user-roles/${encodeURIComponent(username)}`, { role })
      const idx = users.findIndex(u => u.username === username)
      if (idx !== -1) users[idx] = { ...users[idx], role }
      toastSuccess(`Role set to ${role} for ${username}`)
    } catch (e: any) {
      toastError(e.message)
    } finally {
      if (roleSavingUser === username) roleSavingUser = null
    }
  }

  async function createUser() {
    const username = createUserForm.username.trim()
    const password = createUserForm.password
    const role = createUserForm.role || 'viewer'
    if (!username) { toastError('Username is required'); return }
    if (password.length < 6) { toastError('Password must be at least 6 characters'); return }
    creatingUser = true
    try {
      await apiPost('/api/admin/users', { username, password, role })
      toastSuccess(`User "${username}" created`)
      createUserForm = { username: '', password: '', role: 'viewer' }
      createUserSheetOpen = false
      await loadUsers()
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to create user')
    } finally {
      creatingUser = false
    }
  }

  async function confirmDeleteUser() {
    if (!deletingUser) return
    const { id, username } = deletingUser
    deletingUser = null
    try {
      await apiDel(`/api/admin/users/${encodeURIComponent(id)}`)
      toastSuccess(`User "${username}" deleted`)
      await loadUsers()
    } catch (e: unknown) {
      toastError(e instanceof Error ? e.message : 'Failed to delete user')
    }
  }

  function formatTime(ts: string): string {
    try {
      return new Date(ts).toLocaleString()
    } catch {
      return ts
    }
  }

  function truncate(s: string, max = 80): string {
    return s.length > max ? s.slice(0, max) + '...' : s
  }
</script>

<div class="flex flex-col h-full">
  <div class="border-b border-gray-200 dark:border-gray-800">
    <div class="flex flex-col gap-2 px-4 py-3 md:flex-row md:items-center md:gap-4">
      <div class="flex items-center gap-3">
        <Shield size={18} class="text-ch-blue" />
        <h1 class="ds-page-title">Admin Panel</h1>
      </div>
      <nav class="ds-tabs border-0 px-0 pt-0 gap-1 overflow-x-auto whitespace-nowrap" aria-label="Admin Tabs">
        {#each [['overview', 'Overview'], ['users', 'Users'], ['brain', 'Brain'], ['api-keys', 'API Keys']] as [key, label]}
          <button
            class="ds-tab {activeTab === key ? 'ds-tab-active' : ''}"
            onclick={() => switchTab(key as AdminTab)}
          >
            {label}
          </button>
        {/each}
      </nav>
</div>
</div>

<Sheet
  open={providerSheetOpen}
  title="Create Brain Provider"
  size="lg"
  onclose={() => providerSheetOpen = false}
>
  <form
    class="space-y-4"
    onsubmit={(e) => {
      e.preventDefault()
      void createProvider()
    }}
  >
    <div class="flex items-center gap-2">
      <p class="text-xs text-gray-500">Provider controls which model catalog is available to all users.</p>
      <HelpTip text="OpenAI works with managed API keys. OpenAI-compatible is for custom gateways. Ollama usually uses local/base URL endpoints." />
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
      <label class="space-y-1">
        <span class="text-xs text-gray-500">Provider Name</span>
        <input class="ds-input-sm" placeholder="OpenAI Prod" bind:value={providerForm.name} required />
      </label>
      <label class="space-y-1">
        <span class="text-xs text-gray-500">Provider Kind</span>
        <Combobox
          options={providerKindOptions}
          value={providerForm.kind}
          onChange={(v) => providerForm = { ...providerForm, kind: v, baseUrl: providerBaseUrls[v] ?? '' }}
        />
      </label>
      <label class="space-y-1 md:col-span-2">
        <span class="text-xs text-gray-500">Base URL</span>
        <input
          class="ds-input-sm"
          placeholder={providerForm.kind === 'ollama'
            ? 'http://localhost:11434/v1'
            : providerForm.kind === 'openai_compatible'
              ? 'https://your-gateway.example.com'
              : 'https://api.openai.com/v1'}
          bind:value={providerForm.baseUrl}
        />
      </label>
      <label class="space-y-1 md:col-span-2">
        <span class="text-xs text-gray-500">API Key</span>
        <input class="ds-input-sm" type="password" placeholder="sk-..." bind:value={providerForm.apiKey} />
      </label>
    </div>

    <div class="flex flex-wrap items-center gap-4">
      <label class="ds-checkbox-label text-xs">
        <input type="checkbox" class="ds-checkbox" bind:checked={providerForm.isActive} />
        Active
      </label>
      <label class="ds-checkbox-label text-xs">
        <input type="checkbox" class="ds-checkbox" bind:checked={providerForm.isDefault} />
        Default provider
      </label>
    </div>

    <div class="flex items-center justify-end gap-2 pt-2 border-t border-gray-200 dark:border-gray-800">
      <button type="button" class="ds-btn-outline" onclick={() => providerSheetOpen = false}>Cancel</button>
      <button type="submit" class="ds-btn-primary" disabled={!providerForm.name.trim()}>Create Provider</button>
    </div>
  </form>
</Sheet>

<Sheet
  open={skillSheetOpen}
  title="Global Brain Skill"
  size="xl"
  onclose={() => skillSheetOpen = false}
>
  <form
    class="space-y-4"
    onsubmit={(e) => {
      e.preventDefault()
      void saveSkill()
    }}
  >
    <div class="flex items-center gap-2">
      <p class="text-xs text-gray-500">This prompt steers SQL safety, artifact usage, and tool behavior for every chat.</p>
      <HelpTip text="Keep this instruction set practical: SQL guardrails, artifact expectations, and when to ask clarifying questions." />
    </div>

    <label class="space-y-1">
      <span class="text-xs text-gray-500">Skill Name</span>
      <input class="ds-input-sm" bind:value={skillForm.name} required />
    </label>

    <label class="space-y-1">
      <span class="text-xs text-gray-500">Skill Content</span>
      <textarea
        class="ds-input-sm min-h-[58vh] font-mono text-[12px] leading-relaxed resize-y"
        bind:value={skillForm.content}
        placeholder="You are Brain, a senior DuckDB copilot..."
      ></textarea>
    </label>

    <div class="flex flex-wrap items-center gap-4">
      <label class="ds-checkbox-label text-xs">
        <input type="checkbox" class="ds-checkbox" bind:checked={skillForm.isActive} />
        Active
      </label>
      <label class="ds-checkbox-label text-xs">
        <input type="checkbox" class="ds-checkbox" bind:checked={skillForm.isDefault} />
        Default
      </label>
    </div>

    <div class="flex items-center justify-end gap-2 pt-2 border-t border-gray-200 dark:border-gray-800">
      <button type="button" class="ds-btn-outline" onclick={() => skillSheetOpen = false}>Cancel</button>
      <button type="submit" class="ds-btn-primary" disabled={!skillForm.name.trim() || !skillForm.content.trim()}>Save Skill</button>
    </div>
  </form>
</Sheet>

<Sheet
  open={createUserSheetOpen}
  title="Create User"
  size="md"
  onclose={() => createUserSheetOpen = false}
>
  <form
    class="space-y-4"
    onsubmit={(e) => {
      e.preventDefault()
      void createUser()
    }}
  >
    <label class="block space-y-1">
      <span class="text-xs text-gray-500">Username</span>
      <input class="ds-input-sm" placeholder="johndoe" bind:value={createUserForm.username} required />
    </label>
    <label class="block space-y-1">
      <span class="text-xs text-gray-500">Password</span>
      <input class="ds-input-sm" type="password" placeholder="Min 6 characters" bind:value={createUserForm.password} required />
    </label>
    <label class="block space-y-1">
      <span class="text-xs text-gray-500">Role</span>
      <Combobox
        options={roleOptions}
        value={createUserForm.role}
        onChange={(v) => createUserForm = { ...createUserForm, role: v }}
      />
    </label>
    <div class="flex items-center justify-end gap-2 pt-2 border-t border-gray-200 dark:border-gray-800">
      <button type="button" class="ds-btn-outline" onclick={() => createUserSheetOpen = false}>Cancel</button>
      <button type="submit" class="ds-btn-primary" disabled={creatingUser || !createUserForm.username.trim()}>
        {creatingUser ? 'Creating...' : 'Create User'}
      </button>
    </div>
  </form>
</Sheet>

<ConfirmDialog
  open={deletingUser !== null}
  title="Delete user?"
  description={deletingUser ? `Delete user "${deletingUser.username}"? This cannot be undone.` : ''}
  confirmLabel="Delete User"
  destructive
  onconfirm={confirmDeleteUser}
  oncancel={() => deletingUser = null}
/>

<ConfirmDialog
  open={deletingProvider !== null}
  title="Delete provider?"
  description={deletingProvider ? `Delete "${deletingProvider.name}" and all its models? This cannot be undone.` : ''}
  confirmLabel="Delete Provider"
  destructive
  onconfirm={confirmDeleteProvider}
  oncancel={() => deletingProvider = null}
/>

<ConfirmDialog
  open={deletingKey !== null}
  title="Delete API key?"
  description={deletingKey ? `Delete API key "${deletingKey.name}"? This cannot be undone.` : ''}
  confirmLabel="Delete Key"
  destructive
  onconfirm={confirmDeleteKey}
  oncancel={() => deletingKey = null}
/>

<Sheet
  open={createKeySheetOpen}
  title="Create API Key"
  size="md"
  onclose={() => { createKeySheetOpen = false; newKeyResult = null }}
>
  {#if newKeyResult}
    <div class="space-y-4">
      <div class="p-3 rounded-lg bg-yellow-50 dark:bg-yellow-500/10 border border-yellow-200 dark:border-yellow-500/30">
        <p class="text-xs text-yellow-700 dark:text-yellow-400 font-semibold mb-2">Copy your API key now — it won't be shown again!</p>
        <div class="flex items-center gap-2">
          <code class="flex-1 text-xs font-mono bg-white dark:bg-gray-900 p-2 rounded border border-gray-200 dark:border-gray-800 break-all">{newKeyResult.key}</code>
          <button class="ds-btn-outline" onclick={() => copyKey(newKeyResult!.key)}>
            {#if copiedKey}
              <Check size={14} /> Copied
            {:else}
              <Copy size={14} />
            {/if}
          </button>
        </div>
      </div>
      <div class="flex justify-end">
        <button class="ds-btn-primary" onclick={() => { createKeySheetOpen = false; newKeyResult = null }}>Done</button>
      </div>
    </div>
  {:else}
    <form
      class="space-y-4"
      onsubmit={(e) => { e.preventDefault(); void handleCreateKey() }}
    >
      <label class="block space-y-1">
        <span class="text-xs text-gray-500">Key Name</span>
        <input class="ds-input-sm" placeholder="CI/CD Pipeline" bind:value={createKeyForm.name} required />
      </label>
      <label class="block space-y-1">
        <span class="text-xs text-gray-500">Role</span>
        <Combobox
          options={roleOptions}
          value={createKeyForm.role}
          onChange={(v) => createKeyForm = { ...createKeyForm, role: v }}
        />
      </label>
      <label class="block space-y-1">
        <span class="text-xs text-gray-500">Scopes (comma-separated, * for all)</span>
        <input class="ds-input-sm" placeholder="query,ingest,saved-queries" bind:value={createKeyForm.scopes} />
      </label>
      <div class="grid grid-cols-2 gap-3">
        <label class="block space-y-1">
          <span class="text-xs text-gray-500">Rate Limit (req/min)</span>
          <input class="ds-input-sm" type="number" bind:value={createKeyForm.rate_limit_rpm} />
        </label>
        <label class="block space-y-1">
          <span class="text-xs text-gray-500">Expires At (optional)</span>
          <input class="ds-input-sm" type="datetime-local" bind:value={createKeyForm.expires_at} />
        </label>
      </div>
      <div class="flex items-center justify-end gap-2 pt-2 border-t border-gray-200 dark:border-gray-800">
        <button type="button" class="ds-btn-outline" onclick={() => createKeySheetOpen = false}>Cancel</button>
        <button type="submit" class="ds-btn-primary" disabled={!createKeyForm.name.trim()}>Create Key</button>
      </div>
    </form>
  {/if}
</Sheet>

  <!-- Content -->
  <div class="flex-1 overflow-auto p-4">
    {#if activeTab === 'overview'}
      <!-- Stats cards -->
      {#if statsLoading}
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          {#each { length: 4 } as _}
            <div class="ds-stat-card space-y-2">
              <Skeleton width="80px" height="0.75rem" rounded="rounded" />
              <Skeleton width="60px" height="1.75rem" rounded="rounded" />
            </div>
          {/each}
        </div>
      {:else if stats}
	        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
	          <div class="ds-stat-card">
	            <div class="flex items-center gap-2 text-gray-500 text-xs mb-1"><Users size={14} /> Users</div>
	            <div class="text-2xl font-bold text-gray-900 dark:text-gray-100">{stats.users_count}</div>
	          </div>
	          <div class="ds-stat-card">
	            <div class="flex items-center gap-2 text-gray-500 text-xs mb-1"><Database size={14} /> Database</div>
	            <div class="text-2xl font-bold text-gray-900 dark:text-gray-100">DuckDB</div>
	          </div>
	          <div class="ds-stat-card">
	            <div class="flex items-center gap-2 text-gray-500 text-xs mb-1"><Activity size={14} /> Queries</div>
	            <div class="text-2xl font-bold text-gray-900 dark:text-gray-100">{stats.query_count}</div>
	          </div>
	          <div class="ds-stat-card">
	            <div class="flex items-center gap-2 text-gray-500 text-xs mb-1"><LogIn size={14} /> Logins</div>
	            <div class="text-2xl font-bold text-gray-900 dark:text-gray-100">{stats.login_count}</div>
	          </div>
	        </div>
      {/if}

    {:else if activeTab === 'users'}
      {#if usersLoading}
        <div class="flex items-center justify-center py-12"><Spinner /></div>
      {:else}
        <div class="flex items-center justify-between mb-2">
          <div class="flex items-center gap-2">
            <h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Application Users</h2>
            <HelpTip text="Manage user roles (admin, analyst, viewer). Safety rule: the last admin cannot be removed or demoted." />
          </div>
          <button class="ds-btn-outline" onclick={() => createUserSheetOpen = true}>
            <UserPlus size={14} />
            Add User
          </button>
        </div>
        {#if users.length === 0}
          <p class="text-sm text-gray-500 mb-4">No users found</p>
        {:else}
	          <div class="ds-table-wrap mb-6">
	            <table class="ds-table">
              <thead>
                <tr class="ds-table-head-row">
                  <th class="ds-table-th">Username</th>
                  <th class="ds-table-th">Role</th>
                  <th class="ds-table-th">Last Login</th>
                  <th class="ds-table-th-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {#each users as user}
                  <tr class="ds-table-row">
	                    <td class="ds-td-mono">{user.username}</td>
	                    <td class="ds-td">
                      <div class="inline-flex items-center rounded-lg border border-gray-300/80 dark:border-gray-700/80 bg-gray-100/70 dark:bg-gray-900/65 p-1">
                        {#each roleOptions as roleOpt}
                          <button
                            type="button"
                            class="px-2.5 h-7 rounded-md text-xs transition-colors disabled:opacity-60 disabled:cursor-not-allowed
                              {(user.role ?? 'viewer') === roleOpt.value
                                ? 'bg-yellow-100 dark:bg-yellow-500/15 text-ch-orange'
                                : 'text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}"
                            disabled={(user.role ?? 'viewer') === roleOpt.value || roleSavingUser === user.username}
                            onclick={() => setRole(user.username, roleOpt.value)}
                          >
                            {roleOpt.label}
                          </button>
                        {/each}
                      </div>
                    </td>
	                    <td class="ds-td-mono">{user.last_login ? formatTime(user.last_login) : '—'}</td>
	                    <td class="ds-td-right">
                      <div class="flex justify-end gap-2">
                        <button
                          class="text-xs text-red-500 hover:text-red-700"
                          onclick={() => deletingUser = { id: user.id, username: user.username }}
                        >Delete</button>
                      </div>
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {/if}

      {/if}

    {:else if activeTab === 'brain'}
      {#if brainLoading}
        <div class="flex items-center justify-center py-12"><Spinner /></div>
      {:else}
        <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-3 mb-4">
          <div class="flex items-center gap-2">
            <Brain size={16} class="text-ch-blue" />
            <h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Brain Control Center</h2>
            <HelpTip text="Manage AI providers and model availability for all users. Use provider accordions to keep large model lists manageable." />
          </div>
          <div class="flex flex-wrap items-center gap-2">
            <button class="ds-btn-outline" onclick={() => providerSheetOpen = true}>Add Provider</button>
            <button class="ds-btn-outline" onclick={() => openSkillSheet()}>Edit Global Skill</button>
            <button class="ds-btn-outline" onclick={() => loadBrainAdmin()} title="Refresh">
              <RefreshCw size={14} />
            </button>
          </div>
        </div>

        <div class="grid grid-cols-2 md:grid-cols-4 gap-2 mb-4">
          <div class="ds-panel p-2.5">
            <div class="text-[11px] text-gray-500">Providers</div>
            <div class="text-lg font-semibold text-gray-900 dark:text-gray-100">{brainProviders.length}</div>
          </div>
          <div class="ds-panel p-2.5">
            <div class="text-[11px] text-gray-500">Active Providers</div>
            <div class="text-lg font-semibold text-gray-900 dark:text-gray-100">{brainProviders.filter(p => p.is_active).length}</div>
          </div>
          <div class="ds-panel p-2.5">
            <div class="text-[11px] text-gray-500">Models</div>
            <div class="text-lg font-semibold text-gray-900 dark:text-gray-100">{brainModels.length}</div>
          </div>
          <div class="ds-panel p-2.5">
            <div class="text-[11px] text-gray-500">Active Models</div>
            <div class="text-lg font-semibold text-gray-900 dark:text-gray-100">{brainModels.filter(m => m.is_active).length}</div>
          </div>
        </div>

        {#if brainProviders.length === 0}
          <div class="ds-empty">
            <p class="text-sm text-gray-500 mb-2">No Brain providers configured yet.</p>
            <button class="ds-btn-primary" onclick={() => providerSheetOpen = true}>Create First Provider</button>
          </div>
        {:else}
          <div class="ds-table-wrap mb-5 max-h-[32vh] overflow-auto rounded-lg border border-gray-200 dark:border-gray-800">
            <table class="ds-table">
              <thead>
                <tr class="ds-table-head-row sticky top-0 bg-gray-50 dark:bg-gray-900 z-10">
                  <th class="ds-table-th">Provider</th>
                  <th class="ds-table-th">Kind</th>
                  <th class="ds-table-th">Base URL</th>
                  <th class="ds-table-th">Key</th>
                  <th class="ds-table-th">Active</th>
                  <th class="ds-table-th">Default</th>
                  <th class="ds-table-th-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {#each brainProviders as provider}
                  <tr class="ds-table-row">
                    <td class="ds-td-strong">{provider.name}</td>
                    <td class="ds-td-mono">{provider.kind}</td>
                    <td class="ds-td-mono max-w-sm truncate">{provider.base_url || '—'}</td>
                    <td class="ds-td">{provider.has_api_key ? 'Configured' : 'Missing'}</td>
                    <td class="ds-td">
                      <input
                        type="checkbox"
                        class="ds-checkbox"
                        checked={provider.is_active}
                        onchange={(e) => toggleProvider(provider, 'is_active', (e.target as HTMLInputElement).checked)}
                      />
                    </td>
                    <td class="ds-td">
                      <input
                        type="radio"
                        class="ds-radio"
                        name="default-brain-provider"
                        checked={provider.is_default}
                        onchange={() => toggleProvider(provider, 'is_default', true)}
                      />
                    </td>
                    <td class="ds-td-right">
                      <div class="flex justify-end gap-2">
                        <button class="ds-btn-outline" onclick={() => syncProviderModels(provider)}>Sync Models</button>
                        <button class="text-xs text-red-500 hover:text-red-700" onclick={() => deleteProvider(provider)}>Delete</button>
                      </div>
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {/if}

        <div class="ds-panel p-3 mb-3">
          <div class="flex items-center gap-2 mb-2">
            <h3 class="text-sm font-semibold text-gray-800 dark:text-gray-200">Brain Models</h3>
            <HelpTip text="Models are grouped by provider. Expand a provider accordion to activate/deactivate models and choose defaults." />
          </div>
          <div class="grid grid-cols-1 md:grid-cols-4 gap-2">
            <Combobox
              options={providerFilterOptions()}
              value={modelProviderFilter}
              onChange={(v) => modelProviderFilter = v}
            />
            <input
              class="ds-input-sm md:col-span-2"
              placeholder="Search models..."
              bind:value={modelSearch}
            />
            <label class="ds-checkbox-label text-xs px-2">
              <input type="checkbox" class="ds-checkbox" bind:checked={modelShowOnlyActive} />
              Show only active
            </label>
          </div>
          <div class="mt-2 flex flex-wrap items-center gap-2">
            <button class="ds-btn-outline" onclick={() => runModelBulkAction('activate_recommended')}>Activate Recommended</button>
            <button class="ds-btn-outline" onclick={() => runModelBulkAction('activate_all')}>Activate All</button>
            <button class="ds-btn-outline" onclick={() => runModelBulkAction('deactivate_all')}>Deactivate All</button>
          </div>
        </div>

        {#if brainModels.length === 0}
          <p class="text-sm text-gray-500 mb-6">No models synced yet.</p>
        {:else}
          <div class="space-y-2 mb-6">
            {#each visibleProvidersForModels() as provider}
              {@const providerModels = modelsForProvider(provider.id)}
              <details class="ds-card overflow-hidden" open={modelProviderFilter === provider.id || (!modelProviderFilter && provider.is_default)}>
                <summary class="cursor-pointer list-none px-3 py-2.5 flex items-center justify-between bg-gray-50 dark:bg-gray-900">
                  <div class="flex items-center gap-2">
                    <span class="font-medium text-gray-900 dark:text-gray-100">{provider.name}</span>
                    <span class="text-[11px] text-gray-500">{provider.kind}</span>
                  </div>
                  <div class="flex items-center gap-2 text-xs">
                    <span class="ds-badge ds-badge-neutral">{providerModels.filter(m => m.is_active).length} active</span>
                    <span class="text-gray-500">{providerModels.length} total</span>
                  </div>
                </summary>
                <div class="max-h-[36vh] overflow-auto border-t border-gray-200 dark:border-gray-800">
                  {#if providerModels.length > 0}
                    <table class="ds-table">
                      <thead>
                        <tr class="ds-table-head-row sticky top-0 bg-gray-50 dark:bg-gray-900 z-10">
                          <th class="ds-table-th">Model</th>
                          <th class="ds-table-th">Active</th>
                          <th class="ds-table-th">Default</th>
                        </tr>
                      </thead>
                      <tbody>
                        {#each providerModels as model}
                          <tr class="ds-table-row">
                            <td class="ds-td-mono">{model.display_name || model.name}</td>
                            <td class="ds-td">
                              <input
                                type="checkbox"
                                class="ds-checkbox"
                                checked={model.is_active}
                                onchange={(e) => updateModel(model, 'is_active', (e.target as HTMLInputElement).checked)}
                              />
                            </td>
                            <td class="ds-td">
                              <input
                                type="radio"
                                class="ds-radio"
                                name={"default-model-" + model.provider_id}
                                checked={model.is_default}
                                onchange={() => updateModel(model, 'is_default', true)}
                              />
                            </td>
                          </tr>
                        {/each}
                      </tbody>
                    </table>
                  {:else}
                    <p class="text-xs text-gray-500 px-3 py-4">No models match current filters for this provider.</p>
                  {/if}
                </div>
              </details>
            {/each}
          </div>
        {/if}

        <div class="ds-card p-3">
          <div class="flex items-center justify-between mb-2">
            <h3 class="text-sm font-semibold text-gray-800 dark:text-gray-200">Global Brain Skill</h3>
            <button class="ds-btn-outline" onclick={() => openSkillSheet()}>Open Skill Sheet</button>
          </div>
          <p class="text-xs text-gray-500 mb-2">Active prompt preview</p>
          <pre class="text-[11px] leading-relaxed whitespace-pre-wrap text-gray-600 dark:text-gray-300 max-h-36 overflow-auto rounded border border-gray-200 dark:border-gray-800 bg-gray-50 dark:bg-gray-900 p-2">{truncate(skillForm.content || '', 1200)}</pre>
        </div>
      {/if}

    {:else if activeTab === 'api-keys'}
      {#if apiKeysLoading}
        <div class="flex items-center justify-center py-12"><Spinner /></div>
      {:else}
        <div class="flex items-center justify-between mb-4">
          <div class="flex items-center gap-2">
            <Key size={16} class="text-ch-blue" />
            <h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">API Keys</h2>
          </div>
          <button class="ds-btn-primary" onclick={() => createKeySheetOpen = true}>
            <Plus size={14} />
            Create Key
          </button>
        </div>

        {#if apiKeys.length === 0}
          <div class="ds-empty">
            <p class="text-sm text-gray-500 mb-2">No API keys yet. Create one to enable programmatic access.</p>
            <button class="ds-btn-primary" onclick={() => createKeySheetOpen = true}>Create First Key</button>
          </div>
        {:else}
          <div class="ds-table-wrap">
            <table class="ds-table">
              <thead>
                <tr class="ds-table-head-row">
                  <th class="ds-table-th">Name</th>
                  <th class="ds-table-th">Prefix</th>
                  <th class="ds-table-th">Role</th>
                  <th class="ds-table-th">Scopes</th>
                  <th class="ds-table-th">Rate Limit</th>
                  <th class="ds-table-th">Status</th>
                  <th class="ds-table-th">Last Used</th>
                  <th class="ds-table-th-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {#each apiKeys as key}
                  <tr class="ds-table-row">
                    <td class="ds-td-strong">{key.name}</td>
                    <td class="ds-td-mono">{key.key_prefix}...</td>
                    <td class="ds-td">{key.role}</td>
                    <td class="ds-td-mono text-xs">{key.scopes}</td>
                    <td class="ds-td-mono">{key.rate_limit_rpm}/min</td>
                    <td class="ds-td">
                      <button
                        class="ds-badge {key.is_active ? 'ds-badge-success' : 'ds-badge-neutral'}"
                        onclick={() => toggleKeyActive(key)}
                      >
                        {key.is_active ? 'Active' : 'Inactive'}
                      </button>
                    </td>
                    <td class="ds-td-mono text-xs">{key.last_used_at ? formatTime(key.last_used_at) : 'Never'}</td>
                    <td class="ds-td-right">
                      <button class="text-xs text-red-500 hover:text-red-700" onclick={() => deletingKey = key}>Delete</button>
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {/if}
      {/if}

    {/if}
  </div>
</div>
