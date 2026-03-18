<script lang="ts">
  import { fade, fly } from 'svelte/transition'
  import { X } from 'lucide-svelte'

  interface Props {
    open: boolean
    onclose: () => void
  }

  let { open, onclose }: Props = $props()

  const isMac = $derived(
    typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform)
  )

  const mod = $derived(isMac ? 'Cmd' : 'Ctrl')

  const categories = $derived([
    {
      label: 'General',
      shortcuts: [
        { keys: ['?'], action: 'Show this help' },
        { keys: [mod, 'K'], action: 'Command palette' },
        { keys: [mod, 'B'], action: 'Toggle sidebar' },
      ],
    },
    {
      label: 'Editor',
      shortcuts: [
        { keys: [mod, 'Enter'], action: 'Run query' },
        { keys: [mod, 'S'], action: 'Save query/notebook' },
      ],
    },
    {
      label: 'Tabs',
      shortcuts: [
        { keys: [mod, 'Shift', 'N'], action: 'New query tab' },
        { keys: ['Alt', 'N'], action: 'New query tab' },
        { keys: ['Alt', 'W'], action: 'Close tab' },
        { keys: ['Alt', 'D'], action: 'Duplicate tab' },
        { keys: ['Alt', 'S'], action: 'Split tab' },
        { keys: ['F2'], action: 'Rename tab' },
      ],
    },
  ])

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === 'Escape') onclose()
  }
</script>

<svelte:window onkeydown={handleKeydown} />

{#if open}
  <!-- Backdrop -->
  <div
    class="fixed inset-0 z-[70] bg-black/60 backdrop-blur-sm"
    transition:fade={{ duration: 150 }}
    onclick={onclose}
    role="presentation"
  ></div>

  <!-- Dialog -->
  <div class="fixed inset-0 z-[80] flex items-center justify-center p-4">
    <div
      class="bg-gray-50 dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-xl shadow-2xl max-w-lg w-full max-h-[80vh] overflow-auto"
      transition:fly={{ y: 24, duration: 200 }}
      role="dialog"
      aria-modal="true"
      tabindex="0"
      onclick={(e: MouseEvent) => e.stopPropagation()}
      onkeydown={(e: KeyboardEvent) => e.stopPropagation()}
    >
      <!-- Header -->
      <div class="flex items-center justify-between px-5 py-4 border-b border-gray-200 dark:border-gray-800">
        <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Keyboard Shortcuts</h2>
        <button
          class="text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 cursor-pointer"
          onclick={onclose}
        >
          <X size={18} />
        </button>
      </div>

      <!-- Content -->
      <div class="p-5 space-y-5">
        {#each categories as category}
          <section>
            <h3 class="text-xs font-semibold uppercase tracking-wider text-gray-500 mb-2">
              {category.label}
            </h3>
            <div class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1.5 items-center">
              {#each category.shortcuts as shortcut}
                <div class="flex items-center gap-1">
                  {#each shortcut.keys as key, i}
                    {#if i > 0}
                      <span class="text-gray-400 text-xs">+</span>
                    {/if}
                    <kbd class="px-1.5 py-0.5 text-[11px] font-mono bg-gray-200 dark:bg-gray-800 rounded text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-gray-700">
                      {key}
                    </kbd>
                  {/each}
                </div>
                <span class="text-sm text-gray-600 dark:text-gray-400">{shortcut.action}</span>
              {/each}
            </div>
          </section>
        {/each}
      </div>
    </div>
  </div>
{/if}
