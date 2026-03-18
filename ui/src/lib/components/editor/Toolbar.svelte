<script lang="ts">
  import Button from '../common/Button.svelte'
  import { Play, Square, AlignLeft, BookOpen, Save, Clock } from 'lucide-svelte'

  interface Props {
    running?: boolean
    onrun: () => void
    oncancel?: () => void
    onformat?: () => void
    onexplain?: () => void
    onsave?: () => void
    onhistory?: () => void
  }

  let { running = false, onrun, oncancel, onformat, onexplain, onsave, onhistory }: Props = $props()
</script>

<div class="flex items-center gap-2 px-2 py-1.5 border-b border-gray-200 dark:border-gray-800 bg-gray-100/50 dark:bg-gray-900/50">
  {#if running && oncancel}
    <Button size="sm" variant="ghost" onclick={oncancel}>
      <Square size={14} class="text-red-400" />
      <span class="text-red-400">Cancel</span>
    </Button>
  {:else}
    <Button size="sm" onclick={onrun} loading={running}>
      <Play size={14} />
      Run
    </Button>
  {/if}

  {#if onformat}
    <Button size="sm" variant="ghost" onclick={onformat}>
      <AlignLeft size={14} />
      Format
    </Button>
  {/if}

  {#if onexplain}
    <Button size="sm" variant="ghost" onclick={onexplain}>
      <BookOpen size={14} />
      Explain
    </Button>
  {/if}

  {#if onhistory}
    <Button size="sm" variant="ghost" onclick={onhistory}>
      <Clock size={14} />
      History
    </Button>
  {/if}

  <div class="flex-1"></div>

  {#if onsave}
    <Button size="sm" variant="ghost" onclick={onsave}>
      <Save size={14} />
      Save
    </Button>
  {/if}
</div>
