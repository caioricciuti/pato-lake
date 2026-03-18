<script lang="ts">
  import { Handle, Position } from '@xyflow/svelte'
  import { Database } from 'lucide-svelte'

  interface Props {
    data: {
      label: string
      node_type: string
      config?: Record<string, unknown>
    }
  }

  let { data }: Props = $props()
</script>

<div class="rounded-lg border-2 border-yellow-400 dark:border-yellow-600 bg-yellow-50 dark:bg-yellow-900/20 shadow-sm min-w-[160px]">
  <div class="flex items-center gap-2 px-3 py-2">
    <Database size={16} class="text-yellow-600 dark:text-yellow-400 shrink-0" />
    <div class="min-w-0">
      <div class="text-xs font-medium text-gray-800 dark:text-gray-200 truncate">{data.label}</div>
      <div class="text-[10px] text-gray-500 dark:text-gray-400">DuckDB</div>
    </div>
  </div>
  <Handle type="target" position={Position.Left} class="!bg-yellow-500 !w-3 !h-3 !border-2 !border-white dark:!border-gray-900" />
</div>
