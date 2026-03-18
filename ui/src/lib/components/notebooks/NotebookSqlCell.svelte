<script lang="ts">
  import SqlEditor from '../editor/SqlEditor.svelte'

  interface Props {
    value: string
    onrun: (sql: string) => void
    onchange: (content: string) => void
  }

  let { value, onrun, onchange }: Props = $props()
</script>

<div class="notebook-sql-cell">
  <SqlEditor {value} {onrun} {onchange} />
</div>

<style>
  .notebook-sql-cell {
    min-height: 60px;
    max-height: 400px;
    overflow: auto;
  }
  .notebook-sql-cell :global(.cm-editor) {
    height: auto !important;
    min-height: 60px;
  }
  .notebook-sql-cell :global(.cm-scroller) {
    min-height: 60px;
  }
  .notebook-sql-cell :global(.cm-gutters) {
    display: none;
  }
</style>
