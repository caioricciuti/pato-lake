<script lang="ts">
  interface Props {
    width?: string
    height?: string
    rounded?: string
    class?: string
  }

  let {
    width = '100%',
    height = '1rem',
    rounded = 'rounded',
    class: cls = '',
  }: Props = $props()
</script>

<div
  class="animate-pulse bg-gray-200 dark:bg-gray-800 {rounded} {cls}"
  style:width
  style:height
></div>
