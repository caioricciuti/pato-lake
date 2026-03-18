/**
 * safe-json.ts
 *
 * Precision-safe JSON parsing for DuckDB results.
 *
 * Problem: JavaScript's JSON.parse() converts all numbers to IEEE 754 Float64,
 * which only has ~15.9 significant digits. Large integer values like
 * order IDs (e.g. 816687988383154176) are silently rounded to a phantom value
 * (816687988383154200), making them useless as identifiers.
 *
 * Solution: Intercept large integers before they lose precision:
 *   1. Primary: Use TC39 Stage 4 reviver `context.source` (native, zero-cost).
 *      Supported in Chrome 114+, Firefox 135+, Safari 18.4+ (~86% of users).
 *   2. Fallback: json-bigint with `storeAsString: true` for older browsers.
 *
 * Large integers are returned as strings. Consumer code must handle both
 * `number` (safe integers) and `string` (large integers) for numeric columns.
 */

import JSONbig from 'json-bigint'

// Feature-detect TC39 reviver context.source support
let hasReviverSource = false
try {
  // TC39 Stage 4: JSON.parse reviver receives a third `context` argument.
  // TypeScript's lib types don't yet include it, so we cast through unknown.
  const reviverWithContext = (_key: string, _value: unknown, ctx: { source?: string }) => {
    if (typeof ctx?.source === 'string') hasReviverSource = true
    return _value
  }
  JSON.parse('1', reviverWithContext as (key: string, value: unknown) => unknown)
} catch {
  // Older environments may throw on the extra argument; fallback is used
}

// Lazy-initialised fallback parser (json-bigint allocates a parser object)
let _fallbackParser: ReturnType<typeof JSONbig> | null = null
function getFallbackParser() {
  if (!_fallbackParser) {
    _fallbackParser = JSONbig({ storeAsString: true })
  }
  return _fallbackParser
}

/**
 * Parse a JSON string with precision-safe handling of large integers.
 *
 * Safe integers (|n| <= 2^53 - 1) are returned as `number`, exactly as
 * standard JSON.parse does. Large integers are returned as `string` to
 * preserve all digits. Everything else is unchanged.
 */
export function safeParse(text: string): unknown {
  if (hasReviverSource) {
    // TC39 Stage 4: reviver receives a third `context` argument with `source`.
    // TypeScript's lib types don't include this yet, so we cast through unknown.
    const reviverWithContext = (_key: string, value: unknown, ctx: { source: string }) => {
      if (
        typeof value === 'number' &&
        !Number.isSafeInteger(value) &&
        /^-?\d+$/.test(ctx.source)
      ) {
        return ctx.source
      }
      return value
    }
    return JSON.parse(text, reviverWithContext as (key: string, value: unknown) => unknown)
  }

  // Fallback for browsers without reviver context.source support
  return getFallbackParser().parse(text)
}
