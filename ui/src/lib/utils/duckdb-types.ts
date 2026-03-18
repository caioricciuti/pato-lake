/** Map DuckDB types to display categories for cell rendering */
export type DisplayType = 'number' | 'string' | 'date' | 'bool' | 'json' | 'null' | 'unknown'

const NUMBER_TYPES = new Set([
  'TINYINT', 'SMALLINT', 'INTEGER', 'INT', 'BIGINT', 'HUGEINT',
  'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT',
  'FLOAT', 'DOUBLE', 'DECIMAL',
])

const STRING_TYPES = new Set([
  'VARCHAR', 'TEXT', 'STRING', 'CHAR', 'BPCHAR', 'UUID', 'ENUM',
])

const DATE_TYPES = new Set([
  'DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMPTZ',
  'TIMESTAMP_S', 'TIMESTAMP_MS', 'TIMESTAMP_NS', 'INTERVAL',
])

const BOOL_TYPES = new Set(['BOOLEAN', 'BOOL'])

const BINARY_TYPES = new Set(['BLOB', 'BYTEA'])

const COMPLEX_TYPES = new Set(['LIST', 'MAP', 'STRUCT', 'UNION', 'ARRAY', 'JSON'])

export function getDisplayType(duckdbType: string): DisplayType {
  if (!duckdbType) return 'null'

  // Normalize: uppercase, strip leading/trailing whitespace
  const raw = duckdbType.trim().toUpperCase()

  // NULL type
  if (raw === 'NULL') return 'null'

  // Extract base type (handle parameterized types like DECIMAL(18,3), VARCHAR(255), LIST(INT))
  const base = raw.replace(/\(.*\)$/, '').trim()

  if (NUMBER_TYPES.has(base)) return 'number'
  if (DATE_TYPES.has(base)) return 'date'
  if (BOOL_TYPES.has(base)) return 'bool'
  if (STRING_TYPES.has(base)) return 'string'
  if (BINARY_TYPES.has(base)) return 'string'
  if (COMPLEX_TYPES.has(base)) return 'json'

  return 'unknown'
}

/** Check if a value should be right-aligned (numbers) */
export function isRightAligned(duckdbType: string): boolean {
  return getDisplayType(duckdbType) === 'number'
}
