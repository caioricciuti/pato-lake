const SKIP_LIMIT_PATTERN = /^\s*(INSERT|CREATE|DROP|ALTER|TRUNCATE|RENAME|ATTACH|DETACH|COPY|GRANT|REVOKE|SET|USE|SHOW|DESCRIBE|EXPLAIN|SUMMARIZE|PRAGMA|INSTALL|LOAD|CALL|CHECKPOINT|EXPORT|IMPORT)\b/i
const LIMIT_PATTERN = /\bLIMIT\s+\d+/i

/** Check if a query should skip auto-LIMIT (DDL/DML/meta commands) */
export function isWriteQuery(query: string): boolean {
  // Strip leading SQL comments
  const stripped = query.replace(/^\s*--.*$/gm, '').trim()
  return SKIP_LIMIT_PATTERN.test(stripped)
}

/** Check if a query already has a LIMIT clause */
export function hasLimit(query: string): boolean {
  return LIMIT_PATTERN.test(query)
}

/** Append a default LIMIT if the query doesn't have one (for SELECT only) */
export function appendDefaultLimit(query: string, limit = 1000): string {
  if (isWriteQuery(query) || hasLimit(query)) return query
  return query.replace(/;\s*$/, '') + ` LIMIT ${limit}`
}
