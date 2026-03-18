/** Database in the schema tree */
export interface Database {
  name: string
  tables?: Table[]
  expanded?: boolean
  loading?: boolean
}

/** Table in the schema tree */
export interface Table {
  name: string
  schema?: string
  table_type?: string
  columns?: Column[]
  expanded?: boolean
  loading?: boolean
}

/** Column in the schema tree */
export interface Column {
  name: string
  type: string
  is_nullable?: boolean
  default_type?: string
  default_expression?: string
  comment?: string
}
