export interface NavItem {
  label: string
  href: string
  description: string
}

export interface NavGroup {
  label: string
  items: NavItem[]
}

export const navigation: NavGroup[] = [
  {
    label: 'Start',
    items: [
      { label: 'Overview', href: '/', description: 'What thinDB is and where it fits' },
      { label: 'Download', href: '/download/', description: 'Prebuilt server bundles for Linux, macOS, and Windows' },
      { label: 'Install and run', href: '/getting-started/', description: 'Build the server and run your first query' },
      { label: 'Connect a client', href: '/connect/', description: 'MySQL, PostgreSQL, and native wire connections' },
    ],
  },
  {
    label: 'SQL guide',
    items: [
      { label: 'Tables and data', href: '/sql/tables/', description: 'DDL, inserts, updates, deletes, and COPY' },
      { label: 'Query data', href: '/sql/queries/', description: 'SELECT, filters, grouping, CTEs, and subqueries' },
      { label: 'Joins', href: '/sql/joins/', description: 'Join types, predicates, and execution behavior' },
      { label: 'Window functions', href: '/sql/windows/', description: 'Ranking, value, and aggregate windows' },
      { label: 'Built-in functions', href: '/sql/functions/', description: 'Scalar and aggregate SQL function reference' },
    ],
  },
  {
    label: 'Extensibility',
    items: [
      { label: 'SQL functions', href: '/functions/sql/', description: 'Reusable parameterized SQL table functions' },
      { label: 'Zig functions', href: '/functions/zig/', description: 'Compile source on the server or load a library' },
      { label: 'Embedded UDFs', href: '/functions/embedded/', description: 'Register scalar, aggregate, and table UDFs in Zig' },
    ],
  },
  {
    label: 'Operate',
    items: [
      { label: 'Server configuration', href: '/operations/server/', description: 'Ports, memory, parallelism, and diagnostics' },
      { label: 'Storage and durability', href: '/operations/storage/', description: 'WAL, flush, compaction, snapshots, and recovery' },
    ],
  },
  {
    label: 'Reference',
    items: [
      { label: 'Data types', href: '/reference/data-types/', description: 'SQL types, ranges, nullability, and mappings' },
      { label: 'Compatibility', href: '/reference/compatibility/', description: 'Supported features and deliberate limitations' },
    ],
  },
]

export const flatNavigation = navigation.flatMap((group) => group.items)
