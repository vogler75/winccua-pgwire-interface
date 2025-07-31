// Constants used throughout the WinCC UA PostgreSQL Wire Protocol Server

// Database and schema names
pub const DATABASE_NAME: &str = "winccua";
pub const SCHEMA_PG_CATALOG: &str = "pg_catalog";
pub const SCHEMA_PUBLIC: &str = "public";
pub const SCHEMA_INFORMATION_SCHEMA: &str = "information_schema";

// User names
pub const USER_POSTGRES: &str = "postgres";

// PostgreSQL catalog table names
pub const TABLE_PG_NAMESPACE: &str = "pg_namespace";
pub const TABLE_PG_CLASS: &str = "pg_class";
pub const TABLE_PG_PROC: &str = "pg_proc";
pub const TABLE_PG_TYPE: &str = "pg_type";
pub const TABLE_PG_CONSTRAINT: &str = "pg_constraint";
pub const TABLE_PG_STAT_ACTIVITY: &str = "pg_stat_activity";

// Virtual table names
pub const TABLE_TAGVALUES: &str = "tagvalues";
pub const TABLE_LOGGED_TAG_VALUES: &str = "loggedtagvalues";
pub const TABLE_ACTIVE_ALARMS: &str = "activealarms";
pub const TABLE_LOGGED_ALARMS: &str = "loggedalarms";
pub const TABLE_TAG_LIST: &str = "taglist";

// Information schema table names
pub const TABLE_INFORMATION_SCHEMA_TABLES: &str = "information_schema.tables";
pub const TABLE_INFORMATION_SCHEMA_COLUMNS: &str = "information_schema.columns";

// Oracle-style dual table name for FROM-less queries
pub const TABLE_DUAL: &str = "dual";

// Common PostgreSQL OIDs
pub const OID_PG_CATALOG_NAMESPACE: i64 = 11;
pub const OID_PUBLIC_NAMESPACE: i64 = 2200;
pub const OID_INFORMATION_SCHEMA_NAMESPACE: i64 = 13427;
pub const OID_POSTGRES_USER: i64 = 10;

// Default values
pub const DEFAULT_WILDCARD: &str = "*";