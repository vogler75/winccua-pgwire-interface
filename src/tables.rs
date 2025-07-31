use pgwire::api::Type;

#[derive(Debug, Clone, PartialEq)]
pub enum VirtualTable {
    TagValues,
    LoggedTagValues,
    ActiveAlarms,
    LoggedAlarms,
    TagList,
    InformationSchemaTables,
    InformationSchemaColumns,
    PgStatActivity,
    PgNamespace,
    PgClass,
    PgProc,
    PgType,
    PgConstraint,
    FromLessQuery, // For queries without FROM clause like SELECT 1, SELECT VERSION(), etc.
}

impl ToString for VirtualTable {
    fn to_string(&self) -> String {
        match self {
            VirtualTable::TagValues => crate::constants::TABLE_TAGVALUES.to_string(),
            VirtualTable::LoggedTagValues => crate::constants::TABLE_LOGGED_TAG_VALUES.to_string(),
            VirtualTable::ActiveAlarms => crate::constants::TABLE_ACTIVE_ALARMS.to_string(),
            VirtualTable::LoggedAlarms => crate::constants::TABLE_LOGGED_ALARMS.to_string(),
            VirtualTable::TagList => crate::constants::TABLE_TAG_LIST.to_string(),
            VirtualTable::InformationSchemaTables => crate::constants::TABLE_INFORMATION_SCHEMA_TABLES.to_string(),
            VirtualTable::InformationSchemaColumns => crate::constants::TABLE_INFORMATION_SCHEMA_COLUMNS.to_string(),
            VirtualTable::PgStatActivity => crate::constants::TABLE_PG_STAT_ACTIVITY.to_string(),
            VirtualTable::PgNamespace => crate::constants::TABLE_PG_NAMESPACE.to_string(),
            VirtualTable::PgClass => crate::constants::TABLE_PG_CLASS.to_string(),
            VirtualTable::PgProc => crate::constants::TABLE_PG_PROC.to_string(),
            VirtualTable::PgType => crate::constants::TABLE_PG_TYPE.to_string(),
            VirtualTable::PgConstraint => crate::constants::TABLE_PG_CONSTRAINT.to_string(),
            VirtualTable::FromLessQuery => crate::constants::TABLE_DUAL.to_string(),
        }
    }
}

impl VirtualTable {
    pub fn from_name(name: &str) -> Option<Self> {
        let lower_name = name.to_lowercase();
        if lower_name.starts_with("information_schema.") {
            match lower_name.strip_prefix("information_schema.") {
                Some("tables") => Some(Self::InformationSchemaTables),
                Some("columns") => Some(Self::InformationSchemaColumns),
                _ => None,
            }
        } else if lower_name.starts_with("pg_catalog.") {
            match lower_name.strip_prefix("pg_catalog.") {
                Some("pg_namespace") => Some(Self::PgNamespace),
                Some("pg_class") => Some(Self::PgClass),
                Some("pg_proc") => Some(Self::PgProc),
                Some("pg_type") => Some(Self::PgType),
                Some("pg_constraint") => Some(Self::PgConstraint),
                _ => None,
            }
        } else if lower_name.starts_with("public.") {
            // Handle public schema qualified names
            match lower_name.strip_prefix("public.") {
                Some("tagvalues") => Some(Self::TagValues),
                Some("loggedtagvalues") => Some(Self::LoggedTagValues),
                Some("activealarms") => Some(Self::ActiveAlarms),
                Some("loggedalarms") => Some(Self::LoggedAlarms),
                Some("taglist") => Some(Self::TagList),
                _ => None,
            }
        } else {
            match lower_name.as_str() {
                s if s == crate::constants::TABLE_TAGVALUES => Some(Self::TagValues),
                s if s == crate::constants::TABLE_LOGGED_TAG_VALUES => Some(Self::LoggedTagValues),
                s if s == crate::constants::TABLE_ACTIVE_ALARMS => Some(Self::ActiveAlarms),
                s if s == crate::constants::TABLE_LOGGED_ALARMS => Some(Self::LoggedAlarms),
                s if s == crate::constants::TABLE_TAG_LIST => Some(Self::TagList),
                s if s == crate::constants::TABLE_PG_STAT_ACTIVITY => Some(Self::PgStatActivity),
                s if s == crate::constants::TABLE_PG_NAMESPACE => Some(Self::PgNamespace),
                s if s == crate::constants::TABLE_PG_CLASS => Some(Self::PgClass),
                s if s == crate::constants::TABLE_PG_PROC => Some(Self::PgProc),
                s if s == crate::constants::TABLE_PG_TYPE => Some(Self::PgType),
                s if s == crate::constants::TABLE_PG_CONSTRAINT => Some(Self::PgConstraint),
                _ => None,
            }
        }
    }

    pub fn get_schema(&self) -> Vec<(&'static str, Type)> {
        match self {
            Self::TagValues => vec![
                ("tag_name", Type::TEXT),
                ("timestamp", Type::TIMESTAMP),
                ("timestamp_ms", Type::INT8),
                ("numeric_value", Type::NUMERIC),
                ("string_value", Type::TEXT),
                ("quality", Type::TEXT),
            ],
            Self::LoggedTagValues => vec![
                ("tag_name", Type::TEXT),
                ("timestamp", Type::TIMESTAMP),
                ("timestamp_ms", Type::INT8),
                ("numeric_value", Type::NUMERIC),
                ("string_value", Type::TEXT),
                ("quality", Type::TEXT),
            ],
            Self::ActiveAlarms => vec![
                ("name", Type::TEXT),
                ("instance_id", Type::INT4),
                ("alarm_group_id", Type::INT4),
                ("raise_time", Type::TIMESTAMP),
                ("acknowledgment_time", Type::TIMESTAMP),
                ("clear_time", Type::TIMESTAMP),
                ("reset_time", Type::TIMESTAMP),
                ("modification_time", Type::TIMESTAMP),
                ("state", Type::TEXT),
                ("priority", Type::INT4),
                ("event_text", Type::TEXT),
                ("info_text", Type::TEXT),
                ("origin", Type::TEXT),
                ("area", Type::TEXT),
                ("value", Type::TEXT),
                ("host_name", Type::TEXT),
                ("user_name", Type::TEXT),
            ],
            Self::LoggedAlarms => vec![
                ("name", Type::TEXT),
                ("instance_id", Type::INT4),
                ("alarm_group_id", Type::INT4),
                ("raise_time", Type::TIMESTAMP),
                ("acknowledgment_time", Type::TIMESTAMP),
                ("clear_time", Type::TIMESTAMP),
                ("reset_time", Type::TIMESTAMP),
                ("modification_time", Type::TIMESTAMP),
                ("state", Type::TEXT),
                ("priority", Type::INT4),
                ("event_text", Type::TEXT),
                ("info_text", Type::TEXT),
                ("origin", Type::TEXT),
                ("area", Type::TEXT),
                ("value", Type::TEXT),
                ("host_name", Type::TEXT),
                ("user_name", Type::TEXT),
                ("duration", Type::TEXT),
            ],
            Self::TagList => vec![
                ("tag_name", Type::TEXT),
                ("display_name", Type::TEXT),
                ("object_type", Type::TEXT),
                ("data_type", Type::TEXT),
            ],
            Self::InformationSchemaTables => vec![
                ("table_catalog", Type::TEXT),
                ("table_schema", Type::TEXT),
                ("table_name", Type::TEXT),
                ("table_type", Type::TEXT),
                ("self_referencing_column_name", Type::TEXT),
                ("reference_generation", Type::TEXT),
                ("user_defined_type_catalog", Type::TEXT),
                ("user_defined_type_schema", Type::TEXT),
                ("user_defined_type_name", Type::TEXT),
                ("is_insertable_into", Type::TEXT),
                ("is_typed", Type::TEXT),
                ("commit_action", Type::TEXT),
            ],
            Self::InformationSchemaColumns => vec![
                ("table_catalog", Type::TEXT),
                ("table_schema", Type::TEXT),
                ("table_name", Type::TEXT),
                ("column_name", Type::TEXT),
                ("ordinal_position", Type::INT4),
                ("column_default", Type::TEXT),
                ("is_nullable", Type::TEXT),
                ("data_type", Type::TEXT),
                ("character_maximum_length", Type::INT4),
                ("character_octet_length", Type::INT4),
                ("numeric_precision", Type::INT4),
                ("numeric_precision_radix", Type::INT4),
                ("numeric_scale", Type::INT4),
                ("datetime_precision", Type::INT4),
                ("interval_type", Type::TEXT),
                ("interval_precision", Type::INT4),
                ("character_set_catalog", Type::TEXT),
                ("character_set_schema", Type::TEXT),
                ("character_set_name", Type::TEXT),
                ("collation_catalog", Type::TEXT),
                ("collation_schema", Type::TEXT),
                ("collation_name", Type::TEXT),
                ("domain_catalog", Type::TEXT),
                ("domain_schema", Type::TEXT),
                ("domain_name", Type::TEXT),
                ("udt_catalog", Type::TEXT),
                ("udt_schema", Type::TEXT),
                ("udt_name", Type::TEXT),
                ("scope_catalog", Type::TEXT),
                ("scope_schema", Type::TEXT),
                ("scope_name", Type::TEXT),
                ("maximum_cardinality", Type::INT4),
                ("dtd_identifier", Type::TEXT),
                ("is_self_referencing", Type::TEXT),
                ("is_identity", Type::TEXT),
                ("identity_generation", Type::TEXT),
                ("identity_start", Type::TEXT),
                ("identity_increment", Type::TEXT),
                ("identity_maximum", Type::TEXT),
                ("identity_minimum", Type::TEXT),
                ("identity_cycle", Type::TEXT),
                ("is_generated", Type::TEXT),
                ("generation_expression", Type::TEXT),
                ("is_updatable", Type::TEXT),
            ],
            Self::PgStatActivity => vec![
                ("datid", Type::INT4),           // OID of database (always 0 for now)
                ("datname", Type::TEXT),         // Database name
                ("pid", Type::INT4),             // Process ID (connection ID)
                ("usename", Type::TEXT),         // Username
                ("application_name", Type::TEXT), // Client application name
                ("client_addr", Type::TEXT),     // Client IP address
                ("client_hostname", Type::TEXT), // Client hostname (NULL for now)
                ("client_port", Type::INT4),     // Client port
                ("backend_start", Type::TIMESTAMP), // Connection start time
                ("query_start", Type::TIMESTAMP),   // Query start time
                ("query_stop", Type::TIMESTAMP),    // Query completion time
                ("state", Type::TEXT),           // Connection state
                ("query", Type::TEXT),           // Current/last query
                ("graphql_time", Type::INT8),    // GraphQL execution time in ms
                ("datafusion_time", Type::INT8), // DataFusion execution time in ms
                ("overall_time", Type::INT8),    // Overall query execution time in ms
                ("last_alive_sent", Type::TIMESTAMP), // Last time keep-alive was sent
            ],
            Self::PgNamespace => vec![
                ("oid", Type::INT8),      // Schema OID
                ("nspname", Type::TEXT),  // Schema name
                ("nspowner", Type::INT8), // Owner of the schema
                ("nspacl", Type::TEXT),   // Access privileges (simplified as text)
            ],
            Self::PgClass => vec![
                ("oid", Type::INT8),                // Table/relation OID
                ("relname", Type::TEXT),            // Table/relation name
                ("relnamespace", Type::INT8),       // Schema OID
                ("reltype", Type::INT8),            // OID of row type
                ("relowner", Type::INT8),           // Owner of the relation
                ("relam", Type::INT8),              // Access method
                ("relfilenode", Type::INT8),        // File node
                ("reltablespace", Type::INT8),      // Tablespace
                ("relpages", Type::INT8),           // Size in pages
                ("reltuples", Type::FLOAT4),        // Number of rows
                ("reltoastrelid", Type::INT8),      // TOAST table OID
                ("relhasindex", Type::BOOL),        // Has indexes?
                ("relisshared", Type::BOOL),        // Is shared across databases?
                ("relpersistence", Type::TEXT),     // Persistence type
                ("relkind", Type::TEXT),            // Type of relation
                ("relnatts", Type::INT2),           // Number of user attributes
                ("relchecks", Type::INT2),          // Number of CHECK constraints
                ("relhasrules", Type::BOOL),        // Has rules?
                ("relhastriggers", Type::BOOL),     // Has triggers?
                ("relhassubclass", Type::BOOL),     // Has inheritance children?
                ("relrowsecurity", Type::BOOL),     // Row security enabled?
                ("relforcerowsecurity", Type::BOOL), // Force row security?
                ("relispopulated", Type::BOOL),     // Is materialized view populated?
                ("relreplident", Type::TEXT),       // Replica identity
                ("relispartition", Type::BOOL),     // Is a partition?
                ("relacl", Type::TEXT),             // Access privileges
            ],
            Self::PgProc => vec![
                ("oid", Type::INT8),             // Function OID
                ("proname", Type::TEXT),         // Function name
                ("pronamespace", Type::INT8),    // Schema OID
                ("proowner", Type::INT8),        // Owner
                ("prolang", Type::INT8),         // Language OID
                ("procost", Type::FLOAT4),       // Estimated execution cost
                ("prorows", Type::FLOAT4),       // Estimated rows returned
                ("provariadic", Type::INT8),     // Variadic parameter type
                ("prosupport", Type::TEXT),      // Support function
                ("prokind", Type::TEXT),         // Function kind
                ("prosecdef", Type::BOOL),       // Security definer?
                ("proleakproof", Type::BOOL),    // Is leak-proof?
                ("proisstrict", Type::BOOL),     // Strict?
                ("proretset", Type::BOOL),       // Returns a set?
                ("proisagg", Type::BOOL),        // Is aggregate?
                ("proiswindow", Type::BOOL),     // Is window function?
                ("provolatile", Type::TEXT),     // Volatility
                ("proparallel", Type::TEXT),     // Parallel safety
                ("pronargs", Type::INT2),        // Number of input arguments
                ("pronargdefaults", Type::INT2), // Number of default arguments
                ("prorettype", Type::INT8),      // Return type OID
                ("proargtypes", Type::TEXT),     // Argument types (simplified)
                ("proallargtypes", Type::TEXT),  // All argument types
                ("proargmodes", Type::TEXT),     // Argument modes
                ("proargnames", Type::TEXT),     // Argument names
                ("proargdefaults", Type::TEXT),  // Default expressions
                ("protrftypes", Type::TEXT),     // Transform types
                ("prosrc", Type::TEXT),          // Source code
                ("probin", Type::TEXT),          // Binary location
                ("proconfig", Type::TEXT),       // Configuration settings
                ("proacl", Type::TEXT),          // Access privileges
            ],
            Self::PgType => vec![
                ("oid", Type::INT8),             // Type OID
                ("typname", Type::TEXT),         // Type name
                ("typnamespace", Type::INT8),    // Schema OID
                ("typowner", Type::INT8),        // Owner
                ("typlen", Type::INT2),          // Length
                ("typbyval", Type::BOOL),        // Pass by value?
                ("typtype", Type::TEXT),         // Type category
                ("typcategory", Type::TEXT),     // Category
                ("typispreferred", Type::BOOL),  // Is preferred type?
                ("typisdefined", Type::BOOL),    // Is defined?
                ("typdelim", Type::TEXT),        // Delimiter
                ("typrelid", Type::INT8),        // Related table OID
                ("typelem", Type::INT8),         // Element type OID
                ("typarray", Type::INT8),        // Array type OID
                ("typinput", Type::TEXT),        // Input function
                ("typoutput", Type::TEXT),       // Output function
                ("typmodout", Type::TEXT),       // Type modifier output
                ("typmodin", Type::TEXT),        // Type modifier input
                ("typanalyze", Type::TEXT),      // Analyze function
                ("typalign", Type::TEXT),        // Alignment
                ("typstorage", Type::TEXT),      // Storage type
                ("typnotnull", Type::BOOL),      // Not null?
                ("typbasetype", Type::INT8),     // Base type OID
                ("typtypmod", Type::INT4),       // Type modifier
                ("typndims", Type::INT4),        // Number of dimensions
                ("typcollation", Type::INT8),    // Collation OID
                ("typdefault", Type::TEXT),      // Default value
                ("typacl", Type::TEXT),          // Access privileges
            ],
            Self::PgConstraint => vec![
                ("oid", Type::INT8),             // Constraint OID
                ("conname", Type::TEXT),         // Constraint name
                ("connamespace", Type::INT8),    // Schema OID
                ("contype", Type::TEXT),         // Constraint type
                ("condeferrable", Type::BOOL),   // Deferrable?
                ("condeferred", Type::BOOL),     // Initially deferred?
                ("convalidated", Type::BOOL),    // Validated?
                ("conrelid", Type::INT8),        // Related table OID
                ("contypid", Type::INT8),        // Related type OID
                ("conind", Type::INT8),          // Related index OID
                ("confrelid", Type::INT8),       // Foreign table OID
                ("confupdtype", Type::TEXT),     // Foreign key update action
                ("confdeltype", Type::TEXT),     // Foreign key delete action
                ("confmatchtype", Type::TEXT),   // Foreign key match type
                ("conislocal", Type::BOOL),      // Is local?
                ("coninhcount", Type::INT2),     // Inheritance count
                ("connoinherit", Type::BOOL),    // No inherit?
                ("conkey", Type::TEXT),          // Constraint key columns
                ("confkey", Type::TEXT),         // Foreign key columns
                ("conpfeqop", Type::TEXT),       // PK = FK operator
                ("conppeqop", Type::TEXT),       // PK = PK operator
                ("conffeqop", Type::TEXT),       // FK = FK operator
                ("conexclop", Type::TEXT),       // Exclusion operators
                ("conbin", Type::TEXT),          // Check constraint expression
                ("consrc", Type::TEXT),          // Check constraint source
            ],
            Self::FromLessQuery => vec![
                // Empty schema - FROM-less queries don't have predefined columns
                // The actual columns will be determined by the SELECT expressions
            ],
        }
    }

    pub fn get_column_names(&self) -> Vec<&'static str> {
        self.get_schema().into_iter().map(|(name, _)| name).collect()
    }

    #[allow(dead_code)]
    pub fn get_column_types(&self) -> Vec<Type> {
        self.get_schema().into_iter().map(|(_, typ)| typ).collect()
    }

    pub fn has_column(&self, column: &str) -> bool {
        self.get_column_names().contains(&column) || self.is_virtual_column(column)
    }

    pub fn is_virtual_column(&self, column: &str) -> bool {
        match self {
            Self::TagList => matches!(column, "language"),
            Self::LoggedAlarms => matches!(column, "filterString" | "system_name" | "filter_language"),
            _ => false,
        }
    }

    pub fn is_selectable_column(&self, column: &str) -> bool {
        self.get_column_names().contains(&column) && !self.is_virtual_column(column)
    }
}

#[derive(Debug, Clone)]
pub struct ColumnFilter {
    pub column: String,
    pub operator: FilterOperator,
    pub value: FilterValue,
}

#[derive(Debug, Clone)]
pub enum FilterOperator {
    Equal,
    NotEqual,
    Like,
    In,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Between,
}

#[derive(Debug, Clone)]
pub enum FilterValue {
    String(String),
    Number(f64),
    Integer(i64),
    Timestamp(String),
    List(Vec<String>),
    Range(Box<FilterValue>, Box<FilterValue>),
}

impl FilterValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_number(&self) -> Option<f64> {
        match self {
            Self::Number(n) => Some(*n),
            Self::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            Self::Number(n) => Some(*n as i64),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<String>> {
        match self {
            Self::List(list) => Some(list),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SqlResult {
    Query(QueryInfo),
    SetStatement(String), // Contains the SET command that was executed
}

#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub table: VirtualTable,
    pub columns: Vec<String>,
    #[allow(dead_code)]
    pub column_mappings: std::collections::HashMap<String, String>, // alias -> original_column
    pub filters: Vec<ColumnFilter>,
    pub limit: Option<i64>,
    pub order_by: Option<OrderBy>,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub ascending: bool,
}

impl QueryInfo {
    pub fn has_required_tag_filter(&self) -> bool {
        match self.table {
            VirtualTable::TagValues | VirtualTable::LoggedTagValues => {
                self.filters.iter().any(|f| {
                    f.column == "tag_name" && matches!(
                        f.operator, 
                        FilterOperator::Equal | FilterOperator::In | FilterOperator::Like
                    )
                })
            }
            _ => true, // Alarms don't require tag filters
        }
    }

    pub fn get_tag_names(&self) -> Vec<String> {
        for filter in &self.filters {
            if filter.column == "tag_name" {
                match &filter.operator {
                    FilterOperator::Equal => {
                        if let Some(name) = filter.value.as_string() {
                            return vec![name.to_string()];
                        }
                    }
                    FilterOperator::In => {
                        if let Some(names) = filter.value.as_list() {
                            return names.clone();
                        }
                    }
                    FilterOperator::Like => {
                        // LIKE patterns will be resolved via browse function
                        // Return empty here since resolve_like_patterns handles this
                        return vec![];
                    }
                    _ => {}
                }
            }
        }
        vec![]
    }

    pub fn get_timestamp_filter(&self) -> Option<(Option<String>, Option<String>)> {
        let mut start_time = None;
        let mut end_time = None;

        for filter in &self.filters {
            if filter.column == "timestamp" {
                match &filter.operator {
                    FilterOperator::GreaterThan | FilterOperator::GreaterThanOrEqual => {
                        if let FilterValue::Timestamp(ts) = &filter.value {
                            start_time = Some(ts.clone());
                        }
                    }
                    FilterOperator::LessThan | FilterOperator::LessThanOrEqual => {
                        if let FilterValue::Timestamp(ts) = &filter.value {
                            end_time = Some(ts.clone());
                        }
                    }
                    FilterOperator::Between => {
                        if let FilterValue::Range(start, end) = &filter.value {
                            if let FilterValue::Timestamp(ts) = start.as_ref() {
                                start_time = Some(ts.clone());
                            }
                            if let FilterValue::Timestamp(ts) = end.as_ref() {
                                end_time = Some(ts.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        if start_time.is_some() || end_time.is_some() {
            Some((start_time, end_time))
        } else {
            None
        }
    }

    pub fn requires_browse(&self) -> bool {
        self.filters.iter().any(|f| {
            f.column == "tag_name" && matches!(f.operator, FilterOperator::Like)
        })
    }

    pub fn get_like_patterns(&self) -> Vec<String> {
        self.filters
            .iter()
            .filter_map(|f| {
                if f.column == "tag_name" && matches!(f.operator, FilterOperator::Like) {
                    f.value.as_string().map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_name_filters(&self) -> Vec<String> {
        for filter in &self.filters {
            if filter.column == "tag_name" {
                match &filter.operator {
                    FilterOperator::Equal => {
                        if let Some(name) = filter.value.as_string() {
                            return vec![name.to_string()];
                        }
                    }
                    FilterOperator::In => {
                        if let Some(names) = filter.value.as_list() {
                            return names.clone();
                        }
                    }
                    FilterOperator::Like => {
                        if let Some(pattern) = filter.value.as_string() {
                            return vec![pattern.to_string()];
                        }
                    }
                    _ => {}
                }
            }
        }
        vec!["*".to_string()] // Default wildcard
    }

    pub fn get_object_type_filters(&self) -> Vec<String> {
        for filter in &self.filters {
            if filter.column == "object_type" {
                match &filter.operator {
                    FilterOperator::Equal => {
                        if let Some(object_type) = filter.value.as_string() {
                            return vec![object_type.to_string()];
                        }
                    }
                    FilterOperator::In => {
                        if let Some(object_types) = filter.value.as_list() {
                            return object_types.clone();
                        }
                    }
                    _ => {}
                }
            }
        }
        vec![]
    }

    pub fn get_language_filter(&self) -> Option<String> {
        for filter in &self.filters {
            if filter.column == "language" && matches!(filter.operator, FilterOperator::Equal) {
                return filter.value.as_string().map(|s| s.to_string());
            }
        }
        None
    }

    // Methods for LoggedAlarms virtual columns
    pub fn get_filter_string(&self) -> Option<String> {
        for filter in &self.filters {
            if filter.column == "filterString" && matches!(filter.operator, FilterOperator::Equal) {
                return filter.value.as_string().map(|s| s.to_string());
            }
        }
        None
    }

    pub fn get_system_names(&self) -> Vec<String> {
        for filter in &self.filters {
            if filter.column == "system_name" {
                match &filter.operator {
                    FilterOperator::Equal => {
                        if let Some(name) = filter.value.as_string() {
                            return vec![name.to_string()];
                        }
                    }
                    FilterOperator::In => {
                        if let Some(names) = filter.value.as_list() {
                            return names.clone();
                        }
                    }
                    _ => {}
                }
            }
        }
        vec![]
    }

    pub fn get_filter_language(&self) -> Option<String> {
        for filter in &self.filters {
            if filter.column == "filter_language" && matches!(filter.operator, FilterOperator::Equal) {
                return filter.value.as_string().map(|s| s.to_string());
            }
        }
        None
    }

    pub fn get_modification_time_filter(&self) -> Option<(Option<String>, Option<String>)> {
        let mut start_time = None;
        let mut end_time = None;

        for filter in &self.filters {
            if filter.column == "modification_time" {
                match &filter.operator {
                    FilterOperator::GreaterThan | FilterOperator::GreaterThanOrEqual => {
                        if let FilterValue::Timestamp(ts) = &filter.value {
                            start_time = Some(ts.clone());
                        }
                    }
                    FilterOperator::LessThan | FilterOperator::LessThanOrEqual => {
                        if let FilterValue::Timestamp(ts) = &filter.value {
                            end_time = Some(ts.clone());
                        }
                    }
                    FilterOperator::Between => {
                        if let FilterValue::Range(start, end) = &filter.value {
                            if let FilterValue::Timestamp(ts) = start.as_ref() {
                                start_time = Some(ts.clone());
                            }
                            if let FilterValue::Timestamp(ts) = end.as_ref() {
                                end_time = Some(ts.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        if start_time.is_some() || end_time.is_some() {
            Some((start_time, end_time))
        } else {
            None
        }
    }

    pub fn get_raise_time_filter(&self) -> Option<(Option<String>, Option<String>)> {
        let mut start_time = None;
        let mut end_time = None;

        for filter in &self.filters {
            if filter.column == "raise_time" {
                match &filter.operator {
                    FilterOperator::GreaterThan | FilterOperator::GreaterThanOrEqual => {
                        if let FilterValue::Timestamp(ts) = &filter.value {
                            start_time = Some(ts.clone());
                        }
                    }
                    FilterOperator::LessThan | FilterOperator::LessThanOrEqual => {
                        if let FilterValue::Timestamp(ts) = &filter.value {
                            end_time = Some(ts.clone());
                        }
                    }
                    FilterOperator::Between => {
                        if let FilterValue::Range(start, end) = &filter.value {
                            if let FilterValue::Timestamp(ts) = start.as_ref() {
                                start_time = Some(ts.clone());
                            }
                            if let FilterValue::Timestamp(ts) = end.as_ref() {
                                end_time = Some(ts.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        if start_time.is_some() || end_time.is_some() {
            Some((start_time, end_time))
        } else {
            None
        }
    }
}