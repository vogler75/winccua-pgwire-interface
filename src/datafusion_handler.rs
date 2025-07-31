use anyhow::Result;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::catalog::{SchemaProvider, MemorySchemaProvider};
use datafusion::datasource::MemTable;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

pub async fn execute_query(
    sql: &str,
    batch: RecordBatch,
    table_name: &str,
) -> Result<(Vec<RecordBatch>, u64)> {
    let start_time = Instant::now();
    
    let ctx = SessionContext::new();
    
    // Get the default catalog and register our schemas there
    let default_catalog = ctx.catalog("datafusion").unwrap();
    let pg_catalog_schema = Arc::new(MemorySchemaProvider::new());
    let public_schema = Arc::new(MemorySchemaProvider::new());
    
    // Register schemas in the default DataFusion catalog
    default_catalog.register_schema(crate::constants::SCHEMA_PG_CATALOG, pg_catalog_schema.clone())?;
    default_catalog.register_schema(crate::constants::SCHEMA_PUBLIC, public_schema.clone())?;
    
    // Create the table once
    let mem_table = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])?);
    
    // Register the table in the appropriate schema only if it doesn't already exist
    if table_name.starts_with("pg_") {
        // Catalog tables go in pg_catalog schema
        if !pg_catalog_schema.table_exist(table_name) {
            pg_catalog_schema.register_table(table_name.to_string(), mem_table.clone())?;
            debug!("📋 Registered table '{}' in pg_catalog schema", table_name);
        }
        
        // For any pg_catalog table, register commonly-joined related tables
        // This ensures all catalog tables are available for complex queries
        register_related_pg_catalog_tables(&pg_catalog_schema, table_name).await?;
    } else {
        // Regular tables go in public schema
        if !public_schema.table_exist(table_name) {
            public_schema.register_table(table_name.to_string(), mem_table.clone())?;
            debug!("📋 Registered table '{}' in public schema", table_name);
        }
    }
    
    // Preprocess SQL to handle PostgreSQL functions and types
    let processed_sql = preprocess_postgresql_sql(sql);
    debug!("🔧 Original SQL: {}", sql);
    debug!("🔧 Processed SQL: {}", processed_sql);
    
    let df = ctx.sql(&processed_sql).await?;
    let results = df.collect().await?;
    
    let elapsed_ms = start_time.elapsed().as_millis() as u64;
    debug!("⚡ DataFusion query execution completed in {} ms", elapsed_ms);
    
    Ok((results, elapsed_ms))
}

fn preprocess_postgresql_sql(sql: &str) -> String {
    let mut processed_sql = sql.to_string();
    
    // Replace PostgreSQL catalog functions with DataFusion-compatible expressions
    use regex::Regex;
    
    // pg_get_userbyid(oid) -> CASE expression (handle various formats)
    let userbyid_re = Regex::new(r"pg_catalog\.pg_get_userbyid\(([^)]+)\)").unwrap();
    processed_sql = userbyid_re.replace_all(&processed_sql, "CASE WHEN $1 = 10 THEN 'postgres' ELSE 'unknown' END").to_string();
    
    let userbyid_re2 = Regex::new(r"pg_get_userbyid\(([^)]+)\)").unwrap();
    processed_sql = userbyid_re2.replace_all(&processed_sql, "CASE WHEN $1 = 10 THEN 'postgres' ELSE 'unknown' END").to_string();
    
    // pg_get_function_identity_arguments(oid) -> simple literal
    let func_args_re = Regex::new(r"pg_catalog\.pg_get_function_identity_arguments\([^)]+\)").unwrap();
    processed_sql = func_args_re.replace_all(&processed_sql, "'oid'").to_string();
    
    let func_args_re2 = Regex::new(r"pg_get_function_identity_arguments\([^)]+\)").unwrap();
    processed_sql = func_args_re2.replace_all(&processed_sql, "'oid'").to_string();
    
    // pg_get_viewdef function - handle various patterns
    if processed_sql.contains("pg_get_viewdef(") {
        // Replace the entire function call with a simple string literal
        use regex::Regex;
        let re = Regex::new(r"pg_catalog\.pg_get_viewdef\([^)]+\)").unwrap();
        processed_sql = re.replace_all(&processed_sql, "'SELECT * FROM virtual_table'").to_string();
        
        let re2 = Regex::new(r"pg_get_viewdef\([^)]+\)").unwrap();
        processed_sql = re2.replace_all(&processed_sql, "'SELECT * FROM virtual_table'").to_string();
    }
    
    // Replace PostgreSQL type casting that DataFusion doesn't understand
    processed_sql = processed_sql.replace(
        "p.prorettype='pg_catalog.trigger'::pg_catalog.regtype",
        "false" // Our functions are not triggers
    );
    processed_sql = processed_sql.replace(
        "prorettype='pg_catalog.trigger'::pg_catalog.regtype",
        "false"
    );
    
    // Handle regtype casting in general
    if processed_sql.contains("::pg_catalog.regtype") {
        processed_sql = processed_sql.replace("::pg_catalog.regtype", "");
    }
    if processed_sql.contains("::regtype") {
        processed_sql = processed_sql.replace("::regtype", "");
    }
    
    // DON'T remove pg_catalog prefixes - DataFusion needs them to find tables in the right schema
    // Instead, ensure bare pg_* table names are properly qualified
    if processed_sql.contains(" pg_") {
        // For bare pg_* table names, qualify them with pg_catalog
        use regex::Regex;
        // Match patterns like "FROM pg_table" or "JOIN pg_table" but not "FROM pg_catalog.pg_table"
        let re = Regex::new(r"\b(FROM|JOIN|UPDATE|INTO)\s+(pg_\w+)").unwrap();
        
        // First pass: replace all matches, then clean up double-prefixing
        processed_sql = re.replace_all(&processed_sql, "$1 pg_catalog.$2").to_string();
        
        // Clean up any double-prefixing (this handles cases where table was already qualified)
        processed_sql = processed_sql.replace("pg_catalog.pg_catalog.", "pg_catalog.");
    }
    
    processed_sql
}

async fn register_related_pg_catalog_tables(
    pg_catalog_schema: &Arc<MemorySchemaProvider>,
    primary_table: &str,
) -> Result<()> {
    use arrow::array::{Int64Array, StringArray, BooleanArray, Float32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    
    // Always register pg_namespace since it's commonly joined
    if !pg_catalog_schema.table_exist("pg_namespace") {
        debug!("📋 Registering pg_namespace for JOIN");
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("nspname", DataType::Utf8, false),
            Field::new("nspowner", DataType::Int64, false),
            Field::new("nspacl", DataType::Utf8, true),
        ]));

        let oids = vec![crate::constants::OID_PG_CATALOG_NAMESPACE, crate::constants::OID_PUBLIC_NAMESPACE, crate::constants::OID_INFORMATION_SCHEMA_NAMESPACE];
        let nspnames = vec![crate::constants::SCHEMA_PG_CATALOG, crate::constants::SCHEMA_PUBLIC, crate::constants::SCHEMA_INFORMATION_SCHEMA];
        let nspowners = vec![crate::constants::OID_POSTGRES_USER, crate::constants::OID_POSTGRES_USER, crate::constants::OID_POSTGRES_USER];
        let nspacls = vec![None::<String>, None, None];

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(oids)),
                Arc::new(StringArray::from(nspnames)),
                Arc::new(Int64Array::from(nspowners)),
                Arc::new(StringArray::from(nspacls)),
            ],
        )?;

        let mem_table = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?);
        pg_catalog_schema.register_table(crate::constants::TABLE_PG_NAMESPACE.to_string(), mem_table)?;
    }
    
    // Register pg_class for queries about tables/relations
    if (primary_table == crate::constants::TABLE_PG_CLASS || primary_table == crate::constants::TABLE_PG_NAMESPACE) && !pg_catalog_schema.table_exist(crate::constants::TABLE_PG_CLASS) {
        debug!("📋 Registering pg_class for JOIN");
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("relnamespace", DataType::Int64, false),
            Field::new("reltype", DataType::Int64, false),
            Field::new("relowner", DataType::Int64, false),
            Field::new("relam", DataType::Int64, true),
            Field::new("relfilenode", DataType::Int64, true),
            Field::new("reltablespace", DataType::Int64, true),
            Field::new("relpages", DataType::Int64, true),
            Field::new("reltuples", DataType::Float32, true),
            Field::new("reltoastrelid", DataType::Int64, true),
            Field::new("relhasindex", DataType::Boolean, true),
            Field::new("relisshared", DataType::Boolean, true),
            Field::new("relpersistence", DataType::Utf8, true),
            Field::new("relkind", DataType::Utf8, false),
            Field::new("relnatts", DataType::Int16, true),
            Field::new("relchecks", DataType::Int16, true),
            Field::new("relhasrules", DataType::Boolean, true),
            Field::new("relhastriggers", DataType::Boolean, true),
            Field::new("relhassubclass", DataType::Boolean, true),
            Field::new("relrowsecurity", DataType::Boolean, true),
            Field::new("relforcerowsecurity", DataType::Boolean, true),
            Field::new("relispopulated", DataType::Boolean, true),
            Field::new("relreplident", DataType::Utf8, true),
            Field::new("relispartition", DataType::Boolean, true),
            Field::new("relacl", DataType::Utf8, true),
        ]));

        // Create data for our virtual tables
        let table_data = vec![
            (30000i64, crate::constants::TABLE_TAGVALUES, crate::constants::OID_PUBLIC_NAMESPACE, "r"),
            (30001i64, crate::constants::TABLE_LOGGED_TAG_VALUES, crate::constants::OID_PUBLIC_NAMESPACE, "r"),
            (30002i64, crate::constants::TABLE_ACTIVE_ALARMS, crate::constants::OID_PUBLIC_NAMESPACE, "r"),
            (30003i64, crate::constants::TABLE_LOGGED_ALARMS, crate::constants::OID_PUBLIC_NAMESPACE, "r"),  
            (30004i64, crate::constants::TABLE_TAG_LIST, crate::constants::OID_PUBLIC_NAMESPACE, "r"),
        ];

        let len = table_data.len();
        let mut oids = Vec::new();
        let mut relnames = Vec::new();
        let mut relnamespaces = Vec::new();
        let mut relkinds = Vec::new();
        
        for (oid, name, ns, kind) in &table_data {
            oids.push(*oid);
            relnames.push(*name);
            relnamespaces.push(*ns);
            relkinds.push(*kind);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(oids)),
                Arc::new(StringArray::from(relnames)),
                Arc::new(Int64Array::from(relnamespaces)),
                Arc::new(Int64Array::from(vec![0i64; len])), // reltype
                Arc::new(Int64Array::from(vec![10i64; len])), // relowner (postgres)
                Arc::new(Int64Array::from(vec![None::<i64>; len])), // relam
                Arc::new(Int64Array::from(vec![None::<i64>; len])), // relfilenode
                Arc::new(Int64Array::from(vec![None::<i64>; len])), // reltablespace
                Arc::new(Int64Array::from(vec![None::<i64>; len])), // relpages
                Arc::new(Float32Array::from(vec![None::<f32>; len])), // reltuples
                Arc::new(Int64Array::from(vec![None::<i64>; len])), // reltoastrelid
                Arc::new(BooleanArray::from(vec![Some(false); len])), // relhasindex  
                Arc::new(BooleanArray::from(vec![Some(false); len])), // relisshared
                Arc::new(StringArray::from(vec![Some("p"); len])), // relpersistence (permanent)
                Arc::new(StringArray::from(relkinds)),
                Arc::new(arrow::array::Int16Array::from(vec![None::<i16>; len])), // relnatts
                Arc::new(arrow::array::Int16Array::from(vec![Some(0i16); len])), // relchecks
                Arc::new(BooleanArray::from(vec![Some(false); len])), // relhasrules
                Arc::new(BooleanArray::from(vec![Some(false); len])), // relhastriggers
                Arc::new(BooleanArray::from(vec![Some(false); len])), // relhassubclass
                Arc::new(BooleanArray::from(vec![Some(false); len])), // relrowsecurity
                Arc::new(BooleanArray::from(vec![Some(false); len])), // relforcerowsecurity
                Arc::new(BooleanArray::from(vec![Some(true); len])), // relispopulated
                Arc::new(StringArray::from(vec![Some("d"); len])), // relreplident (default)
                Arc::new(BooleanArray::from(vec![Some(false); len])), // relispartition
                Arc::new(StringArray::from(vec![None::<String>; len])), // relacl
            ],
        )?;

        let mem_table = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?);
        pg_catalog_schema.register_table(crate::constants::TABLE_PG_CLASS.to_string(), mem_table)?;
    }
    
    // Register empty pg_proc if needed
    if !pg_catalog_schema.table_exist(crate::constants::TABLE_PG_PROC) {
        debug!("📋 Registering empty pg_proc for JOIN");
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("proname", DataType::Utf8, false),
            Field::new("pronamespace", DataType::Int64, false),
            Field::new("proowner", DataType::Int64, false),
            Field::new("proisagg", DataType::Boolean, false),
            Field::new("proiswindow", DataType::Boolean, false),
            Field::new("prorettype", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::new_null(0)),
                Arc::new(StringArray::new_null(0)),
                Arc::new(Int64Array::new_null(0)),
                Arc::new(Int64Array::new_null(0)),
                Arc::new(BooleanArray::new_null(0)),
                Arc::new(BooleanArray::new_null(0)),
                Arc::new(Int64Array::new_null(0)),
            ],
        )?;

        let mem_table = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?);
        pg_catalog_schema.register_table(crate::constants::TABLE_PG_PROC.to_string(), mem_table)?;
    }
    
    // Register pg_type for type information queries
    if !pg_catalog_schema.table_exist(crate::constants::TABLE_PG_TYPE) {
        debug!("📋 Registering pg_type for type queries");
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typnamespace", DataType::Int64, false),
            Field::new("typowner", DataType::Int64, false),
            Field::new("typlen", DataType::Int16, false),
            Field::new("typbyval", DataType::Boolean, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typcategory", DataType::Utf8, false),
            Field::new("typispreferred", DataType::Boolean, false),
            Field::new("typisdefined", DataType::Boolean, false),
            Field::new("typdelim", DataType::Utf8, false),
            Field::new("typrelid", DataType::Int64, false),
            Field::new("typelem", DataType::Int64, false),
            Field::new("typarray", DataType::Int64, false),
        ]));

        // Add common PostgreSQL types (but not geometry/geography since those are PostGIS-specific)
        let type_data = vec![
            (16i64, "bool", 11i64, 10i64, 1i16, true, "b", "B", false, true, ",", 0i64, 0i64, 1000i64),
            (17i64, "bytea", 11i64, 10i64, -1i16, false, "b", "U", false, true, ",", 0i64, 0i64, 1001i64),
            (18i64, "char", 11i64, 10i64, 1i16, true, "b", "Z", false, true, ",", 0i64, 0i64, 1002i64),
            (19i64, "name", 11i64, 10i64, 64i16, false, "b", "S", false, true, ",", 0i64, 0i64, 1003i64),
            (20i64, "int8", 11i64, 10i64, 8i16, true, "b", "N", false, true, ",", 0i64, 0i64, 1016i64),
            (21i64, "int2", 11i64, 10i64, 2i16, true, "b", "N", false, true, ",", 0i64, 0i64, 1005i64),
            (23i64, "int4", 11i64, 10i64, 4i16, true, "b", "N", true, true, ",", 0i64, 0i64, 1007i64),
            (25i64, "text", 11i64, 10i64, -1i16, false, "b", "S", true, true, ",", 0i64, 0i64, 1009i64),
            (26i64, "oid", 11i64, 10i64, 4i16, true, "b", "N", false, true, ",", 0i64, 0i64, 1028i64),
            (700i64, "float4", 11i64, 10i64, 4i16, true, "b", "N", false, true, ",", 0i64, 0i64, 1021i64),
            (701i64, "float8", 11i64, 10i64, 8i16, true, "b", "N", true, true, ",", 0i64, 0i64, 1022i64),
            (1114i64, "timestamp", 11i64, 10i64, 8i16, true, "b", "D", false, true, ",", 0i64, 0i64, 1115i64),
            (1700i64, "numeric", 11i64, 10i64, -1i16, false, "b", "N", false, true, ",", 0i64, 0i64, 1231i64),
        ];
        
        let _len = type_data.len();
        let mut oids = Vec::new();
        let mut typnames = Vec::new();
        let mut typnamespaces = Vec::new();
        let mut typowners = Vec::new();
        let mut typlens = Vec::new();
        let mut typbyvals = Vec::new();
        let mut typtypes = Vec::new();
        let mut typcategories = Vec::new();
        let mut typispreferred = Vec::new();
        let mut typisdefineds = Vec::new();
        let mut typdelims = Vec::new();
        let mut typrelids = Vec::new();
        let mut typelems = Vec::new();
        let mut typarrays = Vec::new();
        
        for (oid, name, ns, owner, len_val, byval, typ, cat, preferred, defined, delim, relid, elem, array) in &type_data {
            oids.push(*oid);
            typnames.push(*name);
            typnamespaces.push(*ns);
            typowners.push(*owner);
            typlens.push(*len_val);
            typbyvals.push(*byval);
            typtypes.push(*typ);
            typcategories.push(*cat);
            typispreferred.push(*preferred);
            typisdefineds.push(*defined);
            typdelims.push(*delim);
            typrelids.push(*relid);
            typelems.push(*elem);
            typarrays.push(*array);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(oids)),
                Arc::new(StringArray::from(typnames)),
                Arc::new(Int64Array::from(typnamespaces)),
                Arc::new(Int64Array::from(typowners)),
                Arc::new(arrow::array::Int16Array::from(typlens)),
                Arc::new(BooleanArray::from(typbyvals)),
                Arc::new(StringArray::from(typtypes)),
                Arc::new(StringArray::from(typcategories)),
                Arc::new(BooleanArray::from(typispreferred)),
                Arc::new(BooleanArray::from(typisdefineds)),
                Arc::new(StringArray::from(typdelims)),
                Arc::new(Int64Array::from(typrelids)),
                Arc::new(Int64Array::from(typelems)),
                Arc::new(Int64Array::from(typarrays)),
            ],
        )?;

        let mem_table = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?);
        pg_catalog_schema.register_table(crate::constants::TABLE_PG_TYPE.to_string(), mem_table)?;
    }
    
    Ok(())
}
