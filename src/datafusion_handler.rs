use anyhow::Result;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::logical_expr::{ScalarUDF, Signature, Volatility, ScalarUDFImpl, ScalarFunctionArgs, ColumnarValue};
use arrow::datatypes::DataType;
use std::time::Instant;
use tracing::debug;
use crate::catalog;
use std::sync::Arc;

/// Implementation of pg_get_userbyid function
#[derive(Debug)]
struct PgGetUserByIdUDF {
    signature: Signature,
}

impl PgGetUserByIdUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for PgGetUserByIdUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "pg_catalog_pg_get_userbyid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::{Array, Int32Array, Int64Array, StringArray};
        use arrow::compute::kernels::cast;
        
        let args = args.args;
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal("pg_get_userbyid expects 1 argument".to_string()));
        }
        
        let user_oids = args[0].clone().into_array(1)?;
        let mut results = Vec::new();
        
        // Handle both Int32 and Int64 inputs
        match user_oids.data_type() {
            DataType::Int32 => {
                let oid_array = user_oids.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        let oid = oid_array.value(i);
                        let username = match oid {
                            10 => "postgres",
                            0 => "unknown",
                            1 => "template1",
                            _ => "user",
                        };
                        results.push(Some(username.to_string()));
                    }
                }
            }
            DataType::Int64 => {
                let oid_array = user_oids.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        let oid = oid_array.value(i) as i32;
                        let username = match oid {
                            10 => "postgres",
                            0 => "unknown", 
                            1 => "template1",
                            _ => "user",
                        };
                        results.push(Some(username.to_string()));
                    }
                }
            }
            _ => {
                // Try to cast to Int32
                let casted = cast::cast(user_oids.as_ref(), &DataType::Int32)?;
                let casted_i32 = casted.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..casted_i32.len() {
                    if casted_i32.is_null(i) {
                        results.push(None);
                    } else {
                        let oid = casted_i32.value(i);
                        let username = match oid {
                            10 => "postgres",
                            0 => "unknown",
                            1 => "template1",
                            _ => "user",
                        };
                        results.push(Some(username.to_string()));
                    }
                }
            }
        }
        
        let result_array = StringArray::from(results);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

/// Implementation of pg_get_function_identity_arguments function
#[derive(Debug)]
struct PgGetFunctionIdentityArgumentsUDF {
    signature: Signature,
}

impl PgGetFunctionIdentityArgumentsUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for PgGetFunctionIdentityArgumentsUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "pg_get_function_identity_arguments"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::{Array, Int32Array, Int64Array, StringArray};
        use arrow::compute::kernels::cast;
        
        let args = args.args;
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal("pg_get_function_identity_arguments expects 1 argument".to_string()));
        }
        
        let function_oids = args[0].clone().into_array(1)?;
        let mut results = Vec::new();
        
        // Handle both Int32 and Int64 inputs
        match function_oids.data_type() {
            DataType::Int32 => {
                let oid_array = function_oids.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        // For now, return empty string for all function OIDs
                        results.push(Some("".to_string()));
                    }
                }
            }
            DataType::Int64 => {
                let oid_array = function_oids.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        // For now, return empty string for all function OIDs
                        results.push(Some("".to_string()));
                    }
                }
            }
            _ => {
                // Try to cast to Int32
                let casted = cast::cast(function_oids.as_ref(), &DataType::Int32)?;
                let casted_i32 = casted.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..casted_i32.len() {
                    if casted_i32.is_null(i) {
                        results.push(None);
                    } else {
                        // For now, return empty string for all function OIDs
                        results.push(Some("".to_string()));
                    }
                }
            }
        }
        
        let result_array = StringArray::from(results);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

/// Implementation of current_schema() function
#[derive(Debug)]
struct CurrentSchemaUDF {
    signature: Signature,
}

impl CurrentSchemaUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentSchemaUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_schema"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::StringArray;
        
        // Return "public" as the default schema
        let result = StringArray::from(vec!["public"]);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Implementation of version() function
#[derive(Debug)]
struct VersionUDF {
    signature: Signature,
}

impl VersionUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for VersionUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::StringArray;
        
        // Return a PostgreSQL-compatible version string
        let version = "PostgreSQL 15.0 (WinCC UA PostgreSQL Wire Protocol Server v1.0) on x86_64-pc-linux-gnu, compiled by DataFusion, 64-bit";
        let result = StringArray::from(vec![version]);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Implementation of current_database() function
#[derive(Debug)]
struct CurrentDatabaseUDF {
    signature: Signature,
}

impl CurrentDatabaseUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentDatabaseUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_database"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::StringArray;
        
        // Return the default database name
        let result = StringArray::from(vec!["postgres"]);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Implementation of format_type function
#[derive(Debug)]
struct FormatTypeUDF {
    signature: Signature,
}

impl FormatTypeUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for FormatTypeUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "format_type"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::{Array, Int32Array, Int64Array, StringArray};
        
        let args = args.args;
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Internal("format_type expects 2 arguments".to_string()));
        }
        
        let type_oids = args[0].clone().into_array(1)?;
        let _type_mods = args[1].clone().into_array(1)?; // Type modifier (ignored for now)
        
        let mut results = Vec::new();
        
        // Handle both Int32 and Int64 type OIDs
        match type_oids.data_type() {
            DataType::Int32 => {
                let oid_array = type_oids.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        let oid = oid_array.value(i);
                        let type_name = match oid {
                            16 => "boolean",
                            20 => "bigint", 
                            21 => "smallint",
                            23 => "integer",
                            25 => "text",
                            1043 => "character varying",
                            1114 => "timestamp without time zone",
                            1184 => "timestamp with time zone",
                            _ => "unknown",
                        };
                        results.push(Some(type_name.to_string()));
                    }
                }
            }
            DataType::Int64 => {
                let oid_array = type_oids.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        let oid = oid_array.value(i) as i32;
                        let type_name = match oid {
                            16 => "boolean",
                            20 => "bigint",
                            21 => "smallint", 
                            23 => "integer",
                            25 => "text",
                            1043 => "character varying",
                            1114 => "timestamp without time zone",
                            1184 => "timestamp with time zone",
                            _ => "unknown",
                        };
                        results.push(Some(type_name.to_string()));
                    }
                }
            }
            _ => {
                // Return unknown for other types
                for _ in 0..type_oids.len() {
                    results.push(Some("unknown".to_string()));
                }
            }
        }
        
        let result_array = StringArray::from(results);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

/// Implementation of nullif function
#[derive(Debug)]
struct NullIfUDF {
    signature: Signature,
}

impl NullIfUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for NullIfUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "nullif"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        // Return the type of the first argument
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::{Array, Int32Array, Int64Array};
        
        let args = args.args;
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Internal("nullif expects 2 arguments".to_string()));
        }
        
        let first_arg = args[0].clone().into_array(1)?;
        let second_arg = args[1].clone().into_array(1)?;
        
        // For simplicity, handle the case where both arguments are the same type
        match (first_arg.data_type(), second_arg.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let first_array = first_arg.as_any().downcast_ref::<Int32Array>().unwrap();
                let second_array = second_arg.as_any().downcast_ref::<Int32Array>().unwrap();
                
                let mut results = Vec::new();
                for i in 0..first_array.len() {
                    if first_array.is_null(i) {
                        results.push(None);
                    } else if second_array.is_null(i) {
                        results.push(Some(first_array.value(i)));
                    } else if first_array.value(i) == second_array.value(i) {
                        results.push(None); // Return NULL if equal
                    } else {
                        results.push(Some(first_array.value(i)));
                    }
                }
                let result_array = Int32Array::from(results);
                Ok(ColumnarValue::Array(Arc::new(result_array)))
            }
            (DataType::Int64, DataType::Int64) => {
                let first_array = first_arg.as_any().downcast_ref::<Int64Array>().unwrap();
                let second_array = second_arg.as_any().downcast_ref::<Int64Array>().unwrap();
                
                let mut results = Vec::new();
                for i in 0..first_array.len() {
                    if first_array.is_null(i) {
                        results.push(None);
                    } else if second_array.is_null(i) {
                        results.push(Some(first_array.value(i)));
                    } else if first_array.value(i) == second_array.value(i) {
                        results.push(None); // Return NULL if equal
                    } else {
                        results.push(Some(first_array.value(i)));
                    }
                }
                let result_array = Int64Array::from(results);
                Ok(ColumnarValue::Array(Arc::new(result_array)))
            }
            _ => {
                // For other types, just return the first argument (simplified)
                Ok(ColumnarValue::Array(first_arg))
            }
        }
    }
}

/// Register PostgreSQL system functions with DataFusion context
pub fn register_postgresql_functions(ctx: &SessionContext) -> Result<()> {
    ctx.register_udf(ScalarUDF::new_from_impl(PgGetUserByIdUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(PgGetFunctionIdentityArgumentsUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CurrentSchemaUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(VersionUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CurrentDatabaseUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(FormatTypeUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(NullIfUDF::new()));
    Ok(())
}

/// Replace schema-qualified table names with underscore versions and handle PostgreSQL type casts
pub fn normalize_schema_qualified_tables(sql: &str) -> String {
    normalize_schema_qualified_tables_with_username(sql, "postgres")
}

pub fn normalize_schema_qualified_tables_with_username(sql: &str, username: &str) -> String {
    use regex::Regex;
    
    // First, handle PostgreSQL type casts like 'value'::pg_catalog.regtype and 'value'::regclass
    // Pattern to match ::pg_catalog.type_name and remove the cast
    let cast_re = Regex::new(r"::pg_catalog\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
    let mut processed_sql = cast_re.replace_all(sql, "").to_string();
    
    // Also handle standalone PostgreSQL type casts like ::regclass, ::regtype, etc.
    let standalone_cast_re = Regex::new(r"::(regclass|regtype|regproc|regprocedure|regoper|regoperator|regconfig|regdictionary)\b").unwrap();
    processed_sql = standalone_cast_re.replace_all(&processed_sql, "").to_string();
    
    // Handle PostgreSQL special identifiers (session_user, current_user, user)
    // These are identifiers, not functions, so we need to replace them with literal values
    
    // Replace session_user with a string literal containing the actual username
    let session_user_re = Regex::new(r"\bsession_user\b").unwrap();
    processed_sql = session_user_re.replace_all(&processed_sql, &format!("'{}'", username)).to_string();
    
    // Replace current_user with a string literal containing the actual username
    let current_user_re = Regex::new(r"\bcurrent_user\b").unwrap();
    processed_sql = current_user_re.replace_all(&processed_sql, &format!("'{}'", username)).to_string();
    
    // Replace user with a string literal containing the actual username (user is equivalent to current_user)
    let user_re = Regex::new(r"\buser\b").unwrap();
    processed_sql = user_re.replace_all(&processed_sql, &format!("'{}'", username)).to_string();
    
    // Pattern to match pg_catalog.table_name (case insensitive)
    let re = Regex::new(r"(?i)\bpg_catalog\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
    
    // Replace pg_catalog.table_name with pg_catalog_table_name
    processed_sql = re.replace_all(&processed_sql, "pg_catalog_$1").to_string();
    
    // Handle duplicate columns caused by qualified wildcards like "SELECT db.oid, db.*"
    // Since Rust regex doesn't support backreferences, we'll use a simpler approach
    processed_sql = {
        let mut result = processed_sql.clone();
        // Simple pattern matching for common cases like "SELECT alias.column, alias.*"
        if let Some(caps) = Regex::new(r"(?i)\bSELECT\s+([a-zA-Z_][a-zA-Z0-9_]*)\.[a-zA-Z_][a-zA-Z0-9_]*\s*,\s*([a-zA-Z_][a-zA-Z0-9_]*)\.\*").unwrap().captures(&processed_sql) {
            let alias1 = caps.get(1).unwrap().as_str();
            let alias2 = caps.get(2).unwrap().as_str();
            if alias1 == alias2 {
                // Replace the whole SELECT clause with just "SELECT alias.*"
                result = Regex::new(&format!(r"(?i)\bSELECT\s+{}\.[a-zA-Z_][a-zA-Z0-9_]*\s*,\s*{}\.\*", alias1, alias2)).unwrap()
                    .replace(&processed_sql, &format!("SELECT {}.*", alias1)).to_string();
            }
        }
        result
    };
    
    processed_sql
}

pub async fn execute_query(
    sql: &str,
    batch: RecordBatch,
    table_name: &str,
) -> Result<(Vec<RecordBatch>, u64)> {
    let start_time = Instant::now();
    
    let ctx = SessionContext::new();
    
    // Register PostgreSQL system functions
    register_postgresql_functions(&ctx)?;
    
    ctx.register_batch(table_name, batch)?;
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    
    let elapsed_ms = start_time.elapsed().as_millis() as u64;
    debug!("⚡ DataFusion query execution completed in {} ms", elapsed_ms);
    
    Ok((results, elapsed_ms))
}

/// Execute a complex SQL query with catalog tables support
pub async fn execute_query_with_virtual_tables(sql: &str, session: &crate::auth::AuthenticatedSession) -> Result<(Vec<RecordBatch>, u64)> {
    let start_time = Instant::now();
    
    // Pre-process SQL to normalize schema-qualified table names, using actual logged-in username
    let processed_sql = normalize_schema_qualified_tables_with_username(sql, &session.username);
    debug!("🔧 Processed SQL: {} -> {}", sql, processed_sql);
    
    let ctx = SessionContext::new();
    
    // Register PostgreSQL system functions
    register_postgresql_functions(&ctx)?;
    
    // Register all catalog tables if available
    if let Some(catalog) = catalog::get_catalog() {
        for table_name in catalog.get_table_names() {
            if let Some(table) = catalog.get_table(&table_name) {
                debug!("📋 Registering catalog table '{}' with DataFusion", table_name);
                
                // Register each record batch as a separate table if there are multiple batches
                // For simplicity, we'll combine all batches into one table
                if !table.data.is_empty() {
                    let combined_batch = if table.data.len() == 1 {
                        table.data[0].clone()
                    } else {
                        // Combine multiple batches
                        let schema = table.data[0].schema();
                        let mut columns = Vec::new();
                        
                        // Collect all columns from all batches
                        for col_idx in 0..schema.fields().len() {
                            let mut arrays = Vec::new();
                            for batch in &table.data {
                                arrays.push(batch.column(col_idx).clone());
                            }
                            // Concatenate arrays
                            let combined_array = arrow::compute::concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
                            columns.push(combined_array);
                        }
                        
                        RecordBatch::try_new(schema, columns)?
                    };
                    
                    // Register with normalized name (dots replaced with underscores)
                    let normalized_name = table_name.replace('.', "_");
                    debug!("📋 Registering catalog table as: {}", normalized_name);
                    ctx.register_batch(&normalized_name, combined_batch.clone())?;
                    
                    // If the table name starts with pg_catalog., also register without the prefix
                    // for backward compatibility (e.g., both "pg_catalog.pg_class" and pg_class)
                    if table_name.starts_with("pg_catalog.") {
                        if let Some(short_name) = table_name.strip_prefix("pg_catalog.") {
                            debug!("📋 Also registering '{}' as '{}'", table_name, short_name);
                            ctx.register_batch(short_name, combined_batch)?;
                        }
                    }
                }
            }
        }
    }
    
    // Analyze the SQL to determine which virtual tables need data and populate them
    if let Err(e) = populate_virtual_tables_with_data(&ctx, sql, session).await {
        debug!("⚠️ Failed to populate some virtual tables with data: {}", e);
        // Continue execution even if some virtual tables fail to populate
    }
    
    let df = ctx.sql(&processed_sql).await?;
    let results = df.collect().await?;
    
    let elapsed_ms = start_time.elapsed().as_millis() as u64;
    debug!("⚡ DataFusion query execution completed in {} ms", elapsed_ms);
    
    Ok((results, elapsed_ms))
}

/// Execute a complex SQL query with catalog tables support (legacy function)
pub async fn execute_query_with_catalog(sql: &str) -> Result<(Vec<RecordBatch>, u64)> {
    let start_time = Instant::now();
    
    // Pre-process SQL to normalize schema-qualified table names
    let processed_sql = normalize_schema_qualified_tables(sql);
    debug!("🔧 Processed SQL: {} -> {}", sql, processed_sql);
    
    let ctx = SessionContext::new();
    
    // Register PostgreSQL system functions
    register_postgresql_functions(&ctx)?;
    
    // Register all catalog tables if available
    if let Some(catalog) = catalog::get_catalog() {
        for table_name in catalog.get_table_names() {
            if let Some(table) = catalog.get_table(&table_name) {
                debug!("📋 Registering catalog table '{}' with DataFusion", table_name);
                
                // Register each record batch as a separate table if there are multiple batches
                // For simplicity, we'll combine all batches into one table
                if !table.data.is_empty() {
                    let combined_batch = if table.data.len() == 1 {
                        table.data[0].clone()
                    } else {
                        // Combine multiple batches
                        let schema = table.data[0].schema();
                        let mut columns = Vec::new();
                        
                        // Collect all columns from all batches
                        for col_idx in 0..schema.fields().len() {
                            let mut arrays = Vec::new();
                            for batch in &table.data {
                                arrays.push(batch.column(col_idx).clone());
                            }
                            // Concatenate arrays
                            let combined_array = arrow::compute::concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
                            columns.push(combined_array);
                        }
                        
                        RecordBatch::try_new(schema, columns)?
                    };
                    
                    // Register with normalized name (dots replaced with underscores)
                    let normalized_name = table_name.replace('.', "_");
                    debug!("📋 Registering catalog table as: {}", normalized_name);
                    ctx.register_batch(&normalized_name, combined_batch.clone())?;
                    
                    // If the table name starts with pg_catalog., also register without the prefix
                    // for backward compatibility (e.g., both "pg_catalog.pg_class" and pg_class)
                    if table_name.starts_with("pg_catalog.") {
                        if let Some(short_name) = table_name.strip_prefix("pg_catalog.") {
                            debug!("📋 Also registering '{}' as '{}'", table_name, short_name);
                            ctx.register_batch(short_name, combined_batch)?;
                        }
                    }
                }
            }
        }
    }
    
    let df = ctx.sql(&processed_sql).await?;
    let results = df.collect().await?;
    
    let elapsed_ms = start_time.elapsed().as_millis() as u64;
    debug!("⚡ DataFusion catalog query execution completed in {} ms", elapsed_ms);
    
    Ok((results, elapsed_ms))
}

/// Populate virtual tables with actual data based on the SQL query
async fn populate_virtual_tables_with_data(ctx: &SessionContext, sql: &str, session: &crate::auth::AuthenticatedSession) -> Result<()> {
    debug!("🔍 Analyzing SQL to determine which virtual tables need data: {}", sql);
    
    // Parse the SQL to identify which tables are being queried
    let tables_referenced = extract_table_names_from_sql(sql)?;
    debug!("📊 Found references to tables: {:?}", tables_referenced);
    
    for table_name in tables_referenced {
        match table_name.as_str() {
            "tagvalues" => {
                if let Ok(batch) = create_populated_tagvalues_batch(session).await {
                    ctx.register_batch("tagvalues", batch.clone())?;
                    // Also register with datafusion.public schema for compatibility
                    ctx.register_batch("datafusion.public.tagvalues", batch)?;
                    debug!("📊 Populated tagvalues table with real data");
                } else {
                    register_empty_tagvalues_table(ctx).await?;
                    debug!("📊 Registered empty tagvalues table (no data available)");
                }
            }
            "loggedtagvalues" => {
                if let Ok(batch) = create_populated_loggedtagvalues_batch(session).await {
                    ctx.register_batch("loggedtagvalues", batch.clone())?;
                    // Also register with datafusion.public schema for compatibility
                    ctx.register_batch("datafusion.public.loggedtagvalues", batch)?;
                    debug!("📊 Populated loggedtagvalues table with real data");
                } else {
                    register_empty_loggedtagvalues_table(ctx).await?;
                    debug!("📊 Registered empty loggedtagvalues table (no data available)");
                }
            }
            "activealarms" => {
                if let Ok(batch) = create_populated_activealarms_batch(session).await {
                    ctx.register_batch("activealarms", batch.clone())?;
                    // Also register with datafusion.public schema for compatibility
                    ctx.register_batch("datafusion.public.activealarms", batch)?;
                    debug!("📊 Populated activealarms table with real data");
                } else {
                    register_empty_activealarms_table(ctx).await?;
                    debug!("📊 Registered empty activealarms table (no data available)");
                }
            }
            "loggedalarms" => {
                if let Ok(batch) = create_populated_loggedalarms_batch(session).await {
                    ctx.register_batch("loggedalarms", batch.clone())?;
                    // Also register with datafusion.public schema for compatibility
                    ctx.register_batch("datafusion.public.loggedalarms", batch)?;
                    debug!("📊 Populated loggedalarms table with real data");
                } else {
                    register_empty_loggedalarms_table(ctx).await?;
                    debug!("📊 Registered empty loggedalarms table (no data available)");
                }
            }
            "taglist" => {
                if let Ok(batch) = create_populated_taglist_batch(session).await {
                    ctx.register_batch("taglist", batch.clone())?;
                    // Also register with datafusion.public schema for compatibility
                    ctx.register_batch("datafusion.public.taglist", batch)?;
                    debug!("📊 Populated taglist table with real data");
                } else {
                    register_empty_taglist_table(ctx).await?;
                    debug!("📊 Registered empty taglist table (no data available)");
                }
            }
            _ => {
                debug!("⚠️ Unknown virtual table referenced: {}", table_name);
            }
        }
    }
    
    Ok(())
}

/// Register virtual tables with DataFusion by creating empty record batches with proper schema (fallback)
async fn register_virtual_tables(ctx: &SessionContext, _session: &crate::auth::AuthenticatedSession) -> Result<()> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
    
    // Register tagvalues table
    let tagvalues_schema = Arc::new(Schema::new(vec![
        Field::new("tag_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("timestamp_ms", DataType::Int64, true),
        Field::new("numeric_value", DataType::Float64, true),
        Field::new("string_value", DataType::Utf8, true),
        Field::new("quality", DataType::Utf8, true),
    ]));
    
    // Always register empty tables first to ensure they exist, then try to populate with data
    let empty_tagvalues_batch = RecordBatch::try_new(
        tagvalues_schema.clone(),
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        ],
    )?;
    ctx.register_batch("tagvalues", empty_tagvalues_batch)?;
    debug!("📊 Registered empty tagvalues table for DataFusion queries (0 rows)");
    
    // Register loggedtagvalues table (same schema as tagvalues)
    let empty_loggedtagvalues_batch = RecordBatch::try_new(
        tagvalues_schema.clone(),
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        ],
    )?;
    ctx.register_batch("loggedtagvalues", empty_loggedtagvalues_batch)?;
    debug!("📊 Registered empty loggedtagvalues table for DataFusion queries (0 rows)");
    
    // Register activealarms table
    let activealarms_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("instance_id", DataType::Int64, false),
        Field::new("alarm_group_id", DataType::Int64, true),
        Field::new("raise_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("acknowledgment_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("clear_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("reset_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("modification_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("state", DataType::Utf8, false),
        Field::new("priority", DataType::Int64, true),
        Field::new("event_text", DataType::Utf8, true),
        Field::new("info_text", DataType::Utf8, true),
        Field::new("origin", DataType::Utf8, true),
        Field::new("area", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("host_name", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, true),
    ]));
    
    let empty_activealarms_batch = RecordBatch::try_new(
        activealarms_schema.clone(),
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        ],
    )?;
    ctx.register_batch("activealarms", empty_activealarms_batch)?;
    debug!("📊 Registered empty activealarms table for DataFusion queries (0 rows)");
    
    // Register loggedalarms table (same schema as activealarms + duration)
    let loggedalarms_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("instance_id", DataType::Int64, false),
        Field::new("alarm_group_id", DataType::Int64, true),
        Field::new("raise_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("acknowledgment_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("clear_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("reset_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("modification_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("state", DataType::Utf8, false),
        Field::new("priority", DataType::Int64, true),
        Field::new("event_text", DataType::Utf8, true),
        Field::new("info_text", DataType::Utf8, true),
        Field::new("origin", DataType::Utf8, true),
        Field::new("area", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),  
        Field::new("host_name", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, true),
        Field::new("duration", DataType::Utf8, true),
    ]));
    
    let empty_loggedalarms_batch = RecordBatch::try_new(
        loggedalarms_schema.clone(),
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        ],
    )?;
    ctx.register_batch("loggedalarms", empty_loggedalarms_batch)?;
    debug!("📊 Registered empty loggedalarms table for DataFusion queries (0 rows)");
    
    // Register taglist table
    let taglist_schema = Arc::new(Schema::new(vec![
        Field::new("tag_name", DataType::Utf8, false),
        Field::new("display_name", DataType::Utf8, true),
        Field::new("object_type", DataType::Utf8, true),
        Field::new("data_type", DataType::Utf8, true),
    ]));
    
    let empty_taglist_batch = RecordBatch::try_new(
        taglist_schema.clone(),
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        ],
    )?;
    ctx.register_batch("taglist", empty_taglist_batch)?;
    debug!("📊 Registered empty taglist table for DataFusion queries (0 rows)");
    
    Ok(())
}

/// Extract table names from SQL query
fn extract_table_names_from_sql(sql: &str) -> Result<Vec<String>> {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion::sql::sqlparser::ast::{Statement, TableFactor, TableWithJoins};
    
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)?;
    
    let mut table_names = Vec::new();
    
    for statement in ast {
        if let Statement::Query(query) = statement {
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(select) = &*query.body {
                for table_with_joins in &select.from {
                    extract_table_name_from_table_with_joins(table_with_joins, &mut table_names);
                }
            }
        }
    }
    
    Ok(table_names)
}

fn extract_table_name_from_table_with_joins(table_with_joins: &datafusion::sql::sqlparser::ast::TableWithJoins, table_names: &mut Vec<String>) {
    extract_table_name_from_table_factor(&table_with_joins.relation, table_names);
    
    for join in &table_with_joins.joins {
        extract_table_name_from_table_factor(&join.relation, table_names);
    }
}

fn extract_table_name_from_table_factor(table_factor: &datafusion::sql::sqlparser::ast::TableFactor, table_names: &mut Vec<String>) {
    match table_factor {
        datafusion::sql::sqlparser::ast::TableFactor::Table { name, .. } => {
            let table_name = name.to_string().to_lowercase();
            if !table_names.contains(&table_name) {
                table_names.push(table_name);
            }
        }
        _ => {}
    }
}


/// Create populated tagvalues batch with real data
async fn create_populated_tagvalues_batch(session: &crate::auth::AuthenticatedSession) -> Result<RecordBatch> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("tag_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("timestamp_ms", DataType::Int64, true),
        Field::new("numeric_value", DataType::Float64, true),
        Field::new("string_value", DataType::Utf8, true),
        Field::new("quality", DataType::Utf8, true),
    ]));
    
    // Create a query to fetch all tagvalues data (without filters)
    let query_info = crate::tables::QueryInfo {
        table: crate::tables::VirtualTable::TagValues,
        columns: vec![
            "tag_name".to_string(),
            "timestamp".to_string(),
            "timestamp_ms".to_string(),
            "numeric_value".to_string(),
            "string_value".to_string(),
            "quality".to_string(),
        ],
        column_mappings: std::collections::HashMap::new(),
        filters: vec![], // No filters - get all available data
        limit: Some(1000), // Limit to prevent too much data
        order_by: None,
    };
    
    // Try to get a list of available tags first
    let tag_list_query = crate::tables::QueryInfo {
        table: crate::tables::VirtualTable::TagList,
        columns: vec!["tag_name".to_string()],
        column_mappings: std::collections::HashMap::new(),
        filters: vec![],
        limit: Some(100),
        order_by: None,
    };
    
    let tag_names = match crate::query_handler::QueryHandler::fetch_tag_list_data(&tag_list_query, session).await {
        Ok(tags) => {
            let available_tags: Vec<String> = tags.into_iter().take(10).map(|t| t.name).collect();
            debug!("📊 Successfully fetched {} available tags", available_tags.len());
            available_tags
        }
        Err(e) => {
            debug!("📊 Could not fetch tag list: {}, will try to get data without tag filters", e);
            vec![] // Empty list means try without filters
        }
    };
    
    debug!("📊 Attempting to fetch tagvalues data with {} tag names", tag_names.len());
    
    // Create query with tag names filter (if we have tags)
    let mut query_with_tags = query_info.clone();
    if !tag_names.is_empty() {
        query_with_tags.filters = vec![crate::tables::ColumnFilter {
            column: "tag_name".to_string(),
            operator: crate::tables::FilterOperator::In,
            value: crate::tables::FilterValue::List(tag_names.clone()),
        }];
        debug!("📊 Using tag filter: {:?}", tag_names);
    } else {
        debug!("📊 No tag filters available, attempting to fetch without filters");
    }
    
    let results = match crate::query_handler::QueryHandler::fetch_tag_values_data(&query_with_tags, session).await {
        Ok(data) => {
            debug!("📊 Successfully fetched {} tagvalues records", data.len());
            data
        }
        Err(e) => {
            debug!("📊 Failed to fetch tagvalues data: {}", e);
            return Err(anyhow::anyhow!("No tagvalues data available: {}", e));
        }
    };
    
    if results.is_empty() {
        return Err(anyhow::anyhow!("No tagvalues data available"));
    }
    
    // Convert results to Arrow batch
    let (tag_names, timestamps, timestamps_ms, numeric_values, string_values, qualities) = results.into_iter().fold(
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
        |mut acc, result| {
            acc.0.push(result.name);
            if let Some(ref value) = result.value {
                let ts_nanos = chrono::DateTime::parse_from_rfc3339(&value.timestamp)
                    .map(|dt| dt.timestamp_nanos_opt())
                    .unwrap_or_default();
                acc.1.push(ts_nanos);
                acc.2.push(ts_nanos.map(|t| t / 1_000_000));
                acc.3.push(value.value.as_ref().and_then(|v| v.as_f64()));
                acc.4.push(value.value.as_ref().and_then(|v| v.as_str()).map(|s| s.to_string()));
                acc.5.push(value.quality.as_ref().map(|q| q.quality.clone()));
            } else {
                acc.1.push(None);
                acc.2.push(None);
                acc.3.push(None);
                acc.4.push(None);
                acc.5.push(None);
            }
            acc
        },
    );
    
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(tag_names)),
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(Int64Array::from(timestamps_ms)),
            Arc::new(Float64Array::from(numeric_values)),
            Arc::new(StringArray::from(string_values)),
            Arc::new(StringArray::from(qualities)),
        ],
    )?;
    
    Ok(batch)
}

/// Create populated loggedtagvalues batch with real data
async fn create_populated_loggedtagvalues_batch(session: &crate::auth::AuthenticatedSession) -> Result<RecordBatch> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("tag_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("timestamp_ms", DataType::Int64, true),
        Field::new("numeric_value", DataType::Float64, true),
        Field::new("string_value", DataType::Utf8, true),
        Field::new("quality", DataType::Utf8, true),
    ]));
    
    // Try to get a list of available tags first
    let tag_list_query = crate::tables::QueryInfo {
        table: crate::tables::VirtualTable::TagList,
        columns: vec!["tag_name".to_string()],
        column_mappings: std::collections::HashMap::new(),
        filters: vec![],
        limit: Some(100),
        order_by: None,
    };
    
    let tag_names = match crate::query_handler::QueryHandler::fetch_tag_list_data(&tag_list_query, session).await {
        Ok(tags) => tags.into_iter().take(5).map(|t| t.name).collect::<Vec<_>>(), // Limit to first 5 tags for performance
        Err(_) => {
            debug!("📊 Could not fetch tag list for loggedtagvalues");
            return Err(anyhow::anyhow!("No tags available for loggedtagvalues"));
        }
    };
    
    debug!("📊 Fetching loggedtagvalues data for {} tags", tag_names.len());
    
    // Create query with tag names filter and recent time range
    let query_info = crate::tables::QueryInfo {
        table: crate::tables::VirtualTable::LoggedTagValues,
        columns: vec![
            "tag_name".to_string(),
            "timestamp".to_string(),
            "timestamp_ms".to_string(),
            "numeric_value".to_string(),
            "string_value".to_string(),
            "quality".to_string(),
        ],
        column_mappings: std::collections::HashMap::new(),
        filters: vec![
            crate::tables::ColumnFilter {
                column: "tag_name".to_string(),
                operator: crate::tables::FilterOperator::In,
                value: crate::tables::FilterValue::List(tag_names),
            },
            // Add time filter for recent data (last 24 hours)
            crate::tables::ColumnFilter {
                column: "timestamp".to_string(),
                operator: crate::tables::FilterOperator::GreaterThan,
                value: crate::tables::FilterValue::Timestamp(
                    (chrono::Utc::now() - chrono::Duration::days(1)).to_rfc3339()
                ),
            },
        ],
        limit: Some(1000),
        order_by: None,
    };
    
    let results = crate::query_handler::QueryHandler::fetch_logged_tag_values_data(&query_info, session).await?;
    debug!("📊 Fetched {} loggedtagvalues records", results.len());
    
    if results.is_empty() {
        return Err(anyhow::anyhow!("No loggedtagvalues data available"));
    }
    
    // Convert results to Arrow batch
    let (tag_names, timestamps, timestamps_ms, numeric_values, string_values, qualities) = results.into_iter().fold(
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
        |mut acc, result| {
            acc.0.push(result.tag_name);
            let ts_nanos = chrono::DateTime::parse_from_rfc3339(&result.timestamp)
                .map(|dt| dt.timestamp_nanos_opt())
                .unwrap_or_default();
            acc.1.push(ts_nanos);
            acc.2.push(ts_nanos.map(|t| t / 1_000_000));
            acc.3.push(result.value.as_ref().and_then(|v| v.as_f64()));
            acc.4.push(result.value.as_ref().and_then(|v| v.as_str()).map(|s| s.to_string()));
            acc.5.push(result.quality.as_ref().map(|q| q.quality.clone()));
            acc
        },
    );
    
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(tag_names)),
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(Int64Array::from(timestamps_ms)),
            Arc::new(Float64Array::from(numeric_values)),
            Arc::new(StringArray::from(string_values)),
            Arc::new(StringArray::from(qualities)),
        ],
    )?;
    
    Ok(batch)
}

/// Create populated activealarms batch with real data
async fn create_populated_activealarms_batch(session: &crate::auth::AuthenticatedSession) -> Result<RecordBatch> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("instance_id", DataType::Int64, false),
        Field::new("alarm_group_id", DataType::Int64, true),
        Field::new("raise_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("acknowledgment_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("clear_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("reset_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("modification_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("state", DataType::Utf8, false),
        Field::new("priority", DataType::Int64, true),
        Field::new("event_text", DataType::Utf8, true),
        Field::new("info_text", DataType::Utf8, true),
        Field::new("origin", DataType::Utf8, true),
        Field::new("area", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("host_name", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, true),
    ]));
    
    let query_info = crate::tables::QueryInfo {
        table: crate::tables::VirtualTable::ActiveAlarms,
        columns: vec![
            "name".to_string(), "instance_id".to_string(), "alarm_group_id".to_string(),
            "raise_time".to_string(), "acknowledgment_time".to_string(), "clear_time".to_string(),
            "reset_time".to_string(), "modification_time".to_string(), "state".to_string(),
            "priority".to_string(), "event_text".to_string(), "info_text".to_string(),
            "origin".to_string(), "area".to_string(), "value".to_string(),
            "host_name".to_string(), "user_name".to_string(),
        ],
        column_mappings: std::collections::HashMap::new(),
        filters: vec![],
        limit: Some(1000),
        order_by: None,
    };
    
    let results = crate::query_handler::QueryHandler::fetch_active_alarms_data(&query_info, session).await?;
    debug!("📊 Fetched {} activealarms records", results.len());
    
    if results.is_empty() {
        return Err(anyhow::anyhow!("No activealarms data available"));
    }
    
    // Convert results to Arrow batch (implementation similar to the handler)
    let (names, instance_ids, alarm_group_ids, raise_times, acknowledgment_times, clear_times,
         reset_times, modification_times, states, priorities, event_texts, info_texts,
         origins, areas, values, host_names, user_names) = results.into_iter().fold(
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(),
         Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(),
         Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
        |mut acc, result| {
            acc.0.push(result.name);
            acc.1.push(result.instance_id as i64);
            acc.2.push(result.alarm_group_id.map(|id| id as i64));
            
            // Parse timestamps
            let raise_time_nanos = chrono::DateTime::parse_from_rfc3339(&result.raise_time)
                .map(|dt| dt.timestamp_nanos_opt()).unwrap_or_default();
            acc.3.push(raise_time_nanos);
            
            let ack_time_nanos = result.acknowledgment_time.as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .and_then(|dt| dt.timestamp_nanos_opt());
            acc.4.push(ack_time_nanos);
            
            let clear_time_nanos = result.clear_time.as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .and_then(|dt| dt.timestamp_nanos_opt());
            acc.5.push(clear_time_nanos);
            
            let reset_time_nanos = result.reset_time.as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .and_then(|dt| dt.timestamp_nanos_opt());
            acc.6.push(reset_time_nanos);
            
            let modification_time_nanos = chrono::DateTime::parse_from_rfc3339(&result.modification_time)
                .map(|dt| dt.timestamp_nanos_opt()).unwrap_or_default();
            acc.7.push(modification_time_nanos);
            
            acc.8.push(result.state);
            acc.9.push(result.priority.map(|p| p as i64));
            acc.10.push(result.event_text.map(|v| v.join(";")));
            acc.11.push(result.info_text.map(|v| v.join(";")));
            acc.12.push(result.origin);
            acc.13.push(result.area);
            acc.14.push(result.value.map(|v| v.to_string()));
            acc.15.push(result.host_name);
            acc.16.push(result.user_name);
            acc
        },
    );
    
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(instance_ids)),
            Arc::new(Int64Array::from(alarm_group_ids)),
            Arc::new(TimestampNanosecondArray::from(raise_times)),
            Arc::new(TimestampNanosecondArray::from(acknowledgment_times)),
            Arc::new(TimestampNanosecondArray::from(clear_times)),
            Arc::new(TimestampNanosecondArray::from(reset_times)),
            Arc::new(TimestampNanosecondArray::from(modification_times)),
            Arc::new(StringArray::from(states)),
            Arc::new(Int64Array::from(priorities)),
            Arc::new(StringArray::from(event_texts)),
            Arc::new(StringArray::from(info_texts)),
            Arc::new(StringArray::from(origins)),
            Arc::new(StringArray::from(areas)),
            Arc::new(StringArray::from(values)),
            Arc::new(StringArray::from(host_names)),
            Arc::new(StringArray::from(user_names)),
        ],
    )?;
    
    Ok(batch)
}

/// Create empty batch functions for fallback
async fn register_empty_tagvalues_table(ctx: &SessionContext) -> Result<()> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("tag_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("timestamp_ms", DataType::Int64, true),
        Field::new("numeric_value", DataType::Float64, true),
        Field::new("string_value", DataType::Utf8, true),
        Field::new("quality", DataType::Utf8, true),
    ]));
    
    let empty_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        ],
    )?;
    ctx.register_batch("tagvalues", empty_batch.clone())?;
    ctx.register_batch("datafusion.public.tagvalues", empty_batch)?;
    Ok(())
}

// I'll add stubs for the other empty table functions to avoid compilation errors
async fn register_empty_loggedtagvalues_table(ctx: &SessionContext) -> Result<()> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("tag_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("timestamp_ms", DataType::Int64, true),
        Field::new("numeric_value", DataType::Float64, true),
        Field::new("string_value", DataType::Utf8, true),
        Field::new("quality", DataType::Utf8, true),
    ]));
    
    let empty_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        ],
    )?;
    ctx.register_batch("loggedtagvalues", empty_batch.clone())?;
    ctx.register_batch("datafusion.public.loggedtagvalues", empty_batch)?;
    Ok(())
}

async fn register_empty_activealarms_table(ctx: &SessionContext) -> Result<()> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("instance_id", DataType::Int64, false),
        Field::new("alarm_group_id", DataType::Int64, true),
        Field::new("raise_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("acknowledgment_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("clear_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("reset_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("modification_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("state", DataType::Utf8, false),
        Field::new("priority", DataType::Int64, true),
        Field::new("event_text", DataType::Utf8, true),
        Field::new("info_text", DataType::Utf8, true),
        Field::new("origin", DataType::Utf8, true),
        Field::new("area", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("host_name", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, true),
    ]));
    
    let empty_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        ],
    )?;
    ctx.register_batch("activealarms", empty_batch.clone())?;
    ctx.register_batch("datafusion.public.activealarms", empty_batch)?;
    Ok(())
}

async fn register_empty_loggedalarms_table(ctx: &SessionContext) -> Result<()> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("instance_id", DataType::Int64, false),
        Field::new("alarm_group_id", DataType::Int64, true),
        Field::new("raise_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("acknowledgment_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("clear_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("reset_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("modification_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("state", DataType::Utf8, false),
        Field::new("priority", DataType::Int64, true),
        Field::new("event_text", DataType::Utf8, true),
        Field::new("info_text", DataType::Utf8, true),
        Field::new("origin", DataType::Utf8, true),
        Field::new("area", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("host_name", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, true),
        Field::new("duration", DataType::Utf8, true),
    ]));
    
    let empty_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(TimestampNanosecondArray::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        ],
    )?;
    ctx.register_batch("loggedalarms", empty_batch.clone())?;
    ctx.register_batch("datafusion.public.loggedalarms", empty_batch)?;
    Ok(())
}

async fn register_empty_taglist_table(ctx: &SessionContext) -> Result<()> {
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::array::StringArray;
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("tag_name", DataType::Utf8, false),
        Field::new("display_name", DataType::Utf8, true),
        Field::new("object_type", DataType::Utf8, true),
        Field::new("data_type", DataType::Utf8, true),
    ]));
    
    let empty_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        ],
    )?;
    ctx.register_batch("taglist", empty_batch.clone())?;
    ctx.register_batch("datafusion.public.taglist", empty_batch)?;
    Ok(())
}

async fn create_populated_loggedalarms_batch(session: &crate::auth::AuthenticatedSession) -> Result<RecordBatch> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("instance_id", DataType::Int64, false),
        Field::new("alarm_group_id", DataType::Int64, true),
        Field::new("raise_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("acknowledgment_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("clear_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("reset_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("modification_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("state", DataType::Utf8, false),
        Field::new("priority", DataType::Int64, true),
        Field::new("event_text", DataType::Utf8, true),
        Field::new("info_text", DataType::Utf8, true),
        Field::new("origin", DataType::Utf8, true),
        Field::new("area", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("host_name", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, true),
        Field::new("duration", DataType::Utf8, true),
    ]));
    
    let query_info = crate::tables::QueryInfo {
        table: crate::tables::VirtualTable::LoggedAlarms,
        columns: vec![
            "name".to_string(), "instance_id".to_string(), "alarm_group_id".to_string(),
            "raise_time".to_string(), "acknowledgment_time".to_string(), "clear_time".to_string(),
            "reset_time".to_string(), "modification_time".to_string(), "state".to_string(),
            "priority".to_string(), "event_text".to_string(), "info_text".to_string(),
            "origin".to_string(), "area".to_string(), "value".to_string(),
            "host_name".to_string(), "user_name".to_string(), "duration".to_string(),
        ],
        column_mappings: std::collections::HashMap::new(),
        filters: vec![
            // Add time filter for recent data (last 24 hours)
            crate::tables::ColumnFilter {
                column: "modification_time".to_string(),
                operator: crate::tables::FilterOperator::GreaterThan,
                value: crate::tables::FilterValue::Timestamp(
                    (chrono::Utc::now() - chrono::Duration::days(1)).to_rfc3339()
                ),
            },
        ],
        limit: Some(1000),
        order_by: None,
    };
    
    let results = crate::query_handler::QueryHandler::fetch_logged_alarms_data(&query_info, session).await?;
    debug!("📊 Fetched {} loggedalarms records", results.len());
    
    if results.is_empty() {
        return Err(anyhow::anyhow!("No loggedalarms data available"));
    }
    
    // Convert results to Arrow batch (similar to activealarms but with duration)
    let (names, instance_ids, alarm_group_ids, raise_times, acknowledgment_times, clear_times,
         reset_times, modification_times, states, priorities, event_texts, info_texts,
         origins, areas, values, host_names, user_names, durations) = results.into_iter().fold(
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(),
         Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(),
         Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
        |mut acc, result| {
            acc.0.push(result.name);
            acc.1.push(result.instance_id as i64);
            acc.2.push(result.alarm_group_id.map(|id| id as i64));
            
            // Parse timestamps
            let raise_time_nanos = chrono::DateTime::parse_from_rfc3339(&result.raise_time)
                .map(|dt| dt.timestamp_nanos_opt()).unwrap_or_default();
            acc.3.push(raise_time_nanos);
            
            let ack_time_nanos = result.acknowledgment_time.as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .and_then(|dt| dt.timestamp_nanos_opt());
            acc.4.push(ack_time_nanos);
            
            let clear_time_nanos = result.clear_time.as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .and_then(|dt| dt.timestamp_nanos_opt());
            acc.5.push(clear_time_nanos);
            
            let reset_time_nanos = result.reset_time.as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .and_then(|dt| dt.timestamp_nanos_opt());
            acc.6.push(reset_time_nanos);
            
            let modification_time_nanos = chrono::DateTime::parse_from_rfc3339(&result.modification_time)
                .map(|dt| dt.timestamp_nanos_opt()).unwrap_or_default();
            acc.7.push(modification_time_nanos);
            
            acc.8.push(result.state);
            acc.9.push(result.priority.map(|p| p as i64));
            acc.10.push(result.event_text.map(|v| v.join(";")));
            acc.11.push(result.info_text.map(|v| v.join(";")));
            acc.12.push(result.origin);
            acc.13.push(result.area);
            acc.14.push(result.value.map(|v| v.to_string()));
            acc.15.push(result.host_name);
            acc.16.push(result.user_name);
            acc.17.push(result.duration);
            acc
        },
    );
    
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(instance_ids)),
            Arc::new(Int64Array::from(alarm_group_ids)),
            Arc::new(TimestampNanosecondArray::from(raise_times)),
            Arc::new(TimestampNanosecondArray::from(acknowledgment_times)),
            Arc::new(TimestampNanosecondArray::from(clear_times)),
            Arc::new(TimestampNanosecondArray::from(reset_times)),
            Arc::new(TimestampNanosecondArray::from(modification_times)),
            Arc::new(StringArray::from(states)),
            Arc::new(Int64Array::from(priorities)),
            Arc::new(StringArray::from(event_texts)),
            Arc::new(StringArray::from(info_texts)),
            Arc::new(StringArray::from(origins)),
            Arc::new(StringArray::from(areas)),
            Arc::new(StringArray::from(values)),
            Arc::new(StringArray::from(host_names)),
            Arc::new(StringArray::from(user_names)),
            Arc::new(StringArray::from(durations)),
        ],
    )?;
    
    Ok(batch)
}

async fn create_populated_taglist_batch(_session: &crate::auth::AuthenticatedSession) -> Result<RecordBatch> {
    Err(anyhow::anyhow!("Not implemented yet"))
}

/// Execute a query that might involve both regular tables and catalog tables
#[allow(dead_code)]
pub async fn execute_mixed_query(
    sql: &str,
    regular_batches: Vec<(String, RecordBatch)>,
) -> Result<(Vec<RecordBatch>, u64)> {
    let start_time = Instant::now();
    
    let ctx = SessionContext::new();
    
    // Register PostgreSQL system functions
    register_postgresql_functions(&ctx)?;
    
    // Register regular tables (e.g., from GraphQL results)
    for (table_name, batch) in regular_batches {
        debug!("📊 Registering regular table '{}' with DataFusion", table_name);
        ctx.register_batch(&table_name, batch)?;
    }
    
    // Register all catalog tables if available
    if let Some(catalog) = catalog::get_catalog() {
        for table_name in catalog.get_table_names() {
            if let Some(table) = catalog.get_table(&table_name) {
                debug!("📋 Registering catalog table '{}' with DataFusion", table_name);
                
                if !table.data.is_empty() {
                    let combined_batch = if table.data.len() == 1 {
                        table.data[0].clone()
                    } else {
                        // Combine multiple batches
                        let schema = table.data[0].schema();
                        let mut columns = Vec::new();
                        
                        for col_idx in 0..schema.fields().len() {
                            let mut arrays = Vec::new();
                            for batch in &table.data {
                                arrays.push(batch.column(col_idx).clone());
                            }
                            let combined_array = arrow::compute::concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
                            columns.push(combined_array);
                        }
                        
                        RecordBatch::try_new(schema, columns)?
                    };
                    
                    // Register with normalized name (dots replaced with underscores)
                    let normalized_name = table_name.replace('.', "_");
                    debug!("📋 Registering catalog table as: {}", normalized_name);
                    ctx.register_batch(&normalized_name, combined_batch.clone())?;
                    
                    // If the table name starts with pg_catalog., also register without the prefix
                    // for backward compatibility (e.g., both "pg_catalog.pg_class" and pg_class)
                    if table_name.starts_with("pg_catalog.") {
                        if let Some(short_name) = table_name.strip_prefix("pg_catalog.") {
                            debug!("📋 Also registering '{}' as '{}'", table_name, short_name);
                            ctx.register_batch(short_name, combined_batch)?;
                        }
                    }
                }
            }
        }
    }
    
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    
    let elapsed_ms = start_time.elapsed().as_millis() as u64;
    debug!("⚡ DataFusion mixed query execution completed in {} ms", elapsed_ms);
    
    Ok((results, elapsed_ms))
}
