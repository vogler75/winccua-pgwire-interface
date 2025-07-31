use anyhow::Result;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::logical_expr::{ScalarUDF, Signature, Volatility, ScalarUDFImpl, ScalarFunctionArgs, ColumnarValue};
use datafusion::arrow::datatypes::DataType;
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

/// Register PostgreSQL system functions with DataFusion context
fn register_postgresql_functions(ctx: &SessionContext) -> Result<()> {
    ctx.register_udf(ScalarUDF::new_from_impl(PgGetUserByIdUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(PgGetFunctionIdentityArgumentsUDF::new()));
    Ok(())
}

/// Replace schema-qualified table names with underscore versions and handle PostgreSQL type casts
fn normalize_schema_qualified_tables(sql: &str) -> String {
    use regex::Regex;
    
    // First, handle PostgreSQL type casts like 'value'::pg_catalog.regtype
    // Pattern to match ::pg_catalog.type_name and remove the cast
    let cast_re = Regex::new(r"::pg_catalog\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
    let mut processed_sql = cast_re.replace_all(sql, "").to_string();
    
    // Pattern to match pg_catalog.table_name (case insensitive)
    let re = Regex::new(r"(?i)\bpg_catalog\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
    
    // Replace pg_catalog.table_name with pg_catalog_table_name
    processed_sql = re.replace_all(&processed_sql, "pg_catalog_$1").to_string();
    
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
