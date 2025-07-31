use anyhow::Result;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::time::Instant;
use tracing::debug;
use crate::catalog;

/// Replace schema-qualified table names with underscore versions to avoid DataFusion schema parsing
fn normalize_schema_qualified_tables(sql: &str) -> String {
    // Use regex to find pg_catalog.table_name patterns and replace dots with underscores
    use regex::Regex;
    
    // Pattern to match pg_catalog.table_name (case insensitive)
    let re = Regex::new(r"(?i)\bpg_catalog\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
    
    // Replace pg_catalog.table_name with pg_catalog_table_name
    re.replace_all(sql, "pg_catalog_$1").to_string()
}

pub async fn execute_query(
    sql: &str,
    batch: RecordBatch,
    table_name: &str,
) -> Result<(Vec<RecordBatch>, u64)> {
    let start_time = Instant::now();
    
    let ctx = SessionContext::new();
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
