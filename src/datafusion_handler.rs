use anyhow::Result;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::time::Instant;
use tracing::debug;
use crate::catalog;

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
                    
                    // Register with direct name
                    ctx.register_batch(&table_name, combined_batch.clone())?;
                    
                    // Also register with pg_catalog prefix
                    let pg_catalog_name = format!("pg_catalog.{}", table_name);
                    debug!("📋 Also registering as '{}'", pg_catalog_name);
                    ctx.register_batch(&pg_catalog_name, combined_batch)?;
                }
            }
        }
    }
    
    let df = ctx.sql(sql).await?;
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
                    
                    // Register with direct name
                    ctx.register_batch(&table_name, combined_batch.clone())?;
                    
                    // Also register with pg_catalog prefix
                    let pg_catalog_name = format!("pg_catalog.{}", table_name);
                    debug!("📋 Also registering as '{}'", pg_catalog_name);
                    ctx.register_batch(&pg_catalog_name, combined_batch)?;
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
