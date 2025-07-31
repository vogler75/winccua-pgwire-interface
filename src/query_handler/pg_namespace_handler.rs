use crate::query_handler::QueryResult;
use crate::datafusion_handler;
use anyhow::Result;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::debug;

pub async fn handle_pg_namespace_query(
    sql: &str,
) -> Result<QueryResult> {
    debug!("📊 Handling pg_namespace query with DataFusion");

    // Define the schema according to PostgreSQL pg_namespace
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int64, false),
        Field::new("nspname", DataType::Utf8, false),
        Field::new("nspowner", DataType::Int64, false),
        Field::new("nspacl", DataType::Utf8, true), // Simplified as text for now
    ]));

    // Minimal data needed for basic functionality - focusing on public schema as requested
    let oids = vec![crate::constants::OID_PG_CATALOG_NAMESPACE, crate::constants::OID_PUBLIC_NAMESPACE, crate::constants::OID_INFORMATION_SCHEMA_NAMESPACE];
    let nspnames = vec![crate::constants::SCHEMA_PG_CATALOG, crate::constants::SCHEMA_PUBLIC, crate::constants::SCHEMA_INFORMATION_SCHEMA];
    let nspowners = vec![crate::constants::OID_POSTGRES_USER, crate::constants::OID_POSTGRES_USER, crate::constants::OID_POSTGRES_USER];
    let nspacls = vec![None::<String>, None, None]; // No ACL restrictions for now

    // Create a RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(nspnames)),
            Arc::new(Int64Array::from(nspowners)),
            Arc::new(StringArray::from(nspacls)),
        ],
    )?;

    // Execute the query using DataFusion
    let (results, datafusion_time_ms) =
        datafusion_handler::execute_query(sql, batch, crate::constants::TABLE_PG_NAMESPACE).await?;

    // Convert RecordBatch results to QueryResult
    let mut query_result = QueryResult::from_record_batches(results)?;
    query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
    
    debug!("🔍 pg_namespace query completed in {}ms", datafusion_time_ms);
    
    Ok(query_result)
}