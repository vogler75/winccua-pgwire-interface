use anyhow::Result;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::time::Instant;
use tracing::info;

pub async fn execute_query(
    sql: &str,
    batch: RecordBatch,
    table_name: &str,
) -> Result<Vec<RecordBatch>> {
    let start_time = Instant::now();
    
    let ctx = SessionContext::new();
    ctx.register_batch(table_name, batch)?;
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    
    let elapsed_ms = start_time.elapsed().as_millis();
    info!("⚡ DataFusion query execution completed in {} ms", elapsed_ms);
    
    Ok(results)
}
