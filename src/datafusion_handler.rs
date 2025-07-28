use anyhow::Result;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

pub async fn execute_query(
    sql: &str,
    batch: RecordBatch,
    table_name: &str,
) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();
    ctx.register_batch(table_name, batch)?;
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    Ok(results)
}
