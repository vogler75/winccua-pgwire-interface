use crate::query_handler::QueryResult;
use crate::datafusion_handler;
use anyhow::Result;
use arrow::array::{BooleanArray, Int16Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::debug;

pub async fn handle_pg_class_query(
    sql: &str,
) -> Result<QueryResult> {
    debug!("📊 Handling pg_class query with DataFusion");

    // Define the schema - simplified version with most important columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int64, false),
        Field::new("relname", DataType::Utf8, false),
        Field::new("relnamespace", DataType::Int64, false),
        Field::new("reltype", DataType::Int64, false),
        Field::new("relowner", DataType::Int64, false),
        Field::new("relam", DataType::Int64, false),
        Field::new("relfilenode", DataType::Int64, false),
        Field::new("reltablespace", DataType::Int64, false),
        Field::new("relpages", DataType::Int64, true),
        Field::new("reltuples", DataType::Float32, true),
        Field::new("reltoastrelid", DataType::Int64, false),
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
        // Virtual tables (views)
        (16384i64, "tagvalues", 2200i64, "v", 6i16), // 6 columns
        (16385i64, "loggedtagvalues", 2200i64, "v", 6i16),
        (16386i64, "activealarms", 2200i64, "v", 17i16),
        (16387i64, "loggedalarms", 2200i64, "v", 18i16),
        (16388i64, "taglist", 2200i64, "v", 4i16),
        (16389i64, "pg_stat_activity", 11i64, "v", 17i16), // In pg_catalog schema
        
        // Information schema views
        (16390i64, "tables", 13427i64, "v", 12i16),
        (16391i64, "columns", 13427i64, "v", 44i16),
        
        // System catalog tables (minimal set for compatibility)
        (1259i64, "pg_class", 11i64, "r", 26i16),
        (2615i64, "pg_namespace", 11i64, "r", 4i16),
        (1255i64, "pg_proc", 11i64, "r", 30i16),
    ];

    // Build column vectors
    let (oids, relnames, relnamespaces, relkinds, relnatts): (Vec<i64>, Vec<&str>, Vec<i64>, Vec<&str>, Vec<i16>) = 
        table_data.iter().map(|(oid, name, ns, kind, natts)| {
            (*oid, *name, *ns, *kind, *natts)
        }).unzip5();

    let len = oids.len();
    
    // Create arrays for the RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(relnames)),
            Arc::new(Int64Array::from(relnamespaces)),
            Arc::new(Int64Array::from(vec![0i64; len])), // reltype
            Arc::new(Int64Array::from(vec![10i64; len])), // relowner (postgres user)
            Arc::new(Int64Array::from(vec![0i64; len])), // relam
            Arc::new(Int64Array::from(vec![0i64; len])), // relfilenode
            Arc::new(Int64Array::from(vec![0i64; len])), // reltablespace
            Arc::new(Int64Array::from(vec![None::<i64>; len])), // relpages
            Arc::new(arrow::array::Float32Array::from(vec![None::<f32>; len])), // reltuples
            Arc::new(Int64Array::from(vec![0i64; len])), // reltoastrelid
            Arc::new(BooleanArray::from(vec![false; len])), // relhasindex
            Arc::new(BooleanArray::from(vec![false; len])), // relisshared
            Arc::new(StringArray::from(vec!["p"; len])), // relpersistence (permanent)
            Arc::new(StringArray::from(relkinds)),
            Arc::new(Int16Array::from(relnatts)),
            Arc::new(Int16Array::from(vec![0i16; len])), // relchecks
            Arc::new(BooleanArray::from(vec![false; len])), // relhasrules
            Arc::new(BooleanArray::from(vec![false; len])), // relhastriggers
            Arc::new(BooleanArray::from(vec![false; len])), // relhassubclass
            Arc::new(BooleanArray::from(vec![false; len])), // relrowsecurity
            Arc::new(BooleanArray::from(vec![false; len])), // relforcerowsecurity
            Arc::new(BooleanArray::from(vec![true; len])), // relispopulated
            Arc::new(StringArray::from(vec!["d"; len])), // relreplident (default)
            Arc::new(BooleanArray::from(vec![false; len])), // relispartition
            Arc::new(StringArray::from(vec![None::<String>; len])), // relacl
        ],
    )?;

    // Execute the query using DataFusion
    let (results, datafusion_time_ms) =
        datafusion_handler::execute_query(sql, batch, "pg_class").await?;

    // Convert RecordBatch results to QueryResult
    let mut query_result = QueryResult::from_record_batches(results)?;
    query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
    
    debug!("🔍 pg_class query completed in {}ms", datafusion_time_ms);
    
    Ok(query_result)
}

// Helper trait to unzip 5-tuples
trait Unzip5<A, B, C, D, E> {
    fn unzip5(self) -> (Vec<A>, Vec<B>, Vec<C>, Vec<D>, Vec<E>);
}

impl<I, A, B, C, D, E> Unzip5<A, B, C, D, E> for I
where
    I: Iterator<Item = (A, B, C, D, E)>,
{
    fn unzip5(self) -> (Vec<A>, Vec<B>, Vec<C>, Vec<D>, Vec<E>) {
        let (mut a_vec, mut b_vec, mut c_vec, mut d_vec, mut e_vec) = 
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        
        for (a, b, c, d, e) in self {
            a_vec.push(a);
            b_vec.push(b);
            c_vec.push(c);
            d_vec.push(d);
            e_vec.push(e);
        }
        
        (a_vec, b_vec, c_vec, d_vec, e_vec)
    }
}