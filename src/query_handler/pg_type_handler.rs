use crate::query_handler::QueryResult;
use crate::datafusion_handler;
use anyhow::Result;
use arrow::array::{BooleanArray, Int16Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::debug;

pub async fn handle_pg_type_query(
    sql: &str,
) -> Result<QueryResult> {
    debug!("📊 Handling pg_type query with DataFusion");

    // Define the schema - simplified version with most important columns
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
        Field::new("typinput", DataType::Utf8, false),
        Field::new("typoutput", DataType::Utf8, false),
        Field::new("typmodout", DataType::Utf8, false),
        Field::new("typmodin", DataType::Utf8, false),
        Field::new("typanalyze", DataType::Utf8, false),
        Field::new("typalign", DataType::Utf8, false),
        Field::new("typstorage", DataType::Utf8, false),
        Field::new("typnotnull", DataType::Boolean, false),
        Field::new("typbasetype", DataType::Int64, false),
        Field::new("typtypmod", DataType::Int32, false),
        Field::new("typndims", DataType::Int32, false),
        Field::new("typcollation", DataType::Int64, false),
        Field::new("typdefault", DataType::Utf8, true),
        Field::new("typacl", DataType::Utf8, true),
    ]));

    // Create data for essential PostgreSQL types
    let type_data = vec![
        // Basic types that are commonly queried
        (16i64, "bool", "b", "B", true, false, 1i16),           // boolean
        (19i64, "name", "S", "S", false, true, 64i16),          // name
        (20i64, "int8", "N", "N", true, false, 8i16),           // bigint
        (21i64, "int2", "N", "N", true, false, 2i16),           // smallint
        (23i64, "int4", "N", "N", true, false, 4i16),           // integer
        (25i64, "text", "S", "S", false, true, -1i16),          // text
        (26i64, "oid", "N", "N", true, false, 4i16),            // oid
        (700i64, "float4", "N", "N", true, false, 4i16),        // real
        (701i64, "float8", "N", "N", true, false, 8i16),        // double precision
        (1043i64, "varchar", "S", "S", false, true, -1i16),     // varchar
        (1114i64, "timestamp", "D", "D", false, false, 8i16),   // timestamp
        (1700i64, "numeric", "N", "N", false, false, -1i16),    // numeric
    ];

    let len = type_data.len();
    
    // Build column vectors
    let (oids, typnames, typcategories, typaligns, typbyvals, typispreferred, typlens): 
        (Vec<i64>, Vec<&str>, Vec<&str>, Vec<&str>, Vec<bool>, Vec<bool>, Vec<i16>) = 
        type_data.iter().map(|(oid, name, cat, align, byval, preferred, len)| {
            (*oid, *name, *cat, *align, *byval, *preferred, *len)
        }).unzip7();
    
    // Create arrays for the RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(typnames)),
            Arc::new(Int64Array::from(vec![11i64; len])), // typnamespace (pg_catalog)
            Arc::new(Int64Array::from(vec![10i64; len])), // typowner (postgres user)
            Arc::new(Int16Array::from(typlens)),
            Arc::new(BooleanArray::from(typbyvals)),
            Arc::new(StringArray::from(vec!["b"; len])), // typtype (base type)
            Arc::new(StringArray::from(typcategories)),
            Arc::new(BooleanArray::from(typispreferred)),
            Arc::new(BooleanArray::from(vec![true; len])), // typisdefined
            Arc::new(StringArray::from(vec![","; len])), // typdelim
            Arc::new(Int64Array::from(vec![0i64; len])), // typrelid
            Arc::new(Int64Array::from(vec![0i64; len])), // typelem
            Arc::new(Int64Array::from(vec![0i64; len])), // typarray
            Arc::new(StringArray::from(vec!["unknown"; len])), // typinput
            Arc::new(StringArray::from(vec!["unknown"; len])), // typoutput
            Arc::new(StringArray::from(vec!["-"; len])), // typmodout
            Arc::new(StringArray::from(vec!["-"; len])), // typmodin
            Arc::new(StringArray::from(vec!["-"; len])), // typanalyze
            Arc::new(StringArray::from(typaligns)),
            Arc::new(StringArray::from(vec!["p"; len])), // typstorage (plain)
            Arc::new(BooleanArray::from(vec![false; len])), // typnotnull
            Arc::new(Int64Array::from(vec![0i64; len])), // typbasetype
            Arc::new(Int32Array::from(vec![-1i32; len])), // typtypmod
            Arc::new(Int32Array::from(vec![0i32; len])), // typndims
            Arc::new(Int64Array::from(vec![0i64; len])), // typcollation
            Arc::new(StringArray::from(vec![None::<String>; len])), // typdefault
            Arc::new(StringArray::from(vec![None::<String>; len])), // typacl
        ],
    )?;

    // Execute the query using DataFusion
    let (results, datafusion_time_ms) =
        datafusion_handler::execute_query(sql, batch, "pg_type").await?;

    // Convert RecordBatch results to QueryResult
    let mut query_result = QueryResult::from_record_batches(results)?;
    query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
    
    debug!("🔍 pg_type query completed in {}ms", datafusion_time_ms);
    
    Ok(query_result)
}

// Helper trait to unzip 7-tuples
trait Unzip7<A, B, C, D, E, F, G> {
    fn unzip7(self) -> (Vec<A>, Vec<B>, Vec<C>, Vec<D>, Vec<E>, Vec<F>, Vec<G>);
}

impl<I, A, B, C, D, E, F, G> Unzip7<A, B, C, D, E, F, G> for I
where
    I: Iterator<Item = (A, B, C, D, E, F, G)>,
{
    fn unzip7(self) -> (Vec<A>, Vec<B>, Vec<C>, Vec<D>, Vec<E>, Vec<F>, Vec<G>) {
        let (mut a_vec, mut b_vec, mut c_vec, mut d_vec, mut e_vec, mut f_vec, mut g_vec) = 
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        
        for (a, b, c, d, e, f, g) in self {
            a_vec.push(a);
            b_vec.push(b);
            c_vec.push(c);
            d_vec.push(d);
            e_vec.push(e);
            f_vec.push(f);
            g_vec.push(g);
        }
        
        (a_vec, b_vec, c_vec, d_vec, e_vec, f_vec, g_vec)
    }
}