use crate::query_handler::QueryResult;
use crate::datafusion_handler;
use anyhow::Result;
use arrow::array::{BooleanArray, Int16Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::debug;

pub async fn handle_pg_proc_query(
    sql: &str,
) -> Result<QueryResult> {
    debug!("📊 Handling pg_proc query with DataFusion");

    // Define the schema - simplified version with most important columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int64, false),
        Field::new("proname", DataType::Utf8, false),
        Field::new("pronamespace", DataType::Int64, false),
        Field::new("proowner", DataType::Int64, false),
        Field::new("prolang", DataType::Int64, false),
        Field::new("procost", DataType::Float32, true),
        Field::new("prorows", DataType::Float32, true),
        Field::new("provariadic", DataType::Int64, true),
        Field::new("prosupport", DataType::Utf8, true),
        Field::new("prokind", DataType::Utf8, true),
        Field::new("prosecdef", DataType::Boolean, true),
        Field::new("proleakproof", DataType::Boolean, true),
        Field::new("proisstrict", DataType::Boolean, true),
        Field::new("proretset", DataType::Boolean, true),
        Field::new("proisagg", DataType::Boolean, true),
        Field::new("proiswindow", DataType::Boolean, true),
        Field::new("provolatile", DataType::Utf8, true),
        Field::new("proparallel", DataType::Utf8, true),
        Field::new("pronargs", DataType::Int16, true),
        Field::new("pronargdefaults", DataType::Int16, true),
        Field::new("prorettype", DataType::Int64, true),
        Field::new("proargtypes", DataType::Utf8, true), // Simplified as text
        Field::new("proallargtypes", DataType::Utf8, true),
        Field::new("proargmodes", DataType::Utf8, true),
        Field::new("proargnames", DataType::Utf8, true),
        Field::new("proargdefaults", DataType::Utf8, true),
        Field::new("protrftypes", DataType::Utf8, true),
        Field::new("prosrc", DataType::Utf8, true),
        Field::new("probin", DataType::Utf8, true),
        Field::new("proconfig", DataType::Utf8, true),
        Field::new("proacl", DataType::Utf8, true),
    ]));

    // Create data for our custom functions
    let function_data = vec![
        // pg_get_userbyid function
        (
            20000i64,
            "pg_get_userbyid",
            11i64, // pg_catalog namespace
            10i64, // owner
            12i64, // internal language
            "f",   // function
            1i16,  // 1 argument
            19i64, // returns name (OID 19)
            "26",  // takes oid (OID 26)
            "SELECT CASE WHEN $1 = 10 THEN 'postgres'::name ELSE 'unknown'::name END",
        ),
        // pg_get_function_identity_arguments function
        (
            20001i64,
            "pg_get_function_identity_arguments",
            11i64,
            10i64,
            12i64,
            "f",
            1i16,
            25i64, // returns text (OID 25)
            "26",  // takes oid
            "SELECT 'unknown'::text", // Simplified implementation
        ),
        // pg_get_viewdef function
        (
            20002i64,
            "pg_get_viewdef",
            11i64,
            10i64,
            12i64,
            "f",
            1i16,
            25i64, // returns text (OID 25)
            "26",  // takes oid
            "SELECT 'SELECT * FROM virtual_table'::text", // Simplified implementation
        ),
    ];

    // Build column vectors
    let len = function_data.len();
    let (oids, pronames, pronamespaces, proowners, prolangs, prokinds, pronargs, prorettypes, proargtypes, prosrcs): 
        (Vec<i64>, Vec<&str>, Vec<i64>, Vec<i64>, Vec<i64>, Vec<&str>, Vec<i16>, Vec<i64>, Vec<&str>, Vec<&str>) = 
        function_data.iter().map(|(oid, name, ns, owner, lang, kind, nargs, rettype, argtypes, src)| {
            (*oid, *name, *ns, *owner, *lang, *kind, *nargs, *rettype, *argtypes, *src)
        }).unzip10();
    
    // Create arrays for the RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(pronames)),
            Arc::new(Int64Array::from(pronamespaces)),
            Arc::new(Int64Array::from(proowners)),
            Arc::new(Int64Array::from(prolangs)),
            Arc::new(arrow::array::Float32Array::from(vec![100.0f32; len])), // procost
            Arc::new(arrow::array::Float32Array::from(vec![0.0f32; len])), // prorows
            Arc::new(Int64Array::from(vec![None::<i64>; len])), // provariadic
            Arc::new(StringArray::from(vec![None::<String>; len])), // prosupport
            Arc::new(StringArray::from(prokinds)),
            Arc::new(BooleanArray::from(vec![false; len])), // prosecdef
            Arc::new(BooleanArray::from(vec![false; len])), // proleakproof
            Arc::new(BooleanArray::from(vec![true; len])), // proisstrict
            Arc::new(BooleanArray::from(vec![false; len])), // proretset
            Arc::new(BooleanArray::from(vec![false; len])), // proisagg
            Arc::new(BooleanArray::from(vec![false; len])), // proiswindow
            Arc::new(StringArray::from(vec!["i"; len])), // provolatile (immutable)
            Arc::new(StringArray::from(vec!["s"; len])), // proparallel (safe)
            Arc::new(Int16Array::from(pronargs)),
            Arc::new(Int16Array::from(vec![0i16; len])), // pronargdefaults
            Arc::new(Int64Array::from(prorettypes)),
            Arc::new(StringArray::from(proargtypes)),
            Arc::new(StringArray::from(vec![None::<String>; len])), // proallargtypes
            Arc::new(StringArray::from(vec![None::<String>; len])), // proargmodes
            Arc::new(StringArray::from(vec![None::<String>; len])), // proargnames
            Arc::new(StringArray::from(vec![None::<String>; len])), // proargdefaults
            Arc::new(StringArray::from(vec![None::<String>; len])), // protrftypes
            Arc::new(StringArray::from(prosrcs)),
            Arc::new(StringArray::from(vec![None::<String>; len])), // probin
            Arc::new(StringArray::from(vec![None::<String>; len])), // proconfig
            Arc::new(StringArray::from(vec![None::<String>; len])), // proacl
        ],
    )?;

    // Execute the query using DataFusion
    let (results, datafusion_time_ms) =
        datafusion_handler::execute_query(sql, batch, "pg_proc").await?;

    // Convert RecordBatch results to QueryResult
    let mut query_result = QueryResult::from_record_batches(results)?;
    query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
    
    debug!("🔍 pg_proc query completed in {}ms", datafusion_time_ms);
    
    Ok(query_result)
}

// Helper trait to unzip 10-tuples
trait Unzip10<A, B, C, D, E, F, G, H, I, J> {
    fn unzip10(self) -> (Vec<A>, Vec<B>, Vec<C>, Vec<D>, Vec<E>, Vec<F>, Vec<G>, Vec<H>, Vec<I>, Vec<J>);
}

impl<It, A, B, C, D, E, F, G, H, I, J> Unzip10<A, B, C, D, E, F, G, H, I, J> for It
where
    It: Iterator<Item = (A, B, C, D, E, F, G, H, I, J)>,
{
    fn unzip10(self) -> (Vec<A>, Vec<B>, Vec<C>, Vec<D>, Vec<E>, Vec<F>, Vec<G>, Vec<H>, Vec<I>, Vec<J>) {
        let (mut a_vec, mut b_vec, mut c_vec, mut d_vec, mut e_vec, mut f_vec, mut g_vec, mut h_vec, mut i_vec, mut j_vec) = 
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        
        for (a, b, c, d, e, f, g, h, i, j) in self {
            a_vec.push(a);
            b_vec.push(b);
            c_vec.push(c);
            d_vec.push(d);
            e_vec.push(e);
            f_vec.push(f);
            g_vec.push(g);
            h_vec.push(h);
            i_vec.push(i);
            j_vec.push(j);
        }
        
        (a_vec, b_vec, c_vec, d_vec, e_vec, f_vec, g_vec, h_vec, i_vec, j_vec)
    }
}