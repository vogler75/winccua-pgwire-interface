use crate::auth::{AuthenticatedSession, SessionManager};
use crate::query_handler::QueryResult;
use crate::datafusion_handler;
use anyhow::Result;
use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::{debug};

pub async fn handle_pg_stat_activity_query(
    sql: &str,
    _session: &AuthenticatedSession,
    session_manager: Arc<SessionManager>,
) -> Result<QueryResult> {
    debug!("📊 Handling pg_stat_activity query with DataFusion");

    // Get all active connections
    let connections = session_manager.get_connections().await;
    debug!("📊 Found {} active connections", connections.len());

    // Define the schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("datid", DataType::Int64, false),
        Field::new("datname", DataType::Utf8, true),
        Field::new("pid", DataType::Int64, false),
        Field::new("usename", DataType::Utf8, true),
        Field::new("application_name", DataType::Utf8, true),
        Field::new("client_addr", DataType::Utf8, false),
        Field::new("client_hostname", DataType::Utf8, true),
        Field::new("client_port", DataType::Int64, false),
        Field::new("backend_start", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("query_start", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("query_stop", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("state", DataType::Utf8, false),
        Field::new("query", DataType::Utf8, false),
        Field::new("graphql_time", DataType::Int64, true),
        Field::new("datafusion_time", DataType::Int64, true),
        Field::new("overall_time", DataType::Int64, true),
        Field::new("last_alive_sent", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
    ]));

    // Convert connections to Arrow columns
    let (
        datids,
        datnames,
        pids,
        usenames,
        application_names,
        client_addrs,
        client_hostnames,
        client_ports,
        backend_starts,
        query_starts,
        query_stops,
        states,
        queries,
        graphql_times,
        datafusion_times,
        overall_times,
        last_alive_sents,
    ) = connections.into_iter().fold(
        (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ),
        |mut acc, conn| {
            acc.0.push(0i64); // datid - always 0 since we don't have multiple databases
            acc.1.push(conn.database_name);
            acc.2.push(conn.connection_id as i64);
            acc.3.push(conn.username);
            acc.4.push(conn.application_name);
            acc.5.push(conn.client_addr.ip().to_string());
            acc.6.push(None::<String>); // client_hostname - not implemented yet
            acc.7.push(conn.client_addr.port() as i64);
            
            // Convert timestamps to nanoseconds
            let backend_start_nanos = conn.backend_start.timestamp_nanos_opt();
            acc.8.push(backend_start_nanos);
            
            let query_start_nanos = conn.query_start.and_then(|ts| ts.timestamp_nanos_opt());
            acc.9.push(query_start_nanos);
            
            let query_stop_nanos = conn.query_stop.and_then(|ts| ts.timestamp_nanos_opt());
            acc.10.push(query_stop_nanos);
            
            acc.11.push(conn.state.as_str().to_string());
            acc.12.push(conn.last_query);
            acc.13.push(conn.graphql_time_ms.map(|t| t as i64));
            acc.14.push(conn.datafusion_time_ms.map(|t| t as i64));
            acc.15.push(conn.overall_time_ms.map(|t| t as i64));
            
            let last_alive_sent_nanos = conn.last_alive_sent.and_then(|ts| ts.timestamp_nanos_opt());
            acc.16.push(last_alive_sent_nanos);
            
            acc
        },
    );

    // Create a RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(datids)),
            Arc::new(StringArray::from(datnames)),
            Arc::new(Int64Array::from(pids)),
            Arc::new(StringArray::from(usenames)),
            Arc::new(StringArray::from(application_names)),
            Arc::new(StringArray::from(client_addrs)),
            Arc::new(StringArray::from(client_hostnames)),
            Arc::new(Int64Array::from(client_ports)),
            Arc::new(TimestampNanosecondArray::from(backend_starts)),
            Arc::new(TimestampNanosecondArray::from(query_starts)),
            Arc::new(TimestampNanosecondArray::from(query_stops)),
            Arc::new(StringArray::from(states)),
            Arc::new(StringArray::from(queries)),
            Arc::new(Int64Array::from(graphql_times)),
            Arc::new(Int64Array::from(datafusion_times)),
            Arc::new(Int64Array::from(overall_times)),
            Arc::new(TimestampNanosecondArray::from(last_alive_sents)),
        ],
    )?;

    // Execute the query using DataFusion
    let (results, datafusion_time_ms) =
        datafusion_handler::execute_query(sql, batch, "pg_stat_activity").await?;

    // Convert RecordBatch results directly to QueryResult
    let mut query_result = QueryResult::from_record_batches(results)?;
    query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
    
    debug!("🔍 pg_stat_activity query timings: DataFusion={}ms", datafusion_time_ms);
    
    Ok(query_result)
}

