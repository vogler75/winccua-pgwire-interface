
pub mod active_alarms_handler;
pub mod logged_alarms_handler;
pub mod logged_tag_values_handler;
pub mod pg_stat_activity_handler;
pub mod tag_list_handler;
pub mod tag_values_handler;

mod filter;
mod util;

use crate::auth::{AuthenticatedSession, SessionManager};
use crate::datafusion_handler;
use crate::sql_handler::SqlHandler;
use crate::tables::{QueryInfo, SqlResult, VirtualTable};
use anyhow::Result;
use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::{debug, warn};

/// Represents a single value in a query result
#[derive(Debug, Clone)]
pub enum QueryValue {
    Null,
    Text(String),
    Integer(i64),
    Float(f64),
    Timestamp(String),
    Boolean(bool),
}

/// Represents timing information for a query
#[derive(Debug, Default)]
pub struct QueryTimings {
    pub graphql_time_ms: Option<u64>,
    pub datafusion_time_ms: Option<u64>,
    pub overall_time_ms: Option<u64>,
}

/// Represents the result of a SQL query
#[derive(Debug)]
pub struct QueryResult {
    /// Column names in order
    pub columns: Vec<String>,
    /// Column types (PostgreSQL OIDs)
    pub column_types: Vec<u32>,
    /// Rows of data
    pub rows: Vec<Vec<QueryValue>>,
    /// Timing information (if available)
    pub timings: QueryTimings,
}

impl QueryResult {
    /// Create an empty result with just column definitions
    pub fn new(columns: Vec<String>, column_types: Vec<u32>) -> Self {
        Self {
            columns,
            column_types,
            rows: Vec::new(),
            timings: QueryTimings::default(),
        }
    }
    
    /// Add a row to the result
    pub fn add_row(&mut self, row: Vec<QueryValue>) {
        self.rows.push(row);
    }
    
    /// Get the number of rows
    #[allow(dead_code)]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
    
    /// Temporary compatibility function to convert from CSV strings
    /// This should be removed when all handlers are updated
    pub fn from_csv_string(csv_data: &str) -> Result<Self> {
        let lines: Vec<&str> = csv_data.trim().split('\n').collect();
        if lines.is_empty() {
            return Ok(QueryResult::new(vec![], vec![]));
        }
        
        // Parse CSV header with type information
        let headers = super::pg_protocol::response::parse_csv_line(lines[0]);
        let mut column_types: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
        let mut clean_headers: Vec<String> = Vec::new();
        
        for header in &headers {
            if header.contains(':') {
                let parts: Vec<&str> = header.splitn(2, ':').collect();
                if parts.len() == 2 {
                    clean_headers.push(parts[0].to_string());
                    let type_oid = match parts[1] {
                        "NUMERIC" => 1700,
                        "TIMESTAMP" => 1114,
                        "TEXT" => 25,
                        _ => 25,
                    };
                    column_types.insert(parts[0].to_string(), type_oid);
                } else {
                    clean_headers.push(header.clone());
                }
            } else {
                clean_headers.push(header.clone());
            }
        }
        
        // Create column types vector
        let col_types: Vec<u32> = clean_headers.iter()
            .map(|name| column_types.get(name).copied().unwrap_or(25))
            .collect();
        
        let mut result = QueryResult::new(clean_headers, col_types);
        
        // Process data rows
        for line in lines.iter().skip(1) {
            if line.trim().is_empty() {
                continue;
            }
            
            let values = super::pg_protocol::response::parse_csv_line(line);
            let row: Vec<QueryValue> = values.into_iter().map(|v| {
                if v == "NULL" {
                    QueryValue::Null
                } else {
                    QueryValue::Text(v)
                }
            }).collect();
            
            result.add_row(row);
        }
        
        Ok(result)
    }
    
    /// Convert from Arrow RecordBatch
    pub fn from_record_batches(batches: Vec<RecordBatch>) -> Result<Self> {
        if batches.is_empty() {
            return Ok(QueryResult::new(vec![], vec![]));
        }
        
        let schema = batches[0].schema();
        let mut columns = Vec::new();
        let mut column_types = Vec::new();
        
        // Extract column names and types
        for field in schema.fields() {
            columns.push(field.name().clone());
            let postgres_oid = arrow_type_to_postgres_oid(field.data_type());
            column_types.push(postgres_oid);
            tracing::debug!("🔧 Column '{}': Arrow type {:?} -> PostgreSQL OID {}", 
                field.name(), field.data_type(), postgres_oid);
        }
        
        let mut result = QueryResult::new(columns, column_types);
        
        // Process each batch
        for batch in batches {
            let num_rows = batch.num_rows();
            let num_cols = batch.num_columns();
            
            for row_idx in 0..num_rows {
                let mut row = Vec::new();
                
                for col_idx in 0..num_cols {
                    let column = batch.column(col_idx);
                    let value = extract_value_from_array(column, row_idx)?;
                    if row_idx == 0 { // Log first row for debugging
                        tracing::debug!("🔧 Column {}: {:?}", col_idx, value);
                    }
                    row.push(value);
                }
                
                result.add_row(row);
            }
        }
        
        Ok(result)
    }
}

// Convert Arrow DataType to PostgreSQL OID
fn arrow_type_to_postgres_oid(data_type: &DataType) -> u32 {
    match data_type {
        DataType::Boolean => 16,     // bool
        DataType::Int16 => 21,       // int2
        DataType::Int32 => 23,       // int4
        DataType::Int64 => 20,       // int8
        DataType::Float32 => 700,    // float4
        DataType::Float64 => 701,    // float8
        DataType::Utf8 => 25,        // text
        DataType::Timestamp(_, _) => 1114, // timestamp
        _ => 25,                     // default to text
    }
}

// Extract a value from an Arrow array at a specific index
fn extract_value_from_array(array: &dyn arrow::array::Array, index: usize) -> Result<QueryValue> {
    use arrow::array::*;
    
    if array.is_null(index) {
        return Ok(QueryValue::Null);
    }
    
    // Try to downcast to specific array types
    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        Ok(QueryValue::Boolean(arr.value(index)))
    } else if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        Ok(QueryValue::Integer(arr.value(index)))
    } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        Ok(QueryValue::Float(arr.value(index)))
    } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        Ok(QueryValue::Text(arr.value(index).to_string()))
    } else if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let timestamp = arr.value(index);
        let datetime = chrono::DateTime::from_timestamp_nanos(timestamp);
        // Use PostgreSQL TIMESTAMP format: YYYY-MM-DD HH:MM:SS.ssssss
        Ok(QueryValue::Timestamp(datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string()))
    } else {
        // Fallback: convert to string
        Ok(QueryValue::Text(format!("{:?}", array)))
    }
}

pub struct QueryHandler;

impl QueryHandler {
    #[allow(dead_code)]
    pub async fn execute_query(sql: &str, session: &AuthenticatedSession, session_manager: Arc<SessionManager>) -> Result<QueryResult> {
        Self::execute_query_with_connection(sql, session, session_manager, None).await
    }

    pub async fn execute_query_with_connection(sql: &str, session: &AuthenticatedSession, session_manager: Arc<SessionManager>, connection_id: Option<u32>) -> Result<QueryResult> {
        let query_start = std::time::Instant::now();
        // Parse the SQL query
        let sql_result = match SqlHandler::parse_query(sql) {
            Ok(result) => result,
            Err(e) => {
                return Err(e);
            }
        };
        debug!("📋 Parsed SQL result: {:?}", sql_result);

        // Handle based on result type
        let result = match sql_result {
            SqlResult::Query(query_info) => {
                // Execute based on table type
                match query_info.table {
                    VirtualTable::TagValues => {
                        Self::execute_datafusion_query(sql, &query_info, session).await
                    }
                    VirtualTable::LoggedTagValues => {
                        Self::execute_loggedtagvalues_datafusion_query(sql, &query_info, session)
                            .await
                    }
                    VirtualTable::ActiveAlarms => {
                        Self::execute_active_alarms_datafusion_query(sql, &query_info, session).await
                    }
                    VirtualTable::LoggedAlarms => {
                        Self::execute_logged_alarms_datafusion_query(sql, &query_info, session).await
                    }
                    VirtualTable::TagList => {
                        Self::execute_taglist_datafusion_query(sql, &query_info, session).await
                    }
                    VirtualTable::InformationSchemaTables
                    | VirtualTable::InformationSchemaColumns => {
                        crate::information_schema::handle_information_schema_query(&query_info)
                    }
                    VirtualTable::PgStatActivity => {
                        // Use the pg_stat_activity handler with DataFusion
                        crate::query_handler::pg_stat_activity_handler::handle_pg_stat_activity_query(
                            sql, session, session_manager.clone()
                        ).await
                    }
                    VirtualTable::FromLessQuery => {
                        Self::execute_from_less_query(sql, &query_info, session).await
                    }
                }
            }
            SqlResult::SetStatement(set_command) => {
                debug!("✅ Successfully executed SET statement: {}", set_command);
                // Return empty result for SET statements
                Ok(QueryResult::new(vec![], vec![]))
            }
        };

        // Calculate overall execution time and update connection if provided
        let overall_time_ms = query_start.elapsed().as_millis() as u64;
        
        // Update result with overall timing and extract individual timings
        let mut final_result = result?;
        final_result.timings.overall_time_ms = Some(overall_time_ms);
        
        if let Some(conn_id) = connection_id {
            // Update session manager with timing information
            debug!("🔍 Setting query timings for connection {}: GraphQL={:?}ms, DataFusion={:?}ms, Overall={}ms", 
                conn_id, final_result.timings.graphql_time_ms, final_result.timings.datafusion_time_ms, overall_time_ms);
            
            session_manager.set_all_query_timings(
                conn_id,
                final_result.timings.graphql_time_ms,
                final_result.timings.datafusion_time_ms,
                Some(overall_time_ms),
            ).await;
        } else {
            debug!("🔍 No connection_id provided, timing data not saved to session manager");
        }

        Ok(final_result)
    }

    async fn execute_taglist_datafusion_query(
        sql: &str,
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<QueryResult> {
        let graphql_start = std::time::Instant::now();
        let results = Self::fetch_tag_list_data(query_info, session).await?;
        let graphql_time_ms = graphql_start.elapsed().as_millis() as u64;

        // Define the schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_name", DataType::Utf8, false),
            Field::new("display_name", DataType::Utf8, true),
            Field::new("object_type", DataType::Utf8, true),
            Field::new("data_type", DataType::Utf8, true),
        ]));

        // Create columns from the results
        let (tag_names, display_names, object_types, data_types) = results.into_iter().fold(
            (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            |mut acc, result| {
                acc.0.push(result.name);
                acc.1.push(result.display_name);
                acc.2.push(result.object_type);
                acc.3.push(result.data_type);
                acc
            },
        );

        // Create a RecordBatch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(tag_names)),
                Arc::new(StringArray::from(display_names)),
                Arc::new(StringArray::from(object_types)),
                Arc::new(StringArray::from(data_types)),
            ],
        )?;

        // Execute the query using DataFusion
        let (results, datafusion_time_ms) =
            datafusion_handler::execute_query(sql, batch, &query_info.table.to_string()).await?;

        // Convert RecordBatch results directly to QueryResult
        let mut query_result = QueryResult::from_record_batches(results)?;
        
        // Add timing information
        query_result.timings.graphql_time_ms = Some(graphql_time_ms);
        query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
        
        debug!("🔍 TagList query timings: GraphQL={}ms, DataFusion={}ms", graphql_time_ms, datafusion_time_ms);
        
        Ok(query_result)
    }

    async fn execute_loggedtagvalues_datafusion_query(
        sql: &str,
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<QueryResult> {
        let graphql_start = std::time::Instant::now();
        let results = Self::fetch_logged_tag_values_data(query_info, session).await?;
        let graphql_time_ms = graphql_start.elapsed().as_millis() as u64;

        // Define the schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_name", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("timestamp_ms", DataType::Int64, true),
            Field::new("numeric_value", DataType::Float64, true),
            Field::new("string_value", DataType::Utf8, true),
            Field::new("quality", DataType::Utf8, true),
        ]));

        // Create columns from the results
        let (
            tag_names,
            timestamps,
            timestamps_ms,
            numeric_values,
            string_values,
            qualities,
        ) = results.into_iter().fold(
            (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            |mut acc, result| {
                acc.0.push(result.tag_name);
                let ts_nanos = chrono::DateTime::parse_from_rfc3339(&result.timestamp)
                    .map(|dt| dt.timestamp_nanos_opt())
                    .unwrap_or_default();
                acc.1.push(ts_nanos);
                acc.2.push(ts_nanos.map(|t| t / 1_000_000));
                acc.3.push(result.value.as_ref().and_then(|v| v.as_f64()));
                acc.4
                    .push(result.value.as_ref().and_then(|v| v.as_str()).map(|s| s.to_string()));
                acc.5
                    .push(result.quality.map(|q| q.quality));
                acc
            },
        );

        // Create a RecordBatch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(tag_names)),
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(Int64Array::from(timestamps_ms)),
                Arc::new(Float64Array::from(numeric_values)),
                Arc::new(StringArray::from(string_values)),
                Arc::new(StringArray::from(qualities)),
            ],
        )?;

        // Execute the query using DataFusion
        let (results, datafusion_time_ms) =
            datafusion_handler::execute_query(sql, batch, &query_info.table.to_string()).await?;

        // Convert RecordBatch results directly to QueryResult
        let mut query_result = QueryResult::from_record_batches(results)?;
        query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
        query_result.timings.graphql_time_ms = Some(graphql_time_ms);
        Ok(query_result)
    }

    async fn execute_datafusion_query(
        sql: &str,
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<QueryResult> {
        let graphql_start = std::time::Instant::now();
        let results = Self::fetch_tag_values_data(query_info, session).await?;
        let graphql_time_ms = graphql_start.elapsed().as_millis() as u64;

        // Define the schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_name", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("timestamp_ms", DataType::Int64, true),
            Field::new("numeric_value", DataType::Float64, true),
            Field::new("string_value", DataType::Utf8, true),
            Field::new("quality", DataType::Utf8, true),
        ]));

        // Create columns from the results
        let (
            tag_names,
            timestamps,
            timestamps_ms,
            numeric_values,
            string_values,
            qualities,
        ) = results.into_iter().fold(
            (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            |mut acc, result| {
                acc.0.push(result.name);
                if let Some(value) = result.value {
                    let ts_nanos = chrono::DateTime::parse_from_rfc3339(&value.timestamp)
                        .map(|dt| dt.timestamp_nanos_opt())
                        .unwrap_or_default();
                    acc.1.push(ts_nanos);
                    acc.2.push(ts_nanos.map(|t| t / 1_000_000));
                    acc.3.push(value.value.as_ref().and_then(|v| v.as_f64()));
                    acc.4
                        .push(value.value.as_ref().and_then(|v| v.as_str()).map(|s| s.to_string()));
                    acc.5
                        .push(value.quality.map(|q| q.quality));
                } else {
                    acc.1.push(None);
                    acc.2.push(None);
                    acc.3.push(None);
                    acc.4.push(None);
                    acc.5.push(None);
                }
                acc
            },
        );

        // Create a RecordBatch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(tag_names)),
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(Int64Array::from(timestamps_ms)),
                Arc::new(Float64Array::from(numeric_values)),
                Arc::new(StringArray::from(string_values)),
                Arc::new(StringArray::from(qualities)),
            ],
        )?;

        // Execute the query using DataFusion
        let (results, datafusion_time_ms) =
            datafusion_handler::execute_query(sql, batch, &query_info.table.to_string()).await?;

        // Convert RecordBatch results directly to QueryResult
        let mut query_result = QueryResult::from_record_batches(results)?;
        query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
        query_result.timings.graphql_time_ms = Some(graphql_time_ms);
        Ok(query_result)
    }

    async fn execute_from_less_query(
        sql: &str,
        _query_info: &QueryInfo, 
        session: &AuthenticatedSession,
    ) -> Result<QueryResult> {
        debug!("🔍 Executing FROM-less query: {}", sql.trim());
        
        // For SELECT 1 queries, extend the session as a keep-alive
        if sql.trim().to_uppercase().contains("SELECT 1") {
            match session.client.extend_session(&session.token).await {
                Ok(_) => debug!("✅ Session extended successfully for SELECT 1"),
                Err(e) => warn!("⚠️ Failed to extend session: {}", e),
            }
        }
        
        // Use DataFusion to execute the FROM-less query directly
        let ctx = datafusion::prelude::SessionContext::new();
        
        // Execute the query
        let df = ctx.sql(sql).await?;
        let batches = df.collect().await?;
        
        // Convert the results to QueryResult
        QueryResult::from_record_batches(batches)
    }

    async fn execute_active_alarms_datafusion_query(
        sql: &str,
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<QueryResult> {
        let graphql_start = std::time::Instant::now();
        let results = Self::fetch_active_alarms_data(query_info, session).await?;
        let graphql_time_ms = graphql_start.elapsed().as_millis() as u64;

        // Define the schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("instance_id", DataType::Int64, false),
            Field::new("alarm_group_id", DataType::Int64, true),
            Field::new("raise_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("acknowledgment_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("clear_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("reset_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("modification_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("state", DataType::Utf8, false),
            Field::new("priority", DataType::Int64, true),
            Field::new("event_text", DataType::Utf8, true),
            Field::new("info_text", DataType::Utf8, true),
            Field::new("origin", DataType::Utf8, true),
            Field::new("area", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
            Field::new("host_name", DataType::Utf8, true),
            Field::new("user_name", DataType::Utf8, true),
        ]));

        // Create columns from the results
        let (
            names,
            instance_ids,
            alarm_group_ids,
            raise_times,
            acknowledgment_times,
            clear_times,
            reset_times,
            modification_times,
            states,
            priorities,
            event_texts,
            info_texts,
            origins,
            areas,
            values,
            host_names,
            user_names,
        ) = results.into_iter().fold(
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
            |mut acc, result| {
                acc.0.push(result.name);
                acc.1.push(result.instance_id as i64);
                acc.2.push(result.alarm_group_id.map(|id| id as i64));
                
                let raise_time_nanos = chrono::DateTime::parse_from_rfc3339(&result.raise_time)
                    .map(|dt| dt.timestamp_nanos_opt())
                    .unwrap_or_default();
                acc.3.push(raise_time_nanos);
                
                let ack_time_nanos = result.acknowledgment_time.as_ref()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                    .and_then(|dt| dt.timestamp_nanos_opt());
                acc.4.push(ack_time_nanos);
                
                let clear_time_nanos = result.clear_time.as_ref()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                    .and_then(|dt| dt.timestamp_nanos_opt());
                acc.5.push(clear_time_nanos);
                
                let reset_time_nanos = result.reset_time.as_ref()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                    .and_then(|dt| dt.timestamp_nanos_opt());
                acc.6.push(reset_time_nanos);
                
                let modification_time_nanos = chrono::DateTime::parse_from_rfc3339(&result.modification_time)
                    .map(|dt| dt.timestamp_nanos_opt())
                    .unwrap_or_default();
                acc.7.push(modification_time_nanos);
                
                acc.8.push(result.state);
                acc.9.push(result.priority.map(|p| p as i64));
                acc.10.push(result.event_text.map(|v| v.join(";")));
                acc.11.push(result.info_text.map(|v| v.join(";")));
                acc.12.push(result.origin);
                acc.13.push(result.area);
                acc.14.push(result.value.map(|v| v.to_string()));
                acc.15.push(result.host_name);
                acc.16.push(result.user_name);
                acc
            },
        );

        // Create a RecordBatch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(instance_ids)),
                Arc::new(Int64Array::from(alarm_group_ids)),
                Arc::new(TimestampNanosecondArray::from(raise_times)),
                Arc::new(TimestampNanosecondArray::from(acknowledgment_times)),
                Arc::new(TimestampNanosecondArray::from(clear_times)),
                Arc::new(TimestampNanosecondArray::from(reset_times)),
                Arc::new(TimestampNanosecondArray::from(modification_times)),
                Arc::new(StringArray::from(states)),
                Arc::new(Int64Array::from(priorities)),
                Arc::new(StringArray::from(event_texts)),
                Arc::new(StringArray::from(info_texts)),
                Arc::new(StringArray::from(origins)),
                Arc::new(StringArray::from(areas)),
                Arc::new(StringArray::from(values)),
                Arc::new(StringArray::from(host_names)),
                Arc::new(StringArray::from(user_names)),
            ],
        )?;

        // Execute the query using DataFusion
        let (results, datafusion_time_ms) =
            datafusion_handler::execute_query(sql, batch, &query_info.table.to_string()).await?;

        // Convert RecordBatch results directly to QueryResult
        let mut query_result = QueryResult::from_record_batches(results)?;
        query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
        query_result.timings.graphql_time_ms = Some(graphql_time_ms);
        Ok(query_result)
    }

    async fn execute_logged_alarms_datafusion_query(
        sql: &str,
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<QueryResult> {
        let graphql_start = std::time::Instant::now();
        let results = Self::fetch_logged_alarms_data(query_info, session).await?;
        let graphql_time_ms = graphql_start.elapsed().as_millis() as u64;

        // Define the schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("instance_id", DataType::Int64, false),
            Field::new("alarm_group_id", DataType::Int64, true),
            Field::new("raise_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("acknowledgment_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("clear_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("reset_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("modification_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("state", DataType::Utf8, false),
            Field::new("priority", DataType::Int64, true),
            Field::new("event_text", DataType::Utf8, true),
            Field::new("info_text", DataType::Utf8, true),
            Field::new("origin", DataType::Utf8, true),
            Field::new("area", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
            Field::new("host_name", DataType::Utf8, true),
            Field::new("user_name", DataType::Utf8, true),
            Field::new("duration", DataType::Utf8, true),
        ]));

        // Create columns from the results
        let (
            names,
            instance_ids,
            alarm_group_ids,
            raise_times,
            acknowledgment_times,
            clear_times,
            reset_times,
            modification_times,
            states,
            priorities,
            event_texts,
            info_texts,
            origins,
            areas,
            values,
            host_names,
            user_names,
            durations,
        ) = results.into_iter().fold(
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
                Vec::new(),
            ),
            |mut acc, result| {
                acc.0.push(result.name);
                acc.1.push(result.instance_id as i64);
                acc.2.push(result.alarm_group_id.map(|id| id as i64));
                
                let raise_time_nanos = chrono::DateTime::parse_from_rfc3339(&result.raise_time)
                    .map(|dt| dt.timestamp_nanos_opt())
                    .unwrap_or_default();
                acc.3.push(raise_time_nanos);
                
                let ack_time_nanos = result.acknowledgment_time.as_ref()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                    .and_then(|dt| dt.timestamp_nanos_opt());
                acc.4.push(ack_time_nanos);
                
                let clear_time_nanos = result.clear_time.as_ref()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                    .and_then(|dt| dt.timestamp_nanos_opt());
                acc.5.push(clear_time_nanos);
                
                let reset_time_nanos = result.reset_time.as_ref()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                    .and_then(|dt| dt.timestamp_nanos_opt());
                acc.6.push(reset_time_nanos);
                
                let modification_time_nanos = chrono::DateTime::parse_from_rfc3339(&result.modification_time)
                    .map(|dt| dt.timestamp_nanos_opt())
                    .unwrap_or_default();
                acc.7.push(modification_time_nanos);
                
                acc.8.push(result.state);
                acc.9.push(result.priority.map(|p| p as i64));
                acc.10.push(result.event_text.map(|v| v.join(";")));
                acc.11.push(result.info_text.map(|v| v.join(";")));
                acc.12.push(result.origin);
                acc.13.push(result.area);
                acc.14.push(result.value.map(|v| v.to_string()));
                acc.15.push(result.host_name);
                acc.16.push(result.user_name);
                acc.17.push(result.duration);
                acc
            },
        );

        // Create a RecordBatch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(instance_ids)),
                Arc::new(Int64Array::from(alarm_group_ids)),
                Arc::new(TimestampNanosecondArray::from(raise_times)),
                Arc::new(TimestampNanosecondArray::from(acknowledgment_times)),
                Arc::new(TimestampNanosecondArray::from(clear_times)),
                Arc::new(TimestampNanosecondArray::from(reset_times)),
                Arc::new(TimestampNanosecondArray::from(modification_times)),
                Arc::new(StringArray::from(states)),
                Arc::new(Int64Array::from(priorities)),
                Arc::new(StringArray::from(event_texts)),
                Arc::new(StringArray::from(info_texts)),
                Arc::new(StringArray::from(origins)),
                Arc::new(StringArray::from(areas)),
                Arc::new(StringArray::from(values)),
                Arc::new(StringArray::from(host_names)),
                Arc::new(StringArray::from(user_names)),
                Arc::new(StringArray::from(durations)),
            ],
        )?;

        // Execute the query using DataFusion
        let (results, datafusion_time_ms) =
            datafusion_handler::execute_query(sql, batch, &query_info.table.to_string()).await?;

        // Convert RecordBatch results directly to QueryResult
        let mut query_result = QueryResult::from_record_batches(results)?;
        query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
        query_result.timings.graphql_time_ms = Some(graphql_time_ms);
        Ok(query_result)
    }
}
