
pub mod active_alarms_handler;
pub mod logged_alarms_handler;
pub mod logged_tag_values_handler;
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
use tracing::{debug, info, warn};

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
                // Route all queries through unified DataFusion execution
                Self::execute_unified_datafusion_query(sql, &query_info, session, session_manager.clone()).await
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
            if crate::LOG_SQL.load(std::sync::atomic::Ordering::Relaxed) {
                info!("🕐 Query completed in {}ms for connection {} (GraphQL: {:?}ms, DataFusion: {:?}ms)", 
                    overall_time_ms, conn_id, 
                    final_result.timings.graphql_time_ms,
                    final_result.timings.datafusion_time_ms);
            } else {
                debug!("🕐 Query completed in {}ms for connection {} (GraphQL: {:?}ms, DataFusion: {:?}ms)", 
                    overall_time_ms, conn_id, 
                    final_result.timings.graphql_time_ms,
                    final_result.timings.datafusion_time_ms);
            }
        } else {
            debug!("🔍 No connection_id provided, timing data not saved to session manager");
        }

        Ok(final_result)
    }

    async fn execute_unified_datafusion_query(
        sql: &str,
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
        session_manager: Arc<SessionManager>,
    ) -> Result<QueryResult> {
        debug!("🚀 Executing unified DataFusion query for table: {}", query_info.table.to_string());
        
        let graphql_start = std::time::Instant::now();
        
        // Generate data based on table type
        let batch = match query_info.table {
            VirtualTable::TagValues => {
                let results = Self::fetch_tag_values_data(query_info, session).await?;
                Self::create_tag_values_record_batch(results)?
            }
            VirtualTable::LoggedTagValues => {
                let results = Self::fetch_logged_tag_values_data(query_info, session).await?;
                Self::create_logged_tag_values_record_batch(results)?
            }
            VirtualTable::ActiveAlarms => {
                let results = Self::fetch_active_alarms_data(query_info, session).await?;
                Self::create_active_alarms_record_batch(results)?
            }
            VirtualTable::LoggedAlarms => {
                let results = Self::fetch_logged_alarms_data(query_info, session).await?;
                Self::create_logged_alarms_record_batch(results)?
            }
            VirtualTable::TagList => {
                let results = Self::fetch_tag_list_data(query_info, session).await?;
                Self::create_tag_list_record_batch(results)?
            }
            VirtualTable::InformationSchemaTables => {
                Self::create_information_schema_tables_record_batch(query_info)?
            }
            VirtualTable::InformationSchemaColumns => {
                Self::create_information_schema_columns_record_batch(query_info)?
            }
            VirtualTable::PgStatActivity => {
                Self::create_pg_stat_activity_record_batch(session_manager).await?
            }
            VirtualTable::FromLessQuery => {
                // For FROM-less queries, create an empty batch and use DataFusion directly
                return Self::execute_from_less_query_datafusion(sql, session).await;
            }
        };
        
        let graphql_time_ms = graphql_start.elapsed().as_millis() as u64;
        
        // Execute with DataFusion
        let (results, datafusion_time_ms) =
            datafusion_handler::execute_query(sql, batch, &query_info.table.to_string()).await?;

        // Convert results to QueryResult
        let mut query_result = QueryResult::from_record_batches(results)?;
        query_result.timings.graphql_time_ms = Some(graphql_time_ms);
        query_result.timings.datafusion_time_ms = Some(datafusion_time_ms);
        
        debug!("🔍 Unified query timings: GraphQL={}ms, DataFusion={}ms", graphql_time_ms, datafusion_time_ms);
        
        Ok(query_result)
    }

    fn create_tag_list_record_batch(results: Vec<crate::graphql::types::BrowseResult>) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_name", DataType::Utf8, false),
            Field::new("display_name", DataType::Utf8, true),
            Field::new("object_type", DataType::Utf8, true),
            Field::new("data_type", DataType::Utf8, true),
        ]));

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

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(tag_names)),
                Arc::new(StringArray::from(display_names)),
                Arc::new(StringArray::from(object_types)),
                Arc::new(StringArray::from(data_types)),
            ],
        ).map_err(Into::into)
    }

    fn create_logged_tag_values_record_batch(results: Vec<crate::graphql::types::LoggedTagValue>) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_name", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("timestamp_ms", DataType::Int64, true),
            Field::new("numeric_value", DataType::Float64, true),
            Field::new("string_value", DataType::Utf8, true),
            Field::new("quality", DataType::Utf8, true),
        ]));

        let (tag_names, timestamps, timestamp_ms_vec, numeric_values, string_values, qualities) = 
            results.into_iter().fold(
                (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
                |mut acc, result| {
                    acc.0.push(result.tag_name);
                    
                    // Parse timestamp
                    let timestamp_ns = if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&result.timestamp) {
                        Some(dt.timestamp_nanos_opt().unwrap_or(0))
                    } else {
                        None
                    };
                    acc.1.push(timestamp_ns);
                    acc.2.push(timestamp_ns.map(|ns| ns / 1_000_000)); // Convert to milliseconds
                    
                    // Handle values
                    if let Some(value) = result.value {
                        if let Some(num) = value.as_f64() {
                            acc.3.push(Some(num));
                            acc.4.push(None);
                        } else if let Some(str_val) = value.as_str() {
                            acc.3.push(None);
                            acc.4.push(Some(str_val.to_string()));
                        } else {
                            acc.3.push(None);
                            acc.4.push(Some(value.to_string()));
                        }
                    } else {
                        acc.3.push(None);
                        acc.4.push(None);
                    }
                    
                    acc.5.push(result.quality.map(|q| q.quality));
                    acc
                },
            );

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(tag_names)),
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(Int64Array::from(timestamp_ms_vec)),
                Arc::new(Float64Array::from(numeric_values)),
                Arc::new(StringArray::from(string_values)),
                Arc::new(StringArray::from(qualities)),
            ],
        ).map_err(Into::into)
    }

    fn create_tag_values_record_batch(results: Vec<crate::graphql::types::TagValueResult>) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_name", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("timestamp_ms", DataType::Int64, true),
            Field::new("numeric_value", DataType::Float64, true),
            Field::new("string_value", DataType::Utf8, true),
            Field::new("quality", DataType::Utf8, true),
        ]));

        let (tag_names, timestamps, timestamp_ms_vec, numeric_values, string_values, qualities) = 
            results.into_iter().fold(
                (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
                |mut acc, result| {
                    acc.0.push(result.name);
                    
                    if let Some(value) = result.value {
                        // Parse timestamp
                        let timestamp_ns = if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&value.timestamp) {
                            Some(dt.timestamp_nanos_opt().unwrap_or(0))
                        } else {
                            None
                        };
                        acc.1.push(timestamp_ns);
                        acc.2.push(timestamp_ns.map(|ns| ns / 1_000_000));
                        
                        // Handle values
                        if let Some(val) = value.value {
                            if let Some(num) = val.as_f64() {
                                acc.3.push(Some(num));
                                acc.4.push(None);
                            } else if let Some(str_val) = val.as_str() {
                                acc.3.push(None);
                                acc.4.push(Some(str_val.to_string()));
                            } else {
                                acc.3.push(None);
                                acc.4.push(Some(val.to_string()));
                            }
                        } else {
                            acc.3.push(None);
                            acc.4.push(None);
                        }
                        
                        acc.5.push(value.quality.map(|q| q.quality));
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

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(tag_names)),
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(Int64Array::from(timestamp_ms_vec)),
                Arc::new(Float64Array::from(numeric_values)),
                Arc::new(StringArray::from(string_values)),
                Arc::new(StringArray::from(qualities)),
            ],
        ).map_err(Into::into)
    }

    fn create_active_alarms_record_batch(results: Vec<crate::graphql::types::ActiveAlarm>) -> Result<RecordBatch> {
        // Create schema based on active alarms table definition
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("instance_id", DataType::Int64, true),
            Field::new("alarm_group_id", DataType::Int64, true),
            Field::new("raise_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("acknowledgment_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("clear_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("reset_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("modification_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("state", DataType::Utf8, true),
            Field::new("priority", DataType::Int64, true),
            Field::new("event_text", DataType::Utf8, true),
            Field::new("info_text", DataType::Utf8, true),
            Field::new("origin", DataType::Utf8, true),
            Field::new("area", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
            Field::new("host_name", DataType::Utf8, true),
            Field::new("user_name", DataType::Utf8, true),
        ]));

        let (names, instance_ids, alarm_group_ids, raise_times, ack_times, clear_times, 
             reset_times, mod_times, states, priorities, event_texts, info_texts, 
             origins, areas, values, host_names, user_names) = results.into_iter().fold(
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(),
             Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(),
             Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            |mut acc, result| {
                acc.0.push(result.name);
                acc.1.push(Some(result.instance_id as i64));
                acc.2.push(result.alarm_group_id.map(|i| i as i64));
                
                // Parse timestamps  
                acc.3.push(Self::parse_string_timestamp_to_nanos(&result.raise_time));
                acc.4.push(Self::parse_timestamp_to_nanos(&result.acknowledgment_time));
                acc.5.push(Self::parse_timestamp_to_nanos(&result.clear_time));
                acc.6.push(Self::parse_timestamp_to_nanos(&result.reset_time));
                acc.7.push(Self::parse_string_timestamp_to_nanos(&result.modification_time));
                
                acc.8.push(Some(result.state));
                acc.9.push(result.priority.map(|p| p as i64));
                acc.10.push(result.event_text.map(|texts| texts.join(", ")));
                acc.11.push(result.info_text.map(|texts| texts.join(", ")));
                acc.12.push(result.origin);
                acc.13.push(result.area);
                acc.14.push(result.value.map(|v| v.to_string()));
                acc.15.push(result.host_name);
                acc.16.push(result.user_name);
                acc
            },
        );

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(instance_ids)),
                Arc::new(Int64Array::from(alarm_group_ids)),
                Arc::new(TimestampNanosecondArray::from(raise_times)),
                Arc::new(TimestampNanosecondArray::from(ack_times)),
                Arc::new(TimestampNanosecondArray::from(clear_times)),
                Arc::new(TimestampNanosecondArray::from(reset_times)),
                Arc::new(TimestampNanosecondArray::from(mod_times)),
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
        ).map_err(Into::into)
    }

    fn create_logged_alarms_record_batch(results: Vec<crate::graphql::types::LoggedAlarm>) -> Result<RecordBatch> {
        // Similar to active alarms but with duration field
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("instance_id", DataType::Int64, true),
            Field::new("alarm_group_id", DataType::Int64, true),
            Field::new("raise_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("acknowledgment_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("clear_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("reset_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("modification_time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("state", DataType::Utf8, true),
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

        let (names, instance_ids, alarm_group_ids, raise_times, ack_times, clear_times, 
             reset_times, mod_times, states, priorities, event_texts, info_texts, 
             origins, areas, values, host_names, user_names, durations) = results.into_iter().fold(
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(),
             Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(),
             Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            |mut acc, result| {
                acc.0.push(result.name);
                acc.1.push(Some(result.instance_id as i64));
                acc.2.push(result.alarm_group_id.map(|i| i as i64));
                
                // Parse timestamps
                acc.3.push(Self::parse_string_timestamp_to_nanos(&result.raise_time));
                acc.4.push(Self::parse_timestamp_to_nanos(&result.acknowledgment_time));
                acc.5.push(Self::parse_timestamp_to_nanos(&result.clear_time));
                acc.6.push(Self::parse_timestamp_to_nanos(&result.reset_time));
                acc.7.push(Self::parse_string_timestamp_to_nanos(&result.modification_time));
                
                acc.8.push(Some(result.state));
                acc.9.push(result.priority.map(|p| p as i64));
                acc.10.push(result.event_text.map(|texts| texts.join(", ")));
                acc.11.push(result.info_text.map(|texts| texts.join(", ")));
                acc.12.push(result.origin);
                acc.13.push(result.area);
                acc.14.push(result.value.map(|v| v.to_string()));
                acc.15.push(result.host_name);
                acc.16.push(result.user_name);
                acc.17.push(result.duration);
                acc
            },
        );

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(instance_ids)),
                Arc::new(Int64Array::from(alarm_group_ids)),
                Arc::new(TimestampNanosecondArray::from(raise_times)),
                Arc::new(TimestampNanosecondArray::from(ack_times)),
                Arc::new(TimestampNanosecondArray::from(clear_times)),
                Arc::new(TimestampNanosecondArray::from(reset_times)),
                Arc::new(TimestampNanosecondArray::from(mod_times)),
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
        ).map_err(Into::into)
    }

    fn parse_timestamp_to_nanos(timestamp_opt: &Option<String>) -> Option<i64> {
        timestamp_opt.as_ref().and_then(|ts| {
            chrono::DateTime::parse_from_rfc3339(ts)
                .ok()
                .and_then(|dt| dt.timestamp_nanos_opt())
        })
    }

    fn parse_string_timestamp_to_nanos(timestamp: &str) -> Option<i64> {
        chrono::DateTime::parse_from_rfc3339(timestamp)
            .ok()
            .and_then(|dt| dt.timestamp_nanos_opt())
    }

    fn create_information_schema_tables_record_batch(_query_info: &QueryInfo) -> Result<RecordBatch> {
        // Create schema for information_schema.tables
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, true),
            Field::new("table_schema", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, true),
            Field::new("self_referencing_column_name", DataType::Utf8, true),
            Field::new("reference_generation", DataType::Utf8, true),
            Field::new("user_defined_type_catalog", DataType::Utf8, true),
            Field::new("user_defined_type_schema", DataType::Utf8, true),
            Field::new("user_defined_type_name", DataType::Utf8, true),
            Field::new("is_insertable_into", DataType::Utf8, true),
            Field::new("is_typed", DataType::Utf8, true),
            Field::new("commit_action", DataType::Utf8, true),
        ]));

        let tables = vec!["tagvalues", "loggedtagvalues", "activealarms", "loggedalarms", "taglist"];
        let table_catalogs: Vec<Option<String>> = vec![Some("winccua".to_string()); tables.len()];
        let table_schemas: Vec<Option<String>> = vec![Some("public".to_string()); tables.len()];
        let table_names: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
        let table_types: Vec<Option<String>> = vec![Some("VIEW".to_string()); tables.len()];
        let nulls: Vec<Option<String>> = vec![None; tables.len()];
        let nos: Vec<Option<String>> = vec![Some("NO".to_string()); tables.len()];

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(table_catalogs)),
                Arc::new(StringArray::from(table_schemas)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(table_types)),
                Arc::new(StringArray::from(nulls.clone())),
                Arc::new(StringArray::from(nulls.clone())),
                Arc::new(StringArray::from(nulls.clone())),
                Arc::new(StringArray::from(nulls.clone())),
                Arc::new(StringArray::from(nulls.clone())),
                Arc::new(StringArray::from(nos.clone())),
                Arc::new(StringArray::from(nos.clone())),
                Arc::new(StringArray::from(nulls)),
            ],
        ).map_err(Into::into)
    }

    fn create_information_schema_columns_record_batch(_query_info: &QueryInfo) -> Result<RecordBatch> {
        // Create schema for information_schema.columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, true),
            Field::new("table_schema", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int64, false),
            Field::new("column_default", DataType::Utf8, true),
            Field::new("is_nullable", DataType::Utf8, true),
            Field::new("data_type", DataType::Utf8, true),
            Field::new("character_maximum_length", DataType::Int64, true),
            Field::new("character_octet_length", DataType::Int64, true),
            Field::new("numeric_precision", DataType::Int64, true),
            Field::new("numeric_precision_radix", DataType::Int64, true),
            Field::new("numeric_scale", DataType::Int64, true),
            Field::new("datetime_precision", DataType::Int64, true),
            Field::new("interval_type", DataType::Utf8, true),
            Field::new("interval_precision", DataType::Int64, true),
            Field::new("character_set_catalog", DataType::Utf8, true),
            Field::new("character_set_schema", DataType::Utf8, true),
        ]));

        // Generate columns for all tables
        let mut all_columns = Vec::new();
        let table_columns = vec![
            ("tagvalues", vec!["tag_name", "timestamp", "numeric_value", "string_value", "quality"]),
            ("loggedtagvalues", vec!["tag_name", "timestamp", "numeric_value", "string_value", "quality"]),
            ("activealarms", vec!["name", "instance_id", "raise_time", "state", "priority"]),
            ("loggedalarms", vec!["name", "instance_id", "raise_time", "modification_time", "state", "priority"]),
            ("taglist", vec!["tag_name", "display_name", "object_type", "data_type"]),
        ];

        for (table_name, columns) in table_columns {
            for (i, column_name) in columns.iter().enumerate() {
                all_columns.push((table_name.to_string(), column_name.to_string(), i as i64 + 1));
            }
        }

        let table_catalogs: Vec<Option<String>> = vec![Some("winccua".to_string()); all_columns.len()];
        let table_schemas: Vec<Option<String>> = vec![Some("public".to_string()); all_columns.len()];
        let table_names: Vec<String> = all_columns.iter().map(|(t, _, _)| t.clone()).collect();
        let column_names: Vec<String> = all_columns.iter().map(|(_, c, _)| c.clone()).collect();
        let ordinal_positions: Vec<i64> = all_columns.iter().map(|(_, _, p)| *p).collect();
        let column_defaults: Vec<Option<String>> = vec![None; all_columns.len()];
        let is_nullables: Vec<Option<String>> = vec![Some("YES".to_string()); all_columns.len()];
        let data_types: Vec<Option<String>> = vec![Some("text".to_string()); all_columns.len()];
        let nulls: Vec<Option<i64>> = vec![None; all_columns.len()];
        let null_strings: Vec<Option<String>> = vec![None; all_columns.len()];

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(table_catalogs)),
                Arc::new(StringArray::from(table_schemas)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(column_names)),
                Arc::new(Int64Array::from(ordinal_positions)),
                Arc::new(StringArray::from(column_defaults)),
                Arc::new(StringArray::from(is_nullables)),
                Arc::new(StringArray::from(data_types)),
                Arc::new(Int64Array::from(nulls.clone())),
                Arc::new(Int64Array::from(nulls.clone())),
                Arc::new(Int64Array::from(nulls.clone())),
                Arc::new(Int64Array::from(nulls.clone())),
                Arc::new(Int64Array::from(nulls.clone())),
                Arc::new(Int64Array::from(nulls)),
                Arc::new(StringArray::from(null_strings.clone())),
                Arc::new(Int64Array::from(vec![None; all_columns.len()])),
                Arc::new(StringArray::from(null_strings.clone())),
                Arc::new(StringArray::from(null_strings)),
            ],
        ).map_err(Into::into)
    }

    async fn create_pg_stat_activity_record_batch(session_manager: Arc<SessionManager>) -> Result<RecordBatch> {
        // Reuse the existing pg_stat_activity logic but return RecordBatch directly
        let connections = session_manager.get_connections().await;
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("datid", DataType::Int64, false),
            Field::new("datname", DataType::Utf8, true),
            Field::new("pid", DataType::Int64, false),
            Field::new("usename", DataType::Utf8, true),
            Field::new("application_name", DataType::Utf8, true),
            Field::new("client_addr", DataType::Utf8, false),
            Field::new("client_hostname", DataType::Utf8, true),
            Field::new("client_port", DataType::Int64, false),
            Field::new("backend_start", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("query_start", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("query_stop", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("state", DataType::Utf8, true),
            Field::new("query", DataType::Utf8, true),
            Field::new("graphql_time", DataType::Int64, true),
            Field::new("datafusion_time", DataType::Int64, true),
            Field::new("overall_time", DataType::Int64, true),
            Field::new("last_alive_sent", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));

        // Convert connections to arrays (using correct field names)
        let (datids, datnames, pids, usenames, app_names, client_addrs, client_hostnames, client_ports,
             backend_starts, query_starts, query_stops, states, queries, 
             graphql_times, datafusion_times, overall_times, last_alive_sents) = 
            connections.into_iter().fold(
                (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(),
                 Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), 
                 Vec::new(), Vec::new(), Vec::new(), Vec::new()),
                |mut acc, conn| {
                    acc.0.push(0i64); // datid - always 0 since we don't have multiple databases
                    acc.1.push(conn.database_name);
                    acc.2.push(conn.connection_id as i64); // use connection_id as pid
                    acc.3.push(conn.username);
                    acc.4.push(conn.application_name);
                    acc.5.push(conn.client_addr.ip().to_string());
                    acc.6.push(None::<String>); // client_hostname - not implemented
                    acc.7.push(conn.client_addr.port() as i64); // client_port
                    
                    // Convert timestamps to nanoseconds
                    acc.8.push(Some(conn.backend_start.timestamp_nanos_opt().unwrap_or(0)));
                    acc.9.push(conn.query_start.map(|dt| dt.timestamp_nanos_opt().unwrap_or(0)));
                    acc.10.push(conn.query_stop.map(|dt| dt.timestamp_nanos_opt().unwrap_or(0)));
                    
                    acc.11.push(Some(conn.state.as_str().to_string()));
                    acc.12.push(Some(conn.last_query));
                    acc.13.push(conn.graphql_time_ms.map(|t| t as i64));
                    acc.14.push(conn.datafusion_time_ms.map(|t| t as i64));
                    acc.15.push(conn.overall_time_ms.map(|t| t as i64));
                    acc.16.push(conn.last_alive_sent.map(|dt| dt.timestamp_nanos_opt().unwrap_or(0)));
                    acc
                },
            );

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(datids)),
                Arc::new(StringArray::from(datnames)),
                Arc::new(Int64Array::from(pids)),
                Arc::new(StringArray::from(usenames)),
                Arc::new(StringArray::from(app_names)),
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
        ).map_err(Into::into)
    }

    async fn execute_from_less_query_datafusion(sql: &str, session: &AuthenticatedSession) -> Result<QueryResult> {
        debug!("🔍 Executing FROM-less query with DataFusion: {}", sql.trim());
        
        // For SELECT 1 queries, extend the session as a keep-alive
        if sql.trim().to_uppercase().contains("SELECT 1") {
            match session.client.extend_session(&session.token).await {
                Ok(_) => debug!("✅ Session extended successfully for SELECT 1"),
                Err(e) => warn!("⚠️ Failed to extend session: {}", e),
            }
        }
        
        // Use DataFusion to execute the FROM-less query directly
        let ctx = datafusion::prelude::SessionContext::new();
        let df = ctx.sql(sql).await?;
        let batches = df.collect().await?;

        // Convert to QueryResult
        let mut query_result = QueryResult::from_record_batches(batches)?;
        query_result.timings.datafusion_time_ms = Some(0); // No separate datafusion timing for direct queries
        query_result.timings.graphql_time_ms = Some(0); // No GraphQL for FROM-less queries
        
        Ok(query_result)
    }
}
