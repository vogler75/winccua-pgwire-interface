
pub mod active_alarms_handler;
pub mod logged_alarms_handler;
pub mod logged_tag_values_handler;
pub mod tag_list_handler;
pub mod tag_values_handler;

mod filter;
mod util;

use crate::auth::AuthenticatedSession;
use crate::datafusion_handler;
use crate::sql_handler::SqlHandler;
use crate::tables::{QueryInfo, SqlResult, VirtualTable};
use anyhow::Result;
use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::{debug, info, warn};

pub struct QueryHandler;

impl QueryHandler {
    pub async fn execute_query(sql: &str, session: &AuthenticatedSession) -> Result<String> {
        info!("🔍 Executing SQL query: {}", sql.trim());

        // Parse the SQL query
        let sql_result = match SqlHandler::parse_query(sql) {
            Ok(result) => result,
            Err(e) => {
                // Check if this is an unknown table error and log the SQL statement
                let error_msg = e.to_string();
                if error_msg.starts_with("Unknown table:") {
                    warn!("❌ Unknown table in SQL query: {}", sql.trim());
                    warn!("❌ {}", error_msg);
                    warn!("📋 Available tables: tagvalues, loggedtagvalues, activealarms, loggedalarms, taglist");
                }
                return Err(e);
            }
        };
        debug!("📋 Parsed SQL result: {:?}", sql_result);

        // Handle based on result type
        match sql_result {
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
                        Self::execute_active_alarms_query(&query_info, session).await
                    }
                    VirtualTable::LoggedAlarms => {
                        Self::execute_logged_alarms_query(&query_info, session).await
                    }
                    VirtualTable::TagList => {
                        Self::execute_taglist_datafusion_query(sql, &query_info, session).await
                    }
                    VirtualTable::InformationSchemaTables
                    | VirtualTable::InformationSchemaColumns => {
                        crate::information_schema::handle_information_schema_query(&query_info)
                    }
                }
            }
            SqlResult::SetStatement(set_command) => {
                info!("✅ Successfully executed SET statement: {}", set_command);
                // Return a command complete response for SET statements
                Ok("COMMAND_COMPLETE:SET".to_string())
            }
        }
    }

    async fn execute_taglist_datafusion_query(
        sql: &str,
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<String> {
        let results = Self::fetch_tag_list_data(query_info, session).await?;

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
        let results =
            datafusion_handler::execute_query(sql, batch, &query_info.table.to_string()).await?;

        // Format the results back to a CSV string
        let mut csv_bytes = Vec::new();
        if !results.is_empty() {
            let mut writer = arrow::csv::Writer::new(&mut csv_bytes);
            for batch in results {
                writer.write(&batch)?;
            }
        }

        Ok(String::from_utf8(csv_bytes)?)
    }

    async fn execute_loggedtagvalues_datafusion_query(
        sql: &str,
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<String> {
        let results = Self::fetch_logged_tag_values_data(query_info, session).await?;

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
        let results =
            datafusion_handler::execute_query(sql, batch, &query_info.table.to_string()).await?;

        // Format the results back to a CSV string
        let mut csv_bytes = Vec::new();
        if !results.is_empty() {
            let mut writer = arrow::csv::Writer::new(&mut csv_bytes);
            for batch in results {
                writer.write(&batch)?;
            }
        }

        Ok(String::from_utf8(csv_bytes)?)
    }

    async fn execute_datafusion_query(
        sql: &str,
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<String> {
        let results = Self::fetch_tag_values_data(query_info, session).await?;

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
        let results =
            datafusion_handler::execute_query(sql, batch, &query_info.table.to_string()).await?;

        // Format the results back to a CSV string
        let mut csv_bytes = Vec::new();
        if !results.is_empty() {
            let mut writer = arrow::csv::Writer::new(&mut csv_bytes);
            for batch in results {
                writer.write(&batch)?;
            }
        }

        Ok(String::from_utf8(csv_bytes)?)
    }
}
