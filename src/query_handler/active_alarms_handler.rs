use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::QueryInfo;
use anyhow::Result;
use tracing::{debug, info};

impl QueryHandler {
    pub(super) async fn execute_active_alarms_query(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<super::QueryResult> {
        info!("🚨 Executing ActiveAlarms query");

        // Extract filter string if any
        let filter_string = Self::extract_alarm_filter_string(&query_info.filters).unwrap_or_default();
        debug!("🔍 Alarm filter string: {:?}", filter_string);

        // Call GraphQL - use empty system names to get all systems
        let alarm_results = session
            .client
            .get_active_alarms(
                &session.token,
                vec![], // system_names - empty for all systems
                filter_string,
            )
            .await?;
        debug!(
            "✅ GraphQL returned {} active alarms",
            alarm_results.len()
        );

        // Apply additional filters
        let filtered_results = Self::apply_alarm_filters(alarm_results, &query_info.filters)?;
        debug!("✂️  After filtering: {} results", filtered_results.len());

        Self::format_active_alarms_response(filtered_results, query_info)
    }

    pub(super) fn format_active_alarms_response(
        results: Vec<crate::graphql::types::ActiveAlarm>,
        query_info: &QueryInfo,
    ) -> Result<super::QueryResult> {
        let mut response = String::from(&Self::create_csv_header_with_types(query_info));
        let mut row_count = 0;

        for result in results {
            let mut row_values = Vec::new();

            for column in &query_info.columns {
                // Check if this column is an alias, if so get the original column name
                let original_column = query_info.column_mappings.get(column).unwrap_or(column);
                let cell_value = match original_column.as_str() {
                    "name" => result.name.clone(),
                    "instance_id" => result.instance_id.to_string(),
                    "alarm_group_id" => result
                        .alarm_group_id
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "NULL".to_string()),
                    "raise_time" => Self::convert_timestamp_to_postgres_format(&result.raise_time),
                    "acknowledgment_time" => result
                        .acknowledgment_time
                        .as_ref()
                        .map(|t| Self::convert_timestamp_to_postgres_format(t))
                        .unwrap_or_else(|| "NULL".to_string()),
                    "clear_time" => result
                        .clear_time
                        .as_ref()
                        .map(|t| Self::convert_timestamp_to_postgres_format(t))
                        .unwrap_or_else(|| "NULL".to_string()),
                    "reset_time" => result
                        .reset_time
                        .as_ref()
                        .map(|t| Self::convert_timestamp_to_postgres_format(t))
                        .unwrap_or_else(|| "NULL".to_string()),
                    "modification_time" => {
                        Self::convert_timestamp_to_postgres_format(&result.modification_time)
                    }
                    "state" => result.state.clone(),
                    "priority" => result
                        .priority
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "NULL".to_string()),
                    "event_text" => result
                        .event_text
                        .as_ref()
                        .map(|v| v.join(";"))
                        .unwrap_or_else(|| "NULL".to_string()),
                    "info_text" => result
                        .info_text
                        .as_ref()
                        .map(|v| v.join(";"))
                        .unwrap_or_else(|| "NULL".to_string()),
                    "origin" => result
                        .origin
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| "NULL".to_string()),
                    "area" => result
                        .area
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| "NULL".to_string()),
                    "value" => result
                        .value
                        .as_ref()
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "NULL".to_string()),
                    "host_name" => result
                        .host_name
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| "NULL".to_string()),
                    "user_name" => result
                        .user_name
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| "NULL".to_string()),
                    _ => "NULL".to_string(),
                };
                row_values.push(cell_value);
            }

            let row = format!("{}\n", row_values.join(","));
            response.push_str(&row);
            row_count += 1;
        }

        info!("📊 Formatted {} rows for ActiveAlarms query", row_count);
        Ok(super::QueryResult::from_csv_string(&response)?)
    }
}