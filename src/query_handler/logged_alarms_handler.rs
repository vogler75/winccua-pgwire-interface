use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::QueryInfo;
use anyhow::Result;
use chrono::Utc;
use tracing::{debug, info};

impl QueryHandler {
    pub(super) async fn execute_logged_alarms_query(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<String> {
        info!("📚 Executing LoggedAlarms query");

        // Get modification_time range (prioritize over timestamp)
        let (start_time, mut end_time) = query_info
            .get_modification_time_filter()
            .or_else(|| query_info.get_timestamp_filter())
            .unwrap_or((None, None));

        // If endtime is not specified, use current UTC time
        if end_time.is_none() {
            let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
            debug!("📅 No endtime specified, using current UTC time: {}", now);
            end_time = Some(now);
        }

        debug!("⏰ Time range: {:?} to {:?}", start_time, end_time);

        // Get virtual column parameters
        let filter_string = query_info.get_filter_string().unwrap_or_default();
        let system_names = query_info.get_system_names();
        let filter_language = query_info.get_filter_language();

        // Get limit for maxNumberOfResults
        let limit = query_info.limit.map(|l| l as i32);

        // Debug GraphQL query parameters
        debug!("🔧 GraphQL query parameters:");
        debug!("  📋 systemNames: {:?}", system_names);
        debug!("  🔍 filterString: {:?}", filter_string);
        debug!("  🌐 filterLanguage: {:?}", filter_language);
        debug!("  ⏰ startTime: {:?}", start_time);
        debug!("  ⏰ endTime: {:?}", end_time);
        debug!("  📊 maxNumberOfResults: {:?}", limit);

        // Call GraphQL
        let alarm_results = session
            .client
            .get_logged_alarms(
                &session.token,
                system_names,
                filter_string,
                start_time,
                end_time,
                limit,
                filter_language,
            )
            .await?;

        debug!("✅ GraphQL returned {} logged alarms", alarm_results.len());

        // Apply additional filters (for non-virtual columns)
        let filtered_results =
            Self::apply_logged_alarm_filters(alarm_results, &query_info.filters)?;
        debug!("✂️  After filtering: {} results", filtered_results.len());

        Self::format_logged_alarms_response(filtered_results, query_info)
    }

    pub(super) fn format_logged_alarms_response(
        results: Vec<crate::graphql::types::LoggedAlarm>,
        query_info: &QueryInfo,
    ) -> Result<String> {
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
                    "duration" => result
                        .duration
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

        info!("📊 Formatted {} rows for LoggedAlarms query", row_count);
        Ok(response)
    }
}
