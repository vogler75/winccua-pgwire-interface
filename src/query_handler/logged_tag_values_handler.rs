use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::QueryInfo;
use anyhow::{anyhow, Result};
use chrono::Utc;
use tracing::{debug, info, warn};

impl QueryHandler {
    pub(super) async fn execute_logged_tag_values_query(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<String> {
        info!("📈 Executing LoggedTagValues query");

        // Get tag names - handle LIKE patterns via browse if needed
        let tag_names = if query_info.requires_browse() {
            info!("🔍 LoggedTagValues query contains LIKE patterns, using browse to resolve tag names");
            Self::resolve_like_patterns(&query_info, session).await?
        } else {
            let tag_names = query_info.get_tag_names();
            if tag_names.is_empty() {
                return Err(anyhow!(
                    "LoggedTagValues queries must specify tag names in WHERE clause"
                ));
            }
            debug!("🏷️  Requesting logged tag names: {:?}", tag_names);
            tag_names
        };

        if tag_names.is_empty() {
            info!("📭 No tags found matching the LIKE criteria");
            return Ok(Self::create_csv_header_with_types(&query_info));
        }

        // Get timestamp range
        let (start_time, mut end_time) = query_info.get_timestamp_filter().unwrap_or((None, None));

        // If endtime is not specified, use current UTC time
        if end_time.is_none() {
            let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
            debug!("📅 No endtime specified, using current UTC time: {}", now);
            end_time = Some(now);
        }

        debug!("⏰ Time range: {:?} to {:?}", start_time, end_time);

        // Get limit
        let limit = query_info.limit.unwrap_or(1000); // Default limit for historical data
        debug!("📏 Limit: {}", limit);

        // Call loggedTagValues with all tag names
        debug!("📊 Querying logged values for {} tags", tag_names.len());

        // Determine sorting mode based on ORDER BY clause
        let sorting_mode = if let Some(order_by) = &query_info.order_by {
            if order_by.column == "timestamp" {
                if order_by.ascending {
                    Some("TIME_ASC".to_string())
                } else {
                    Some("TIME_DESC".to_string())
                }
            } else {
                // Default to TIME_ASC for non-timestamp ordering
                Some("TIME_ASC".to_string())
            }
        } else {
            // Default to TIME_ASC when no ordering is specified
            Some("TIME_ASC".to_string())
        };
        debug!("🔄 Using GraphQL sortingMode: {:?}", sorting_mode);

        let logged_results_response = session
            .client
            .get_logged_tag_values(
                &session.token,
                tag_names,
                start_time,
                end_time,
                Some(limit as i32),
                sorting_mode,
            )
            .await?;

        // Convert LoggedTagValuesResult to LoggedTagValue format
        let mut all_values = Vec::new();
        for result in logged_results_response {
            if let Some(error) = &result.error {
                // Check if the error code indicates failure (non-zero)
                let error_code = error.code.as_deref().unwrap_or("1"); // Default to "1" (failure) if no code
                if error_code != "0" {
                    let description = error.description.as_deref().unwrap_or("Unknown error");
                    warn!(
                        "⚠️  Error for logged tag {} (code {}): {}",
                        result.logging_tag_name, error_code, description
                    );
                    continue;
                }
                // If code is "0", this is actually a success despite being in the error field
                debug!(
                    "✅ Logged tag {} successful with code 0, description: {:?}",
                    result.logging_tag_name, error.description
                );
            }

            for value_entry in &result.values {
                all_values.push(crate::graphql::types::LoggedTagValue {
                    tag_name: result.logging_tag_name.clone(),
                    timestamp: value_entry.value.timestamp.clone(),
                    value: value_entry.value.value.clone(),
                    quality: value_entry.value.quality.clone(),
                });
            }
        }

        debug!("✅ Got {} total logged values", all_values.len());

        // Apply additional filters and sorting
        let filtered_results = Self::apply_logged_filters(all_values, &query_info.filters)?;
        debug!(
            "✂️  After filtering: {} results",
            filtered_results.len()
        );

        Self::format_logged_tag_values_response(filtered_results, query_info)
    }

    pub(super) fn format_logged_tag_values_response(
        results: Vec<crate::graphql::types::LoggedTagValue>,
        query_info: &QueryInfo,
    ) -> Result<String> {
        let mut response = String::from(&Self::create_csv_header_with_types(query_info));
        let mut row_count = 0;

        // Sort results if ORDER BY is specified
        let mut sorted_results = results;
        if let Some(order_by) = &query_info.order_by {
            if order_by.column == "timestamp" {
                sorted_results.sort_by(|a, b| {
                    if order_by.ascending {
                        a.timestamp.cmp(&b.timestamp)
                    } else {
                        b.timestamp.cmp(&a.timestamp)
                    }
                });
            }
        }

        // Apply limit
        if let Some(limit) = query_info.limit {
            sorted_results.truncate(limit as usize);
        }

        for result in sorted_results {
            let mut row_values = Vec::new();

            for column in &query_info.columns {
                // Check if this column is an alias, if so get the original column name
                let original_column = query_info.column_mappings.get(column).unwrap_or(column);
                let cell_value = match original_column.as_str() {
                    "tag_name" => result.tag_name.clone(),
                    "timestamp" => Self::convert_timestamp_to_postgres_format(&result.timestamp),
                    "timestamp_ms" => Self::convert_timestamp_to_ms_epoch(&result.timestamp),
                    "numeric_value" => result
                        .value
                        .as_ref()
                        .and_then(|v| v.as_f64())
                        .map(|n| {
                            // Format numeric values appropriately
                            if n.fract() == 0.0 {
                                // Whole number - format as integer
                                format!("{}", n as i64)
                            } else {
                                // Decimal number - use appropriate precision
                                format!("{}", n)
                            }
                        })
                        .unwrap_or_else(|| "NULL".to_string()),
                    "string_value" => result
                        .value
                        .as_ref()
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "NULL".to_string()),
                    "quality" => result
                        .quality
                        .as_ref()
                        .map(|q| q.quality.clone())
                        .unwrap_or_else(|| "NULL".to_string()),
                    _ => "NULL".to_string(),
                };
                row_values.push(cell_value);
            }

            let row = format!("{}\n", row_values.join(","));
            response.push_str(&row);
            row_count += 1;
        }

        info!(
            "📊 Formatted {} rows for LoggedTagValues query",
            row_count
        );
        Ok(response)
    }
}
