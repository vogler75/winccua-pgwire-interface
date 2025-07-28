use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::QueryInfo;
use anyhow::{anyhow, Result};
use tracing::{debug, info, warn};

impl QueryHandler {
    pub(super) async fn execute_tag_values_query(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<String> {
        info!("📊 Executing TagValues query");

        // Get tag names from the WHERE clause
        let tag_names = query_info.get_tag_names();

        // Check if we need to use browse for LIKE patterns
        let final_tag_names = if query_info.requires_browse() {
            info!("🔍 Query contains LIKE patterns, using browse to resolve tag names");
            Self::resolve_like_patterns(&query_info, session).await?
        } else {
            // For non-LIKE queries, we must have explicit tag names
            if tag_names.is_empty() {
                return Err(anyhow!(
                    "TagValues queries must specify tag names in WHERE clause"
                ));
            }
            debug!("🏷️  Requesting tag names: {:?}", tag_names);
            tag_names
        };

        if final_tag_names.is_empty() {
            info!("📭 No tags found matching the criteria");
            return Ok(Self::create_csv_header_with_types(&query_info));
        }

        debug!("🎯 Final tag names to query: {:?}", final_tag_names);

        // Call GraphQL
        let tag_results = session
            .client
            .get_tag_values(&session.token, final_tag_names, false)
            .await?;
        debug!("✅ GraphQL returned {} tag results", tag_results.len());

        // Filter and format results
        let filtered_results = Self::apply_filters(tag_results, &query_info.filters)?;
        debug!("✂️  After filtering: {} results", filtered_results.len());

        Self::format_tag_values_response(filtered_results, &query_info)
    }

    pub(super) fn format_tag_values_response(
        results: Vec<crate::graphql::types::TagValueResult>,
        query_info: &QueryInfo,
    ) -> Result<String> {
        let mut response = String::from(&Self::create_csv_header_with_types(query_info));
        let mut row_count = 0;

        for result in results {
            if let Some(error) = &result.error {
                // Check if the error code indicates failure (non-zero)
                let error_code = error.code.as_deref().unwrap_or("1"); // Default to "1" (failure) if no code
                if error_code != "0" {
                    let description = error.description.as_deref().unwrap_or("Unknown error");
                    warn!(
                        "⚠️  Error for tag {} (code {}): {}",
                        result.name, error_code, description
                    );
                    continue;
                }
                // If code is "0", this is actually a success despite being in the error field
                debug!(
                    "✅ Tag {} successful with code 0, description: {:?}",
                    result.name, error.description
                );
            }

            if let Some(value) = &result.value {
                let mut row_values = Vec::new();

                for column in &query_info.columns {
                    // Check if this column is an alias, if so get the original column name
                    let original_column = query_info.column_mappings.get(column).unwrap_or(column);
                    let cell_value = match original_column.as_str() {
                        "tag_name" => result.name.clone(),
                        "timestamp" => Self::convert_timestamp_to_postgres_format(&value.timestamp),
                        "timestamp_ms" => Self::convert_timestamp_to_ms_epoch(&value.timestamp),
                        "numeric_value" => value
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
                        "string_value" => value
                            .value
                            .as_ref()
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "NULL".to_string()),
                        "quality" => value
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
        }

        info!("📊 Formatted {} rows for TagValues query", row_count);
        Ok(response)
    }
}
