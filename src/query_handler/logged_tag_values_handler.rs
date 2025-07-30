use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::QueryInfo;
use anyhow::{anyhow, Result};
use chrono::Utc;
use std::time::Instant;
use tracing::{debug, info, warn};

impl QueryHandler {
    pub(super) async fn fetch_logged_tag_values_data(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<Vec<crate::graphql::types::LoggedTagValue>> {
        debug!("📈 Fetching LoggedTagValues data");

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
            return Ok(Vec::new());
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

        let graphql_start = Instant::now();
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
        let graphql_elapsed_ms = graphql_start.elapsed().as_millis();

        // Convert LoggedTagValuesResult to LoggedTagValue format
        let mut all_values = Vec::new();
        for result in logged_results_response {
            if let Some(error) = &result.error {
                let error_code = error.code.as_deref().unwrap_or("1");
                if error_code != "0" {
                    let description = error.description.as_deref().unwrap_or("Unknown error");
                    warn!(
                        "⚠️  Error for logged tag {} (code {}): {}",
                        result.logging_tag_name, error_code, description
                    );
                    continue;
                }
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

        info!("🚀 GraphQL query for LoggedTagValues completed in {} ms, fetched {} rows", graphql_elapsed_ms, all_values.len());

        // Apply additional filters and sorting
        let filtered_results = Self::apply_logged_filters(all_values, &query_info.filters)?;
        debug!(
            "✂️  After filtering: {} results",
            filtered_results.len()
        );

        Ok(filtered_results)
    }
}
