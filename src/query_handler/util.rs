use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::{ColumnFilter, FilterOperator, QueryInfo};
use anyhow::Result;
use std::time::Instant;
use tracing::{debug, info};

impl QueryHandler {
    pub(super) async fn resolve_like_patterns(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<Vec<String>> {
        let patterns = query_info.get_like_patterns();
        let mut resolved_names = Vec::new();

        for pattern in patterns {
            debug!("🔍 Resolving LIKE pattern: '{}'", pattern);

            // For LoggedTagValues, auto-append ":*" if pattern doesn't contain ":"
            let processed_pattern =
                if matches!(query_info.table, crate::tables::VirtualTable::LoggedTagValues) {
                    if !pattern.contains(':') {
                        let new_pattern = format!("{}:*", pattern);
                        debug!(
                            "📝 Auto-appended ':*' to LoggedTagValues pattern: '{}' -> '{}'",
                            pattern, new_pattern
                        );
                        new_pattern
                    } else {
                        pattern.clone()
                    }
                } else {
                    pattern.clone()
                };

            // Convert SQL LIKE pattern to GraphQL browse pattern
            let browse_pattern = Self::convert_like_to_browse_pattern(&processed_pattern);
            debug!(
                "🌐 Converted to browse pattern: '{}' -> '{}'",
                processed_pattern, browse_pattern
            );

            // Call appropriate GraphQL browse function based on table type
            let graphql_start = Instant::now();
            let browse_results = match query_info.table {
                crate::tables::VirtualTable::LoggedTagValues => {
                    debug!("🗂️  Using browse_logging_tags for LoggedTagValues with objectTypeFilters=LOGGINGTAG");
                    session
                        .client
                        .browse_logging_tags(&session.token, vec![browse_pattern.clone()])
                        .await?
                }
                _ => {
                    debug!("🗂️  Using standard browse_tags for non-LoggedTagValues table");
                    session
                        .client
                        .browse_tags(&session.token, vec![browse_pattern.clone()])
                        .await?
                }
            };
            let graphql_elapsed_ms = graphql_start.elapsed().as_millis();
            info!("🚀 GraphQL browse for LIKE pattern '{}' completed in {} ms", pattern, graphql_elapsed_ms);
            debug!(
                "📋 Browse returned {} tags for pattern '{}'",
                browse_results.len(),
                browse_pattern
            );

            // Extract just the names from BrowseResult
            let tag_names: Vec<String> = browse_results.into_iter().map(|br| br.name).collect();
            if !tag_names.is_empty() {
                debug!("🏷️  Found matching tags: {:?}", tag_names);
            } else {
                debug!("🏷️  No tags found matching pattern '{}'", browse_pattern);
            }
            resolved_names.extend(tag_names);
        }

        // Remove duplicates
        resolved_names.sort();
        resolved_names.dedup();

        Ok(resolved_names)
    }

    pub(super) fn convert_like_to_browse_pattern(sql_pattern: &str) -> String {
        // Convert SQL LIKE pattern to GraphQL browse pattern
        // SQL LIKE: % = any characters, _ = single character
        // GraphQL browse typically supports * for wildcards

        // Handle common patterns:
        if sql_pattern == "%" {
            // Special case: % alone means match all
            "*".to_string()
        } else if sql_pattern.starts_with('%')
            && sql_pattern.ends_with('%')
            && sql_pattern.matches('%').count() == 2
        {
            // Simple %middle% -> *middle* (contains pattern, only 2 % chars)
            let middle = sql_pattern.trim_start_matches('%').trim_end_matches('%');
            format!("*{}*", middle)
        } else if sql_pattern.ends_with('%') && sql_pattern.matches('%').count() == 1 {
            // Simple prefix% -> prefix* (starts with pattern, only 1 % char)
            let prefix = sql_pattern.trim_end_matches('%');
            format!("{}*", prefix)
        } else if sql_pattern.starts_with('%') && sql_pattern.matches('%').count() == 1 {
            // Simple %suffix -> *suffix (ends with pattern, only 1 % char)
            let suffix = sql_pattern.trim_start_matches('%');
            format!("*{}*", suffix)
        } else if sql_pattern.contains('%') || sql_pattern.contains('_') {
            // Complex patterns: convert % to * for GraphQL
            sql_pattern.replace('%', "*")
        } else {
            // No wildcards: exact match or try as prefix pattern
            format!("{}*", sql_pattern)
        }
    }

    pub(super) fn extract_alarm_filter_string(filters: &[ColumnFilter]) -> Option<String> {
        // Look for text-based filters that can be used as alarm filter strings
        for filter in filters {
            match filter.column.as_str() {
                "name" | "event_text" | "info_text" => {
                    if matches!(filter.operator, FilterOperator::Like | FilterOperator::Equal) {
                        if let Some(text) = filter.value.as_string() {
                            return Some(text.replace('%', "")); // Remove SQL wildcards
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }

}
