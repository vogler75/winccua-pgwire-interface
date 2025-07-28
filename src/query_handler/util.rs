use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::{ColumnFilter, FilterOperator, QueryInfo};
use anyhow::Result;
use chrono::{DateTime, Utc};
use tracing::{debug, warn};

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

    pub(super) fn convert_timestamp_to_postgres_format(timestamp_str: &str) -> String {
        // GraphQL returns UTC timestamps - format as TIMESTAMP (without timezone)
        // PostgreSQL TIMESTAMP format: YYYY-MM-DD HH:MM:SS.ssssss (no timezone)

        // If it's already in PostgreSQL TIMESTAMP format, keep it as-is
        if timestamp_str.matches('-').count() == 2
            && timestamp_str.contains(' ')
            && timestamp_str.contains(':')
            && !timestamp_str.contains('+')
            && !timestamp_str.contains('Z')
        {
            return timestamp_str.to_string();
        }

        // Try to parse and reformat to TIMESTAMP (without timezone)
        // First try parsing as ISO 8601 format (most common from GraphQL)
        if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
            // Format as TIMESTAMP without timezone information
            return dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        }

        // Try parsing without timezone (assume it's already UTC)
        if let Ok(dt) = timestamp_str.parse::<DateTime<Utc>>() {
            return dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        }

        // Try common formats and convert to TIMESTAMP format
        for format in &[
            "%Y-%m-%dT%H:%M:%S%.fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.f%z",
            "%Y-%m-%dT%H:%M:%S%z",
        ] {
            if let Ok(dt) = DateTime::parse_from_str(timestamp_str, format) {
                // Format as TIMESTAMP without timezone
                return dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            }
        }

        // If all parsing attempts fail, return the original string
        warn!(
            "Failed to parse timestamp '{}', using as-is",
            timestamp_str
        );
        timestamp_str.to_string()
    }

    pub(super) fn create_csv_header_with_types(query_info: &QueryInfo) -> String {
        // Create header with type information that the formatter can use
        // Format: column1:type1,column2:type2,etc
        let header_with_types: Vec<String> = query_info
            .columns
            .iter()
            .map(|column| {
                // Resolve alias to original column name to get the correct type
                let original_column = query_info.column_mappings.get(column).unwrap_or(column);
                let type_info = match original_column.as_str() {
                    "numeric_value" | "timestamp_ms" => "NUMERIC",
                    "timestamp"
                    | "raise_time"
                    | "acknowledgment_time"
                    | "clear_time"
                    | "reset_time"
                    | "modification_time" => "TIMESTAMP",
                    _ => "TEXT",
                };
                format!("{}:{}", column, type_info)
            })
            .collect();

        format!("{}\n", header_with_types.join(","))
    }
}
