use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::QueryInfo;
use anyhow::Result;
use tracing::{debug, info};

impl QueryHandler {
    pub(super) async fn execute_tag_list_query(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<String> {
        info!("📋 Executing TagList query");

        // Debug: Show all filters
        debug!("🔍 All filters in query: {:?}", query_info.filters);

        // Get name filters from WHERE clause and convert SQL wildcards to GraphQL format
        let raw_name_filters = query_info.get_name_filters();
        let name_filters: Vec<String> = raw_name_filters
            .iter()
            .map(|filter| filter.replace('%', "*"))
            .collect();
        debug!("🔍 Raw name filters: {:?}", raw_name_filters);
        debug!("🔍 Converted name filters: {:?}", name_filters);

        // Get object type filters from WHERE clause
        let object_type_filters = query_info.get_object_type_filters();
        debug!("🔍 Object type filters: {:?}", object_type_filters);

        // Get language filter (virtual column)
        let language = query_info
            .get_language_filter()
            .unwrap_or_else(|| "en-US".to_string());
        debug!("🌐 Language filter: {}", language);

        // Call GraphQL browse with filters
        let browse_results = if object_type_filters.is_empty() {
            // Standard browse call
            session
                .client
                .browse_tags(&session.token, name_filters)
                .await?
        } else {
            // Extended browse call with object type filters
            session
                .client
                .browse_tags_with_object_type(
                    &session.token,
                    name_filters,
                    object_type_filters,
                    language,
                )
                .await?
        };

        debug!("✅ GraphQL browse returned {} results", browse_results.len());

        // Apply post-processing filters (for columns not supported by GraphQL)
        let filtered_results = Self::apply_browse_filters(browse_results, &query_info.filters)?;
        debug!(
            "✂️  After post-processing filters: {} results",
            filtered_results.len()
        );

        Self::format_tag_list_response(filtered_results, query_info)
    }

    pub(super) fn format_tag_list_response(
        results: Vec<crate::graphql::types::BrowseResult>,
        query_info: &QueryInfo,
    ) -> Result<String> {
        // Use the same CSV format as TagValues for proper column separation
        let mut response = String::from(&Self::create_csv_header_with_types(query_info));

        // Collect all rows first
        let mut rows: Vec<Vec<String>> = results
            .iter()
            .map(|result| {
                query_info
                    .columns
                    .iter()
                    .map(|column| {
                        // Check if this column is an alias, if so get the original column name
                        let original_column =
                            query_info.column_mappings.get(column).unwrap_or(column);
                        match original_column.as_str() {
                            "tag_name" => result.name.clone(),
                            "display_name" => result
                                .display_name
                                .as_deref()
                                .unwrap_or("NULL")
                                .to_string(),
                            "object_type" => result
                                .object_type
                                .as_deref()
                                .unwrap_or("NULL")
                                .to_string(),
                            "data_type" => {
                                result.data_type.as_deref().unwrap_or("NULL").to_string()
                            }
                            _ => "NULL".to_string(),
                        }
                    })
                    .collect()
            })
            .collect();

        // Apply DISTINCT if specified
        if query_info.distinct {
            rows.sort();
            rows.dedup();
            debug!(
                "🔄 Applied DISTINCT: {} unique rows after deduplication",
                rows.len()
            );
        }

        // Apply limit and output rows
        let mut row_count = 0;
        for row in rows {
            response.push_str(&format!("{}\n", row.join(",")));
            row_count += 1;

            // Apply limit if specified
            if let Some(limit) = query_info.limit {
                if row_count >= limit {
                    break;
                }
            }
        }

        info!(
            "📊 Formatted {} rows for TagList query{}",
            row_count,
            if query_info.distinct {
                " (with DISTINCT)"
            } else {
                ""
            }
        );
        Ok(response)
    }
}