use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::QueryInfo;
use anyhow::{anyhow, Result};
use std::time::Instant;
use tracing::{debug, info};

impl QueryHandler {
    pub(super) async fn fetch_tag_values_data(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<Vec<crate::graphql::types::TagValueResult>> {
        debug!("📊 Fetching TagValues data");

        // Get tag names from the WHERE clause
        let tag_names = query_info.get_tag_names();

        // Check if we need to use browse for LIKE patterns
        let final_tag_names = if query_info.requires_browse() {
            debug!("🔍 Query contains LIKE patterns, using browse to resolve tag names");
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
            return Ok(Vec::new());
        }

        debug!("🎯 Final tag names to query: {:?}", final_tag_names);

        // Call GraphQL
        let graphql_start = Instant::now();
        let tag_results = session
            .client
            .get_tag_values(&session.token, final_tag_names, false)
            .await?;
        let graphql_elapsed_ms = graphql_start.elapsed().as_millis();
        debug!("🚀 GraphQL query for TagValues completed in {} ms with {} results", graphql_elapsed_ms, tag_results.len());

        // Filter and format results
        let filtered_results = Self::apply_filters(tag_results, &query_info.filters)?;
        debug!("✂️  After filtering: {} results", filtered_results.len());

        Ok(filtered_results)
    }
}
