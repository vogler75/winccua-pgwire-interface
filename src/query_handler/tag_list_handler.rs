use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::QueryInfo;
use anyhow::Result;
use tracing::{debug, info};

impl QueryHandler {
    pub(super) async fn fetch_tag_list_data(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<Vec<crate::graphql::types::BrowseResult>> {
        info!("📋 Fetching TagList data");

        // Get name filters from WHERE clause and convert SQL wildcards to GraphQL format
        let raw_name_filters = query_info.get_name_filters();
        let name_filters: Vec<String> = raw_name_filters
            .iter()
            .map(|filter| filter.replace('%', "*"))
            .collect();
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
            session
                .client
                .browse_tags(&session.token, name_filters)
                .await?
        } else {
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

        Ok(filtered_results)
    }
}