use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::QueryInfo;
use anyhow::Result;
use std::time::Instant;
use tracing::debug;

impl QueryHandler {
    pub(super) async fn fetch_active_alarms_data(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<Vec<crate::graphql::types::ActiveAlarm>> {
        // Extract filter string if any
        let filter_string = Self::extract_alarm_filter_string(&query_info.filters).unwrap_or_default();
        debug!("🔍 Alarm filter string: {:?}", filter_string);

        // Call GraphQL - use empty system names to get all systems
        let graphql_start = Instant::now();
        let alarm_results = session
            .client
            .get_active_alarms(
                &session.token,
                vec![], // system_names - empty for all systems
                filter_string,
            )
            .await?;
        let graphql_elapsed_ms = graphql_start.elapsed().as_millis();
        debug!(
            "✅ GraphQL returned {} active alarms",
            alarm_results.len()
        );
        debug!("🚀 GraphQL query for ActiveAlarms completed in {} ms", graphql_elapsed_ms);

        // Apply additional filters
        let filtered_results = Self::apply_alarm_filters(alarm_results, &query_info.filters)?;
        debug!("✂️  After filtering: {} results", filtered_results.len());

        Ok(filtered_results)
    }

}