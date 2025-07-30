use crate::auth::AuthenticatedSession;
use crate::query_handler::QueryHandler;
use crate::tables::QueryInfo;
use anyhow::Result;
use std::time::Instant;
use tracing::{debug, info};

impl QueryHandler {
    pub(super) async fn fetch_logged_alarms_data(
        query_info: &QueryInfo,
        session: &AuthenticatedSession,
    ) -> Result<Vec<crate::graphql::types::LoggedAlarm>> {
        info!("📚 Fetching LoggedAlarms data");

        // Get time range (prioritize raise_time over modification_time over timestamp)
        let (start_time, mut end_time) = query_info
            .get_raise_time_filter()
            .or_else(|| query_info.get_modification_time_filter())
            .or_else(|| query_info.get_timestamp_filter())
            .unwrap_or((None, None));

        // If endtime is not specified, use current UTC time
        if end_time.is_none() {
            let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
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
        let graphql_start = Instant::now();
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
        let graphql_elapsed_ms = graphql_start.elapsed().as_millis();
        info!("🚀 GraphQL query for LoggedAlarms completed in {} ms", graphql_elapsed_ms);

        debug!("✅ GraphQL returned {} logged alarms", alarm_results.len());

        // Apply additional filters (for non-virtual columns)
        let filtered_results =
            Self::apply_logged_alarm_filters(alarm_results, &query_info.filters)?;
        debug!("✂️  After filtering: {} results", filtered_results.len());

        Ok(filtered_results)
    }

}
