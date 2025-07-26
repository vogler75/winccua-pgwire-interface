use crate::auth::AuthenticatedSession;
use crate::sql_handler::SqlHandler;
use crate::tables::{ColumnFilter, FilterOperator, FilterValue, QueryInfo, VirtualTable};
use anyhow::{anyhow, Result};
use tracing::{debug, info, warn};

pub struct QueryHandler;

impl QueryHandler {
    pub async fn execute_query(sql: &str, session: &AuthenticatedSession) -> Result<String> {
        info!("🔍 Executing SQL query: {}", sql.trim());
        
        // Parse the SQL query
        let query_info = match SqlHandler::parse_query(sql) {
            Ok(info) => info,
            Err(e) => {
                // Check if this is an unknown table error and log the SQL statement
                let error_msg = e.to_string();
                if error_msg.starts_with("Unknown table:") {
                    warn!("❌ Unknown table in SQL query: {}", sql.trim());
                    warn!("❌ {}", error_msg);
                    warn!("📋 Available tables: tagvalues, loggedtagvalues, activealarms, loggedalarms");
                }
                return Err(e);
            }
        };
        debug!("📋 Parsed query: {:?}", query_info);
        
        // Execute based on table type
        match query_info.table {
            VirtualTable::TagValues => Self::execute_tag_values_query(&query_info, session).await,
            VirtualTable::LoggedTagValues => Self::execute_logged_tag_values_query(&query_info, session).await,
            VirtualTable::ActiveAlarms => Self::execute_active_alarms_query(&query_info, session).await,
            VirtualTable::LoggedAlarms => Self::execute_logged_alarms_query(&query_info, session).await,
        }
    }
    
    async fn execute_tag_values_query(query_info: &QueryInfo, session: &AuthenticatedSession) -> Result<String> {
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
                return Err(anyhow!("TagValues queries must specify tag names in WHERE clause"));
            }
            debug!("🏷️  Requesting tag names: {:?}", tag_names);
            tag_names
        };
        
        if final_tag_names.is_empty() {
            info!("📭 No tags found matching the criteria");
            return Ok(Self::create_csv_header(&query_info.columns));
        }
        
        debug!("🎯 Final tag names to query: {:?}", final_tag_names);
        
        // Call GraphQL
        let tag_results = session.client.get_tag_values(&session.token, final_tag_names, false).await?;
        debug!("✅ GraphQL returned {} tag results", tag_results.len());
        
        // Filter and format results
        let filtered_results = Self::apply_filters(tag_results, &query_info.filters)?;
        debug!("✂️  After filtering: {} results", filtered_results.len());
        
        Self::format_tag_values_response(filtered_results, &query_info)
    }
    
    async fn execute_logged_tag_values_query(query_info: &QueryInfo, session: &AuthenticatedSession) -> Result<String> {
        info!("📈 Executing LoggedTagValues query");
        
        // Get tag names - handle LIKE patterns via browse if needed
        let tag_names = if query_info.requires_browse() {
            info!("🔍 LoggedTagValues query contains LIKE patterns, using browse to resolve tag names");
            Self::resolve_like_patterns(&query_info, session).await?
        } else {
            let tag_names = query_info.get_tag_names();
            if tag_names.is_empty() {
                return Err(anyhow!("LoggedTagValues queries must specify tag names in WHERE clause"));
            }
            debug!("🏷️  Requesting logged tag names: {:?}", tag_names);
            tag_names
        };
        
        if tag_names.is_empty() {
            info!("📭 No tags found matching the LIKE criteria");
            return Ok(Self::create_csv_header(&query_info.columns));
        }
        
        // Get timestamp range
        let (start_time, end_time) = query_info.get_timestamp_filter().unwrap_or((None, None));
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
        
        let logged_results_response = session.client.get_logged_tag_values(
            &session.token,
            tag_names,
            start_time,
            end_time,
            Some(limit as i32),
            sorting_mode
        ).await?;
        
        // Convert LoggedTagValuesResult to LoggedTagValue format
        let mut all_values = Vec::new();
        for result in logged_results_response {
            if let Some(error) = &result.error {
                // Check if the error code indicates failure (non-zero)
                let error_code = error.code.as_deref().unwrap_or("1"); // Default to "1" (failure) if no code
                if error_code != "0" {
                    let description = error.description.as_deref().unwrap_or("Unknown error");
                    warn!("⚠️  Error for logged tag {} (code {}): {}", result.logging_tag_name, error_code, description);
                    continue;
                }
                // If code is "0", this is actually a success despite being in the error field
                debug!("✅ Logged tag {} successful with code 0, description: {:?}", result.logging_tag_name, error.description);
            }
            
            for value_entry in &result.values {
                all_values.push(crate::graphql::types::LoggedTagValue {
                    tag_name: result.logging_tag_name.clone(),
                    timestamp: value_entry.value.timestamp.clone(),
                    value: value_entry.value.value.clone(),
                });
            }
        }
        
        debug!("✅ Got {} total logged values", all_values.len());
        
        // Apply additional filters and sorting
        let filtered_results = Self::apply_logged_filters(all_values, &query_info.filters)?;
        debug!("✂️  After filtering: {} results", filtered_results.len());
        
        Self::format_logged_tag_values_response(filtered_results, query_info)
    }
    
    async fn execute_active_alarms_query(query_info: &QueryInfo, session: &AuthenticatedSession) -> Result<String> {
        info!("🚨 Executing ActiveAlarms query");
        
        // Extract filter string if any
        let filter_string = Self::extract_alarm_filter_string(&query_info.filters).unwrap_or_default();
        debug!("🔍 Alarm filter string: {:?}", filter_string);
        
        // Call GraphQL - use empty system names to get all systems
        let alarm_results = session.client.get_active_alarms(
            &session.token,
            vec![], // system_names - empty for all systems
            filter_string
        ).await?;
        debug!("✅ GraphQL returned {} active alarms", alarm_results.len());
        
        // Apply additional filters
        let filtered_results = Self::apply_alarm_filters(alarm_results, &query_info.filters)?;
        debug!("✂️  After filtering: {} results", filtered_results.len());
        
        Self::format_active_alarms_response(filtered_results, query_info)
    }
    
    async fn execute_logged_alarms_query(query_info: &QueryInfo, session: &AuthenticatedSession) -> Result<String> {
        info!("📚 Executing LoggedAlarms query");
        
        // Get timestamp range
        let (start_time, end_time) = query_info.get_timestamp_filter().unwrap_or((None, None));
        debug!("⏰ Time range: {:?} to {:?}", start_time, end_time);
        
        // Extract filter string
        let filter_string = Self::extract_alarm_filter_string(&query_info.filters).unwrap_or_default();
        debug!("🔍 Alarm filter string: {:?}", filter_string);
        
        // Get limit
        let limit = query_info.limit.map(|l| l as i32);
        
        // Call GraphQL
        let alarm_results = session.client.get_logged_alarms(
            &session.token,
            vec![], // system_names - empty for all systems
            filter_string,
            start_time,
            end_time,
            limit
        ).await?;
        
        debug!("✅ GraphQL returned {} logged alarms", alarm_results.len());
        
        // Apply additional filters
        let filtered_results = Self::apply_logged_alarm_filters(alarm_results, &query_info.filters)?;
        debug!("✂️  After filtering: {} results", filtered_results.len());
        
        Self::format_logged_alarms_response(filtered_results, query_info)
    }
    
    async fn resolve_like_patterns(query_info: &QueryInfo, session: &AuthenticatedSession) -> Result<Vec<String>> {
        let patterns = query_info.get_like_patterns();
        let mut resolved_names = Vec::new();
        
        for pattern in patterns {
            debug!("🔍 Resolving LIKE pattern: '{}'", pattern);
            
            // Convert SQL LIKE pattern to GraphQL browse pattern
            let browse_pattern = Self::convert_like_to_browse_pattern(&pattern);
            debug!("🌐 Converted to browse pattern: '{}' -> '{}'", pattern, browse_pattern);
            
            // Call GraphQL browse
            let browse_results = session.client.browse_tags(&session.token, vec![browse_pattern.clone()]).await?;
            debug!("📋 Browse returned {} tags for pattern '{}'", browse_results.len(), browse_pattern);
            
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
    
    fn convert_like_to_browse_pattern(sql_pattern: &str) -> String {
        // Convert SQL LIKE pattern to GraphQL browse pattern
        // SQL LIKE: % = any characters, _ = single character
        // GraphQL browse typically supports * for wildcards
        
        // Handle common patterns:
        if sql_pattern == "%" {
            // Special case: % alone means match all
            "*".to_string()
        } else if sql_pattern.starts_with('%') && sql_pattern.ends_with('%') && sql_pattern.matches('%').count() == 2 {
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
            format!("*{}", suffix)
        } else if sql_pattern.contains('%') || sql_pattern.contains('_') {
            // Complex patterns: convert % to * and _ to ? for GraphQL
            sql_pattern.replace('%', "*").replace('_', "?")
        } else {
            // No wildcards: exact match or try as prefix pattern
            format!("{}*", sql_pattern)
        }
    }
    
    fn extract_alarm_filter_string(filters: &[ColumnFilter]) -> Option<String> {
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
    
    fn apply_filters(results: Vec<crate::graphql::types::TagValueResult>, filters: &[ColumnFilter]) -> Result<Vec<crate::graphql::types::TagValueResult>> {
        let mut filtered = Vec::new();
        
        for result in results {
            let mut include = true;
            
            // Check if this result passes all filters
            for filter in filters {
                match filter.column.as_str() {
                    "tag_name" => {
                        // tag_name filters are already applied in the GraphQL query
                        continue;
                    }
                    "numeric_value" => {
                        if let Some(value) = &result.value {
                            if let Some(numeric_val) = value.value.as_ref().and_then(|v| v.as_f64()) {
                                if !Self::check_numeric_filter(numeric_val, &filter.operator, &filter.value) {
                                    include = false;
                                    break;
                                }
                            }
                        }
                    }
                    "string_value" => {
                        if let Some(value) = &result.value {
                            if let Some(string_val) = value.value.as_ref().and_then(|v| v.as_str()) {
                                if !Self::check_string_filter(string_val, &filter.operator, &filter.value) {
                                    include = false;
                                    break;
                                }
                            }
                        }
                    }
                    _ => {
                        // Unknown filter column, skip
                        continue;
                    }
                }
            }
            
            if include {
                filtered.push(result);
            }
        }
        
        Ok(filtered)
    }
    
    fn apply_logged_filters(results: Vec<crate::graphql::types::LoggedTagValue>, filters: &[ColumnFilter]) -> Result<Vec<crate::graphql::types::LoggedTagValue>> {
        // Similar to apply_filters but for logged tag values
        let mut filtered = Vec::new();
        
        for result in results {
            let mut include = true;
            
            for filter in filters {
                match filter.column.as_str() {
                    "tag_name" | "timestamp" => {
                        // These are handled by the GraphQL query
                        continue;
                    }
                    "numeric_value" => {
                        if let Some(numeric_val) = result.value.as_ref().and_then(|v| v.as_f64()) {
                            if !Self::check_numeric_filter(numeric_val, &filter.operator, &filter.value) {
                                include = false;
                                break;
                            }
                        }
                    }
                    "string_value" => {
                        if let Some(string_val) = result.value.as_ref().and_then(|v| v.as_str()) {
                            if !Self::check_string_filter(string_val, &filter.operator, &filter.value) {
                                include = false;
                                break;
                            }
                        }
                    }
                    _ => continue,
                }
            }
            
            if include {
                filtered.push(result);
            }
        }
        
        Ok(filtered)
    }
    
    fn apply_alarm_filters(results: Vec<crate::graphql::types::ActiveAlarm>, filters: &[ColumnFilter]) -> Result<Vec<crate::graphql::types::ActiveAlarm>> {
        let mut filtered = Vec::new();
        
        for result in results {
            let mut include = true;
            
            for filter in filters {
                match filter.column.as_str() {
                    "priority" => {
                        if let Some(priority_val) = filter.value.as_integer() {
                            let alarm_priority = result.priority.unwrap_or(0) as i64;
                            if !Self::check_numeric_filter(alarm_priority as f64, &filter.operator, &FilterValue::Integer(priority_val)) {
                                include = false;
                                break;
                            }
                        }
                    }
                    "name" | "event_text" | "info_text" => {
                        // These are handled by the filter_string in GraphQL
                        continue;
                    }
                    _ => continue,
                }
            }
            
            if include {
                filtered.push(result);
            }
        }
        
        Ok(filtered)
    }
    
    fn apply_logged_alarm_filters(results: Vec<crate::graphql::types::LoggedAlarm>, filters: &[ColumnFilter]) -> Result<Vec<crate::graphql::types::LoggedAlarm>> {
        // Similar logic to apply_alarm_filters but for logged alarms
        let mut filtered = Vec::new();
        
        for result in results {
            let mut include = true;
            
            for filter in filters {
                match filter.column.as_str() {
                    "priority" => {
                        if let Some(priority_val) = filter.value.as_integer() {
                            let alarm_priority = result.priority.unwrap_or(0) as i64;
                            if !Self::check_numeric_filter(alarm_priority as f64, &filter.operator, &FilterValue::Integer(priority_val)) {
                                include = false;
                                break;
                            }
                        }
                    }
                    "timestamp" => {
                        // Handled by GraphQL query
                        continue;
                    }
                    _ => continue,
                }
            }
            
            if include {
                filtered.push(result);
            }
        }
        
        Ok(filtered)
    }
    
    fn check_numeric_filter(value: f64, operator: &FilterOperator, filter_value: &FilterValue) -> bool {
        match operator {
            FilterOperator::Equal => {
                if let Some(target) = filter_value.as_number() {
                    (value - target).abs() < f64::EPSILON
                } else {
                    false
                }
            }
            FilterOperator::NotEqual => {
                if let Some(target) = filter_value.as_number() {
                    (value - target).abs() >= f64::EPSILON
                } else {
                    true
                }
            }
            FilterOperator::GreaterThan => {
                if let Some(target) = filter_value.as_number() {
                    value > target
                } else {
                    false
                }
            }
            FilterOperator::LessThan => {
                if let Some(target) = filter_value.as_number() {
                    value < target
                } else {
                    false
                }
            }
            FilterOperator::GreaterThanOrEqual => {
                if let Some(target) = filter_value.as_number() {
                    value >= target
                } else {
                    false
                }
            }
            FilterOperator::LessThanOrEqual => {
                if let Some(target) = filter_value.as_number() {
                    value <= target
                } else {
                    false
                }
            }
            _ => false, // Other operators not applicable to numeric values
        }
    }
    
    fn check_string_filter(value: &str, operator: &FilterOperator, filter_value: &FilterValue) -> bool {
        match operator {
            FilterOperator::Equal => {
                if let Some(target) = filter_value.as_string() {
                    value == target
                } else {
                    false
                }
            }
            FilterOperator::NotEqual => {
                if let Some(target) = filter_value.as_string() {
                    value != target
                } else {
                    true
                }
            }
            FilterOperator::Like => {
                if let Some(pattern) = filter_value.as_string() {
                    Self::matches_like_pattern(value, pattern)
                } else {
                    false
                }
            }
            _ => false, // Other operators not applicable to string values
        }
    }
    
    fn matches_like_pattern(value: &str, pattern: &str) -> bool {
        // Simple LIKE pattern matching (% = any characters, _ = single character)
        let regex_pattern = pattern
            .replace('%', ".*")
            .replace('_', ".");
        
        if let Ok(regex) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
            regex.is_match(value)
        } else {
            // Fallback to simple contains check
            value.contains(&pattern.replace('%', ""))
        }
    }
    
    fn create_csv_header(columns: &[String]) -> String {
        format!("{}\n", columns.join(","))
    }
    
    fn format_tag_values_response(results: Vec<crate::graphql::types::TagValueResult>, query_info: &QueryInfo) -> Result<String> {
        let mut response = String::from(&Self::create_csv_header(&query_info.columns));
        let mut row_count = 0;
        
        for result in results {
            if let Some(error) = &result.error {
                // Check if the error code indicates failure (non-zero)
                let error_code = error.code.as_deref().unwrap_or("1"); // Default to "1" (failure) if no code
                if error_code != "0" {
                    let description = error.description.as_deref().unwrap_or("Unknown error");
                    warn!("⚠️  Error for tag {} (code {}): {}", result.name, error_code, description);
                    continue;
                }
                // If code is "0", this is actually a success despite being in the error field
                debug!("✅ Tag {} successful with code 0, description: {:?}", result.name, error.description);
            }
            
            if let Some(value) = &result.value {
                let mut row_values = Vec::new();
                
                for column in &query_info.columns {
                    let cell_value = match column.as_str() {
                        "tag_name" => result.name.clone(),
                        "timestamp" => value.timestamp.clone(),
                        "numeric_value" => {
                            value.value.as_ref()
                                .and_then(|v| v.as_f64())
                                .map(|n| n.to_string())
                                .unwrap_or_else(|| "NULL".to_string())
                        }
                        "string_value" => {
                            value.value.as_ref()
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| "NULL".to_string())
                        }
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
    
    fn format_logged_tag_values_response(results: Vec<crate::graphql::types::LoggedTagValue>, query_info: &QueryInfo) -> Result<String> {
        let mut response = String::from(&Self::create_csv_header(&query_info.columns));
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
                let cell_value = match column.as_str() {
                    "tag_name" => result.tag_name.clone(),
                    "timestamp" => result.timestamp.clone(),
                    "numeric_value" => {
                        result.value.as_ref()
                            .and_then(|v| v.as_f64())
                            .map(|n| n.to_string())
                            .unwrap_or_else(|| "NULL".to_string())
                    }
                    "string_value" => {
                        result.value.as_ref()
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "NULL".to_string())
                    }
                    _ => "NULL".to_string(),
                };
                row_values.push(cell_value);
            }
            
            let row = format!("{}\n", row_values.join(","));
            response.push_str(&row);
            row_count += 1;
        }
        
        info!("📊 Formatted {} rows for LoggedTagValues query", row_count);
        Ok(response)
    }
    
    fn format_active_alarms_response(results: Vec<crate::graphql::types::ActiveAlarm>, query_info: &QueryInfo) -> Result<String> {
        let mut response = String::from(&Self::create_csv_header(&query_info.columns));
        let mut row_count = 0;
        
        for result in results {
            let mut row_values = Vec::new();
            
            for column in &query_info.columns {
                let cell_value = match column.as_str() {
                    "name" => result.name.clone(),
                    "instance_id" => result.instance_id.to_string(),
                    "alarm_group_id" => result.alarm_group_id.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    "raise_time" => result.raise_time.clone(),
                    "acknowledgment_time" => result.acknowledgment_time.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "clear_time" => result.clear_time.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "reset_time" => result.reset_time.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "modification_time" => result.modification_time.clone(),
                    "state" => result.state.clone(),
                    "priority" => result.priority.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    "event_text" => result.event_text.as_ref().map(|v| v.join(";")).unwrap_or_else(|| "NULL".to_string()),
                    "info_text" => result.info_text.as_ref().map(|v| v.join(";")).unwrap_or_else(|| "NULL".to_string()),
                    "origin" => result.origin.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "area" => result.area.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "value" => result.value.as_ref().map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    "host_name" => result.host_name.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "user_name" => result.user_name.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    _ => "NULL".to_string(),
                };
                row_values.push(cell_value);
            }
            
            let row = format!("{}\n", row_values.join(","));
            response.push_str(&row);
            row_count += 1;
        }
        
        info!("📊 Formatted {} rows for ActiveAlarms query", row_count);
        Ok(response)
    }
    
    fn format_logged_alarms_response(results: Vec<crate::graphql::types::LoggedAlarm>, query_info: &QueryInfo) -> Result<String> {
        let mut response = String::from(&Self::create_csv_header(&query_info.columns));
        let mut row_count = 0;
        
        for result in results {
            let mut row_values = Vec::new();
            
            for column in &query_info.columns {
                let cell_value = match column.as_str() {
                    "name" => result.name.clone(),
                    "instance_id" => result.instance_id.to_string(),
                    "alarm_group_id" => result.alarm_group_id.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    "raise_time" => result.raise_time.clone(),
                    "acknowledgment_time" => result.acknowledgment_time.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "clear_time" => result.clear_time.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "reset_time" => result.reset_time.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "modification_time" => result.modification_time.clone(),
                    "state" => result.state.clone(),
                    "priority" => result.priority.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    "event_text" => result.event_text.as_ref().map(|v| v.join(";")).unwrap_or_else(|| "NULL".to_string()),
                    "info_text" => result.info_text.as_ref().map(|v| v.join(";")).unwrap_or_else(|| "NULL".to_string()),
                    "origin" => result.origin.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "area" => result.area.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "value" => result.value.as_ref().map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    "host_name" => result.host_name.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "user_name" => result.user_name.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    "duration" => result.duration.as_ref().cloned().unwrap_or_else(|| "NULL".to_string()),
                    _ => "NULL".to_string(),
                };
                row_values.push(cell_value);
            }
            
            let row = format!("{}\n", row_values.join(","));
            response.push_str(&row);
            row_count += 1;
        }
        
        info!("📊 Formatted {} rows for LoggedAlarms query", row_count);
        Ok(response)
    }
}