use crate::query_handler::QueryHandler;
use crate::tables::{ColumnFilter, FilterOperator, FilterValue};
use anyhow::Result;

impl QueryHandler {
    pub(super) fn apply_filters(
        results: Vec<crate::graphql::types::TagValueResult>,
        filters: &[ColumnFilter],
    ) -> Result<Vec<crate::graphql::types::TagValueResult>> {
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
                            if let Some(numeric_val) = value.value.as_ref().and_then(|v| v.as_f64())
                            {
                                if !Self::check_numeric_filter(
                                    numeric_val,
                                    &filter.operator,
                                    &filter.value,
                                ) {
                                    include = false;
                                    break;
                                }
                            }
                        }
                    }
                    "string_value" => {
                        if let Some(value) = &result.value {
                            if let Some(string_val) = value.value.as_ref().and_then(|v| v.as_str())
                            {
                                if !Self::check_string_filter(
                                    string_val,
                                    &filter.operator,
                                    &filter.value,
                                ) {
                                    include = false;
                                    break;
                                }
                            }
                        }
                    }
                    "quality" => {
                        if let Some(value) = &result.value {
                            if let Some(quality) = &value.quality {
                                if !Self::check_string_filter(
                                    &quality.quality,
                                    &filter.operator,
                                    &filter.value,
                                ) {
                                    include = false;
                                    break;
                                }
                            } else {
                                // No quality available, check if filter expects NULL
                                if !Self::check_null_filter(&filter.operator, &filter.value) {
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

    pub(super) fn apply_browse_filters(
        results: Vec<crate::graphql::types::BrowseResult>,
        filters: &[ColumnFilter],
    ) -> Result<Vec<crate::graphql::types::BrowseResult>> {
        let mut filtered = Vec::new();

        for result in results {
            let mut include = true;

            // Check if this result passes all filters
            for filter in filters {
                match filter.column.as_str() {
                    "tag_name" | "object_type" => {
                        // These filters are already applied in the GraphQL query
                        continue;
                    }
                    "display_name" => {
                        // Post-process filtering for display_name (not supported by GraphQL)
                        let display_name = result.display_name.as_deref().unwrap_or("");
                        if !Self::check_string_filter(
                            display_name,
                            &filter.operator,
                            &filter.value,
                        ) {
                            include = false;
                            break;
                        }
                    }
                    "data_type" => {
                        // Post-process filtering for data_type
                        let data_type = result.data_type.as_deref().unwrap_or("");
                        if !Self::check_string_filter(data_type, &filter.operator, &filter.value) {
                            include = false;
                            break;
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

    pub(super) fn apply_logged_filters(
        results: Vec<crate::graphql::types::LoggedTagValue>,
        filters: &[ColumnFilter],
    ) -> Result<Vec<crate::graphql::types::LoggedTagValue>> {
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
                            if !Self::check_numeric_filter(
                                numeric_val,
                                &filter.operator,
                                &filter.value,
                            ) {
                                include = false;
                                break;
                            }
                        }
                    }
                    "string_value" => {
                        if let Some(string_val) = result.value.as_ref().and_then(|v| v.as_str()) {
                            if !Self::check_string_filter(
                                string_val,
                                &filter.operator,
                                &filter.value,
                            ) {
                                include = false;
                                break;
                            }
                        }
                    }
                    "quality" => {
                        if let Some(quality) = &result.quality {
                            if !Self::check_string_filter(
                                &quality.quality,
                                &filter.operator,
                                &filter.value,
                            ) {
                                include = false;
                                break;
                            }
                        } else {
                            // No quality available, check if filter expects NULL
                            if !Self::check_null_filter(&filter.operator, &filter.value) {
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

    pub(super) fn apply_alarm_filters(
        results: Vec<crate::graphql::types::ActiveAlarm>,
        filters: &[ColumnFilter],
    ) -> Result<Vec<crate::graphql::types::ActiveAlarm>> {
        let mut filtered = Vec::new();

        for result in results {
            let mut include = true;

            for filter in filters {
                match filter.column.as_str() {
                    "priority" => {
                        if let Some(priority_val) = filter.value.as_integer() {
                            let alarm_priority = result.priority.unwrap_or(0) as i64;
                            if !Self::check_numeric_filter(
                                alarm_priority as f64,
                                &filter.operator,
                                &FilterValue::Integer(priority_val),
                            ) {
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

    pub(super) fn apply_logged_alarm_filters(
        results: Vec<crate::graphql::types::LoggedAlarm>,
        filters: &[ColumnFilter],
    ) -> Result<Vec<crate::graphql::types::LoggedAlarm>> {
        // Similar logic to apply_alarm_filters but for logged alarms
        let mut filtered = Vec::new();

        for result in results {
            let mut include = true;

            for filter in filters {
                match filter.column.as_str() {
                    "priority" => {
                        if let Some(priority_val) = filter.value.as_integer() {
                            let alarm_priority = result.priority.unwrap_or(0) as i64;
                            if !Self::check_numeric_filter(
                                alarm_priority as f64,
                                &filter.operator,
                                &FilterValue::Integer(priority_val),
                            ) {
                                include = false;
                                break;
                            }
                        }
                    }
                    "timestamp" | "modification_time" => {
                        // Handled by GraphQL query
                        continue;
                    }
                    "filterString" | "system_name" | "filter_language" => {
                        // Virtual columns - handled by GraphQL query, skip in post-processing
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

    pub(super) fn check_numeric_filter(
        value: f64,
        operator: &FilterOperator,
        filter_value: &FilterValue,
    ) -> bool {
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

    pub(super) fn check_string_filter(
        value: &str,
        operator: &FilterOperator,
        filter_value: &FilterValue,
    ) -> bool {
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

    pub(super) fn matches_like_pattern(value: &str, pattern: &str) -> bool {
        // Simple LIKE pattern matching (% = any characters)
        let regex_pattern = pattern.replace('%', ".*");

        if let Ok(regex) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
            regex.is_match(value)
        } else {
            // Fallback to simple contains check
            value.contains(&pattern.replace('%', ""))
        }
    }

    pub(super) fn check_null_filter(operator: &FilterOperator, filter_value: &FilterValue) -> bool {
        match operator {
            FilterOperator::Equal => {
                // Check if the filter is looking for NULL values
                if let Some(target) = filter_value.as_string() {
                    target.to_uppercase() == "NULL"
                } else {
                    false
                }
            }
            FilterOperator::NotEqual => {
                // If filtering for NOT NULL, then missing values should be excluded
                if let Some(target) = filter_value.as_string() {
                    target.to_uppercase() != "NULL"
                } else {
                    true
                }
            }
            _ => false, // Other operators don't make sense for NULL checks
        }
    }
}
