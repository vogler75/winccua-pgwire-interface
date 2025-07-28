use super::types::*;
use anyhow::{anyhow, Result};
use reqwest::Client;
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct GraphQLClient {
    client: Client,
    url: String,
}

impl GraphQLClient {
    pub fn new(url: String) -> Self {
        Self {
            client: Client::new(),
            url,
        }
    }

    pub async fn login(&self, username: &str, password: &str) -> Result<Session> {
        let query = r#"
            mutation Login($username: String!, $password: String!) {
                login(username: $username, password: $password) {
                    token
                    expires
                    user {
                        id
                        name
                        fullName
                        language
                    }
                    error {
                        code
                        description
                    }
                }
            }
        "#;

        let request = LoginRequest {
            query: query.to_string(),
            variables: LoginVariables {
                username: username.to_string(),
                password: password.to_string(),
            },
        };

        debug!("Sending login request for user: {}", username);
        
        let response = self
            .client
            .post(&self.url)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("GraphQL request failed with status: {}", response.status()));
        }

        let login_response: LoginResponse = response.json().await?;

        debug!("Login response: {:?}", login_response);

        if let Some(errors) = login_response.errors {
            let error_msg = errors.iter()
                .map(|e| e.description.as_deref().unwrap_or("Unknown error"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(anyhow!("Login failed: {}", error_msg));
        }

        let session = login_response
            .data
            .ok_or_else(|| anyhow!("No data in login response"))?
            .login;

        debug!("Session data: {:?}", session);

        if let Some(error) = &session.error {
            debug!("Session contains error field: {:?}", error);
            
            // Check if the error code indicates failure (non-zero)
            let error_code = error.code.as_deref().unwrap_or("1"); // Default to "1" (failure) if no code
            debug!("Error code: '{}' (success if '0')", error_code);
            
            if error_code != "0" {
                let description = error.description.as_deref().unwrap_or("Unknown error");
                let message = error.message.as_deref().unwrap_or("No additional message");
                error!("Authentication failed - code: {}, description: {}, message: {}", error_code, description, message);
                return Err(anyhow!("Login failed (code {}): {} - {}", error_code, description, message));
            }
            // If code is "0", this is actually a success despite being in the error field
            info!("Login successful with code 0, description: {:?}", error.description);
        }

        info!("Successfully logged in user: {}", username);
        Ok(session)
    }

    pub async fn get_tag_values(&self, token: &str, names: Vec<String>, direct_read: bool) -> Result<Vec<TagValueResult>> {
        let query = r#"
            query TagValues($names: [String!]!, $directRead: Boolean!) {
                tagValues(names: $names, directRead: $directRead) {
                    name
                    value {
                        value
                        timestamp
                        quality {
                            quality
                        }
                    }
                    error {
                        code
                        description
                    }
                }
            }
        "#;

        let request = TagValuesRequest {
            query: query.to_string(),
            variables: TagValuesVariables {
                names,
                direct_read,
            },
        };

        debug!("Getting tag values with {} names", request.variables.names.len());

        let response = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("GraphQL request failed with status: {}", response.status()));
        }

        let tag_response: TagValuesResponse = response.json().await?;

        if let Some(errors) = tag_response.errors {
            let error_msg = errors.iter()
                .map(|e| e.description.as_deref().unwrap_or("Unknown error"))
                .collect::<Vec<_>>()
                .join(", ");
            error!("TagValues query errors: {}", error_msg);
        }

        Ok(tag_response
            .data
            .map(|d| d.tag_values)
            .unwrap_or_default())
    }

    pub async fn get_logged_tag_values(
        &self,
        token: &str,
        names: Vec<String>,
        start_time: Option<String>,
        end_time: Option<String>,
        max_values: Option<i32>,
        sorting_mode: Option<String>,
    ) -> Result<Vec<LoggedTagValuesResult>> {
        let query = r#"
            query LoggedTagValues($names: [String!]!, $startTime: Timestamp, $endTime: Timestamp, $maxNumberOfValues: Int, $sortingMode: LoggedTagValuesSortingModeEnum) {
                loggedTagValues(names: $names, startTime: $startTime, endTime: $endTime, maxNumberOfValues: $maxNumberOfValues, sortingMode: $sortingMode) {
                    loggingTagName
                    values {
                        value {
                            value
                            timestamp
                            quality {
                                quality
                            }
                        }
                        flags
                    }
                    error {
                        code
                        description
                    }
                }
            }
        "#;

        // Store copies for error reporting
        let names_copy = names.clone();
        let start_time_copy = start_time.clone();
        let end_time_copy = end_time.clone();
        let sorting_mode_copy = sorting_mode.clone();

        let request = LoggedTagValuesRequest {
            query: query.to_string(),
            variables: LoggedTagValuesVariables {
                names,
                start_time,
                end_time,
                max_number_of_values: max_values,
                sorting_mode,
            },
        };

        debug!("🚀 Generated GraphQL query:");
        debug!("📄 Query: {}", query);
        debug!("🔧 Variables: {:#?}", request.variables);
        debug!("Getting logged tag values");

        let response = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await?;
            error!("LoggedTagValues GraphQL request failed with status: {} - Body: {}", status, error_text);
            return Err(anyhow!("GraphQL request failed with status: {} - {}", status, error_text));
        }

        let response_text = response.text().await?;
        
        // First check if this is an error response
        if response_text.contains("\"errors\"") && response_text.contains("\"loggedTagValues\":null") {
            // Parse just to get the error message
            if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_text) {
                if let Some(errors) = error_response.get("errors").and_then(|e| e.as_array()) {
                    let mut error_details = Vec::new();
                    
                    for error in errors {
                        let message = error.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                        let mut detail = format!("- {}", message);
                        
                        // Add location info if available
                        if let Some(locations) = error.get("locations").and_then(|l| l.as_array()) {
                            let loc_strs: Vec<String> = locations.iter()
                                .filter_map(|loc| {
                                    let line = loc.get("line").and_then(|l| l.as_i64());
                                    let col = loc.get("column").and_then(|c| c.as_i64());
                                    match (line, col) {
                                        (Some(l), Some(c)) => Some(format!("line {}, col {}", l, c)),
                                        _ => None
                                    }
                                })
                                .collect();
                            if !loc_strs.is_empty() {
                                detail.push_str(&format!(" (at {})", loc_strs.join("; ")));
                            }
                        }
                        
                        // Add path info if available
                        if let Some(path) = error.get("path").and_then(|p| p.as_array()) {
                            let path_str: Vec<String> = path.iter()
                                .filter_map(|p| p.as_str().map(|s| s.to_string()))
                                .collect();
                            if !path_str.is_empty() {
                                detail.push_str(&format!(" [path: {}]", path_str.join(".")));
                            }
                        }
                        
                        // Add extension code if available
                        if let Some(code) = error.get("extensions")
                            .and_then(|e| e.get("code"))
                            .and_then(|c| c.as_str()) {
                            detail.push_str(&format!(" [code: {}]", code));
                        }
                        
                        error_details.push(detail);
                    }
                    
                    let error_msg = format!(
                        "LoggedTagValues query failed with {} error(s):\n{}\n\nQuery variables were: names={:?}, startTime={:?}, endTime={:?}, maxNumberOfValues={:?}, sortingMode={:?}\n\nGraphQL Query:\n{}\n\nFull request JSON:\n{}",
                        error_details.len(),
                        error_details.join("\n"),
                        names_copy,
                        start_time_copy,
                        end_time_copy,
                        max_values,
                        sorting_mode_copy,
                        query,
                        serde_json::to_string_pretty(&request).unwrap_or_else(|_| "Failed to serialize request".to_string())
                    );
                    error!("{}", error_msg);
                    return Err(anyhow!("{}", error_msg));
                }
            }
            return Err(anyhow!("LoggedTagValues query failed with unknown error. Response: {}", response_text));
        }
        
        let logged_response: LoggedTagValuesResponse = serde_json::from_str(&response_text)
            .map_err(|e| {
                error!("Failed to parse LoggedTagValues response: {}", e);
                error!("Response was: {}", response_text);
                anyhow!("Failed to parse LoggedTagValues response: {}", e)
            })?;

        if let Some(errors) = logged_response.errors {
            let error_msg = errors.iter()
                .map(|e| e.message.as_deref().unwrap_or(e.description.as_deref().unwrap_or("Unknown error")))
                .collect::<Vec<_>>()
                .join(", ");
            if !error_msg.is_empty() {
                return Err(anyhow!("LoggedTagValues query errors: {}", error_msg));
            }
        }

        Ok(logged_response
            .data
            .map(|d| d.logged_tag_values)
            .unwrap_or_default())
    }

    pub async fn get_active_alarms(
        &self,
        token: &str,
        system_names: Vec<String>,
        filter_string: String,
    ) -> Result<Vec<ActiveAlarm>> {
        let query = r#"
            query ActiveAlarms($systemNames: [String!], $filterString: String!) {
                activeAlarms(systemNames: $systemNames, filterString: $filterString) {
                    name
                    instanceID
                    alarmGroupID
                    raiseTime
                    acknowledgmentTime
                    clearTime
                    resetTime
                    modificationTime
                    state
                    priority
                    eventText
                    infoText
                    origin
                    area
                    value
                    hostName
                    userName
                }
            }
        "#;

        let request = ActiveAlarmsRequest {
            query: query.to_string(),
            variables: ActiveAlarmsVariables {
                system_names,
                filter_string,
                filter_language: "en-US".to_string(),
                languages: vec!["en-US".to_string()],
            },
        };

        debug!("Getting active alarms");

        let response = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("GraphQL request failed with status: {}", response.status()));
        }

        let alarms_response: ActiveAlarmsResponse = response.json().await?;

        if let Some(errors) = alarms_response.errors {
            let error_msg = errors.iter()
                .map(|e| e.description.as_deref().unwrap_or("Unknown error"))
                .collect::<Vec<_>>()
                .join(", ");
            error!("ActiveAlarms query errors: {}", error_msg);
        }

        Ok(alarms_response
            .data
            .map(|d| d.active_alarms)
            .unwrap_or_default())
    }

    pub async fn get_logged_alarms(
        &self,
        token: &str,
        system_names: Vec<String>,
        filter_string: String,
        start_time: Option<String>,
        end_time: Option<String>,
        max_results: Option<i32>,
        filter_language: Option<String>,
    ) -> Result<Vec<LoggedAlarm>> {
        let query = r#"
            query LoggedAlarms($systemNames: [String], $filterString: String, $filterLanguage: String, $startTime: Timestamp, $endTime: Timestamp, $maxNumberOfResults: Int) {
                loggedAlarms(systemNames: $systemNames, filterString: $filterString, filterLanguage: $filterLanguage, startTime: $startTime, endTime: $endTime, maxNumberOfResults: $maxNumberOfResults) {
                    name
                    instanceID
                    alarmGroupID
                    raiseTime
                    acknowledgmentTime
                    clearTime
                    resetTime
                    modificationTime
                    state
                    priority
                    eventText
                    infoText
                    origin
                    area
                    value
                    hostName
                    userName
                    duration
                }
            }
        "#;

        let request = LoggedAlarmsRequest {
            query: query.to_string(),
            variables: LoggedAlarmsVariables {
                system_names: if system_names.is_empty() { None } else { Some(system_names) },
                filter_string: if filter_string.is_empty() { None } else { Some(filter_string) },
                filter_language,
                languages: None, // Set to None since we don't set it from SQL queries
                start_time,
                end_time,
                max_number_of_results: max_results,
            },
        };

        debug!("🚀 Generated GraphQL query:");
        debug!("📄 Query: {}", query);
        debug!("🔧 Variables: {:#?}", request.variables);
        debug!("Getting logged alarms");

        let response = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("GraphQL request failed with status: {}", response.status()));
        }

        let alarms_response: LoggedAlarmsResponse = response.json().await?;

        if let Some(errors) = alarms_response.errors {
            let error_msg = errors.iter()
                .map(|e| e.description.as_deref().unwrap_or("Unknown error"))
                .collect::<Vec<_>>()
                .join(", ");
            error!("LoggedAlarms query errors: {}", error_msg);
        }

        Ok(alarms_response
            .data
            .map(|d| d.logged_alarms)
            .unwrap_or_default())
    }

    pub async fn browse_tags(&self, token: &str, name_filters: Vec<String>) -> Result<Vec<BrowseResult>> {
        let query = r#"
            query Browse($nameFilters: [String!]!) {
                browse(nameFilters: $nameFilters) {
                    name
                    displayName
                    objectType
                    dataType
                }
            }
        "#;

        let request = BrowseRequest {
            query: query.to_string(),
            variables: BrowseVariables {
                name_filters,
                object_type_filters: vec![],
                base_type_filters: vec![],
                language: "en-US".to_string(),
            },
        };

        debug!("🚀 Generated GraphQL query:");
        debug!("📄 Query: {}", query);
        debug!("🔧 Variables: {:#?}", request.variables);
        debug!("Browsing tags");

        let response = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let response_text = response.text().await.unwrap_or_else(|_| "Failed to read response".to_string());
            error!("GraphQL browse_tags request failed with status: {}", status);
            error!("GraphQL request body: {}", serde_json::to_string_pretty(&request).unwrap_or_else(|_| "Failed to serialize request".to_string()));
            error!("GraphQL response body: {}", response_text);
            return Err(anyhow!("GraphQL request failed with status: {}", status));
        }

        let browse_response: BrowseResponse = response.json().await?;

        if let Some(errors) = browse_response.errors {
            let error_msg = errors.iter()
                .map(|e| e.description.as_deref().unwrap_or("Unknown error"))
                .collect::<Vec<_>>()
                .join(", ");
            error!("Browse query errors: {}", error_msg);
        }

        Ok(browse_response
            .data
            .map(|d| d.browse)
            .unwrap_or_default())
    }

    pub async fn browse_tags_with_object_type(&self, token: &str, name_filters: Vec<String>, object_type_filters: Vec<String>, language: String) -> Result<Vec<BrowseResult>> {
        let query = r#"
            query Browse($nameFilters: [String!]!, $objectTypeFilters: [ObjectTypesEnum!]!, $language: String!) {
                browse(nameFilters: $nameFilters, objectTypeFilters: $objectTypeFilters, language: $language) {
                    name
                    displayName
                    objectType
                    dataType
                }
            }
        "#;

        let request = BrowseRequest {
            query: query.to_string(),
            variables: BrowseVariables {
                name_filters: name_filters.clone(),
                object_type_filters: object_type_filters.clone(),
                base_type_filters: vec![],
                language: language.clone(),
            },
        };

        debug!("🚀 Generated GraphQL query:");
        debug!("📄 Query: {}", query);
        debug!("🔧 Variables: {:#?}", request.variables);
        debug!("Browsing tags with object type filters");

        let response = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let response_text = response.text().await.unwrap_or_else(|_| "Failed to read response".to_string());
            error!("GraphQL browse_tags_with_object_type request failed with status: {}", status);
            error!("GraphQL request body: {}", serde_json::to_string_pretty(&request).unwrap_or_else(|_| "Failed to serialize request".to_string()));
            error!("GraphQL response body: {}", response_text);
            return Err(anyhow!("GraphQL request failed with status: {}", status));
        }

        let browse_response: BrowseResponse = response.json().await?;

        if let Some(errors) = browse_response.errors {
            let error_msg = errors.iter()
                .map(|e| e.description.as_deref().unwrap_or("Unknown error"))
                .collect::<Vec<_>>()
                .join(", ");
            error!("Browse tags with object type query errors: {}", error_msg);
            return Err(anyhow!("Browse query failed: {}", error_msg));
        }

        Ok(browse_response
            .data
            .map(|d| d.browse)
            .unwrap_or_default())
    }

    pub async fn browse_logging_tags(&self, token: &str, name_filters: Vec<String>) -> Result<Vec<BrowseResult>> {
        // Try first with objectTypeFilters (newer API)
        let query_with_filters = r#"
            query Browse($nameFilters: [String!]!, $objectTypeFilters: [ObjectTypesEnum!]!) {
                browse(nameFilters: $nameFilters, objectTypeFilters: $objectTypeFilters) {
                    name
                    displayName
                    objectType
                    dataType
                }
            }
        "#;

        let request_with_filters = BrowseRequest {
            query: query_with_filters.to_string(),
            variables: BrowseVariables {
                name_filters: name_filters.clone(),
                object_type_filters: vec!["LOGGINGTAG".to_string()],
                base_type_filters: vec![],
                language: "en-US".to_string(),
            },
        };

        debug!("Browsing logging tags with objectTypeFilters=LOGGINGTAG");
        debug!("GraphQL query: {}", query_with_filters);
        debug!("GraphQL variables: nameFilters={:?}, objectTypeFilters={:?}", 
               request_with_filters.variables.name_filters, request_with_filters.variables.object_type_filters);

        let response = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&request_with_filters)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let response_text = response.text().await.unwrap_or_else(|_| "Failed to read response".to_string());
            error!("GraphQL browse_logging_tags with objectTypeFilters failed with status: {}", status);
            error!("GraphQL request body: {}", serde_json::to_string_pretty(&request_with_filters).unwrap_or_else(|_| "Failed to serialize request".to_string()));
            error!("GraphQL response body: {}", response_text);
            return Err(anyhow!("GraphQL request failed with status: {}", status));
        }

        let browse_response: BrowseResponse = response.json().await?;

        if let Some(errors) = browse_response.errors {
            let error_msg = errors.iter()
                .map(|e| e.description.as_deref().unwrap_or("Unknown error"))
                .collect::<Vec<_>>()
                .join(", ");
            error!("Browse logging tags query errors: {}", error_msg);
            return Err(anyhow!("Browse query failed: {}", error_msg));
        }

        Ok(browse_response
            .data
            .map(|d| d.browse)
            .unwrap_or_default())
    }

    pub async fn extend_session(&self, token: &str) -> Result<Session> {
        let query = r#"
            mutation ExtendSession {
                extendSession {
                    token
                    expires
                    error {
                        code
                        description
                    }
                }
            }
        "#;

        let request = serde_json::json!({
            "query": query,
        });

        debug!("Sending extendSession request");

        let response = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "GraphQL request failed with status: {}",
                response.status()
            ));
        }

        let extend_session_response: ExtendSessionResponse = response.json().await?;

        if let Some(errors) = extend_session_response.errors {
            let error_msg = errors
                .iter()
                .map(|e| e.description.as_deref().unwrap_or("Unknown error"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(anyhow!("extendSession failed: {}", error_msg));
        }

        let session = extend_session_response
            .data
            .ok_or_else(|| anyhow!("No data in extendSession response"))?
            .extend_session;

        if let Some(error) = &session.error {
            let error_code = error.code.as_deref().unwrap_or("1");
            if error_code != "0" {
                let description = error.description.as_deref().unwrap_or("Unknown error");
                error!(
                    "Failed to extend session - code: {}, description: {}",
                    error_code, description
                );
                return Err(anyhow!(
                    "Failed to extend session (code {}): {}",
                    error_code,
                    description
                ));
            }
        }

        info!("Successfully extended session");
        Ok(session)
    }
}

pub async fn validate_connection(url: &str) -> Result<()> {
    let client = Client::new();
    
    // First try a simple introspection query
    let introspection_query = serde_json::json!({
        "query": "{ __schema { queryType { name } } }"
    });
    
    debug!("Attempting GraphQL introspection query to: {}", url);
    let response = client
        .post(url)
        .json(&introspection_query)
        .send()
        .await?;
    
    if response.status().is_success() {
        let response_text = response.text().await?;
        debug!("GraphQL introspection response: {}", response_text);
        
        // Check if it's a valid GraphQL response
        if response_text.contains("\"data\"") || response_text.contains("\"__schema\"") {
            return Ok(());
        } else if response_text.contains("\"errors\"") {
            // GraphQL endpoint is working but introspection might be disabled
            debug!("Introspection disabled, trying fallback validation");
            return validate_with_simple_query(&client, url).await;
        } else {
            return Err(anyhow!("Invalid GraphQL response format: {}", response_text));
        }
    } else {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        
        // If introspection fails, try a simpler approach
        if status.as_u16() == 400 {
            debug!("Introspection failed with 400, trying fallback validation");
            return validate_with_simple_query(&client, url).await;
        }
        
        return Err(anyhow!("GraphQL server returned status: {} - {}", status, error_text));
    }
}

async fn validate_with_simple_query(client: &Client, url: &str) -> Result<()> {
    // Try a minimal query that should work on most GraphQL servers
    let simple_query = serde_json::json!({
        "query": "{ __typename }"
    });
    
    debug!("Attempting simple GraphQL query to: {}", url);
    let response = client
        .post(url)
        .json(&simple_query)
        .send()
        .await?;
    
    if response.status().is_success() {
        let response_text = response.text().await?;
        debug!("Simple query response: {}", response_text);
        
        if response_text.contains("\"data\"") {
            return Ok(());
        } else if response_text.contains("\"errors\"") {
            // Even if there are errors, if we get a GraphQL response format, the endpoint is working
            if response_text.contains("\"message\"") {
                return Ok(());
            }
        }
        
        Err(anyhow!("Server responded but not in GraphQL format: {}", response_text))
    } else {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        Err(anyhow!("GraphQL validation failed with status: {} - {}", status, error_text))
    }
}