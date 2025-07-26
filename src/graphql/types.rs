use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequest {
    pub query: String,
    pub variables: LoginVariables,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginVariables {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginResponse {
    pub data: Option<LoginData>,
    pub errors: Option<Vec<GraphQLError>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginData {
    pub login: Session,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub token: String,
    pub expires: String,
    pub user: Option<User>,
    pub error: Option<GraphQLError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub name: String,
    #[serde(rename = "fullName")]
    pub full_name: Option<String>,
    pub language: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLError {
    pub code: Option<String>,
    pub description: Option<String>,
    pub message: Option<String>,
}

// Tag Values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagValuesRequest {
    pub query: String,
    pub variables: TagValuesVariables,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagValuesVariables {
    pub names: Vec<String>,
    #[serde(rename = "directRead")]
    pub direct_read: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagValuesResponse {
    pub data: Option<TagValuesData>,
    pub errors: Option<Vec<GraphQLError>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagValuesData {
    #[serde(rename = "tagValues")]
    pub tag_values: Vec<TagValueResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagValueResult {
    pub name: String,
    pub value: Option<Value>,
    pub error: Option<GraphQLError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Value {
    pub value: Option<serde_json::Value>,
    pub timestamp: String,
    pub quality: Option<Quality>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quality {
    pub quality: String,
}

// Logged Tag Values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedTagValuesRequest {
    pub query: String,
    pub variables: LoggedTagValuesVariables,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedTagValuesVariables {
    pub names: Vec<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<String>,
    #[serde(rename = "endTime")]
    pub end_time: Option<String>,
    #[serde(rename = "maxNumberOfValues")]
    pub max_number_of_values: Option<i32>,
    #[serde(rename = "sortingMode")]
    pub sorting_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedTagValuesResponse {
    pub data: Option<LoggedTagValuesData>,
    pub errors: Option<Vec<GraphQLError>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedTagValuesData {
    #[serde(rename = "loggedTagValues")]
    pub logged_tag_values: Vec<LoggedTagValuesResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedTagValuesResult {
    #[serde(rename = "loggingTagName")]
    pub logging_tag_name: String,
    pub values: Vec<LoggedValue>,
    pub error: Option<GraphQLError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedValue {
    pub value: Value,
    pub flags: Option<Vec<String>>,
}

// Simplified type for query handler processing
#[derive(Debug, Clone)]
pub struct LoggedTagValue {
    pub tag_name: String,
    pub timestamp: String,
    pub value: Option<serde_json::Value>,
}

// Active Alarms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveAlarmsRequest {
    pub query: String,
    pub variables: ActiveAlarmsVariables,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveAlarmsVariables {
    #[serde(rename = "systemNames")]
    pub system_names: Vec<String>,
    #[serde(rename = "filterString")]
    pub filter_string: String,
    #[serde(rename = "filterLanguage")]
    pub filter_language: String,
    pub languages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveAlarmsResponse {
    pub data: Option<ActiveAlarmsData>,
    pub errors: Option<Vec<GraphQLError>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveAlarmsData {
    #[serde(rename = "activeAlarms")]
    pub active_alarms: Vec<ActiveAlarm>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveAlarm {
    pub name: String,
    #[serde(rename = "instanceID")]
    pub instance_id: i32,
    #[serde(rename = "alarmGroupID")]
    pub alarm_group_id: Option<i32>,
    #[serde(rename = "raiseTime")]
    pub raise_time: String,
    #[serde(rename = "acknowledgmentTime")]
    pub acknowledgment_time: Option<String>,
    #[serde(rename = "clearTime")]
    pub clear_time: Option<String>,
    #[serde(rename = "resetTime")]
    pub reset_time: Option<String>,
    #[serde(rename = "modificationTime")]
    pub modification_time: String,
    pub state: String,
    pub priority: Option<i32>,
    #[serde(rename = "eventText")]
    pub event_text: Option<Vec<String>>,
    #[serde(rename = "infoText")]
    pub info_text: Option<Vec<String>>,
    pub origin: Option<String>,
    pub area: Option<String>,
    pub value: Option<serde_json::Value>,
    #[serde(rename = "hostName")]
    pub host_name: Option<String>,
    #[serde(rename = "userName")]
    pub user_name: Option<String>,
}

// Logged Alarms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedAlarmsRequest {
    pub query: String,
    pub variables: LoggedAlarmsVariables,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedAlarmsVariables {
    #[serde(rename = "systemNames")]
    pub system_names: Vec<String>,
    #[serde(rename = "filterString")]
    pub filter_string: String,
    #[serde(rename = "filterLanguage")]
    pub filter_language: String,
    pub languages: Vec<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<String>,
    #[serde(rename = "endTime")]
    pub end_time: Option<String>,
    #[serde(rename = "maxNumberOfResults")]
    pub max_number_of_results: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedAlarmsResponse {
    pub data: Option<LoggedAlarmsData>,
    pub errors: Option<Vec<GraphQLError>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedAlarmsData {
    #[serde(rename = "loggedAlarms")]
    pub logged_alarms: Vec<LoggedAlarm>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggedAlarm {
    pub name: String,
    #[serde(rename = "instanceID")]
    pub instance_id: i32,
    #[serde(rename = "alarmGroupID")]
    pub alarm_group_id: Option<i32>,
    #[serde(rename = "raiseTime")]
    pub raise_time: String,
    #[serde(rename = "acknowledgmentTime")]
    pub acknowledgment_time: Option<String>,
    #[serde(rename = "clearTime")]
    pub clear_time: Option<String>,
    #[serde(rename = "resetTime")]
    pub reset_time: Option<String>,
    #[serde(rename = "modificationTime")]
    pub modification_time: String,
    pub state: String,
    pub priority: Option<i32>,
    #[serde(rename = "eventText")]
    pub event_text: Option<Vec<String>>,
    #[serde(rename = "infoText")]
    pub info_text: Option<Vec<String>>,
    pub origin: Option<String>,
    pub area: Option<String>,
    pub value: Option<serde_json::Value>,
    #[serde(rename = "hostName")]
    pub host_name: Option<String>,
    #[serde(rename = "userName")]
    pub user_name: Option<String>,
    pub duration: Option<String>,
}

// Browse
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowseRequest {
    pub query: String,
    pub variables: BrowseVariables,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowseVariables {
    #[serde(rename = "nameFilters")]
    pub name_filters: Vec<String>,
    #[serde(rename = "objectTypeFilters")]
    pub object_type_filters: Vec<String>,
    #[serde(rename = "baseTypeFilters")]
    pub base_type_filters: Vec<String>,
    pub language: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowseResponse {
    pub data: Option<BrowseData>,
    pub errors: Option<Vec<GraphQLError>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowseData {
    pub browse: Vec<BrowseResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowseResult {
    pub name: String,
    #[serde(rename = "displayName")]
    pub display_name: Option<String>,
    #[serde(rename = "objectType")]
    pub object_type: Option<String>,
    #[serde(rename = "dataType")]
    pub data_type: Option<String>,
}