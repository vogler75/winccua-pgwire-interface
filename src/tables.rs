use pgwire::api::Type;

#[derive(Debug, Clone, PartialEq)]
pub enum VirtualTable {
    TagValues,
    LoggedTagValues,
    ActiveAlarms,
    LoggedAlarms,
    TagList,
}

impl VirtualTable {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "tagvalues" => Some(Self::TagValues),
            "loggedtagvalues" => Some(Self::LoggedTagValues),
            "activealarms" => Some(Self::ActiveAlarms),
            "loggedalarms" => Some(Self::LoggedAlarms),
            "taglist" => Some(Self::TagList),
            _ => None,
        }
    }

    pub fn get_schema(&self) -> Vec<(&'static str, Type)> {
        match self {
            Self::TagValues => vec![
                ("tag_name", Type::TEXT),
                ("timestamp", Type::TIMESTAMP),
                ("timestamp_ms", Type::INT8),
                ("numeric_value", Type::NUMERIC),
                ("string_value", Type::TEXT),
                ("quality", Type::TEXT),
            ],
            Self::LoggedTagValues => vec![
                ("tag_name", Type::TEXT),
                ("timestamp", Type::TIMESTAMP),
                ("timestamp_ms", Type::INT8),
                ("numeric_value", Type::NUMERIC),
                ("string_value", Type::TEXT),
                ("quality", Type::TEXT),
            ],
            Self::ActiveAlarms => vec![
                ("name", Type::TEXT),
                ("instance_id", Type::INT4),
                ("alarm_group_id", Type::INT4),
                ("raise_time", Type::TIMESTAMP),
                ("acknowledgment_time", Type::TIMESTAMP),
                ("clear_time", Type::TIMESTAMP),
                ("reset_time", Type::TIMESTAMP),
                ("modification_time", Type::TIMESTAMP),
                ("state", Type::TEXT),
                ("priority", Type::INT4),
                ("event_text", Type::TEXT),
                ("info_text", Type::TEXT),
                ("origin", Type::TEXT),
                ("area", Type::TEXT),
                ("value", Type::TEXT),
                ("host_name", Type::TEXT),
                ("user_name", Type::TEXT),
            ],
            Self::LoggedAlarms => vec![
                ("name", Type::TEXT),
                ("instance_id", Type::INT4),
                ("alarm_group_id", Type::INT4),
                ("raise_time", Type::TIMESTAMP),
                ("acknowledgment_time", Type::TIMESTAMP),
                ("clear_time", Type::TIMESTAMP),
                ("reset_time", Type::TIMESTAMP),
                ("modification_time", Type::TIMESTAMP),
                ("state", Type::TEXT),
                ("priority", Type::INT4),
                ("event_text", Type::TEXT),
                ("info_text", Type::TEXT),
                ("origin", Type::TEXT),
                ("area", Type::TEXT),
                ("value", Type::TEXT),
                ("host_name", Type::TEXT),
                ("user_name", Type::TEXT),
                ("duration", Type::TEXT),
            ],
            Self::TagList => vec![
                ("tag_name", Type::TEXT),
                ("display_name", Type::TEXT),
                ("object_type", Type::TEXT),
                ("data_type", Type::TEXT),
            ],
        }
    }

    pub fn get_column_names(&self) -> Vec<&'static str> {
        self.get_schema().into_iter().map(|(name, _)| name).collect()
    }

    #[allow(dead_code)]
    pub fn get_column_types(&self) -> Vec<Type> {
        self.get_schema().into_iter().map(|(_, typ)| typ).collect()
    }

    pub fn has_column(&self, column: &str) -> bool {
        self.get_column_names().contains(&column) || self.is_virtual_column(column)
    }

    pub fn is_virtual_column(&self, column: &str) -> bool {
        match self {
            Self::TagList => column == "language",
            Self::LoggedAlarms => matches!(column, "filterString" | "system_name" | "filter_language"),
            _ => false,
        }
    }

    pub fn is_selectable_column(&self, column: &str) -> bool {
        self.get_column_names().contains(&column) && !self.is_virtual_column(column)
    }
}

#[derive(Debug, Clone)]
pub struct ColumnFilter {
    pub column: String,
    pub operator: FilterOperator,
    pub value: FilterValue,
}

#[derive(Debug, Clone)]
pub enum FilterOperator {
    Equal,
    NotEqual,
    Like,
    In,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Between,
}

#[derive(Debug, Clone)]
pub enum FilterValue {
    String(String),
    Number(f64),
    Integer(i64),
    Timestamp(String),
    List(Vec<String>),
    Range(Box<FilterValue>, Box<FilterValue>),
}

impl FilterValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_number(&self) -> Option<f64> {
        match self {
            Self::Number(n) => Some(*n),
            Self::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            Self::Number(n) => Some(*n as i64),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<String>> {
        match self {
            Self::List(list) => Some(list),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SqlResult {
    Query(QueryInfo),
    SetStatement(String), // Contains the SET command that was executed
}

#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub table: VirtualTable,
    pub columns: Vec<String>,
    pub column_mappings: std::collections::HashMap<String, String>, // alias -> original_column
    pub filters: Vec<ColumnFilter>,
    pub limit: Option<i64>,
    pub order_by: Option<OrderBy>,
    pub distinct: bool,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub ascending: bool,
}

impl QueryInfo {
    pub fn has_required_tag_filter(&self) -> bool {
        match self.table {
            VirtualTable::TagValues | VirtualTable::LoggedTagValues => {
                self.filters.iter().any(|f| {
                    f.column == "tag_name" && matches!(
                        f.operator, 
                        FilterOperator::Equal | FilterOperator::In | FilterOperator::Like
                    )
                })
            }
            _ => true, // Alarms don't require tag filters
        }
    }

    pub fn get_tag_names(&self) -> Vec<String> {
        for filter in &self.filters {
            if filter.column == "tag_name" {
                match &filter.operator {
                    FilterOperator::Equal => {
                        if let Some(name) = filter.value.as_string() {
                            return vec![name.to_string()];
                        }
                    }
                    FilterOperator::In => {
                        if let Some(names) = filter.value.as_list() {
                            return names.clone();
                        }
                    }
                    FilterOperator::Like => {
                        // LIKE patterns will be resolved via browse function
                        // Return empty here since resolve_like_patterns handles this
                        return vec![];
                    }
                    _ => {}
                }
            }
        }
        vec![]
    }

    pub fn get_timestamp_filter(&self) -> Option<(Option<String>, Option<String>)> {
        let mut start_time = None;
        let mut end_time = None;

        for filter in &self.filters {
            if filter.column == "timestamp" {
                match &filter.operator {
                    FilterOperator::GreaterThan | FilterOperator::GreaterThanOrEqual => {
                        if let FilterValue::Timestamp(ts) = &filter.value {
                            start_time = Some(ts.clone());
                        }
                    }
                    FilterOperator::LessThan | FilterOperator::LessThanOrEqual => {
                        if let FilterValue::Timestamp(ts) = &filter.value {
                            end_time = Some(ts.clone());
                        }
                    }
                    FilterOperator::Between => {
                        if let FilterValue::Range(start, end) = &filter.value {
                            if let FilterValue::Timestamp(ts) = start.as_ref() {
                                start_time = Some(ts.clone());
                            }
                            if let FilterValue::Timestamp(ts) = end.as_ref() {
                                end_time = Some(ts.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        if start_time.is_some() || end_time.is_some() {
            Some((start_time, end_time))
        } else {
            None
        }
    }

    pub fn requires_browse(&self) -> bool {
        self.filters.iter().any(|f| {
            f.column == "tag_name" && matches!(f.operator, FilterOperator::Like)
        })
    }

    pub fn get_like_patterns(&self) -> Vec<String> {
        self.filters
            .iter()
            .filter_map(|f| {
                if f.column == "tag_name" && matches!(f.operator, FilterOperator::Like) {
                    f.value.as_string().map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_name_filters(&self) -> Vec<String> {
        for filter in &self.filters {
            if filter.column == "tag_name" {
                match &filter.operator {
                    FilterOperator::Equal => {
                        if let Some(name) = filter.value.as_string() {
                            return vec![name.to_string()];
                        }
                    }
                    FilterOperator::In => {
                        if let Some(names) = filter.value.as_list() {
                            return names.clone();
                        }
                    }
                    FilterOperator::Like => {
                        if let Some(pattern) = filter.value.as_string() {
                            return vec![pattern.to_string()];
                        }
                    }
                    _ => {}
                }
            }
        }
        vec!["*".to_string()] // Default wildcard
    }

    pub fn get_object_type_filters(&self) -> Vec<String> {
        for filter in &self.filters {
            if filter.column == "object_type" {
                match &filter.operator {
                    FilterOperator::Equal => {
                        if let Some(object_type) = filter.value.as_string() {
                            return vec![object_type.to_string()];
                        }
                    }
                    FilterOperator::In => {
                        if let Some(object_types) = filter.value.as_list() {
                            return object_types.clone();
                        }
                    }
                    _ => {}
                }
            }
        }
        vec![]
    }

    pub fn get_language_filter(&self) -> Option<String> {
        for filter in &self.filters {
            if filter.column == "language" && matches!(filter.operator, FilterOperator::Equal) {
                return filter.value.as_string().map(|s| s.to_string());
            }
        }
        None
    }

    // Methods for LoggedAlarms virtual columns
    pub fn get_filter_string(&self) -> Option<String> {
        for filter in &self.filters {
            if filter.column == "filterString" && matches!(filter.operator, FilterOperator::Equal) {
                return filter.value.as_string().map(|s| s.to_string());
            }
        }
        None
    }

    pub fn get_system_names(&self) -> Vec<String> {
        for filter in &self.filters {
            if filter.column == "system_name" {
                match &filter.operator {
                    FilterOperator::Equal => {
                        if let Some(name) = filter.value.as_string() {
                            return vec![name.to_string()];
                        }
                    }
                    FilterOperator::In => {
                        if let Some(names) = filter.value.as_list() {
                            return names.clone();
                        }
                    }
                    _ => {}
                }
            }
        }
        vec![]
    }

    pub fn get_filter_language(&self) -> Option<String> {
        for filter in &self.filters {
            if filter.column == "filter_language" && matches!(filter.operator, FilterOperator::Equal) {
                return filter.value.as_string().map(|s| s.to_string());
            }
        }
        None
    }

    pub fn get_modification_time_filter(&self) -> Option<(Option<String>, Option<String>)> {
        let mut start_time = None;
        let mut end_time = None;

        for filter in &self.filters {
            if filter.column == "modification_time" {
                match &filter.operator {
                    FilterOperator::GreaterThan | FilterOperator::GreaterThanOrEqual => {
                        if let FilterValue::Timestamp(ts) = &filter.value {
                            start_time = Some(ts.clone());
                        }
                    }
                    FilterOperator::LessThan | FilterOperator::LessThanOrEqual => {
                        if let FilterValue::Timestamp(ts) = &filter.value {
                            end_time = Some(ts.clone());
                        }
                    }
                    FilterOperator::Between => {
                        if let FilterValue::Range(start, end) = &filter.value {
                            if let FilterValue::Timestamp(ts) = start.as_ref() {
                                start_time = Some(ts.clone());
                            }
                            if let FilterValue::Timestamp(ts) = end.as_ref() {
                                end_time = Some(ts.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        if start_time.is_some() || end_time.is_some() {
            Some((start_time, end_time))
        } else {
            None
        }
    }
}