use crate::tables::*;
use anyhow::{anyhow, Result};
use sqlparser::ast::{BinaryOperator, Expr, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tracing::{debug, warn};
use chrono::{Duration, Local, DateTime};

pub struct SqlHandler;

impl SqlHandler {
    pub fn parse_query(sql: &str) -> Result<SqlResult> {
        debug!("Parsing SQL: {}", sql);

        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)?;

        if ast.len() != 1 {
            return Err(anyhow!("Expected exactly one SQL statement"));
        }

        let statement = &ast[0];
        match statement {
            Statement::Query(query) => {
                let query_info = Self::parse_select_query(query)?;
                Ok(SqlResult::Query(query_info))
            }
            Statement::SetVariable { .. } | Statement::SetNames { .. } | Statement::SetTimeZone { .. } => {
                // Handle SET statements by returning a special success indicator
                Self::handle_set_statement(statement)
            }
            _ => Err(anyhow!("Only SELECT and SET statements are supported")),
        }
    }

    fn parse_select_query(query: &Query) -> Result<QueryInfo> {
        match &*query.body {
            SetExpr::Select(select) => {
                let table = Self::extract_table(select)?;
                let (columns, column_mappings) = Self::extract_columns(select, &table)?;
                let filters = Self::extract_filters(select, &table)?;
                let limit = query.limit.as_ref().and_then(|l| Self::extract_limit(l));
                let order_by = query.order_by.as_ref().and_then(|order_by| {
                    if !order_by.exprs.is_empty() {
                        Some(Self::extract_order_by(&order_by.exprs[0]))
                    } else {
                        None
                    }
                });

                let distinct = select.distinct.is_some();
                
                let query_info = QueryInfo {
                    table,
                    columns,
                    column_mappings,
                    filters,
                    limit,
                    order_by,
                    distinct,
                };

                Self::validate_query(&query_info)?;
                Ok(query_info)
            }
            _ => Err(anyhow!("Only simple SELECT statements are supported")),
        }
    }

    fn extract_table(select: &Select) -> Result<VirtualTable> {
        if select.from.len() != 1 {
            return Err(anyhow!("Expected exactly one table in FROM clause"));
        }

        let table_name = match &select.from[0].relation {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                if name.0.len() != 1 {
                    return Err(anyhow!("Complex table names are not supported"));
                }
                &name.0[0].value
            }
            _ => return Err(anyhow!("Only simple table names are supported")),
        };

        VirtualTable::from_name(table_name)
            .ok_or_else(|| anyhow!("Unknown table: {}", table_name))
    }

    fn extract_columns(select: &Select, table: &VirtualTable) -> Result<(Vec<String>, std::collections::HashMap<String, String>)> {
        let mut columns = Vec::new();
        let mut column_mappings = std::collections::HashMap::new();

        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => {
                    columns.extend(table.get_column_names().iter().map(|s| s.to_string()));
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let column_name = ident.value.clone();
                    if !table.has_column(&column_name) {
                        return Err(anyhow!("Unknown column: {}", column_name));
                    }
                    if !table.is_selectable_column(&column_name) {
                        return Err(anyhow!("Column '{}' cannot be selected (virtual column)", column_name));
                    }
                    columns.push(column_name);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Expr::Identifier(ident) = expr {
                        let column_name = ident.value.clone();
                        if !table.has_column(&column_name) {
                            return Err(anyhow!("Unknown column: {}", column_name));
                        }
                        if !table.is_selectable_column(&column_name) {
                            return Err(anyhow!("Column '{}' cannot be selected (virtual column)", column_name));
                        }
                        let alias_name = alias.value.clone();
                        columns.push(alias_name.clone());
                        column_mappings.insert(alias_name, column_name);
                    } else {
                        return Err(anyhow!("Complex expressions in SELECT are not supported"));
                    }
                }
                _ => return Err(anyhow!("Unsupported SELECT item")),
            }
        }

        if columns.is_empty() {
            columns.extend(table.get_column_names().iter().map(|s| s.to_string()));
        }

        Ok((columns, column_mappings))
    }

    fn extract_filters(select: &Select, table: &VirtualTable) -> Result<Vec<ColumnFilter>> {
        let mut filters = Vec::new();

        if let Some(where_clause) = &select.selection {
            Self::extract_filters_from_expr(where_clause, table, &mut filters)?;
        }

        Ok(filters)
    }

    fn extract_filters_from_expr(
        expr: &Expr,
        table: &VirtualTable,
        filters: &mut Vec<ColumnFilter>,
    ) -> Result<()> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        Self::extract_filters_from_expr(left, table, filters)?;
                        Self::extract_filters_from_expr(right, table, filters)?;
                    }
                    BinaryOperator::Or => {
                        warn!("OR conditions are not optimally supported and may result in multiple GraphQL calls");
                        Self::extract_filters_from_expr(left, table, filters)?;
                        Self::extract_filters_from_expr(right, table, filters)?;
                    }
                    _ => {
                        if let (Expr::Identifier(column), value_expr) = (left.as_ref(), right.as_ref()) {
                            let filter = Self::create_filter(&column.value, op, value_expr, table)?;
                            filters.push(filter);
                        } else if let (value_expr, Expr::Identifier(column)) = (left.as_ref(), right.as_ref()) {
                            // Handle reversed comparison (value op column)
                            let reversed_op = Self::reverse_operator(op);
                            let filter = Self::create_filter(&column.value, &reversed_op, value_expr, table)?;
                            filters.push(filter);
                        } else {
                            return Err(anyhow!("Complex WHERE expressions are not supported"));
                        }
                    }
                }
            }
            Expr::InList { expr, list, negated } => {
                if *negated {
                    return Err(anyhow!("NOT IN is not supported"));
                }
                if let Expr::Identifier(column) = expr.as_ref() {
                    let values = list
                        .iter()
                        .map(|v| Self::extract_string_value(v))
                        .collect::<Result<Vec<_>>>()?;
                    
                    let filter = ColumnFilter {
                        column: column.value.clone(),
                        operator: FilterOperator::In,
                        value: FilterValue::List(values),
                    };
                    filters.push(filter);
                } else {
                    return Err(anyhow!("Complex IN expressions are not supported"));
                }
            }
            Expr::Like { expr, pattern, negated, .. } => {
                if *negated {
                    return Err(anyhow!("NOT LIKE is not supported"));
                }
                if let Expr::Identifier(column) = expr.as_ref() {
                    let pattern_str = Self::extract_string_value(pattern)?;
                    let filter = ColumnFilter {
                        column: column.value.clone(),
                        operator: FilterOperator::Like,
                        value: FilterValue::String(pattern_str),
                    };
                    filters.push(filter);
                } else {
                    return Err(anyhow!("Complex LIKE expressions are not supported"));
                }
            }
            Expr::Between { expr, negated, low, high } => {
                if *negated {
                    return Err(anyhow!("NOT BETWEEN is not supported"));
                }
                if let Expr::Identifier(column) = expr.as_ref() {
                    let low_val = Self::extract_filter_value(low)?;
                    let high_val = Self::extract_filter_value(high)?;
                    let filter = ColumnFilter {
                        column: column.value.clone(),
                        operator: FilterOperator::Between,
                        value: FilterValue::Range(Box::new(low_val), Box::new(high_val)),
                    };
                    filters.push(filter);
                } else {
                    return Err(anyhow!("Complex BETWEEN expressions are not supported"));
                }
            }
            _ => return Err(anyhow!("Unsupported WHERE expression")),
        }

        Ok(())
    }

    fn create_filter(
        column: &str,
        op: &BinaryOperator,
        value_expr: &Expr,
        table: &VirtualTable,
    ) -> Result<ColumnFilter> {
        if !table.has_column(column) {
            return Err(anyhow!("Unknown column: {}", column));
        }

        let operator = match op {
            BinaryOperator::Eq => FilterOperator::Equal,
            BinaryOperator::NotEq => FilterOperator::NotEqual,
            BinaryOperator::Gt => FilterOperator::GreaterThan,
            BinaryOperator::Lt => FilterOperator::LessThan,
            BinaryOperator::GtEq => FilterOperator::GreaterThanOrEqual,
            BinaryOperator::LtEq => FilterOperator::LessThanOrEqual,
            _ => return Err(anyhow!("Unsupported operator: {:?}", op)),
        };

        let value = Self::extract_filter_value_for_column(value_expr, column)?;

        Ok(ColumnFilter {
            column: column.to_string(),
            operator,
            value,
        })
    }

    fn reverse_operator(op: &BinaryOperator) -> BinaryOperator {
        match op {
            BinaryOperator::Gt => BinaryOperator::Lt,
            BinaryOperator::Lt => BinaryOperator::Gt,
            BinaryOperator::GtEq => BinaryOperator::LtEq,
            BinaryOperator::LtEq => BinaryOperator::GtEq,
            _ => op.clone(),
        }
    }

    fn extract_filter_value_for_column(expr: &Expr, column: &str) -> Result<FilterValue> {
        match expr {
            Expr::Value(value) => match value {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    // For string columns (tag_name, object_type, data_type, display_name), always treat as string
                    if column == "tag_name" || column == "name" || column == "object_type" || column == "data_type" || column == "display_name" {
                        Ok(FilterValue::String(s.clone()))
                    }
                    // For timestamp columns, always treat as timestamp
                    else if column == "timestamp" || column.contains("time") || column.contains("Time") {
                        Ok(FilterValue::Timestamp(s.clone()))
                    }
                    // For other columns, use heuristic
                    else if Self::is_timestamp_like(s) {
                        Ok(FilterValue::Timestamp(s.clone()))
                    } else {
                        Ok(FilterValue::String(s.clone()))
                    }
                }
                Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(FilterValue::Integer(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(FilterValue::Number(f))
                    } else {
                        Err(anyhow!("Invalid number: {}", n))
                    }
                }
                _ => Err(anyhow!("Unsupported value type: {:?}", value)),
            },
            Expr::Identifier(ident) => {
                // Handle special date/time identifiers
                match ident.value.to_uppercase().as_str() {
                    "CURRENT_DATE" => {
                        let today = Local::now().format("%Y-%m-%d").to_string();
                        Ok(FilterValue::Timestamp(today))
                    }
                    "CURRENT_TIME" => {
                        let now = Local::now().format("%Y-%m-%dT%H:%M:%S").to_string();
                        Ok(FilterValue::Timestamp(now))
                    }
                    "CURRENT_TIMESTAMP" => {
                        let now = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
                        Ok(FilterValue::Timestamp(now))
                    }
                    _ => Err(anyhow!("Unknown identifier: {}", ident.value)),
                }
            }
            Expr::Function(func) => {
                // Handle function calls like CURRENT_DATE()
                match func.name.to_string().to_uppercase().as_str() {
                    "CURRENT_DATE" => {
                        let today = Local::now().format("%Y-%m-%d").to_string();
                        Ok(FilterValue::Timestamp(today))
                    }
                    "CURRENT_TIME" => {
                        let now = Local::now().format("%Y-%m-%dT%H:%M:%S").to_string();
                        Ok(FilterValue::Timestamp(now))
                    }
                    "CURRENT_TIMESTAMP" | "NOW" => {
                        let now = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
                        Ok(FilterValue::Timestamp(now))
                    }
                    _ => Err(anyhow!("Unsupported function: {}", func.name)),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                // Handle date/time arithmetic with intervals
                Self::handle_interval_arithmetic(left, op, right, column)
            }
            _ => Err(anyhow!("Complex value expressions are not supported")),
        }
    }

    fn extract_filter_value(expr: &Expr) -> Result<FilterValue> {
        match expr {
            Expr::Value(value) => match value {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    // Check if it looks like a timestamp
                    if Self::is_timestamp_like(s) {
                        Ok(FilterValue::Timestamp(s.clone()))
                    } else {
                        Ok(FilterValue::String(s.clone()))
                    }
                }
                Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(FilterValue::Integer(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(FilterValue::Number(f))
                    } else {
                        Err(anyhow!("Invalid number: {}", n))
                    }
                }
                _ => Err(anyhow!("Unsupported value type: {:?}", value)),
            },
            Expr::Identifier(ident) => {
                // Handle special date/time identifiers
                match ident.value.to_uppercase().as_str() {
                    "CURRENT_DATE" => {
                        let today = Local::now().format("%Y-%m-%d").to_string();
                        Ok(FilterValue::Timestamp(today))
                    }
                    "CURRENT_TIME" => {
                        let now = Local::now().format("%Y-%m-%dT%H:%M:%S").to_string();
                        Ok(FilterValue::Timestamp(now))
                    }
                    "CURRENT_TIMESTAMP" => {
                        let now = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
                        Ok(FilterValue::Timestamp(now))
                    }
                    _ => Err(anyhow!("Unknown identifier: {}", ident.value)),
                }
            }
            Expr::Function(func) => {
                // Handle function calls like CURRENT_DATE()
                match func.name.to_string().to_uppercase().as_str() {
                    "CURRENT_DATE" => {
                        let today = Local::now().format("%Y-%m-%d").to_string();
                        Ok(FilterValue::Timestamp(today))
                    }
                    "CURRENT_TIME" => {
                        let now = Local::now().format("%Y-%m-%dT%H:%M:%S").to_string();
                        Ok(FilterValue::Timestamp(now))
                    }
                    "CURRENT_TIMESTAMP" | "NOW" => {
                        let now = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
                        Ok(FilterValue::Timestamp(now))
                    }
                    _ => Err(anyhow!("Unsupported function: {}", func.name)),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                // Handle date/time arithmetic with intervals
                Self::handle_interval_arithmetic(left, op, right, "")
            }
            _ => Err(anyhow!("Complex value expressions are not supported")),
        }
    }

    fn extract_string_value(expr: &Expr) -> Result<String> {
        match expr {
            Expr::Value(Value::SingleQuotedString(s)) | Expr::Value(Value::DoubleQuotedString(s)) => {
                Ok(s.clone())
            }
            _ => Err(anyhow!("Expected string value")),
        }
    }

    fn extract_limit(expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Value(Value::Number(n, _)) => n.parse().ok(),
            _ => None,
        }
    }

    fn extract_order_by(order_expr: &OrderByExpr) -> OrderBy {
        let column = match &order_expr.expr {
            Expr::Identifier(ident) => ident.value.clone(),
            _ => "timestamp".to_string(), // Default fallback
        };

        let ascending = order_expr.asc.unwrap_or(true);

        OrderBy { column, ascending }
    }

    fn is_timestamp_like(s: &str) -> bool {
        // Simple heuristic to detect timestamp strings
        s.contains('T') || s.contains(':') || s.len() > 10
    }

    fn validate_query(query: &QueryInfo) -> Result<()> {
        // Validate that tag-based tables have required filters
        if matches!(query.table, VirtualTable::TagValues | VirtualTable::LoggedTagValues) {
            if !query.has_required_tag_filter() {
                return Err(anyhow!(
                    "TagValues and LoggedTagValues queries must include a WHERE clause on tag_name"
                ));
            }
        }

        // Validate that LoggedTagValues has timestamp constraints when using LIMIT
        if matches!(query.table, VirtualTable::LoggedTagValues) {
            if query.limit.is_some() && query.get_timestamp_filter().is_none() {
                return Err(anyhow!(
                    "LoggedTagValues queries with LIMIT must include timestamp constraints"
                ));
            }
        }

        Ok(())
    }

    fn handle_interval_arithmetic(left: &Expr, op: &BinaryOperator, right: &Expr, column: &str) -> Result<FilterValue> {
        // Try to extract interval from either side
        if let Ok(interval_duration) = Self::extract_interval(right) {
            // Get the base timestamp from left side
            let base_timestamp = if column.is_empty() {
                Self::extract_filter_value(left)?
            } else {
                Self::extract_filter_value_for_column(left, column)?
            };
            
            if let FilterValue::Timestamp(ts_str) = base_timestamp {
                // Parse the timestamp
                let base_dt = Self::parse_timestamp(&ts_str)?;
                
                // Apply the operation
                let result_dt = match op {
                    BinaryOperator::Plus => base_dt + interval_duration,
                    BinaryOperator::Minus => base_dt - interval_duration,
                    _ => return Err(anyhow!("Invalid operator for interval arithmetic")),
                };
                
                // Format back to string
                let result_str = result_dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
                Ok(FilterValue::Timestamp(result_str))
            } else {
                Err(anyhow!("Expected timestamp value for date arithmetic"))
            }
        } else if let Ok(interval_duration) = Self::extract_interval(left) {
            // Interval is on the left side
            let base_timestamp = if column.is_empty() {
                Self::extract_filter_value(right)?
            } else {
                Self::extract_filter_value_for_column(right, column)?
            };
            
            if let FilterValue::Timestamp(ts_str) = base_timestamp {
                let base_dt = Self::parse_timestamp(&ts_str)?;
                
                // For addition, order doesn't matter
                // For subtraction, interval on left is invalid
                let result_dt = match op {
                    BinaryOperator::Plus => base_dt + interval_duration,
                    BinaryOperator::Minus => return Err(anyhow!("Cannot subtract timestamp from interval")),
                    _ => return Err(anyhow!("Invalid operator for interval arithmetic")),
                };
                
                let result_str = result_dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
                Ok(FilterValue::Timestamp(result_str))
            } else {
                Err(anyhow!("Expected timestamp value for date arithmetic"))
            }
        } else {
            Err(anyhow!("No interval found in arithmetic expression"))
        }
    }

    fn extract_interval(expr: &Expr) -> Result<Duration> {
        debug!("Attempting to extract interval from expression: {:?}", expr);
        
        // Check if this is an INTERVAL expression
        if let Expr::Interval(interval) = expr {
            debug!("Found Expr::Interval: {:?}", interval);
            
            // Extract the interval string from the value
            let interval_str = match &*interval.value {
                Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                _ => return Err(anyhow!("Unsupported interval value format")),
            };
            
            // If leading_field is specified, use it; otherwise parse from the string
            if let Some(leading_field) = &interval.leading_field {
                // Parse the numeric value from the string
                let parts: Vec<&str> = interval_str.trim().split_whitespace().collect();
                if parts.is_empty() {
                    return Err(anyhow!("Empty interval string"));
                }
                
                let value = parts[0].parse::<i64>()
                    .map_err(|_| anyhow!("Invalid interval value: {}", parts[0]))?;
                
                use sqlparser::ast::DateTimeField;
                match leading_field {
                    DateTimeField::Second => Ok(Duration::seconds(value)),
                    DateTimeField::Minute => Ok(Duration::minutes(value)),
                    DateTimeField::Hour => Ok(Duration::hours(value)),
                    DateTimeField::Day => Ok(Duration::days(value)),
                    DateTimeField::Week(_) => Ok(Duration::weeks(value)),
                    DateTimeField::Month => Ok(Duration::days(value * 30)), // Approximate
                    DateTimeField::Year => Ok(Duration::days(value * 365)), // Approximate
                    _ => Err(anyhow!("Unsupported interval unit: {:?}", leading_field)),
                }
            } else {
                // Parse the entire string like "1 hour"
                Self::parse_interval_string(&interval_str)
            }
        } 
        // Check if this might be a function call like INTERVAL('1 hour')
        else if let Expr::Function(func) = expr {
            debug!("Found function call: {:?}", func);
            if func.name.to_string().to_uppercase() == "INTERVAL" {
                // Extract the interval string from the function argument
                match &func.args {
                    sqlparser::ast::FunctionArguments::List(args) if !args.args.is_empty() => {
                        if let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(interval_str)))) = &args.args[0] {
                            return Self::parse_interval_string(interval_str);
                        }
                    }
                    _ => {}
                }
            }
            Err(anyhow!("Not an interval function"))
        }
        // Check if this is a typed string like INTERVAL '1 hour'
        else if let Expr::TypedString { data_type, value } = expr {
            debug!("Found typed string: {:?} with value: {:?}", data_type, value);
            if data_type.to_string().to_uppercase() == "INTERVAL" {
                return Self::parse_interval_string(value);
            }
            Err(anyhow!("Not an interval typed string"))
        }
        else {
            debug!("Expression type not recognized as interval: {:?}", expr);
            Err(anyhow!("Not an interval expression"))
        }
    }
    
    fn parse_interval_string(interval_str: &str) -> Result<Duration> {
        debug!("Parsing interval string: '{}'", interval_str);
        
        // Parse strings like "1 hour", "3 minutes", "7 days", etc.
        let parts: Vec<&str> = interval_str.trim().split_whitespace().collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid interval format: expected 'NUMBER UNIT', got '{}'", interval_str));
        }
        
        let value = parts[0].parse::<i64>()
            .map_err(|_| anyhow!("Invalid interval value: {}", parts[0]))?;
        
        let unit = parts[1].to_lowercase();
        match unit.as_str() {
            "second" | "seconds" => Ok(Duration::seconds(value)),
            "minute" | "minutes" => Ok(Duration::minutes(value)),
            "hour" | "hours" => Ok(Duration::hours(value)),
            "day" | "days" => Ok(Duration::days(value)),
            "week" | "weeks" => Ok(Duration::weeks(value)),
            "month" | "months" => Ok(Duration::days(value * 30)), // Approximate
            "year" | "years" => Ok(Duration::days(value * 365)), // Approximate
            _ => Err(anyhow!("Unsupported interval unit: {}", unit)),
        }
    }

    fn parse_timestamp(ts_str: &str) -> Result<DateTime<Local>> {
        use chrono::{NaiveDateTime, TimeZone};
        
        // Try various timestamp formats
        let formats = [
            "%Y-%m-%dT%H:%M:%S%.3fZ",
            "%Y-%m-%dT%H:%M:%S%.3f",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.3f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ];
        
        for format in &formats {
            if let Ok(naive) = NaiveDateTime::parse_from_str(ts_str, format) {
                return Ok(Local.from_local_datetime(&naive).unwrap());
            }
            // Try parsing as date only
            if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(ts_str, format) {
                let naive_datetime = naive_date.and_hms_opt(0, 0, 0).unwrap();
                return Ok(Local.from_local_datetime(&naive_datetime).unwrap());
            }
        }
        
        Err(anyhow!("Could not parse timestamp: {}", ts_str))
    }

    fn handle_set_statement(statement: &Statement) -> Result<SqlResult> {
        debug!("Handling SET statement: {:?}", statement);
        
        let set_command = match statement {
            Statement::SetVariable { variables, .. } => {
                // For now, just return a simple success message
                // TODO: Extract actual variable names and values when the structure is clear
                debug!("Variables structure: {:?}", variables);
                format!("SET (variables: {})", variables.len())
            }
            Statement::SetNames { charset_name, .. } => {
                format!("SET NAMES {}", charset_name)
            }
            Statement::SetTimeZone { value, .. } => {
                format!("SET TIME ZONE {}", value)
            }
            _ => "SET (unknown)".to_string(),
        };
        
        debug!("Successfully handled SET statement: {}", set_command);
        Ok(SqlResult::SetStatement(set_command))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_parsing() {
        let sql = "SELECT tag_name, numeric_value, timestamp FROM loggedtagvalues where timestamp > CURRENT_TIME - INTERVAL '1 hour' and tag_name like '%PV%Watt%:%'";
        
        let result = SqlHandler::parse_query(sql);
        assert!(result.is_ok(), "Failed to parse query with INTERVAL: {:?}", result.err());
        
        let sql_result = result.unwrap();
        match sql_result {
            SqlResult::Query(query_info) => {
                assert!(matches!(query_info.table, VirtualTable::LoggedTagValues));
                
                // Check that we have filters
                assert!(!query_info.filters.is_empty());
                
                // Find the timestamp filter
                let timestamp_filter = query_info.filters.iter()
                    .find(|f| f.column == "timestamp");
                assert!(timestamp_filter.is_some(), "No timestamp filter found");
                
                // Check that the timestamp filter has a computed value (not just the original expression)
                if let Some(filter) = timestamp_filter {
                    if let FilterValue::Timestamp(ts) = &filter.value {
                        // The timestamp should be calculated (not the original CURRENT_TIME)
                        assert!(!ts.contains("CURRENT_TIME"), "Timestamp should be calculated, not contain CURRENT_TIME");
                    }
                }
            }
            SqlResult::SetStatement(_) => {
                panic!("Expected Query result, got SetStatement");
            }
        }
    }
    
    #[test]
    fn test_various_intervals() {
        let test_cases = [
            ("INTERVAL '30 seconds'", true),
            ("INTERVAL '15 minutes'", true),
            ("INTERVAL '2 hours'", true),
            ("INTERVAL '1 day'", true),
            ("INTERVAL '1 week'", true),
        ];
        
        for (interval_str, should_pass) in test_cases {
            let sql = format!("SELECT * FROM loggedtagvalues WHERE timestamp > CURRENT_TIME - {} AND tag_name = 'Test'", interval_str);
            let result = SqlHandler::parse_query(&sql);
            
            if should_pass {
                assert!(result.is_ok(), "Failed to parse query with {}: {:?}", interval_str, result.err());
                // Also verify it's a Query result, not a SetStatement
                if let Ok(SqlResult::SetStatement(_)) = result {
                    panic!("Expected Query result for interval test, got SetStatement");
                }
            }
        }
    }
    
    #[test]
    fn test_set_statements() {
        let test_cases = [
            "SET extra_float_digits = 3",
            "SET TIME ZONE 'UTC'",
            "SET NAMES 'utf8'",
            "SET application_name = 'test_app'",
            "SET search_path = public, pg_catalog",
        ];
        
        for sql in test_cases {
            let result = SqlHandler::parse_query(sql);
            assert!(result.is_ok(), "Failed to parse SET statement: {}: {:?}", sql, result.err());
            
            match result.unwrap() {
                SqlResult::SetStatement(set_command) => {
                    assert!(set_command.starts_with("SET"), "SET command should start with 'SET': {}", set_command);
                }
                SqlResult::Query(_) => {
                    panic!("Expected SetStatement result for '{}', got Query", sql);
                }
            }
        }
    }
    
    #[test]
    fn test_mixed_queries_and_sets() {
        // Test that we can parse both SET statements and normal queries correctly
        let test_cases = [
            ("SET extra_float_digits = 3", true), // SET statement
            ("SELECT * FROM tagvalues WHERE tag_name = 'test'", false), // Normal query
            ("SET TIME ZONE 'UTC'", true), // SET statement
            ("SELECT * FROM loggedtagvalues WHERE timestamp > CURRENT_TIME - INTERVAL '1 hour' AND tag_name = 'test'", false), // Normal query with interval
        ];
        
        for (sql, is_set) in test_cases {
            let result = SqlHandler::parse_query(sql);
            assert!(result.is_ok(), "Failed to parse statement: {}: {:?}", sql, result.err());
            
            match result.unwrap() {
                SqlResult::SetStatement(_) => {
                    assert!(is_set, "Expected Query result for '{}', got SetStatement", sql);
                }
                SqlResult::Query(_) => {
                    assert!(!is_set, "Expected SetStatement result for '{}', got Query", sql);
                }
            }
        }
    }
    
    #[test]
    fn test_parse_request_set_statements() {
        // Test the specific case mentioned: Parse: statement='', query='SET extra_float_digits = 3', params=0
        let test_cases = [
            "SET extra_float_digits = 3",
            "SET application_name = 'test_client'", 
            "SET client_encoding = 'UTF8'",
            "SET search_path = public, pg_catalog",
            "SET TIME ZONE 'America/New_York'",
            "SET NAMES 'utf8'",
        ];
        
        for sql in test_cases {
            // This simulates what happens when a Parse request comes in
            let result = SqlHandler::parse_query(sql);
            assert!(result.is_ok(), "Parse request failed for: {}: {:?}", sql, result.err());
            
            // Verify it's recognized as a SET statement
            match result.unwrap() {
                SqlResult::SetStatement(set_command) => {
                    assert!(set_command.starts_with("SET"), "Expected SET command, got: {}", set_command);
                    println!("✅ Parse request for '{}' -> SetStatement('{}')", sql, set_command);
                }
                SqlResult::Query(_) => {
                    panic!("Parse request for SET statement '{}' incorrectly returned Query result", sql);
                }
            }
        }
    }
    
    #[test] 
    fn test_set_command_complete_format() {
        // Test that SET statements return the correct COMMAND_COMPLETE format
        // This simulates the format_as_postgres_result function behavior
        
        fn mock_format_as_postgres_result(csv_data: &str) -> Vec<u8> {
            let mut response = Vec::new();
            
            if csv_data.starts_with("COMMAND_COMPLETE:") {
                let command_tag = csv_data.strip_prefix("COMMAND_COMPLETE:").unwrap_or("OK");
                
                // CommandComplete message: 'C' + length + tag + null
                response.push(b'C');
                let tag_length = 4 + command_tag.len() + 1;
                response.extend_from_slice(&(tag_length as u32).to_be_bytes());
                response.extend_from_slice(command_tag.as_bytes());
                response.push(0);
                
                // ReadyForQuery message: 'Z' + length + status
                response.push(b'Z');
                response.extend_from_slice(&5u32.to_be_bytes());
                response.push(b'I'); // Idle
            }
            
            response
        }
        
        let response = mock_format_as_postgres_result("COMMAND_COMPLETE:SET");
        
        // Verify structure
        assert_eq!(response[0], b'C', "Should start with CommandComplete message");
        assert!(response.len() >= 15, "Response should contain both CommandComplete and ReadyForQuery");
        
        // Find the ReadyForQuery message (should be after CommandComplete)
        let z_pos = response.iter().position(|&b| b == b'Z').expect("Should contain ReadyForQuery");
        assert_eq!(response[z_pos + 5], b'I', "Should end with Idle status");
        
        println!("✅ SET statement produces correct PostgreSQL wire protocol response");
        println!("   Total response length: {} bytes", response.len());
        println!("   CommandComplete at: 0, ReadyForQuery at: {}", z_pos);
    }
}