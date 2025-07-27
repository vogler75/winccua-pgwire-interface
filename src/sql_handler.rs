use crate::tables::*;
use anyhow::{anyhow, Result};
use sqlparser::ast::{BinaryOperator, Expr, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tracing::{debug, warn};
use chrono::Local;

pub struct SqlHandler;

impl SqlHandler {
    pub fn parse_query(sql: &str) -> Result<QueryInfo> {
        debug!("Parsing SQL: {}", sql);

        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)?;

        if ast.len() != 1 {
            return Err(anyhow!("Expected exactly one SQL statement"));
        }

        let statement = &ast[0];
        match statement {
            Statement::Query(query) => Self::parse_select_query(query),
            _ => Err(anyhow!("Only SELECT statements are supported")),
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
}