use crate::tables::{ColumnFilter, FilterOperator, QueryInfo, VirtualTable};
use anyhow::Result;
use std::collections::HashMap;

pub fn handle_information_schema_query(query_info: &QueryInfo) -> Result<String> {
    match query_info.table {
        VirtualTable::InformationSchemaTables => generate_tables_response(query_info),
        VirtualTable::InformationSchemaColumns => generate_columns_response(query_info),
        _ => unreachable!(),
    }
}

fn generate_tables_response(query_info: &QueryInfo) -> Result<String> {
    let all_tables = vec![
        "tagvalues",
        "loggedtagvalues",
        "activealarms",
        "loggedalarms",
        "taglist",
    ];

    let mut rows: Vec<HashMap<String, String>> = all_tables
        .into_iter()
        .map(|table_name| {
            let mut row = HashMap::new();
            row.insert("table_catalog".to_string(), "winccua".to_string());
            row.insert("table_schema".to_string(), "public".to_string());
            row.insert("table_name".to_string(), table_name.to_string());
            row.insert("table_type".to_string(), "VIEW".to_string());
            row.insert("self_referencing_column_name".to_string(), "NULL".to_string());
            row.insert("reference_generation".to_string(), "NULL".to_string());
            row.insert("user_defined_type_catalog".to_string(), "NULL".to_string());
            row.insert("user_defined_type_schema".to_string(), "NULL".to_string());
            row.insert("user_defined_type_name".to_string(), "NULL".to_string());
            row.insert("is_insertable_into".to_string(), "NO".to_string());
            row.insert("is_typed".to_string(), "NO".to_string());
            row.insert("commit_action".to_string(), "NULL".to_string());
            row
        })
        .collect();

    // Apply filters
    rows.retain(|row| apply_filters(row, &query_info.filters));

    // Apply ordering
    if let Some(order_by) = &query_info.order_by {
        rows.sort_by(|a, b| {
            let a_val = a.get(&order_by.column).unwrap();
            let b_val = b.get(&order_by.column).unwrap();
            if order_by.ascending {
                a_val.cmp(b_val)
            } else {
                b_val.cmp(a_val)
            }
        });
    }

    // Format response
    let headers = query_info.columns.join(",");
    let mut csv_response = format!("{}\n", headers);
    for row in rows {
        let line = query_info
            .columns
            .iter()
            .map(|col| row.get(col).cloned().unwrap_or_else(|| "NULL".to_string()))
            .collect::<Vec<String>>()
            .join(",");
        csv_response.push_str(&format!("{}\n", line));
    }

    Ok(csv_response)
}

fn generate_columns_response(query_info: &QueryInfo) -> Result<String> {
    let table_columns = vec![
        ("tagvalues", vec!["tag_name", "timestamp", "numeric_value", "string_value", "quality"]),
        ("loggedtagvalues", vec!["tag_name", "timestamp", "numeric_value", "string_value", "quality"]),
        ("activealarms", vec!["name", "instance_id", "raise_time", "state", "priority"]),
        ("loggedalarms", vec!["name", "instance_id", "raise_time", "modification_time", "state", "priority"]),
        ("taglist", vec!["tag_name", "display_name", "object_type", "data_type"]),
    ];

    let mut rows: Vec<HashMap<String, String>> = Vec::new();
    for (table_name, columns) in table_columns {
        for (i, column_name) in columns.iter().enumerate() {
            let data_type = match *column_name {
                "timestamp" | "raise_time" | "modification_time" => "timestamp without time zone",
                "numeric_value" => "numeric",
                "instance_id" | "priority" => "integer",
                _ => "character varying",
            };
            let mut row = HashMap::new();
            row.insert("table_catalog".to_string(), "winccua".to_string());
            row.insert("table_schema".to_string(), "public".to_string());
            row.insert("table_name".to_string(), table_name.to_string());
            row.insert("column_name".to_string(), column_name.to_string());
            row.insert("ordinal_position".to_string(), (i + 1).to_string());
            row.insert("column_default".to_string(), "NULL".to_string());
            row.insert("is_nullable".to_string(), "YES".to_string());
            row.insert("data_type".to_string(), data_type.to_string());
            row.insert("character_maximum_length".to_string(), "NULL".to_string());
            row.insert("character_octet_length".to_string(), "NULL".to_string());
            row.insert("numeric_precision".to_string(), "NULL".to_string());
            row.insert("numeric_precision_radix".to_string(), "NULL".to_string());
            row.insert("numeric_scale".to_string(), "NULL".to_string());
            row.insert("datetime_precision".to_string(), "NULL".to_string());
            row.insert("interval_type".to_string(), "NULL".to_string());
            row.insert("interval_precision".to_string(), "NULL".to_string());
            row.insert("character_set_catalog".to_string(), "NULL".to_string());
            row.insert("character_set_schema".to_string(), "NULL".to_string());
            row.insert("character_set_name".to_string(), "NULL".to_string());
            row.insert("collation_catalog".to_string(), "NULL".to_string());
            row.insert("collation_schema".to_string(), "NULL".to_string());
            row.insert("collation_name".to_string(), "NULL".to_string());
            row.insert("domain_catalog".to_string(), "NULL".to_string());
            row.insert("domain_schema".to_string(), "NULL".to_string());
            row.insert("domain_name".to_string(), "NULL".to_string());
            row.insert("udt_catalog".to_string(), "winccua".to_string());
            row.insert("udt_schema".to_string(), "pg_catalog".to_string());
            row.insert("udt_name".to_string(), "varchar".to_string());
            row.insert("scope_catalog".to_string(), "NULL".to_string());
            row.insert("scope_schema".to_string(), "NULL".to_string());
            row.insert("scope_name".to_string(), "NULL".to_string());
            row.insert("maximum_cardinality".to_string(), "NULL".to_string());
            row.insert("dtd_identifier".to_string(), (i + 1).to_string());
            row.insert("is_self_referencing".to_string(), "NO".to_string());
            row.insert("is_identity".to_string(), "NO".to_string());
            row.insert("identity_generation".to_string(), "NULL".to_string());
            row.insert("identity_start".to_string(), "NULL".to_string());
            row.insert("identity_increment".to_string(), "NULL".to_string());
            row.insert("identity_maximum".to_string(), "NULL".to_string());
            row.insert("identity_minimum".to_string(), "NULL".to_string());
            row.insert("identity_cycle".to_string(), "NO".to_string());
            row.insert("is_generated".to_string(), "NEVER".to_string());
            row.insert("generation_expression".to_string(), "NULL".to_string());
            row.insert("is_updatable".to_string(), "NO".to_string());
            rows.push(row);
        }
    }

    // Apply filters
    rows.retain(|row| apply_filters(row, &query_info.filters));

    // Apply ordering
    if let Some(order_by) = &query_info.order_by {
        rows.sort_by(|a, b| {
            let a_val = a.get(&order_by.column).unwrap();
            let b_val = b.get(&order_by.column).unwrap();
            if order_by.ascending {
                a_val.cmp(b_val)
            } else {
                b_val.cmp(a_val)
            }
        });
    }

    // Format response
    let headers = query_info.columns.join(",");
    let mut csv_response = format!("{}\n", headers);
    for row in rows {
        let line = query_info
            .columns
            .iter()
            .map(|col| row.get(col).cloned().unwrap_or_else(|| "NULL".to_string()))
            .collect::<Vec<String>>()
            .join(",");
        csv_response.push_str(&format!("{}\n", line));
    }

    Ok(csv_response)
}

fn apply_filters(row: &HashMap<String, String>, filters: &[ColumnFilter]) -> bool {
    for filter in filters {
        if let Some(value) = row.get(&filter.column) {
            match filter.operator {
                FilterOperator::Equal => {
                    if let Some(filter_value) = filter.value.as_string() {
                        if value != filter_value {
                            return false;
                        }
                    } else {
                        // Type mismatch, filter fails
                        return false;
                    }
                }
                _ => {
                    // For now, only support simple equality filters
                }
            }
        }
    }
    true
}