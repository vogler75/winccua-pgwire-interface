use anyhow::Result;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::logical_expr::{ScalarUDF, Signature, Volatility, ScalarUDFImpl, ScalarFunctionArgs, ColumnarValue};
use arrow::datatypes::DataType;
use std::time::Instant;
use tracing::debug;
use crate::catalog;
use std::sync::Arc;

/// Implementation of pg_get_userbyid function
#[derive(Debug)]
struct PgGetUserByIdUDF {
    signature: Signature,
}

impl PgGetUserByIdUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for PgGetUserByIdUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "pg_catalog_pg_get_userbyid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::{Array, Int32Array, Int64Array, StringArray};
        use arrow::compute::kernels::cast;
        
        let args = args.args;
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal("pg_get_userbyid expects 1 argument".to_string()));
        }
        
        let user_oids = args[0].clone().into_array(1)?;
        let mut results = Vec::new();
        
        // Handle both Int32 and Int64 inputs
        match user_oids.data_type() {
            DataType::Int32 => {
                let oid_array = user_oids.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        let oid = oid_array.value(i);
                        let username = match oid {
                            10 => "postgres",
                            0 => "unknown",
                            1 => "template1",
                            _ => "user",
                        };
                        results.push(Some(username.to_string()));
                    }
                }
            }
            DataType::Int64 => {
                let oid_array = user_oids.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        let oid = oid_array.value(i) as i32;
                        let username = match oid {
                            10 => "postgres",
                            0 => "unknown", 
                            1 => "template1",
                            _ => "user",
                        };
                        results.push(Some(username.to_string()));
                    }
                }
            }
            _ => {
                // Try to cast to Int32
                let casted = cast::cast(user_oids.as_ref(), &DataType::Int32)?;
                let casted_i32 = casted.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..casted_i32.len() {
                    if casted_i32.is_null(i) {
                        results.push(None);
                    } else {
                        let oid = casted_i32.value(i);
                        let username = match oid {
                            10 => "postgres",
                            0 => "unknown",
                            1 => "template1",
                            _ => "user",
                        };
                        results.push(Some(username.to_string()));
                    }
                }
            }
        }
        
        let result_array = StringArray::from(results);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

/// Implementation of pg_get_function_identity_arguments function
#[derive(Debug)]
struct PgGetFunctionIdentityArgumentsUDF {
    signature: Signature,
}

impl PgGetFunctionIdentityArgumentsUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for PgGetFunctionIdentityArgumentsUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "pg_get_function_identity_arguments"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::{Array, Int32Array, Int64Array, StringArray};
        use arrow::compute::kernels::cast;
        
        let args = args.args;
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal("pg_get_function_identity_arguments expects 1 argument".to_string()));
        }
        
        let function_oids = args[0].clone().into_array(1)?;
        let mut results = Vec::new();
        
        // Handle both Int32 and Int64 inputs
        match function_oids.data_type() {
            DataType::Int32 => {
                let oid_array = function_oids.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        // For now, return empty string for all function OIDs
                        results.push(Some("".to_string()));
                    }
                }
            }
            DataType::Int64 => {
                let oid_array = function_oids.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        // For now, return empty string for all function OIDs
                        results.push(Some("".to_string()));
                    }
                }
            }
            _ => {
                // Try to cast to Int32
                let casted = cast::cast(function_oids.as_ref(), &DataType::Int32)?;
                let casted_i32 = casted.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..casted_i32.len() {
                    if casted_i32.is_null(i) {
                        results.push(None);
                    } else {
                        // For now, return empty string for all function OIDs
                        results.push(Some("".to_string()));
                    }
                }
            }
        }
        
        let result_array = StringArray::from(results);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

/// Implementation of current_schema() function
#[derive(Debug)]
struct CurrentSchemaUDF {
    signature: Signature,
}

impl CurrentSchemaUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentSchemaUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_schema"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::StringArray;
        
        // Return "public" as the default schema
        let result = StringArray::from(vec!["public"]);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Implementation of version() function
#[derive(Debug)]
struct VersionUDF {
    signature: Signature,
}

impl VersionUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for VersionUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::StringArray;
        
        // Return a PostgreSQL-compatible version string
        let version = "PostgreSQL 15.0 (WinCC UA PostgreSQL Wire Protocol Server v1.0) on x86_64-pc-linux-gnu, compiled by DataFusion, 64-bit";
        let result = StringArray::from(vec![version]);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Implementation of current_database() function
#[derive(Debug)]
struct CurrentDatabaseUDF {
    signature: Signature,
}

impl CurrentDatabaseUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentDatabaseUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_database"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::StringArray;
        
        // Return the default database name
        let result = StringArray::from(vec!["postgres"]);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Implementation of format_type function
#[derive(Debug)]
struct FormatTypeUDF {
    signature: Signature,
}

impl FormatTypeUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for FormatTypeUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "format_type"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::{Array, Int32Array, Int64Array, StringArray};
        
        let args = args.args;
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Internal("format_type expects 2 arguments".to_string()));
        }
        
        let type_oids = args[0].clone().into_array(1)?;
        let _type_mods = args[1].clone().into_array(1)?; // Type modifier (ignored for now)
        
        let mut results = Vec::new();
        
        // Handle both Int32 and Int64 type OIDs
        match type_oids.data_type() {
            DataType::Int32 => {
                let oid_array = type_oids.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        let oid = oid_array.value(i);
                        let type_name = match oid {
                            16 => "boolean",
                            20 => "bigint", 
                            21 => "smallint",
                            23 => "integer",
                            25 => "text",
                            1043 => "character varying",
                            1114 => "timestamp without time zone",
                            1184 => "timestamp with time zone",
                            _ => "unknown",
                        };
                        results.push(Some(type_name.to_string()));
                    }
                }
            }
            DataType::Int64 => {
                let oid_array = type_oids.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..oid_array.len() {
                    if oid_array.is_null(i) {
                        results.push(None);
                    } else {
                        let oid = oid_array.value(i) as i32;
                        let type_name = match oid {
                            16 => "boolean",
                            20 => "bigint",
                            21 => "smallint", 
                            23 => "integer",
                            25 => "text",
                            1043 => "character varying",
                            1114 => "timestamp without time zone",
                            1184 => "timestamp with time zone",
                            _ => "unknown",
                        };
                        results.push(Some(type_name.to_string()));
                    }
                }
            }
            _ => {
                // Return unknown for other types
                for _ in 0..type_oids.len() {
                    results.push(Some("unknown".to_string()));
                }
            }
        }
        
        let result_array = StringArray::from(results);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

/// Implementation of nullif function
#[derive(Debug)]
struct NullIfUDF {
    signature: Signature,
}

impl NullIfUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for NullIfUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "nullif"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        // Return the type of the first argument
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use arrow::array::{Array, Int32Array, Int64Array};
        
        let args = args.args;
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Internal("nullif expects 2 arguments".to_string()));
        }
        
        let first_arg = args[0].clone().into_array(1)?;
        let second_arg = args[1].clone().into_array(1)?;
        
        // For simplicity, handle the case where both arguments are the same type
        match (first_arg.data_type(), second_arg.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let first_array = first_arg.as_any().downcast_ref::<Int32Array>().unwrap();
                let second_array = second_arg.as_any().downcast_ref::<Int32Array>().unwrap();
                
                let mut results = Vec::new();
                for i in 0..first_array.len() {
                    if first_array.is_null(i) {
                        results.push(None);
                    } else if second_array.is_null(i) {
                        results.push(Some(first_array.value(i)));
                    } else if first_array.value(i) == second_array.value(i) {
                        results.push(None); // Return NULL if equal
                    } else {
                        results.push(Some(first_array.value(i)));
                    }
                }
                let result_array = Int32Array::from(results);
                Ok(ColumnarValue::Array(Arc::new(result_array)))
            }
            (DataType::Int64, DataType::Int64) => {
                let first_array = first_arg.as_any().downcast_ref::<Int64Array>().unwrap();
                let second_array = second_arg.as_any().downcast_ref::<Int64Array>().unwrap();
                
                let mut results = Vec::new();
                for i in 0..first_array.len() {
                    if first_array.is_null(i) {
                        results.push(None);
                    } else if second_array.is_null(i) {
                        results.push(Some(first_array.value(i)));
                    } else if first_array.value(i) == second_array.value(i) {
                        results.push(None); // Return NULL if equal
                    } else {
                        results.push(Some(first_array.value(i)));
                    }
                }
                let result_array = Int64Array::from(results);
                Ok(ColumnarValue::Array(Arc::new(result_array)))
            }
            _ => {
                // For other types, just return the first argument (simplified)
                Ok(ColumnarValue::Array(first_arg))
            }
        }
    }
}

/// Register PostgreSQL system functions with DataFusion context
pub fn register_postgresql_functions(ctx: &SessionContext) -> Result<()> {
    ctx.register_udf(ScalarUDF::new_from_impl(PgGetUserByIdUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(PgGetFunctionIdentityArgumentsUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CurrentSchemaUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(VersionUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CurrentDatabaseUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(FormatTypeUDF::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(NullIfUDF::new()));
    Ok(())
}

/// Handle duplicate column names in SELECT projections by adding auto-generated aliases
fn handle_duplicate_column_names(sql: &str) -> String {
    use datafusion::sql::sqlparser::ast::Statement;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    
    // Try to parse the SQL - if it fails, return the original SQL
    let dialect = GenericDialect {};
    let ast = match Parser::parse_sql(&dialect, sql) {
        Ok(ast) if ast.len() == 1 => ast,
        _ => return sql.to_string(),
    };
    
    let statement = &ast[0];
    match statement {
        Statement::Query(query) => {
            if let Some(modified_query) = add_column_aliases_to_query(query) {
                modified_query.to_string()
            } else {
                sql.to_string()
            }
        }
        _ => sql.to_string(),
    }
}

/// Extract the base column name from an expression (e.g., "oid" from "o.oid")
fn get_base_column_name(expr: &datafusion::sql::sqlparser::ast::Expr) -> String {
    use datafusion::sql::sqlparser::ast::Expr;
    
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => {
            // For compound identifiers like "o.oid", return the last part
            parts.last().map(|p| p.value.clone()).unwrap_or_else(|| expr.to_string())
        }
        _ => expr.to_string(),
    }
}

/// Add aliases to duplicate column names in a query
fn add_column_aliases_to_query(query: &datafusion::sql::sqlparser::ast::Query) -> Option<datafusion::sql::sqlparser::ast::Query> {
    use datafusion::sql::sqlparser::ast::{SetExpr, SelectItem};
    use std::collections::HashMap;
    match &*query.body {
        SetExpr::Select(select) => {
            let mut column_counts: HashMap<String, i32> = HashMap::new();
            let mut modified_projection = Vec::new();
            let mut has_modifications = false;
            
            // Process each projection item
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        // Get the base column name for tracking duplicates
                        let base_name = get_base_column_name(expr);
                        let count = column_counts.entry(base_name.clone()).or_insert(0);
                        *count += 1;
                        
                        if *count > 1 {
                            // Create alias for duplicate column
                            let alias_name = format!("{}_{}", base_name, count);
                            let alias = datafusion::sql::sqlparser::ast::Ident::new(alias_name);
                            modified_projection.push(SelectItem::ExprWithAlias {
                                expr: expr.clone(),
                                alias,
                            });
                            has_modifications = true;
                        } else {
                            modified_projection.push(item.clone());
                        }
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        // Already has alias, just track the alias name
                        let alias_name = alias.value.clone();
                        let count = column_counts.entry(alias_name.clone()).or_insert(0);
                        *count += 1;
                        
                        if *count > 1 {
                            // Create new alias for duplicate alias name
                            let new_alias_name = format!("{}_{}", alias_name, count);
                            let new_alias = datafusion::sql::sqlparser::ast::Ident::new(new_alias_name);
                            modified_projection.push(SelectItem::ExprWithAlias {
                                expr: expr.clone(),
                                alias: new_alias,
                            });
                            has_modifications = true;
                        } else {
                            modified_projection.push(item.clone());
                        }
                    }
                    _ => {
                        // Wildcards and other items - keep as is
                        modified_projection.push(item.clone());
                    }
                }
            }
            
            if has_modifications {
                let mut modified_select = select.as_ref().clone();
                modified_select.projection = modified_projection;
                
                let mut modified_query = query.clone();
                modified_query.body = Box::new(SetExpr::Select(Box::new(modified_select)));
                
                Some(modified_query)
            } else {
                None
            }
        }
        _ => None
    }
}

/// Replace schema-qualified table names with underscore versions and handle PostgreSQL type casts
pub fn normalize_schema_qualified_tables(sql: &str) -> String {
    normalize_schema_qualified_tables_with_username(sql, "postgres")
}

pub fn normalize_schema_qualified_tables_with_username(sql: &str, username: &str) -> String {
    use regex::Regex;
    
    // First, handle PostgreSQL type casts like 'value'::pg_catalog.regtype and 'value'::regclass
    // Pattern to match ::pg_catalog.type_name and remove the cast
    let cast_re = Regex::new(r"::pg_catalog\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
    let mut processed_sql = cast_re.replace_all(sql, "").to_string();
    
    // Also handle standalone PostgreSQL type casts like ::regclass, ::regtype, etc.
    let standalone_cast_re = Regex::new(r"::(regclass|regtype|regproc|regprocedure|regoper|regoperator|regconfig|regdictionary)\b").unwrap();
    processed_sql = standalone_cast_re.replace_all(&processed_sql, "").to_string();
    
    // Handle PostgreSQL special identifiers (session_user, current_user, user)
    // These are identifiers, not functions, so we need to replace them with literal values
    
    // Replace session_user with a string literal containing the actual username and proper column alias
    let session_user_re = Regex::new(r"\bsession_user\b").unwrap();
    processed_sql = session_user_re.replace_all(&processed_sql, &format!("'{}' AS session_user", username)).to_string();
    
    // Replace current_user with a string literal containing the actual username and proper column alias
    let current_user_re = Regex::new(r"\bcurrent_user\b").unwrap();
    processed_sql = current_user_re.replace_all(&processed_sql, &format!("'{}' AS current_user", username)).to_string();
    
    // Replace user with a string literal containing the actual username and proper column alias (user is equivalent to current_user)
    let user_re = Regex::new(r"\buser\b").unwrap();
    processed_sql = user_re.replace_all(&processed_sql, &format!("'{}' AS user", username)).to_string();
    
    // Fix current_schema() function to have proper column alias
    let current_schema_func_re = Regex::new(r"\bcurrent_schema\(\)").unwrap();
    processed_sql = current_schema_func_re.replace_all(&processed_sql, "current_schema() AS current_schema").to_string();
    
    // Pattern to match pg_catalog.table_name (case insensitive)
    let re = Regex::new(r"(?i)\bpg_catalog\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
    
    // Replace pg_catalog.table_name with pg_catalog_table_name
    processed_sql = re.replace_all(&processed_sql, "pg_catalog_$1").to_string();
    
    // Note: PostgreSQL returns duplicate columns when both explicit selection and wildcard include the same column
    // For example: "SELECT db.oid, db.*" returns oid twice - once for explicit selection, once from wildcard
    // We preserve this behavior by NOT removing duplicates, matching PostgreSQL's semantics
    
    // Handle duplicate column names in SELECT projections by adding aliases
    processed_sql = handle_duplicate_column_names(&processed_sql);
    
    processed_sql
}

pub async fn execute_query(
    sql: &str,
    batch: RecordBatch,
    table_name: &str,
) -> Result<(Vec<RecordBatch>, u64)> {
    let start_time = Instant::now();
    
    // Pre-process SQL to handle duplicate column names and normalize schema-qualified table names
    let processed_sql = normalize_schema_qualified_tables(sql);
    debug!("🔧 Processed SQL: {} -> {}", sql, processed_sql);
    
    let ctx = SessionContext::new();
    
    // Register PostgreSQL system functions
    register_postgresql_functions(&ctx)?;
    
    ctx.register_batch(table_name, batch)?;
    let df = ctx.sql(&processed_sql).await?;
    let results = df.collect().await?;
    
    let elapsed_ms = start_time.elapsed().as_millis() as u64;
    debug!("⚡ DataFusion query execution completed in {} ms", elapsed_ms);
    
    Ok((results, elapsed_ms))
}

/// Execute a complex SQL query with catalog tables support (legacy function)
pub async fn execute_query_with_catalog(sql: &str) -> Result<(Vec<RecordBatch>, u64)> {
    let start_time = Instant::now();
    
    // Pre-process SQL to normalize schema-qualified table names
    let processed_sql = normalize_schema_qualified_tables(sql);
    debug!("🔧 Processed SQL: {} -> {}", sql, processed_sql);
    
    let ctx = SessionContext::new();
    
    // Register PostgreSQL system functions
    register_postgresql_functions(&ctx)?;
    
    // Register all catalog tables if available
    if let Some(catalog) = catalog::get_catalog() {
        for table_name in catalog.get_table_names() {
            if let Some(table) = catalog.get_table(&table_name) {
                debug!("📋 Registering catalog table '{}' with DataFusion", table_name);
                
                // Register each record batch as a separate table if there are multiple batches
                // For simplicity, we'll combine all batches into one table
                if !table.data.is_empty() {
                    let combined_batch = if table.data.len() == 1 {
                        table.data[0].clone()
                    } else {
                        // Combine multiple batches
                        let schema = table.data[0].schema();
                        let mut columns = Vec::new();
                        
                        // Collect all columns from all batches
                        for col_idx in 0..schema.fields().len() {
                            let mut arrays = Vec::new();
                            for batch in &table.data {
                                arrays.push(batch.column(col_idx).clone());
                            }
                            // Concatenate arrays
                            let combined_array = arrow::compute::concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
                            columns.push(combined_array);
                        }
                        
                        RecordBatch::try_new(schema, columns)?
                    };
                    
                    // Register with normalized name (dots replaced with underscores)
                    let normalized_name = table_name.replace('.', "_");
                    debug!("📋 Registering catalog table as: {}", normalized_name);
                    ctx.register_batch(&normalized_name, combined_batch.clone())?;
                    
                    // If the table name starts with pg_catalog., also register without the prefix
                    // for backward compatibility (e.g., both "pg_catalog.pg_class" and pg_class)
                    if table_name.starts_with("pg_catalog.") {
                        if let Some(short_name) = table_name.strip_prefix("pg_catalog.") {
                            debug!("📋 Also registering '{}' as '{}'", table_name, short_name);
                            ctx.register_batch(short_name, combined_batch)?;
                        }
                    }
                }
            }
        }
    }
    
    let df = ctx.sql(&processed_sql).await?;
    let results = df.collect().await?;
    
    let elapsed_ms = start_time.elapsed().as_millis() as u64;
    debug!("⚡ DataFusion catalog query execution completed in {} ms", elapsed_ms);
    
    Ok((results, elapsed_ms))
}

// I'll add stubs for the other empty table functions to avoid compilation errors

/// Execute a query that might involve both regular tables and catalog tables
#[allow(dead_code)]
pub async fn execute_mixed_query(
    sql: &str,
    regular_batches: Vec<(String, RecordBatch)>,
) -> Result<(Vec<RecordBatch>, u64)> {
    let start_time = Instant::now();
    
    let ctx = SessionContext::new();
    
    // Register PostgreSQL system functions
    register_postgresql_functions(&ctx)?;
    
    // Register regular tables (e.g., from GraphQL results)
    for (table_name, batch) in regular_batches {
        debug!("📊 Registering regular table '{}' with DataFusion", table_name);
        ctx.register_batch(&table_name, batch)?;
    }
    
    // Register all catalog tables if available
    if let Some(catalog) = catalog::get_catalog() {
        for table_name in catalog.get_table_names() {
            if let Some(table) = catalog.get_table(&table_name) {
                debug!("📋 Registering catalog table '{}' with DataFusion", table_name);
                
                if !table.data.is_empty() {
                    let combined_batch = if table.data.len() == 1 {
                        table.data[0].clone()
                    } else {
                        // Combine multiple batches
                        let schema = table.data[0].schema();
                        let mut columns = Vec::new();
                        
                        for col_idx in 0..schema.fields().len() {
                            let mut arrays = Vec::new();
                            for batch in &table.data {
                                arrays.push(batch.column(col_idx).clone());
                            }
                            let combined_array = arrow::compute::concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
                            columns.push(combined_array);
                        }
                        
                        RecordBatch::try_new(schema, columns)?
                    };
                    
                    // Register with normalized name (dots replaced with underscores)
                    let normalized_name = table_name.replace('.', "_");
                    debug!("📋 Registering catalog table as: {}", normalized_name);
                    ctx.register_batch(&normalized_name, combined_batch.clone())?;
                    
                    // If the table name starts with pg_catalog., also register without the prefix
                    // for backward compatibility (e.g., both "pg_catalog.pg_class" and pg_class)
                    if table_name.starts_with("pg_catalog.") {
                        if let Some(short_name) = table_name.strip_prefix("pg_catalog.") {
                            debug!("📋 Also registering '{}' as '{}'", table_name, short_name);
                            ctx.register_batch(short_name, combined_batch)?;
                        }
                    }
                }
            }
        }
    }
    
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    
    let elapsed_ms = start_time.elapsed().as_millis() as u64;
    debug!("⚡ DataFusion mixed query execution completed in {} ms", elapsed_ms);
    
    Ok((results, elapsed_ms))
}
