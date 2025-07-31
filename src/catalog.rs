use anyhow::{Context, Result};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int64Array, Float64Array, 
    StringArray, TimestampMillisecondArray, RecordBatchOptions
};
use pgwire::api::Type as PgType;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use tracing::{info, warn, debug};

/// Represents a catalog table loaded from SQLite
#[derive(Debug, Clone)]
pub struct CatalogTable {
    #[allow(dead_code)]
    pub name: String,
    #[allow(dead_code)]
    pub schema: Arc<Schema>,
    pub data: Vec<RecordBatch>,
    pub pg_schema: Vec<(String, PgType)>,
}

/// Global catalog manager that loads and manages SQLite-based catalog tables
#[derive(Debug)]
pub struct CatalogManager {
    tables: HashMap<String, CatalogTable>,
}

impl CatalogManager {
    /// Create a new empty catalog manager
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Load catalog database from SQLite file
    pub fn load_from_sqlite<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let path = db_path.as_ref();
        info!("📚 Loading catalog database from: {}", path.display());

        if !path.exists() {
            warn!("⚠️  Catalog database file not found: {}", path.display());
            return Ok(Self::new()); // Return empty catalog if file doesn't exist
        }

        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open SQLite database: {}", path.display()))?;

        let mut catalog = Self::new();
        
        // Get list of all tables in the database
        let table_names = catalog.get_table_names_from_db(&conn)?;
        info!("📋 Found {} tables in catalog database", table_names.len());

        // Load each table
        for table_name in table_names {
            match catalog.load_table(&conn, &table_name) {
                Ok(table) => {
                    info!("✅ Loaded catalog table '{}' with {} rows", 
                          table_name, table.data.iter().map(|b| b.num_rows()).sum::<usize>());
                    catalog.tables.insert(table_name.clone(), table);
                }
                Err(e) => {
                    warn!("❌ Failed to load catalog table '{}': {}", table_name, e);
                }
            }
        }

        info!("🎯 Catalog loading complete: {} tables loaded", catalog.tables.len());
        Ok(catalog)
    }

    /// Get list of table names from SQLite database
    fn get_table_names_from_db(&self, conn: &Connection) -> Result<Vec<String>> {
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )?;
        
        let table_names: Result<Vec<String>, rusqlite::Error> = stmt
            .query_map([], |row| {
                Ok(row.get::<_, String>(0)?)
            })?
            .collect();

        Ok(table_names?)
    }

    /// Load a single table from SQLite
    fn load_table(&self, conn: &Connection, table_name: &str) -> Result<CatalogTable> {
        debug!("Loading table schema and data for: {}", table_name);

        // Get table schema information
        let schema_info = self.get_table_schema(conn, table_name)?;
        let (arrow_schema, pg_schema) = self.create_schemas(&schema_info)?;

        // Load all data from the table
        let mut stmt = conn.prepare(&format!("SELECT * FROM \"{}\"", table_name))?;
        let column_count = stmt.column_count();
        
        let mut rows_data: Vec<Vec<Option<String>>> = Vec::new();
        let rows = stmt.query_map([], |row| {
            let mut row_data = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let value: Option<String> = match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => None,
                    rusqlite::types::ValueRef::Integer(i) => Some(i.to_string()),
                    rusqlite::types::ValueRef::Real(f) => Some(f.to_string()),
                    rusqlite::types::ValueRef::Text(s) => Some(String::from_utf8_lossy(s).to_string()),
                    rusqlite::types::ValueRef::Blob(b) => Some(hex::encode(b)),
                };
                row_data.push(value);
            }
            Ok(row_data)
        })?;

        for row in rows {
            rows_data.push(row?);
        }

        // Convert to Arrow RecordBatch
        let record_batch = self.create_record_batch(&arrow_schema, &schema_info, rows_data)?;
        
        Ok(CatalogTable {
            name: table_name.to_string(),
            schema: arrow_schema,
            data: vec![record_batch],
            pg_schema,
        })
    }

    /// Get schema information for a table
    fn get_table_schema(&self, conn: &Connection, table_name: &str) -> Result<Vec<ColumnInfo>> {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table_name))?;
        let column_infos: Result<Vec<ColumnInfo>, rusqlite::Error> = stmt
            .query_map([], |row| {
                Ok(ColumnInfo {
                    name: row.get(1)?,
                    sqlite_type: row.get(2)?,
                    not_null: row.get::<_, i32>(3)? != 0,
                    default_value: row.get(4)?,
                    is_pk: row.get::<_, i32>(5)? != 0,
                })
            })?
            .collect();

        Ok(column_infos?)
    }

    /// Create Arrow and PostgreSQL schemas from column information
    fn create_schemas(&self, columns: &[ColumnInfo]) -> Result<(Arc<Schema>, Vec<(String, PgType)>)> {
        let mut arrow_fields = Vec::new();
        let mut pg_schema = Vec::new();

        for col in columns {
            let (arrow_type, pg_type) = self.map_sqlite_type(&col.sqlite_type);
            arrow_fields.push(Field::new(&col.name, arrow_type, !col.not_null));
            pg_schema.push((col.name.clone(), pg_type));
        }

        let arrow_schema = Arc::new(Schema::new(arrow_fields));
        Ok((arrow_schema, pg_schema))
    }

    /// Map SQLite type to Arrow and PostgreSQL types
    fn map_sqlite_type(&self, sqlite_type: &str) -> (DataType, PgType) {
        let lower = sqlite_type.to_lowercase();
        match lower.as_str() {
            "integer" | "int" => (DataType::Int64, PgType::INT8),
            "real" | "float" | "double" => (DataType::Float64, PgType::FLOAT8),
            "text" | "varchar" | "char" => (DataType::Utf8, PgType::TEXT),
            "boolean" | "bool" => (DataType::Boolean, PgType::BOOL),
            "timestamp" | "datetime" => (DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None), PgType::TIMESTAMP),
            _ => (DataType::Utf8, PgType::TEXT), // Default to text
        }
    }

    /// Create Arrow RecordBatch from raw data
    fn create_record_batch(
        &self, 
        schema: &Arc<Schema>, 
        column_info: &[ColumnInfo],
        rows: Vec<Vec<Option<String>>>
    ) -> Result<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for (col_idx, _col_info) in column_info.iter().enumerate() {
            let field = &schema.fields()[col_idx];
            let column_data: Vec<Option<String>> = rows.iter()
                .map(|row| row.get(col_idx).cloned().flatten())
                .collect();

            let array: ArrayRef = match field.data_type() {
                DataType::Int64 => {
                    let values: Vec<Option<i64>> = column_data.iter()
                        .map(|v| v.as_ref().and_then(|s| s.parse().ok()))
                        .collect();
                    Arc::new(Int64Array::from(values))
                }
                DataType::Float64 => {
                    let values: Vec<Option<f64>> = column_data.iter()
                        .map(|v| v.as_ref().and_then(|s| s.parse().ok()))
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                DataType::Boolean => {
                    let values: Vec<Option<bool>> = column_data.iter()
                        .map(|v| v.as_ref().and_then(|s| {
                            match s.to_lowercase().as_str() {
                                "true" | "1" | "yes" | "on" => Some(true),
                                "false" | "0" | "no" | "off" => Some(false),
                                _ => None,
                            }
                        }))
                        .collect();
                    Arc::new(BooleanArray::from(values))
                }
                DataType::Timestamp(_, _) => {
                    let values: Vec<Option<i64>> = column_data.iter()
                        .map(|v| v.as_ref().and_then(|s| {
                            // Try to parse ISO 8601 timestamp
                            chrono::DateTime::parse_from_rfc3339(s)
                                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                    .map(|dt| dt.and_utc().into()))
                                .map(|dt| dt.timestamp_millis())
                                .ok()
                        }))
                        .collect();
                    Arc::new(TimestampMillisecondArray::from(values))
                }
                _ => {
                    // Default to string
                    Arc::new(StringArray::from(column_data))
                }
            };

            arrays.push(array);
        }

        RecordBatch::try_new_with_options(
            schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows.len()))
        ).with_context(|| "Failed to create RecordBatch from catalog data")
    }

    /// Check if a table exists in the catalog
    pub fn has_table(&self, table_name: &str) -> bool {
        self.tables.contains_key(&table_name.to_lowercase())
    }

    /// Get a catalog table by name
    pub fn get_table(&self, table_name: &str) -> Option<&CatalogTable> {
        self.tables.get(&table_name.to_lowercase())
    }

    /// Get all table names
    pub fn get_table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get number of loaded tables
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

/// Column information from SQLite PRAGMA
#[derive(Debug)]
struct ColumnInfo {
    name: String,
    sqlite_type: String,
    not_null: bool,
    #[allow(dead_code)]
    default_value: Option<String>,
    #[allow(dead_code)]
    is_pk: bool,
}

/// Global static instance of the catalog manager
static CATALOG_MANAGER: OnceLock<CatalogManager> = OnceLock::new();

/// Initialize the global catalog manager
pub fn init_catalog<P: AsRef<Path>>(db_path: P) -> Result<()> {
    let manager = CatalogManager::load_from_sqlite(db_path)?;
    
    CATALOG_MANAGER.set(manager).map_err(|_| {
        anyhow::anyhow!("Catalog manager already initialized")
    })?;
    
    Ok(())
}

/// Get reference to the global catalog manager
pub fn get_catalog() -> Option<&'static CatalogManager> {
    CATALOG_MANAGER.get()
}

/// Check if catalog system is initialized
#[allow(dead_code)]
pub fn is_initialized() -> bool {
    CATALOG_MANAGER.get().is_some()
}