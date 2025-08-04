use anyhow::Result;
use arrow::array::Array;
use clap::Parser;
use pgwire::api::Type as PgType;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tracing::{info, warn, debug};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

// Global flag for SQL logging
pub static LOG_SQL: AtomicBool = AtomicBool::new(false);

// Global settings cache
#[derive(Debug, Clone)]
pub struct PostgreSQLSetting {
    pub name: String,
    pub setting: String,
    pub vartype: String,
}

// Global settings cache
static GLOBAL_SETTINGS: once_cell::sync::Lazy<Arc<RwLock<HashMap<String, PostgreSQLSetting>>>> = 
    once_cell::sync::Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Initialize global settings cache from pg_catalog.pg_settings table
async fn load_global_settings() -> Result<()> {
    if let Some(catalog) = catalog::get_catalog() {
        if let Some(settings_table) = catalog.get_table("pg_catalog.pg_settings").or_else(|| catalog.get_table("pg_settings")) {
            info!("📋 Loading PostgreSQL settings from catalog database");
            
            // Create a DataFusion context to query the settings
            let ctx = datafusion::prelude::SessionContext::new();
            
            // Register the settings table
            if !settings_table.data.is_empty() {
                let combined_batch = if settings_table.data.len() == 1 {
                    settings_table.data[0].clone()
                } else {
                    // Combine multiple batches if needed
                    let schema = settings_table.data[0].schema();
                    let mut columns = Vec::new();
                    
                    for col_idx in 0..schema.fields().len() {
                        let mut arrays = Vec::new();
                        for batch in &settings_table.data {
                            arrays.push(batch.column(col_idx).clone());
                        }
                        let combined_array = arrow::compute::concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
                        columns.push(combined_array);
                    }
                    
                    arrow::record_batch::RecordBatch::try_new(schema, columns)?
                };
                
                ctx.register_batch("pg_settings", combined_batch)?;
                
                // Query name, setting, and vartype columns
                let df = ctx.sql("SELECT name, setting, vartype FROM pg_settings").await?;
                let results = df.collect().await?;
                
                let mut settings_cache = GLOBAL_SETTINGS.write().unwrap();
                settings_cache.clear();
                
                // Process each batch
                for batch in results {
                    let num_rows = batch.num_rows();
                    let name_array = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                    let setting_array = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                    let vartype_array = batch.column(2).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                    
                    for row_idx in 0..num_rows {
                        if !name_array.is_null(row_idx) && !setting_array.is_null(row_idx) && !vartype_array.is_null(row_idx) {
                            let name = name_array.value(row_idx).to_string();
                            let setting = setting_array.value(row_idx).to_string();
                            let vartype = vartype_array.value(row_idx).to_string();
                            
                            settings_cache.insert(name.clone().to_lowercase(), PostgreSQLSetting {
                                name: name.clone(),
                                setting,
                                vartype,
                            });
                        }
                    }
                }
                
                let count = settings_cache.len();
                info!("✅ Loaded {} PostgreSQL settings from pg_catalog.pg_settings", count);
                return Ok(());
            }
        }
    }
    
    // Fallback: Load default settings if catalog is not available
    warn!("📋 Catalog database not available, using default PostgreSQL settings");
    load_default_settings();
    Ok(())
}

/// Load default PostgreSQL settings as fallback
fn load_default_settings() {
    let mut settings_cache = GLOBAL_SETTINGS.write().unwrap();
    settings_cache.clear();
    
    // Add essential PostgreSQL settings with their types
    let default_settings = vec![
        ("transaction_isolation", "read committed", "text"),
        ("application_name", "WinCC PGWire Protocol Server", "text"),
        ("client_encoding", "UTF8", "text"),
        ("datestyle", "ISO, MDY", "text"),
        ("extra_float_digits", "0", "integer"),
        ("max_identifier_length", "63", "integer"),
        ("server_version", "15.0", "text"),
        ("server_version_num", "150000", "integer"),
        ("timezone", "UTC", "text"),
    ];
    
    for (name, setting, vartype) in default_settings {
        settings_cache.insert(name.to_lowercase().to_string(), PostgreSQLSetting {
            name: name.to_string(),
            setting: setting.to_string(),
            vartype: vartype.to_string(),
        });
    }
    
    info!("✅ Loaded {} default PostgreSQL settings", settings_cache.len());
}

/// Get a PostgreSQL setting by name
pub fn get_postgresql_setting(name: &str) -> Option<PostgreSQLSetting> {
    let settings_cache = GLOBAL_SETTINGS.read().unwrap();
    settings_cache.get(&name.to_lowercase()).cloned()
}

/// Format PostgreSQL type for display
fn format_pg_type(pg_type: &PgType) -> &'static str {
    match *pg_type {
        PgType::BOOL => "BOOLEAN",
        PgType::INT2 => "SMALLINT",
        PgType::INT4 => "INTEGER", 
        PgType::INT8 => "BIGINT",
        PgType::FLOAT4 => "REAL",
        PgType::FLOAT8 => "DOUBLE PRECISION",
        PgType::NUMERIC => "NUMERIC",
        PgType::TEXT => "TEXT",
        PgType::VARCHAR => "VARCHAR",
        PgType::CHAR => "CHAR",
        PgType::TIMESTAMP => "TIMESTAMP",
        PgType::TIMESTAMPTZ => "TIMESTAMPTZ",
        PgType::DATE => "DATE",
        PgType::TIME => "TIME",
        PgType::TIMETZ => "TIMETZ",
        _ => "UNKNOWN",
    }
}

mod auth;
mod catalog;
mod datafusion_handler;
mod graphql;
mod information_schema;
mod keep_alive;
mod pg_protocol;
mod query_handler;
mod sql_handler;
mod tables;
mod tls;

// Custom formatter for consistent module name width
const MODULE_NAME_WIDTH: usize = 40;

struct CustomFormatter;

impl<S, N> FormatEvent<S, N> for CustomFormatter
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        
        // Format timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(now.as_secs() as i64, now.subsec_nanos())
            .unwrap_or_default();
        
        // Format module name with fixed width (right-padded) and remove common prefix
        let target = metadata.target();
        let cleaned_target = target.strip_prefix("winccua_pgwire_protocol::").unwrap_or(target);
        let padded_target = format!("{:<width$}", cleaned_target, width = MODULE_NAME_WIDTH);
        
        // Write formatted log line
        write!(
            writer,
            "{} {:>5} {}: ",
            datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            metadata.level(),
            padded_target
        )?;
        
        // Format the event fields
        ctx.format_fields(writer.by_ref(), event)?;
        
        writeln!(writer)
    }
}

#[derive(Parser, Debug)]
#[command(name = "winccua-pgwire-protocol")]
#[command(about = "PostgreSQL wire protocol server for WinCC UA GraphQL backend")]
pub struct Args {
    /// Address to bind the PostgreSQL server to
    #[arg(long, default_value = "127.0.0.1:5432")]
    pub bind_addr: SocketAddr,

    /// GraphQL server URL (also reads from GRAPHQL_HTTP_URL env var)
    #[arg(long)]
    pub graphql_url: Option<String>,

    /// Enable debug logging
    #[arg(long)]
    pub debug: bool,

    /// Enable TLS/SSL support
    #[arg(long)]
    pub tls_enabled: bool,

    /// Path to TLS certificate file (PEM format)
    #[arg(long)]
    pub tls_cert: Option<String>,

    /// Path to TLS private key file (PEM format)
    #[arg(long)]
    pub tls_key: Option<String>,

    /// Path to CA certificate file for client certificate verification (optional)
    #[arg(long)]
    pub tls_ca_cert: Option<String>,

    /// Require client certificates for authentication
    #[arg(long)]
    pub tls_require_client_cert: bool,

    /// Session extension interval in seconds (default: 600 = 10 minutes)
    #[arg(long, default_value_t = 600)]
    pub session_extension_interval: u64,

    /// Keep-alive interval in seconds (default: 30 seconds)
    #[arg(long, default_value_t = 30)]
    pub keep_alive_interval: u64,

    /// Enable SQL query logging at INFO level (default: logs at DEBUG level)
    #[arg(long)]
    pub log_sql: bool,

    /// Suppress connection and authentication log messages
    #[arg(long)]
    pub quiet_connections: bool,

    /// Path to catalog SQLite database file (optional)
    /// - If not specified, looks for 'catalog.db' in current directory
    /// - Use 'none' to explicitly disable catalog database
    #[arg(long)]
    pub catalog_db: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Get GraphQL URL from args or environment
    let graphql_url = args
        .graphql_url
        .or_else(|| std::env::var("GRAPHQL_HTTP_URL").ok())
        .expect("GraphQL URL must be provided via --graphql-url or GRAPHQL_HTTP_URL environment variable");

    // Initialize logging with custom formatter for consistent module name width
    let log_level = if args.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!(
            "{}={},winccua_pgwire_protocol={}",
            env!("CARGO_PKG_NAME").replace('-', "_"),
            log_level,
            log_level
        ))
        .event_format(CustomFormatter)
        .init();

    info!("Starting WinCC UA PostgreSQL Wire Protocol Server");
    info!("Binding to: {}", args.bind_addr);
    info!("GraphQL URL: {}", graphql_url);
    info!("Session extension interval: {} seconds", args.session_extension_interval);
    info!("Keep-alive interval: {} seconds", args.keep_alive_interval);
    
    // Set global SQL logging flag
    LOG_SQL.store(args.log_sql, Ordering::Relaxed);
    if args.log_sql {
        info!("SQL query logging: ENABLED (INFO level)");
    } else {
        info!("SQL query logging: DEBUG level only");
    }

    // Validate GraphQL connection
    info!("Validating GraphQL connection to: {}", graphql_url);
    match graphql::client::validate_connection(&graphql_url).await {
        Ok(()) => {
            info!("✅ GraphQL connection validated successfully");
        }
        Err(e) => {
            warn!("⚠️  GraphQL connection validation failed: {}", e);
            warn!("This could mean:");
            warn!("  - GraphQL server is not running");
            warn!("  - URL is incorrect (current: {})", graphql_url);
            warn!("  - Network connectivity issues");
            warn!("  - Server doesn't support introspection queries");
            warn!("Server will start anyway, but authentication will likely fail.");
        }
    }

    // Initialize catalog database
    // Priority: 1) --catalog-db none (explicitly disabled)
    //          2) --catalog-db <path> (explicit path)
    //          3) ./catalog.db (default if exists)
    //          4) No catalog (if default doesn't exist)
    let catalog_path = if let Some(catalog_arg) = &args.catalog_db {
        if catalog_arg.to_lowercase() == "none" {
            None
        } else {
            Some(catalog_arg.clone())
        }
    } else {
        // Check for default catalog.db file
        let default_path = "catalog.db";
        if std::path::Path::new(default_path).exists() {
            info!("📚 Found default catalog database: {}", default_path);
            Some(default_path.to_string())
        } else {
            None
        }
    };

    if let Some(catalog_path) = catalog_path {
        info!("📚 Initializing catalog database from: {}", catalog_path);
        match catalog::init_catalog(&catalog_path) {
            Ok(()) => {
                let catalog = catalog::get_catalog().unwrap();
                info!("✅ Catalog database initialized successfully with {} tables", 
                      catalog.table_count());
                
                // Print detailed table information
                for table_name in catalog.get_table_names() {
                    if let Some(table) = catalog.get_table(&table_name) {
                        // Simple catalog table message
                        let display_msg = format!("📋 Catalog table '{}' - {} rows:", 
                                                 table_name,
                                                 table.data.iter().map(|b| b.num_rows()).sum::<usize>());
                        info!("{}", display_msg);
                        
                        // Print column information
                        for (col_name, pg_type) in &table.pg_schema {
                            debug!("   └─ {} ({})", col_name, format_pg_type(pg_type));
                        }
                    } else {
                        info!("📋 Catalog table: {} (schema unavailable)", table_name);
                    }
                }
            }
            Err(e) => {
                warn!("⚠️  Failed to initialize catalog database: {}", e);
                warn!("Server will continue without catalog tables");
            }
        }
    } else {
        if args.catalog_db.as_ref().map(|s| s.to_lowercase()) == Some("none".to_string()) {
            info!("📚 Catalog database explicitly disabled (--catalog-db none)");
        } else {
            info!("📚 No catalog database found (use --catalog-db <path> or place catalog.db in current directory)");
        }
    }

    // Load PostgreSQL settings from catalog or defaults
    load_global_settings().await?;

    // Setup TLS configuration if enabled
    let tls_config = if args.tls_enabled {
        let cert_path = args.tls_cert.ok_or_else(|| {
            anyhow::anyhow!("TLS certificate path (--tls-cert) is required when TLS is enabled")
        })?;
        let key_path = args.tls_key.ok_or_else(|| {
            anyhow::anyhow!("TLS private key path (--tls-key) is required when TLS is enabled")
        })?;

        let mut config = crate::tls::TlsConfig::new(cert_path, key_path);
        
        if let Some(ca_cert) = args.tls_ca_cert {
            config = config.with_ca_cert(ca_cert);
        }
        
        if args.tls_require_client_cert {
            config = config.require_client_cert(true);
        }
        
        Some(config)
    } else {
        None
    };

    // For now, always use the simple server with improved PostgreSQL compatibility
    // The pgwire library API is too complex and has changed significantly
    if tls_config.is_some() {
        info!("🐘 Starting PostgreSQL-compatible server with TLS support 🔒");
    } else {
        info!("🐘 Starting PostgreSQL-compatible server");
    }
    
    let server = pg_protocol::PgProtocolServer::with_keep_alive(
        graphql_url, 
        tls_config, 
        args.session_extension_interval,
        args.keep_alive_interval
    )
    .with_quiet_connections(args.quiet_connections);
    server.start(args.bind_addr).await?;

    Ok(())
}
