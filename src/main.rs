use anyhow::Result;
use clap::Parser;
use std::fmt;
use std::net::SocketAddr;
use tracing::{info, warn};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

mod auth;
mod datafusion_handler;
mod graphql;
mod information_schema;
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

    /// Use PostgreSQL wire protocol (default) instead of simple TCP protocol
    #[arg(long, default_value_t = true)]
    pub pgwire: bool,

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
        info!("🐘🔒 Starting PostgreSQL-compatible server with TLS support");
    } else {
        info!("🐘 Starting PostgreSQL-compatible server (enhanced simple protocol)");
    }
    
    let server = pg_protocol::PgProtocolServer::new(graphql_url, tls_config, args.session_extension_interval);
    server.start(args.bind_addr).await?;

    Ok(())
}
