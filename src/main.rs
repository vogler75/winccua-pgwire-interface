use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use tracing::{info, warn};

mod auth;
mod graphql;
mod information_schema;
mod pg_protocol;
mod query_handler;
mod sql_handler;
mod tables;

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

    /// Skip PostgreSQL authentication and use provided username for GraphQL
    #[arg(long)]
    pub no_auth_username: Option<String>,

    /// Skip PostgreSQL authentication and use provided password for GraphQL
    #[arg(long)]
    pub no_auth_password: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Get GraphQL URL from args or environment
    let graphql_url = args
        .graphql_url
        .or_else(|| std::env::var("GRAPHQL_HTTP_URL").ok())
        .expect("GraphQL URL must be provided via --graphql-url or GRAPHQL_HTTP_URL environment variable");

    // Initialize logging
    let log_level = if args.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!(
            "{}={},winccua_pgwire_protocol={}",
            env!("CARGO_PKG_NAME").replace('-', "_"),
            log_level,
            log_level
        ))
        .init();

    info!("Starting WinCC UA PostgreSQL Wire Protocol Server");
    info!("Binding to: {}", args.bind_addr);
    info!("GraphQL URL: {}", graphql_url);

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

    // Check if no-auth mode is enabled
    let no_auth_config = if let (Some(username), Some(password)) =
        (&args.no_auth_username, &args.no_auth_password)
    {
        info!(
            "🔓 No-auth mode enabled: using username '{}' for all connections",
            username
        );
        Some((username.clone(), password.clone()))
    } else if args.no_auth_username.is_some() || args.no_auth_password.is_some() {
        return Err(anyhow::anyhow!(
            "Both --no-auth-username and --no-auth-password must be provided together"
        ));
    } else {
        None
    };

    // For now, always use the simple server with improved PostgreSQL compatibility
    // The pgwire library API is too complex and has changed significantly
    info!("🐘 Starting PostgreSQL-compatible server (enhanced simple protocol)");
    let server = pg_protocol::PgProtocolServer::new(graphql_url, no_auth_config);
    server.start(args.bind_addr).await?;

    Ok(())
}
