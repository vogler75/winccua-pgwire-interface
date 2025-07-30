mod authentication;
mod connection_handler;
mod message_handler;
mod query_execution;
pub(crate) mod response;
mod startup;

use crate::auth::SessionManager;
use crate::tls::TlsConfig;
use anyhow::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

// Extended Query Protocol structures
#[derive(Debug, Clone)]
struct PreparedStatement {
    #[allow(dead_code)]
    name: String,
    query: String,
    #[allow(dead_code)]
    parameter_types: Vec<u32>, // PostgreSQL OID types
}

#[derive(Debug, Clone)]
struct Portal {
    #[allow(dead_code)]
    name: String,
    statement_name: String,
    parameters: Vec<Option<String>>, // Parameter values
}

// Connection state for Extended Query Protocol
#[derive(Debug)]
struct ConnectionState {
    prepared_statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    #[allow(dead_code)]
    scram_context: Option<ScramSha256Context>, // SCRAM authentication state
}

// SCRAM authentication stages
#[derive(Debug, Clone)]
pub(super) enum ScramStage {
    Initial, // Waiting for SASLInitialResponse
    Continue, // Sent server-first, waiting for client-final
    #[allow(dead_code)]
    Final, // Sent server-final, authentication complete
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(super) struct ScramSha256Context {
    username: String,
    client_nonce: String,
    server_nonce: String,
    salt: Vec<u8>,
    iteration_count: u32,
    client_first_bare: String,
    server_first: String,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
    stage: ScramStage,
}

pub struct PgProtocolServer {
    session_manager: Arc<SessionManager>,
    tls_config: Option<TlsConfig>,
    quiet_connections: bool,
}

impl PgProtocolServer {
    pub fn new(graphql_url: String, tls_config: Option<TlsConfig>, session_extension_interval: u64) -> Self {
        Self {
            session_manager: Arc::new(SessionManager::with_extension_interval(graphql_url, session_extension_interval)),
            tls_config,
            quiet_connections: false,
        }
    }

    pub fn with_quiet_connections(mut self, quiet: bool) -> Self {
        self.quiet_connections = quiet;
        // Also update the session manager
        let session_manager = Arc::new(
            SessionManager::with_extension_interval(
                self.session_manager.graphql_url().to_string(), 
                self.session_manager.extension_interval_secs()
            ).with_quiet_connections(quiet)
        );
        self.session_manager = session_manager;
        self
    }

    pub async fn start(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("🐘 PostgreSQL-like server listening on {}", addr);

        // Create TLS acceptor if TLS is configured
        let tls_acceptor = if let Some(ref tls_config) = self.tls_config {
            let server_config = crate::tls::create_server_config(tls_config)?;
            Some(tokio_rustls::TlsAcceptor::from(server_config))
        } else {
            None
        };

        loop {
            debug!("🎧 Waiting for new connections...");

            let (socket, client_addr) = listener.accept().await?;
            if !self.quiet_connections {
                info!("🌟 Accepted new connection from {}", client_addr);
            }

            let session_manager = self.session_manager.clone();
            let tls_acceptor = tls_acceptor.clone();
            let quiet_connections = self.quiet_connections;
            
            tokio::spawn(async move {
                debug!("🚀 Starting connection handler for {}", client_addr);

                if let Err(e) = connection_handler::handle_connection(
                    socket, 
                    session_manager.clone(), 
                    client_addr,
                    tls_acceptor,
                    quiet_connections
                ).await
                {
                    // Check if this is a connection error that might leave orphaned sessions
                    let error_str = e.to_string();
                    if error_str.contains("close_notify") || 
                       error_str.contains("Connection reset by peer") ||
                       error_str.contains("Broken pipe") ||
                       error_str.contains("peer closed connection") ||
                       error_str.contains("Connection closed") ||
                       error_str.contains("UnexpectedEof") ||
                       error_str.contains("connection was closed") {
                        if !quiet_connections {
                            debug!("🔌 Client {} disconnected abruptly ({}), performing cleanup", client_addr, error_str);
                        }
                        // Clean up any orphaned connections for this client address
                        session_manager.cleanup_connections_by_address(client_addr).await;
                    } else {
                        error!("💥 Error handling connection from {}: {}", client_addr, e);
                    }
                } else {
                    debug!(
                        "✅ Connection handler completed successfully for {}",
                        client_addr
                    );
                }

                if !quiet_connections {
                    info!("👋 Connection from {} closed", client_addr);
                }
            });
        }
    }
}