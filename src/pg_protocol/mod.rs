mod authentication;
mod connection_handler;
mod message_handler;
mod query_execution;
pub(crate) mod response;
mod startup;

use crate::auth::SessionManager;
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
}

impl PgProtocolServer {
    pub fn new(graphql_url: String) -> Self {
        Self {
            session_manager: Arc::new(SessionManager::new(graphql_url)),
        }
    }

    pub async fn start(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("PostgreSQL-like server listening on {}", addr);

        loop {
            debug!("🎧 Waiting for new connections...");

            let (socket, client_addr) = listener.accept().await?;
            info!("🌟 Accepted new connection from {}", client_addr);

            let session_manager = self.session_manager.clone();
            tokio::spawn(async move {
                debug!("🚀 Starting connection handler for {}", client_addr);

                if let Err(e) =
                    connection_handler::handle_connection(socket, session_manager)
                        .await
                {
                    error!("💥 Error handling connection from {}: {}", client_addr, e);
                } else {
                    debug!(
                        "✅ Connection handler completed successfully for {}",
                        client_addr
                    );
                }

                info!("👋 Connection from {} closed", client_addr);
            });
        }
    }
}