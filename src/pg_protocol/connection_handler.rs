use crate::auth::SessionManager;
use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, trace, warn};

use super::startup::handle_postgres_startup;

pub(super) async fn handle_connection(
    mut socket: TcpStream,
    session_manager: Arc<SessionManager>,
) -> Result<()> {
    let peer_addr = socket.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
    info!("🔌 New connection established from {}", peer_addr);

    // Read first few bytes to see what kind of connection this is
    let mut peek_buffer = [0; 32];
    debug!("📖 Reading initial data from {}", peer_addr);

    let n = socket.read(&mut peek_buffer).await?;
    if n == 0 {
        warn!(
            "⚠️  Connection from {} closed immediately (no data received)",
            peer_addr
        );
        return Ok(());
    }

    debug!("📊 Received {} bytes from {}", n, peer_addr);
    trace!("🔍 Raw bytes: {:02x?}", &peek_buffer[..n]);

    // Check if this is an SSL request first
    if n >= 8 && is_ssl_request(&peek_buffer[..n]) {
        info!("🔒 SSL connection request detected from {}!", peer_addr);
        info!("   📌 SSL Status: Not supported by server");
        info!("   💡 Client should fall back to unencrypted connection");

        // PostgreSQL SSL response format:
        // 'N' (0x4E) = SSL not supported
        // 'S' (0x53) = SSL supported (we don't support this)
        let ssl_response = b"N";
        if let Err(e) = socket.write_all(ssl_response).await {
            error!("❌ Failed to send SSL rejection to {}: {}", peer_addr, e);
            return Ok(());
        }

        debug!("✅ Sent SSL rejection ('N') to {}", peer_addr);

        // After rejecting SSL, the client should send a normal startup message
        debug!(
            "📖 Waiting for startup message after SSL rejection from {}",
            peer_addr
        );

        let mut startup_buffer = [0; 1024];
        let startup_n = socket.read(&mut startup_buffer).await?;
        if startup_n == 0 {
            warn!("⚠️  Client {} disconnected after SSL rejection", peer_addr);
            return Ok(());
        }

        debug!(
            "📊 Received {} bytes after SSL rejection from {}",
            startup_n, peer_addr
        );
        trace!(
            "🔍 Startup message bytes: {:02x?}",
            &startup_buffer[..startup_n]
        );

        // Now handle the startup message as a regular PostgreSQL connection
        return handle_postgres_startup(
            socket,
            session_manager,
            &startup_buffer[..startup_n],
        )
        .await;
    }
    // Check if this looks like PostgreSQL wire protocol (non-SSL)
    else if n >= 8 && is_postgres_wire_protocol(&peek_buffer[..n]) {
        warn!("🐘 PostgreSQL wire protocol detected from {}!", peer_addr);

        // For now, attempt to handle it as PostgreSQL startup
        return handle_postgres_startup(socket, session_manager, &peek_buffer[..n])
            .await;
    }

    // Try to interpret as simple text protocol
    let initial_data = String::from_utf8_lossy(&peek_buffer[..n]);
    debug!("📄 Initial data as text: {:?}", initial_data);

    // Check if this looks like an authentication attempt
    if initial_data.contains(':') {
        info!("🔐 Processing authentication attempt from {}", peer_addr);
        return handle_simple_text_protocol(
            socket,
            session_manager,
            initial_data.to_string(),
        )
        .await;
    }

    // If we can't identify the protocol, read more data
    warn!(
        "❓ Unknown protocol from {}. Trying to read more data...",
        peer_addr
    );
    let mut full_buffer = Vec::from(&peek_buffer[..n]);
    let mut temp_buffer = [0; 1024];

    // Try to read more data with a timeout
    tokio::select! {
        result = socket.read(&mut temp_buffer) => {
            match result {
                Ok(additional_bytes) => {
                    if additional_bytes > 0 {
                        full_buffer.extend_from_slice(&temp_buffer[..additional_bytes]);
                        debug!("📊 Read additional {} bytes (total: {})", additional_bytes, full_buffer.len());

                        let full_data = String::from_utf8_lossy(&full_buffer);
                        debug!("📄 Full data as text: {:?}", full_data);

                        if full_data.contains(':') {
                            return handle_simple_text_protocol(socket, session_manager, full_data.to_string()).await;
                        }
                    }
                }
                Err(e) => {
                    error!("❌ Error reading additional data from {}: {}", peer_addr, e);
                }
            }
        }
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {
            warn!("⏰ Timeout waiting for more data from {}", peer_addr);
        }
    }

    error!(
        "❌ Unable to identify protocol from {}. Closing connection.",
        peer_addr
    );
    let error_msg = "ERROR: Unrecognized protocol. Expected format: 'username:password'\n";
    let _ = socket.write_all(error_msg.as_bytes()).await;

    Ok(())
}

fn is_postgres_wire_protocol(data: &[u8]) -> bool {
    // PostgreSQL wire protocol starts with a 4-byte length field
    // followed by a 4-byte protocol version (usually 196608 for 3.0)
    if data.len() < 8 {
        return false;
    }

    // Check for common PostgreSQL wire protocol patterns
    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);

    // PostgreSQL protocol version 3.0 = 196608 (0x00030000)
    // SSL request = 80877103 (0x04d2162f)
    // Cancel request = 80877102 (0x04d2162e)
    trace!(
        "🔍 Postgres check: length={}, version={} (0x{:08x})",
        length,
        version,
        version
    );

    version == 196608
        || version == 80877103
        || version == 80877102
        || (length > 8 && length < 10000 && version > 0)
}

fn is_ssl_request(data: &[u8]) -> bool {
    if data.len() < 8 {
        return false;
    }

    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);

    // SSL request magic number
    version == 80877103 && length == 8
}

async fn handle_simple_text_protocol(
    mut socket: TcpStream,
    session_manager: Arc<SessionManager>,
    initial_data: String,
) -> Result<()> {
    let peer_addr = socket.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
    info!("📝 Using simple text protocol with {}", peer_addr);

    // Parse authentication from initial data
    debug!("🔐 Processing auth data: {:?}", initial_data.trim());

    let parts: Vec<&str> = initial_data.trim().split(':').collect();
    if parts.len() != 2 {
        warn!(
            "❌ Invalid auth format from {}: expected 'username:password'",
            peer_addr
        );
        socket
            .write_all(b"ERROR: Invalid auth format. Expected 'username:password'\n")
            .await?;
        return Ok(());
    }

    let username = parts[0];
    let password = parts[1];

    info!(
        "🔑 Authentication attempt: user='{}' from {}",
        username, peer_addr
    );

    // Authenticate
    match session_manager.authenticate(username, password).await {
        Ok(session) => {
            info!(
                "✅ Authentication successful for user '{}' from {}",
                username, peer_addr
            );
            socket
                .write_all(b"OK: Authentication successful\n")
                .await?;

            // Query processing loop
            info!("🔄 Starting query loop for {}", peer_addr);
            let mut buffer = [0; 4096];

            loop {
                debug!("📖 Waiting for query from {}", peer_addr);

                let n = socket.read(&mut buffer).await?;
                if n == 0 {
                    info!("🔌 Connection closed by client {}", peer_addr);
                    break;
                }

                let query = String::from_utf8_lossy(&buffer[..n]);
                info!("📥 Query received from {}: {}", peer_addr, query.trim());

                if query.trim().to_lowercase().starts_with("select") {
                    debug!("🔍 Processing SELECT query from {}", peer_addr);
                    match super::query_execution::handle_simple_query(&query, &session).await {
                        Ok(response) => {
                            debug!(
                                "📤 Sending response to {} ({} bytes)",
                                peer_addr,
                                response.len()
                            );
                            socket.write_all(&response).await?;
                        }
                        Err(e) => {
                            error!("❌ Query processing error for {}: {}", peer_addr, e);
                            let error_msg = format!("ERROR: Query failed: {}\n", e);
                            socket.write_all(error_msg.as_bytes()).await?;
                        }
                    }
                } else {
                    warn!("❌ Unsupported query type from {}: {}", peer_addr, query.trim());
                    socket
                        .write_all(b"ERROR: Only SELECT queries are supported\n")
                        .await?;
                }
            }
        }
        Err(e) => {
            error!(
                "❌ Authentication failed for user '{}' from {}: {}",
                username, peer_addr, e
            );
            let error_msg = format!("ERROR: Authentication failed: {}\n", e);
            socket.write_all(error_msg.as_bytes()).await?;
        }
    }

    info!("🔌 Connection with {} ended", peer_addr);
    Ok(())
}