use crate::auth::SessionManager;
use crate::keep_alive::{send_keep_alive_probe, create_parameter_status_keepalive};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{interval, timeout, Duration};
use tracing::{debug, error, info, warn};
use anyhow::Result;

use super::authentication::{create_postgres_md5_request, create_postgres_scram_sha256_request, parse_postgres_password, parse_sasl_initial_response, AuthContext, parse_scram_client_first, scram_sha256_server_first_message, create_postgres_sasl_continue_response, parse_sasl_response, parse_scram_client_final, scram_sha256_verify_client_proof, create_postgres_sasl_final_response, compute_postgres_md5_hash, verify_postgres_md5_auth};
use super::message_handler::handle_postgres_message;
use super::response::{create_postgres_auth_ok_response, create_postgres_error_response};
use super::{ConnectionState, ScramStage};

pub(super) async fn handle_postgres_startup(
    socket: TcpStream,
    session_manager: Arc<SessionManager>,
    data: &[u8],
    peer_addr: SocketAddr,
    quiet_connections: bool,
    keep_alive_interval: u64,
) -> Result<()> {
    handle_postgres_startup_stream(socket, session_manager, data, Some(peer_addr), quiet_connections, keep_alive_interval).await
}

pub(super) async fn handle_postgres_startup_stream<T>(
    mut socket: T,
    session_manager: Arc<SessionManager>,
    data: &[u8],
    socket_addr: Option<SocketAddr>,
    quiet_connections: bool,
    keep_alive_interval: u64,
) -> Result<()> 
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let peer_addr_str = socket_addr.map(|a| a.to_string()).unwrap_or_else(|| "client".to_string());
    if !quiet_connections {
        info!("🐘 Handling PostgreSQL startup from {}", peer_addr_str);
    }

    // Initialize connection_id at function level
    let mut connection_id: Option<u32> = None;

    if data.len() < 8 {
        error!(
            "❌ Invalid startup message length from {}: {} bytes",
            peer_addr_str,
            data.len()
        );
        return Ok(());
    }

    // Ensure we have the complete message
    let complete_data = match read_complete_postgres_message_stream(&mut socket, data).await {
        Ok(data) => data,
        Err(e) => {
            error!(
                "❌ Failed to read complete startup message from {}: {}",
                peer_addr_str, e
            );
            return Ok(());
        }
    };

    let length =
        u32::from_be_bytes([complete_data[0], complete_data[1], complete_data[2], complete_data[3]]);
    let version =
        u32::from_be_bytes([complete_data[4], complete_data[5], complete_data[6], complete_data[7]]);

    if !quiet_connections {
        info!(
            "📋 Startup message: length={}, version={} (0x{:08x})",
            length, version, version
        );
    }

    // Dump full startup message for debugging
    if !quiet_connections {
        info!("🔍 Full startup message dump from {}:", peer_addr_str);
        info!("   📏 Total length: {} bytes", complete_data.len());
        info!("   📊 Message length field: {} bytes", length);
        info!("   🔢 Protocol version: {} (0x{:08x})", version, version);
    }

    // Hex dump of the entire startup message
    let hex_dump = hex::encode(&complete_data);
    debug!("   🔍 Hex dump (full message): {}", hex_dump);

    // ASCII interpretation (printable characters only)
    let ascii_dump: String = complete_data
        .iter()
        .map(|&b| if b >= 32 && b <= 126 { b as char } else { '.' })
        .collect();
    debug!("   📝 ASCII dump: {}", ascii_dump);

    // Parse startup parameters if this is a v3.0 protocol message
    if version == 196608 {
        // PostgreSQL 3.0 protocol

        // Initialize connection state for Extended Query Protocol and SCRAM authentication
        let mut connection_state = ConnectionState {
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            scram_context: None,
        };
        debug!("✅ PostgreSQL 3.0 protocol detected");

        // Extract parameters (user, database, etc.)
        if complete_data.len() > 8 {
            let params_data = &complete_data[8..];
            let params = parse_startup_parameters(params_data);
            if !quiet_connections {
                info!("📋 Client connection parameters from {}:", peer_addr_str);
                for (key, value) in &params {
                    match key.as_str() {
                        "user" => info!("   👤 User: {}", value),
                        "database" => info!("   🗄️  Database: {}", value),
                        "application_name" => info!("   📱 Application: {}", value),
                        "client_encoding" => info!("   🔤 Encoding: {}", value),
                        "DateStyle" => info!("   📅 Date Style: {}", value),
                        "TimeZone" => info!("   🌍 Timezone: {}", value),
                        "extra_float_digits" => info!("   🔢 Float Digits: {}", value),
                        "search_path" => info!("   🔍 Search Path: {}", value),
                        "sslmode" => info!("   🔒 SSL Mode: {}", value),
                        _ => info!("   📌 {}: {}", key, value),
                    }
                }
            }
            if params.is_empty() {
                warn!("⚠️  No parameters found in startup message");
                debug!(
                    "🔍 Raw parameter data: {:?}",
                    String::from_utf8_lossy(params_data)
                );
            } else if !quiet_connections {
                info!("📊 Total parameters received: {}", params.len());
            }
        }

        // Extract username and application_name from startup parameters for authentication
        let (username, application_name) = if complete_data.len() > 8 {
            let params_data = &complete_data[8..];
            let params = parse_startup_parameters(params_data);
            debug!("🔍 All startup parameters: {:?}", params);

            let user = params.get("user").cloned().unwrap_or_else(|| {
                error!(
                    "❌ No 'user' parameter found in startup message from {}",
                    peer_addr_str
                );
                error!(
                    "🔍 Available parameters: {:?}",
                    params.keys().collect::<Vec<_>>()
                );
                error!("🔍 This might be a Grafana or other client configuration issue");
                "unknown".to_string()
            });

            if user == "unknown" {
                error!(
                    "🔍 Startup message hex dump (first 128 bytes): {}",
                    hex::encode(&data[..data.len().min(128)])
                );
                error!("💡 Check client configuration - ensure username is specified");
            }

            let app_name = params.get("application_name").cloned().unwrap_or_else(|| "unknown".to_string());
            (user, app_name)
        } else {
            warn!(
                "⚠️  Startup message too short from {}: {} bytes",
                peer_addr_str,
                data.len()
            );
            ("unknown".to_string(), "unknown".to_string())
        };

        if !quiet_connections {
            info!(
                "🔐 PostgreSQL client {} requesting authentication for user: {}",
                peer_addr_str, username
            );
        }


        // Normal authentication flow
        // Choose authentication method:
        // 1. Use MD5 by default for maximum compatibility (psycopg2, etc.)
        // 2. SCRAM-SHA-256 available but not default due to limited client support
        // Note: For SCRAM, username comes in SASL Initial Response, not startup message

        let prefer_scram = false; // Use MD5 for better compatibility with Python clients

        let (auth_request, auth_context) = if prefer_scram {
            if !quiet_connections {
                info!("🔐 Offering SCRAM-SHA-256 authentication (preferred method)");
                if username == "unknown" {
                    info!("   💡 Username will be provided in SASL Initial Response");
                } else {
                    info!("   👤 Startup username: {}", username);
                }
            }
            (
                create_postgres_scram_sha256_request(),
                AuthContext::Scram,
            )
        } else {
            if !quiet_connections {
                info!("🔐 Sending MD5 authentication request");
            }
            let (auth_request, salt) = create_postgres_md5_request();
            debug!(
                "🧂 Generated salt for MD5 auth: {:02x}{:02x}{:02x}{:02x}",
                salt[0], salt[1], salt[2], salt[3]
            );
            (auth_request, AuthContext::Md5(salt))
        };

        debug!("📤 Sending password authentication request to {}", peer_addr_str);
        if let Err(e) = socket.write_all(&auth_request).await {
            error!("❌ Failed to send auth request to {}: {}", peer_addr_str, e);
            return Ok(());
        }

        // Wait for authentication response (SASL or password)
        if matches!(auth_context, AuthContext::Scram) {
            debug!("📖 Waiting for SASL Initial Response from {}", peer_addr_str);
        } else {
            debug!("📖 Waiting for password response from {}", peer_addr_str);
        }

        let mut auth_buffer = [0; 1024];
        let auth_n = socket.read(&mut auth_buffer).await?;
        if auth_n == 0 {
            if !quiet_connections {
                warn!("⚠️  Client {} disconnected during authentication", peer_addr_str);
            }
            return Ok(());
        }

        debug!(
            "📊 Received {} bytes authentication response from {}",
            auth_n, peer_addr_str
        );

        // Handle authentication based on the context
        let (username_final, password_final) = match &auth_context {
            AuthContext::Scram => {
                // Handle SCRAM-SHA-256 authentication with full protocol implementation
                if auth_n > 0 && auth_buffer[0] == b'p' {
                    // Parse SASL Initial Response
                    match parse_sasl_initial_response(&auth_buffer[..auth_n]) {
                        Ok((mechanism, initial_response)) => {
                            if mechanism != "SCRAM-SHA-256" {
                                warn!("🔄 Client requested unsupported SASL mechanism '{}', falling back to MD5", mechanism);

                                // Send MD5 auth request
                                let (md5_request, salt) = create_postgres_md5_request();
                                debug!("🧂 Generated salt for MD5 fallback: {:02x}{:02x}{:02x}{:02x}", salt[0], salt[1], salt[2], salt[3]);
                                socket.write_all(&md5_request).await?;

                                // Wait for password response
                                let mut password_buffer = [0; 1024];
                                let password_n = socket.read(&mut password_buffer).await?;
                                if password_n == 0 {
                                    if !quiet_connections {
                                        warn!(
                                            "⚠️  Client {} disconnected during MD5 fallback",
                                            peer_addr_str
                                        );
                                    }
                                    return Ok(());
                                }

                                let password = parse_postgres_password(&password_buffer[..password_n]);
                                if password.is_none() {
                                    error!("❌ Invalid password format during MD5 fallback from {}", peer_addr_str);
                                    let error_response = create_postgres_error_response("28P01", "Invalid password format");
                                    socket.write_all(&error_response).await?;
                                    return Ok(());
                                }
                                (username.clone(), password.unwrap())
                            } else {
                                // Implement SCRAM-SHA-256 protocol
                                if !quiet_connections {
                                    info!("🔐 Starting SCRAM-SHA-256 authentication for client {}", peer_addr_str);
                                }
                                debug!("📨 SCRAM Initial Response: {}", initial_response);

                                // Parse client-first message
                                let (scram_username, client_nonce) =
                                    match parse_scram_client_first(&initial_response) {
                                        Ok((u, n)) => (u, n),
                                        Err(e) => {
                                            error!("❌ Failed to parse SCRAM client-first from {}: {}", peer_addr_str, e);
                                            let error_response = create_postgres_error_response("28P01", &format!("Invalid SCRAM client-first: {}", e));
                                            socket.write_all(&error_response).await?;
                                            return Ok(());
                                        }
                                    };

                                info!(
                                    "👤 SCRAM username: '{}', client nonce: '{}'",
                                    scram_username, client_nonce
                                );

                                // Generate server-first message
                                let (server_first, mut scram_context) =
                                    scram_sha256_server_first_message(
                                        &client_nonce,
                                        &scram_username,
                                    );
                                scram_context.client_first_bare =
                                    format!("n={},r={}", scram_username, client_nonce);
                                scram_context.stage = ScramStage::Continue;

                                // We'll handle SCRAM context storage after authentication completes

                                debug!("📨 Sending SCRAM server-first: {}", server_first);

                                // Send SASL Continue with server-first
                                let continue_response =
                                    create_postgres_sasl_continue_response(&server_first);
                                socket.write_all(&continue_response).await?;

                                // Wait for client-final message
                                let mut client_final_buffer = [0; 1024];
                                let client_final_n = socket.read(&mut client_final_buffer).await?;
                                if client_final_n == 0 {
                                    if !quiet_connections {
                                        warn!(
                                            "⚠️  Client {} disconnected during SCRAM client-final",
                                            peer_addr_str
                                        );
                                    }
                                    return Ok(());
                                }

                                debug!(
                                    "📊 Received {} bytes SCRAM client-final from {}",
                                    client_final_n, peer_addr_str
                                );

                                // Parse SASL Response (client-final)
                                let client_final_data =
                                    match parse_sasl_response(&client_final_buffer[..client_final_n])
                                    {
                                        Ok(data) => data,
                                        Err(e) => {
                                            error!("❌ Failed to parse SCRAM client-final from {}: {}", peer_addr_str, e);
                                            let error_response = create_postgres_error_response("28P01", &format!("Invalid SCRAM client-final: {}", e));
                                            socket.write_all(&error_response).await?;
                                            return Ok(());
                                        }
                                    };

                                debug!("📨 SCRAM client-final: {}", client_final_data);

                                // Parse client-final message
                                let (client_final_without_proof, client_proof) =
                                    match parse_scram_client_final(&client_final_data) {
                                        Ok((cf, cp)) => (cf, cp),
                                        Err(e) => {
                                            error!("❌ Failed to parse SCRAM client-final content from {}: {}", peer_addr_str, e);
                                            let error_response = create_postgres_error_response("28P01", &format!("Invalid SCRAM client-final format: {}", e));
                                            socket.write_all(&error_response).await?;
                                            return Ok(());
                                        }
                                    };

                                // Get known password for verification
                                let known_password = match scram_username.as_str() {
                                    "username1" => "password1",
                                    "grafana" => "password1",
                                    "testuser" => "password1",
                                    _ => {
                                        warn!(
                                            "⚠️  Unknown user '{}' for SCRAM authentication",
                                            scram_username
                                        );
                                        let error_response = create_postgres_error_response(
                                            "28000",
                                            "Authentication failed",
                                        );
                                        socket.write_all(&error_response).await?;
                                        return Ok(());
                                    }
                                };

                                // Verify client proof and generate server-final
                                // Use the scram_context directly since we're in the same scope
                                match scram_sha256_verify_client_proof(
                                    &scram_context,
                                    &client_final_without_proof,
                                    &client_proof,
                                    known_password,
                                ) {
                                    Ok(server_final) => {
                                        if !quiet_connections {
                                            info!("✅ SCRAM-SHA-256 authentication successful for user '{}'", scram_username);
                                        }
                                        debug!("📨 Sending SCRAM server-final: {}", server_final);

                                        // Send SASL Final
                                        let final_response =
                                            create_postgres_sasl_final_response(&server_final);
                                        socket.write_all(&final_response).await?;

                                        // Authentication successful - use the SCRAM username and a dummy password for GraphQL
                                        (scram_username, known_password.to_string())
                                    }
                                    Err(e) => {
                                        error!("❌ SCRAM-SHA-256 verification failed for user '{}' from {}: {}", scram_username, peer_addr_str, e);
                                        let error_response = create_postgres_error_response(
                                            "28P01",
                                            "Authentication failed",
                                        );
                                        socket.write_all(&error_response).await?;
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "❌ Failed to parse SASL Initial Response from {}: {}",
                                peer_addr_str, e
                            );
                            error!(
                                "🔍 SASL message hex dump: {}",
                                hex::encode(&auth_buffer[..auth_n.min(64)])
                            );
                            let error_response = create_postgres_error_response(
                                "28P01",
                                &format!("Invalid SASL message: {}", e),
                            );
                            socket.write_all(&error_response).await?;
                            return Ok(());
                        }
                    }
                } else {
                    error!(
                        "❌ Expected SASL Initial Response from SCRAM client {}",
                        peer_addr_str
                    );
                    error!(
                        "🔍 Received message hex dump: {}",
                        hex::encode(&auth_buffer[..auth_n.min(64)])
                    );
                    let error_response = create_postgres_error_response(
                        "28P01",
                        "Expected SASL Initial Response",
                    );
                    socket.write_all(&error_response).await?;
                    return Ok(());
                }
            }
            AuthContext::Md5(_salt) => {
                // Parse password from response (handles both cleartext and MD5)
                let password = parse_postgres_password(&auth_buffer[..auth_n]);
                if password.is_none() {
                    error!("❌ Invalid password format from {}", peer_addr_str);
                    error!(
                        "🔍 Password message hex dump: {}",
                        hex::encode(&auth_buffer[..auth_n.min(64)])
                    );
                    if auth_n > 0 {
                        error!(
                            "🔍 First byte: 0x{:02x} (expected 'p' = 0x70)",
                            auth_buffer[0]
                        );
                    }
                    let error_response =
                        create_postgres_error_response("28P01", "Invalid password format");
                    socket.write_all(&error_response).await?;
                    return Ok(());
                }
                (username.clone(), password.unwrap())
            }
        };

        if !quiet_connections {
            info!(
                "🔑 Authenticating user '{}' from {} via GraphQL",
                username_final, peer_addr_str
            );
        }

        // Handle MD5 authentication
        let (is_md5_valid, actual_password) = if password_final.starts_with("md5") {
            if !quiet_connections {
                info!("🔐 Received MD5 password response from {}", peer_addr_str);
            }
            debug!("🔍 MD5 response: {}", password_final);

            // For MD5 verification, we need to know the original password
            // In a real implementation, you'd store password hashes in a database
            // For now, we'll hardcode known user credentials for testing
            let known_password = match username_final.as_str() {
                "username1" => "password1",
                "grafana" => "password1", // Allow grafana user with same password
                "testuser" => "password1",
                _ => {
                    warn!(
                        "⚠️  Unknown user '{}' for MD5 authentication",
                        username_final
                    );
                    ""
                }
            };

            if known_password.is_empty() {
                (false, String::new())
            } else {
                let is_valid = match &auth_context {
                    AuthContext::Md5(salt) => {
                        let expected_hash =
                            compute_postgres_md5_hash(&username_final, known_password, salt);
                        debug!("🔍 Expected MD5 hash: {}", expected_hash);
                        debug!("🔍 Received MD5 hash: {}", password_final);
                        let valid = verify_postgres_md5_auth(
                            &username_final,
                            known_password,
                            salt,
                            &password_final,
                        );
                        if !quiet_connections {
                            info!(
                                "🔍 MD5 verification for user '{}': {}",
                                username_final,
                                if valid { "✅ PASSED" } else { "❌ FAILED" }
                            );
                        }
                        valid
                    }
                    AuthContext::Scram => {
                        if !quiet_connections {
                            info!(
                                "🔍 SCRAM-SHA-256 verification for user '{}' (not fully implemented)",
                                username_final
                            );
                        }
                        true // For now, accept SCRAM attempts
                    }
                };
                (is_valid, known_password.to_string())
            }
        } else {
            if !quiet_connections {
                info!("🔐 Received cleartext password from {}", peer_addr_str);
            }
            (true, password_final)
        };

        if !is_md5_valid {
            error!(
                "❌ Authentication failed for user '{}' from {}",
                username_final, peer_addr_str
            );
            let error_response =
                create_postgres_error_response("28P01", "MD5 authentication failed");
            socket.write_all(&error_response).await?;
            return Ok(());
        }

        // Authenticate with GraphQL using the actual password
        let authenticated_session =
            match session_manager.authenticate(&username_final, &actual_password).await {
                Ok(session) => {
                    if !quiet_connections {
                        info!(
                            "✅ Authentication successful for user '{}' from {}",
                            username_final, peer_addr_str
                        );
                    }

                    // Send authentication OK response
                    let auth_ok_response = create_postgres_auth_ok_response();
                    debug!("📤 Sending authentication OK to {}", peer_addr_str);
                    if let Err(e) = socket.write_all(&auth_ok_response).await {
                        error!("❌ Failed to send auth OK to {}: {}", peer_addr_str, e);
                        return Ok(());
                    }

                    session
                }
                Err(e) => {
                    error!(
                        "❌ Authentication failed for user '{}' from {}: {}",
                        username_final, peer_addr_str, e
                    );
                    let error_response = create_postgres_error_response(
                        "28P01",
                        &format!("Authentication failed: {}", e),
                    );
                    socket.write_all(&error_response).await?;
                    return Ok(());
                }
            };

        // Register the connection after successful authentication
        connection_id = if let Some(addr) = socket_addr {
            match session_manager.register_connection(
                &authenticated_session.session_id,
                addr,
                application_name.clone(),
            ).await {
                Ok(id) => Some(id),
                Err(e) => {
                    error!("❌ Failed to register connection: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Main query processing loop
        if !quiet_connections {
            info!("🔄 Starting PostgreSQL query loop for {}", peer_addr_str);
        }
        let mut buffer = vec![0; 4096];
        
        // Set up keep-alive interval
        let mut keep_alive_timer = interval(Duration::from_secs(keep_alive_interval));
        keep_alive_timer.tick().await; // Skip the immediate first tick

        loop {
            debug!("📖 Waiting for PostgreSQL query from {}", peer_addr_str);

            // Use tokio::select to handle both data and keep-alive timer
            tokio::select! {
                // Handle incoming data with a timeout
                result = timeout(Duration::from_secs(60), socket.read(&mut buffer)) => {
                    match result {
                        Ok(Ok(n)) => {
                            if n == 0 {
                                if !quiet_connections {
                                    info!("🔌 PostgreSQL connection closed by client {}", peer_addr_str);
                                }
                                break;
                            }
                            
                            // Process the received data

            debug!(
                "📊 Received {} bytes from PostgreSQL client {}",
                n, peer_addr_str
            );

            let mut pos = 0;
            let mut response_buffer = Vec::new();
            
            // Log all incoming messages in this batch
            debug!("📨 Processing batch of {} bytes from {}", n, peer_addr_str);
            let mut temp_pos = 0;
            while temp_pos < n && temp_pos + 5 <= n {
                let msg_type = buffer[temp_pos] as char;
                let msg_len = u32::from_be_bytes([
                    buffer[temp_pos + 1],
                    buffer[temp_pos + 2],
                    buffer[temp_pos + 3],
                    buffer[temp_pos + 4],
                ]) as usize;
                debug!("   Incoming message: type='{}' length={}", msg_type, msg_len);
                temp_pos += 1 + msg_len;
                if temp_pos > n {
                    break;
                }
            }
            
            while pos < n {
                let message_slice = &buffer[pos..n];
                if message_slice.len() < 5 {
                    // Not enough data for a full message header
                    break;
                }

                let message_len = u32::from_be_bytes([
                    message_slice[1],
                    message_slice[2],
                    message_slice[3],
                    message_slice[4],
                ]) as usize;
                
                let total_message_len = 1 + message_len;

                if message_slice.len() < total_message_len {
                    // Incomplete message in the buffer
                    break;
                }

                match handle_postgres_message(
                    &message_slice[..total_message_len],
                    &mut connection_state,
                    &authenticated_session,
                    session_manager.clone(),
                    connection_id,
                    quiet_connections,
                )
                .await
                {
                    Ok(response) => {
                        if !response.is_empty() {
                            debug!("📤 Adding {} bytes to response buffer for message type '{}'", 
                                response.len(), 
                                message_slice[0] as char
                            );
                            response_buffer.extend_from_slice(&response);
                        }
                    }
                    Err(e) => {
                        if e.to_string() == "TERMINATE_CONNECTION" {
                            if !quiet_connections {
                                info!("👋 Client {} requested connection termination", peer_addr_str);
                            }
                            // Unregister the connection before returning
                            if let Some(conn_id) = connection_id {
                                session_manager.unregister_connection(conn_id).await;
                            }
                            return Ok(());
                        } else {
                            error!("❌ Error for {}: {}", peer_addr_str, e);
                            let mut error_response = create_postgres_error_response(
                                "42000",
                                &format!("Query failed: {}", e),
                            );
                            error_response.extend_from_slice(&super::response::create_ready_for_query_response());
                            response_buffer.extend_from_slice(&error_response);
                        }
                    }
                }
                pos += total_message_len;
            }

            if !response_buffer.is_empty() {
                debug!(
                    "📤 Sending PostgreSQL response to {} ({} bytes)",
                    peer_addr_str,
                    response_buffer.len()
                );
                
                
                socket.write_all(&response_buffer).await?;
            }
                        }
                        Ok(Err(e)) => {
                            error!("❌ Read error from {}: {}", peer_addr_str, e);
                            break;
                        }
                        Err(_) => {
                            warn!("⏰ Read timeout from {} (60s)", peer_addr_str);
                            // Connection might be stale, but we'll try keep-alive first
                        }
                    }
                }
                
                // Keep-alive timer fired
                _ = keep_alive_timer.tick() => {
                    debug!("💓 Keep-alive timer fired for {}", peer_addr_str);
                    
                    // Send a keep-alive probe
                    match send_keep_alive_probe(&mut socket).await {
                        Ok(true) => {
                            // Keep-alive probe successful
                            if let Some(conn_id) = connection_id {
                                session_manager.update_last_alive_sent(conn_id).await;
                            }
                            
                            // Also send a PostgreSQL-level keep-alive (ParameterStatus)
                            let keepalive_msg = create_parameter_status_keepalive();
                            if let Err(e) = socket.write_all(&keepalive_msg).await {
                                warn!("⚠️ Failed to send PostgreSQL keep-alive to {}: {}", peer_addr_str, e);
                                break;
                            }
                        }
                        Ok(false) => {
                            // Connection is dead
                            warn!("💔 Keep-alive detected dead connection to {}", peer_addr_str);
                            break;
                        }
                        Err(e) => {
                            warn!("⚠️ Keep-alive error for {}: {}", peer_addr_str, e);
                            // Continue anyway, might be temporary
                        }
                    }
                }
            }
        }
    } else {
        warn!("❌ Unsupported PostgreSQL protocol version: 0x{:08x}", version);

        let error_response = create_postgres_error_response(
            "08P01", // Connection exception - protocol violation
            &format!(
                "Unsupported protocol version: 0x{:08x}. Expected PostgreSQL v3.0 (0x00030000).",
                version
            ),
        );

        if let Err(e) = socket.write_all(&error_response).await {
            error!("❌ Failed to send error response to {}: {}", peer_addr_str, e);
        }
    }

    // Unregister the connection if it was registered
    if let Some(conn_id) = connection_id {
        session_manager.unregister_connection(conn_id).await;
    }

    if !quiet_connections {
        info!("🔌 PostgreSQL connection attempt from {} handled", peer_addr_str);
    }
    Ok(())
}

fn parse_startup_parameters(data: &[u8]) -> std::collections::HashMap<String, String> {
    let mut params = std::collections::HashMap::new();
    let mut pos = 0;

    while pos < data.len() {
        // Find null-terminated parameter name
        let name_start = pos;
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }

        if pos >= data.len() {
            break;
        }

        let name = String::from_utf8_lossy(&data[name_start..pos]).to_string();
        pos += 1; // Skip null terminator

        if name.is_empty() {
            break; // End of parameters
        }

        // Find null-terminated parameter value
        let value_start = pos;
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }

        if pos >= data.len() {
            break;
        }

        let value = String::from_utf8_lossy(&data[value_start..pos]).to_string();
        pos += 1; // Skip null terminator

        params.insert(name, value);
    }

    params
}

async fn read_complete_postgres_message_stream<T>(
    socket: &mut T,
    initial_data: &[u8],
) -> Result<Vec<u8>>
where
    T: AsyncRead + Unpin,
{
    if initial_data.len() < 4 {
        return Err(anyhow::anyhow!("Not enough data for message length"));
    }

    let expected_length = u32::from_be_bytes([
        initial_data[0],
        initial_data[1],
        initial_data[2],
        initial_data[3],
    ]) as usize;
    let mut complete_message = Vec::with_capacity(expected_length);
    complete_message.extend_from_slice(initial_data);

    // Read remaining bytes if needed
    while complete_message.len() < expected_length {
        let mut buffer = vec![0; expected_length - complete_message.len()];
        let n = socket.read(&mut buffer).await?;
        if n == 0 {
            return Err(anyhow::anyhow!(
                "Connection closed while reading message"
            ));
        }
        complete_message.extend_from_slice(&buffer[..n]);
    }

    Ok(complete_message)
}
