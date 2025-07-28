use crate::auth::SessionManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};
use anyhow::Result;

use super::authentication::{create_postgres_md5_request, create_postgres_scram_sha256_request, parse_postgres_password, parse_sasl_initial_response, AuthContext, parse_scram_client_first, scram_sha256_server_first_message, create_postgres_sasl_continue_response, parse_sasl_response, parse_scram_client_final, scram_sha256_verify_client_proof, create_postgres_sasl_final_response, compute_postgres_md5_hash, verify_postgres_md5_auth};
use super::message_handler::handle_postgres_message;
use super::response::{create_postgres_auth_ok_response, create_postgres_error_response};
use super::{ConnectionState, ScramStage};

pub(super) async fn handle_postgres_startup(
    mut socket: TcpStream,
    session_manager: Arc<SessionManager>,
    data: &[u8],
    no_auth_config: Option<(String, String)>,
) -> Result<()> {
    let peer_addr = socket.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
    info!("🐘 Handling PostgreSQL startup from {}", peer_addr);

    if data.len() < 8 {
        error!(
            "❌ Invalid startup message length from {}: {} bytes",
            peer_addr,
            data.len()
        );
        return Ok(());
    }

    // Ensure we have the complete message
    let complete_data = match read_complete_postgres_message(&mut socket, data).await {
        Ok(data) => data,
        Err(e) => {
            error!(
                "❌ Failed to read complete startup message from {}: {}",
                peer_addr, e
            );
            return Ok(());
        }
    };

    let length =
        u32::from_be_bytes([complete_data[0], complete_data[1], complete_data[2], complete_data[3]]);
    let version =
        u32::from_be_bytes([complete_data[4], complete_data[5], complete_data[6], complete_data[7]]);

    info!(
        "📋 Startup message: length={}, version={} (0x{:08x})",
        length, version, version
    );

    // Dump full startup message for debugging
    info!("🔍 Full startup message dump from {}:", peer_addr);
    info!("   📏 Total length: {} bytes", complete_data.len());
    info!("   📊 Message length field: {} bytes", length);
    info!("   🔢 Protocol version: {} (0x{:08x})", version, version);

    // Hex dump of the entire startup message
    let hex_dump = hex::encode(&complete_data);
    info!("   🔍 Hex dump (full message): {}", hex_dump);

    // ASCII interpretation (printable characters only)
    let ascii_dump: String = complete_data
        .iter()
        .map(|&b| if b >= 32 && b <= 126 { b as char } else { '.' })
        .collect();
    info!("   📝 ASCII dump: {}", ascii_dump);

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
            info!("📋 Client connection parameters from {}:", peer_addr);
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
            if params.is_empty() {
                warn!("⚠️  No parameters found in startup message");
                debug!(
                    "🔍 Raw parameter data: {:?}",
                    String::from_utf8_lossy(params_data)
                );
            } else {
                info!("📊 Total parameters received: {}", params.len());
            }
        }

        // Extract username from startup parameters for authentication
        let username = if complete_data.len() > 8 {
            let params_data = &complete_data[8..];
            let params = parse_startup_parameters(params_data);
            debug!("🔍 All startup parameters: {:?}", params);

            let user = params.get("user").cloned().unwrap_or_else(|| {
                error!(
                    "❌ No 'user' parameter found in startup message from {}",
                    peer_addr
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

            user
        } else {
            warn!(
                "⚠️  Startup message too short from {}: {} bytes",
                peer_addr,
                data.len()
            );
            "unknown".to_string()
        };

        info!(
            "🔐 PostgreSQL client {} requesting authentication for user: {}",
            peer_addr, username
        );

        // Check if no-auth mode is enabled
        if let Some((no_auth_username, no_auth_password)) = &no_auth_config {
            info!(
                "🔓 No-auth mode: bypassing PostgreSQL authentication for client {}",
                peer_addr
            );
            info!(
                "🔓 Using configured credentials: username='{}' for GraphQL authentication",
                no_auth_username
            );

            // Skip PostgreSQL authentication and directly authenticate with GraphQL
            let authenticated_session =
                match session_manager.authenticate(no_auth_username, no_auth_password).await {
                    Ok(session) => {
                        info!("✅ No-auth GraphQL authentication successful for configured user '{}' from {}", no_auth_username, peer_addr);

                        // Send authentication OK response immediately
                        let auth_ok_response = create_postgres_auth_ok_response();
                        debug!(
                            "📤 Sending authentication OK to {} (no-auth mode)",
                            peer_addr
                        );
                        if let Err(e) = socket.write_all(&auth_ok_response).await {
                            error!("❌ Failed to send auth OK to {}: {}", peer_addr, e);
                            return Ok(());
                        }

                        session
                    }
                    Err(e) => {
                        error!("❌ No-auth GraphQL authentication failed for user '{}' from {}: {}", no_auth_username, peer_addr, e);
                        let error_response = create_postgres_error_response(
                            "28P01",
                            &format!("GraphQL authentication failed: {}", e),
                        );
                        socket.write_all(&error_response).await?;
                        return Ok(());
                    }
                };

            // Skip to query processing loop
            info!(
                "🔄 Starting PostgreSQL query loop for {} (no-auth mode)",
                peer_addr
            );
            let mut buffer = [0; 4096];

            loop {
                debug!("📖 Waiting for PostgreSQL query from {}", peer_addr);

                let n = socket.read(&mut buffer).await?;
                if n == 0 {
                    info!("🔌 PostgreSQL connection closed by client {}", peer_addr);
                    break;
                }

                debug!(
                    "📊 Received {} bytes from PostgreSQL client {}",
                    n, peer_addr
                );

                // Handle PostgreSQL messages (both Simple and Extended Query Protocol)
                match handle_postgres_message(&buffer[..n], &mut connection_state, &authenticated_session).await {
                    Ok(response) => {
                        if !response.is_empty() {
                            debug!("📤 Sending PostgreSQL response to {} ({} bytes)", peer_addr, response.len());
                            socket.write_all(&response).await?;
                        }
                    }
                    Err(e) => {
                        // Check if this is a terminate request
                        if e.to_string() == "TERMINATE_CONNECTION" {
                            info!("👋 Client {} requested connection termination (no-auth mode)", peer_addr);
                            break; // Exit the query loop gracefully
                        } else if e.to_string() == "INCOMPLETE_MESSAGE" {
                            // Incomplete message is normal, just continue waiting for more data
                            debug!("📨 Incomplete message from {}, waiting for more data", peer_addr);
                            continue;
                        } else {
                            error!("❌ Message processing error for {}: {}", peer_addr, e);
                            let mut error_response = create_postgres_error_response("42000", &format!("Query failed: {}", e));

                            // Add ready-for-query message after error to prevent client hang
                            error_response.push(b'Z');
                            error_response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
                            error_response.push(b'I'); // Status: 'I' = idle

                            socket.write_all(&error_response).await?;
                        }
                    }
                }
            }

            return Ok(());
        }

        // Normal authentication flow
        // Choose authentication method:
        // 1. Use MD5 by default for maximum compatibility (psycopg2, etc.)
        // 2. SCRAM-SHA-256 available but not default due to limited client support
        // Note: For SCRAM, username comes in SASL Initial Response, not startup message

        let prefer_scram = false; // Use MD5 for better compatibility with Python clients

        let (auth_request, auth_context) = if prefer_scram {
            info!("🔐 Offering SCRAM-SHA-256 authentication (preferred method)");
            if username == "unknown" {
                info!("   💡 Username will be provided in SASL Initial Response");
            } else {
                info!("   👤 Startup username: {}", username);
            }
            (
                create_postgres_scram_sha256_request(),
                AuthContext::Scram,
            )
        } else {
            info!("🔐 Sending MD5 authentication request");
            let (auth_request, salt) = create_postgres_md5_request();
            debug!(
                "🧂 Generated salt for MD5 auth: {:02x}{:02x}{:02x}{:02x}",
                salt[0], salt[1], salt[2], salt[3]
            );
            (auth_request, AuthContext::Md5(salt))
        };

        debug!("📤 Sending password authentication request to {}", peer_addr);
        if let Err(e) = socket.write_all(&auth_request).await {
            error!("❌ Failed to send auth request to {}: {}", peer_addr, e);
            return Ok(());
        }

        // Wait for authentication response (SASL or password)
        if matches!(auth_context, AuthContext::Scram) {
            debug!("📖 Waiting for SASL Initial Response from {}", peer_addr);
        } else {
            debug!("📖 Waiting for password response from {}", peer_addr);
        }

        let mut auth_buffer = [0; 1024];
        let auth_n = socket.read(&mut auth_buffer).await?;
        if auth_n == 0 {
            warn!("⚠️  Client {} disconnected during authentication", peer_addr);
            return Ok(());
        }

        debug!(
            "📊 Received {} bytes authentication response from {}",
            auth_n, peer_addr
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
                                    warn!(
                                        "⚠️  Client {} disconnected during MD5 fallback",
                                        peer_addr
                                    );
                                    return Ok(());
                                }

                                let password = parse_postgres_password(&password_buffer[..password_n]);
                                if password.is_none() {
                                    error!("❌ Invalid password format during MD5 fallback from {}", peer_addr);
                                    let error_response = create_postgres_error_response("28P01", "Invalid password format");
                                    socket.write_all(&error_response).await?;
                                    return Ok(());
                                }
                                (username.clone(), password.unwrap())
                            } else {
                                // Implement SCRAM-SHA-256 protocol
                                info!("🔐 Starting SCRAM-SHA-256 authentication for client {}", peer_addr);
                                debug!("📨 SCRAM Initial Response: {}", initial_response);

                                // Parse client-first message
                                let (scram_username, client_nonce) =
                                    match parse_scram_client_first(&initial_response) {
                                        Ok((u, n)) => (u, n),
                                        Err(e) => {
                                            error!("❌ Failed to parse SCRAM client-first from {}: {}", peer_addr, e);
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
                                    warn!(
                                        "⚠️  Client {} disconnected during SCRAM client-final",
                                        peer_addr
                                    );
                                    return Ok(());
                                }

                                debug!(
                                    "📊 Received {} bytes SCRAM client-final from {}",
                                    client_final_n, peer_addr
                                );

                                // Parse SASL Response (client-final)
                                let client_final_data =
                                    match parse_sasl_response(&client_final_buffer[..client_final_n])
                                    {
                                        Ok(data) => data,
                                        Err(e) => {
                                            error!("❌ Failed to parse SCRAM client-final from {}: {}", peer_addr, e);
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
                                            error!("❌ Failed to parse SCRAM client-final content from {}: {}", peer_addr, e);
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
                                        info!("✅ SCRAM-SHA-256 authentication successful for user '{}'", scram_username);
                                        debug!("📨 Sending SCRAM server-final: {}", server_final);

                                        // Send SASL Final
                                        let final_response =
                                            create_postgres_sasl_final_response(&server_final);
                                        socket.write_all(&final_response).await?;

                                        // Authentication successful - use the SCRAM username and a dummy password for GraphQL
                                        (scram_username, known_password.to_string())
                                    }
                                    Err(e) => {
                                        error!("❌ SCRAM-SHA-256 verification failed for user '{}' from {}: {}", scram_username, peer_addr, e);
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
                                peer_addr, e
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
                        peer_addr
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
                    error!("❌ Invalid password format from {}", peer_addr);
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

        info!(
            "🔑 Authenticating user '{}' from {} via GraphQL",
            username_final, peer_addr
        );

        // Handle MD5 authentication
        let (is_md5_valid, actual_password) = if password_final.starts_with("md5") {
            info!("🔐 Received MD5 password response from {}", peer_addr);
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
                        info!(
                            "🔍 MD5 verification for user '{}': {}",
                            username_final,
                            if valid { "✅ PASSED" } else { "❌ FAILED" }
                        );
                        valid
                    }
                    AuthContext::Scram => {
                        info!(
                            "🔍 SCRAM-SHA-256 verification for user '{}' (not fully implemented)",
                            username_final
                        );
                        true // For now, accept SCRAM attempts
                    }
                };
                (is_valid, known_password.to_string())
            }
        } else {
            info!("🔐 Received cleartext password from {}", peer_addr);
            (true, password_final)
        };

        if !is_md5_valid {
            error!(
                "❌ Authentication failed for user '{}' from {}",
                username_final, peer_addr
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
                    info!(
                        "✅ Authentication successful for user '{}' from {}",
                        username_final, peer_addr
                    );

                    // Send authentication OK response
                    let auth_ok_response = create_postgres_auth_ok_response();
                    debug!("📤 Sending authentication OK to {}", peer_addr);
                    if let Err(e) = socket.write_all(&auth_ok_response).await {
                        error!("❌ Failed to send auth OK to {}: {}", peer_addr, e);
                        return Ok(());
                    }

                    session
                }
                Err(e) => {
                    error!(
                        "❌ Authentication failed for user '{}' from {}: {}",
                        username_final, peer_addr, e
                    );
                    let error_response = create_postgres_error_response(
                        "28P01",
                        &format!("Authentication failed: {}", e),
                    );
                    socket.write_all(&error_response).await?;
                    return Ok(());
                }
            };

        // Now try to handle simple queries
        info!("🔄 Starting PostgreSQL query loop for {}", peer_addr);
        let mut buffer = [0; 4096];

        loop {
            debug!("📖 Waiting for PostgreSQL query from {}", peer_addr);

            let n = socket.read(&mut buffer).await?;
            if n == 0 {
                info!("🔌 PostgreSQL connection closed by client {}", peer_addr);
                break;
            }

            debug!(
                "📊 Received {} bytes from PostgreSQL client {}",
                n, peer_addr
            );

            // Handle PostgreSQL messages (both Simple and Extended Query Protocol)
            match handle_postgres_message(&buffer[..n], &mut connection_state, &authenticated_session).await {
                Ok(response) => {
                    if !response.is_empty() {
                        debug!("📤 Sending PostgreSQL response to {} ({} bytes)", peer_addr, response.len());
                        socket.write_all(&response).await?;
                    }
                }
                Err(e) => {
                    // Check if this is a terminate request
                    if e.to_string() == "TERMINATE_CONNECTION" {
                        info!("👋 Client {} requested connection termination", peer_addr);
                        break; // Exit the query loop gracefully
                    } else if e.to_string() == "INCOMPLETE_MESSAGE" {
                        // Incomplete message is normal, just continue waiting for more data
                        debug!("📨 Incomplete message from {}, waiting for more data", peer_addr);
                        continue;
                    } else {
                        error!("❌ Message processing error for {}: {}", peer_addr, e);
                        let mut error_response = create_postgres_error_response("42000", &format!("Query failed: {}", e));

                        // Add ready-for-query message after error to prevent client hang
                        error_response.push(b'Z');
                        error_response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
                        error_response.push(b'I'); // Status: 'I' = idle

                        socket.write_all(&error_response).await?;
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
            error!("❌ Failed to send error response to {}: {}", peer_addr, e);
        }
    }

    info!("🔌 PostgreSQL connection attempt from {} handled", peer_addr);
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

async fn read_complete_postgres_message(
    socket: &mut TcpStream,
    initial_data: &[u8],
) -> Result<Vec<u8>> {
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
