use crate::auth::SessionManager;
use anyhow::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, error, debug, warn, trace};

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
enum ScramStage {
    Initial,      // Waiting for SASLInitialResponse  
    Continue,     // Sent server-first, waiting for client-final
    #[allow(dead_code)]
    Final,        // Sent server-final, authentication complete
}

pub struct SimpleServer {
    session_manager: Arc<SessionManager>,
    no_auth_config: Option<(String, String)>, // (username, password) for no-auth mode
}

impl SimpleServer {
    pub fn new(graphql_url: String, no_auth_config: Option<(String, String)>) -> Self {
        Self {
            session_manager: Arc::new(SessionManager::new(graphql_url)),
            no_auth_config,
        }
    }

    pub async fn start(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Simple PostgreSQL-like server listening on {}", addr);

        loop {
            debug!("🎧 Waiting for new connections...");
            
            let (socket, client_addr) = listener.accept().await?;
            info!("🌟 Accepted new connection from {}", client_addr);

            let session_manager = self.session_manager.clone();
            let no_auth_config = self.no_auth_config.clone();
            tokio::spawn(async move {
                debug!("🚀 Starting connection handler for {}", client_addr);
                
                if let Err(e) = handle_connection(socket, session_manager, no_auth_config).await {
                    error!("💥 Error handling connection from {}: {}", client_addr, e);
                } else {
                    debug!("✅ Connection handler completed successfully for {}", client_addr);
                }
                
                info!("👋 Connection from {} closed", client_addr);
            });
        }
    }
}

async fn handle_connection(mut socket: TcpStream, session_manager: Arc<SessionManager>, no_auth_config: Option<(String, String)>) -> Result<()> {
    let peer_addr = socket.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
    info!("🔌 New connection established from {}", peer_addr);
    
    
    // Read first few bytes to see what kind of connection this is
    let mut peek_buffer = [0; 32];
    debug!("📖 Reading initial data from {}", peer_addr);
    
    let n = socket.read(&mut peek_buffer).await?;
    if n == 0 {
        warn!("⚠️  Connection from {} closed immediately (no data received)", peer_addr);
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
        debug!("📖 Waiting for startup message after SSL rejection from {}", peer_addr);
        
        let mut startup_buffer = [0; 1024];
        let startup_n = socket.read(&mut startup_buffer).await?;
        if startup_n == 0 {
            warn!("⚠️  Client {} disconnected after SSL rejection", peer_addr);
            return Ok(());
        }
        
        debug!("📊 Received {} bytes after SSL rejection from {}", startup_n, peer_addr);
        trace!("🔍 Startup message bytes: {:02x?}", &startup_buffer[..startup_n]);
        
        // Now handle the startup message as a regular PostgreSQL connection
        return handle_postgres_startup(socket, session_manager, &startup_buffer[..startup_n], no_auth_config).await;
    }
    
    // Check if this looks like PostgreSQL wire protocol (non-SSL)
    else if n >= 8 && is_postgres_wire_protocol(&peek_buffer[..n]) {
        warn!("🐘 PostgreSQL wire protocol detected from {}!", peer_addr);
        
        // For now, attempt to handle it as PostgreSQL startup
        return handle_postgres_startup(socket, session_manager, &peek_buffer[..n], no_auth_config).await;
    }
    
    // Try to interpret as simple text protocol
    let initial_data = String::from_utf8_lossy(&peek_buffer[..n]);
    debug!("📄 Initial data as text: {:?}", initial_data);
    
    // Check if this looks like an authentication attempt
    if initial_data.contains(':') {
        info!("🔐 Processing authentication attempt from {}", peer_addr);
        return handle_simple_text_protocol(socket, session_manager, initial_data.to_string(), no_auth_config).await;
    }
    
    // If we can't identify the protocol, read more data
    warn!("❓ Unknown protocol from {}. Trying to read more data...", peer_addr);
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
                            return handle_simple_text_protocol(socket, session_manager, full_data.to_string(), no_auth_config).await;
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
    
    error!("❌ Unable to identify protocol from {}. Closing connection.", peer_addr);
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
    trace!("🔍 Postgres check: length={}, version={} (0x{:08x})", length, version, version);
    
    version == 196608 || version == 80877103 || version == 80877102 || 
    (length > 8 && length < 10000 && version > 0)
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

async fn handle_postgres_startup(mut socket: TcpStream, session_manager: Arc<SessionManager>, data: &[u8], no_auth_config: Option<(String, String)>) -> Result<()> {
    let peer_addr = socket.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
    info!("🐘 Handling PostgreSQL startup from {}", peer_addr);
    
    if data.len() < 8 {
        error!("❌ Invalid startup message length from {}: {} bytes", peer_addr, data.len());
        return Ok(());
    }
    
    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    
    info!("📋 Startup message: length={}, version={} (0x{:08x})", length, version, version);
    
    // Dump full startup message for debugging
    info!("🔍 Full startup message dump from {}:", peer_addr);
    info!("   📏 Total length: {} bytes", data.len());
    info!("   📊 Message length field: {} bytes", length);
    info!("   🔢 Protocol version: {} (0x{:08x})", version, version);
    
    // Hex dump of the entire startup message
    let hex_dump = hex::encode(data);
    info!("   🔍 Hex dump (full message): {}", hex_dump);
    
    // ASCII interpretation (printable characters only)
    let ascii_dump: String = data.iter()
        .map(|&b| if b >= 32 && b <= 126 { b as char } else { '.' })
        .collect();
    info!("   📝 ASCII dump: {}", ascii_dump);
    
    // Parse startup parameters if this is a v3.0 protocol message
    if version == 196608 { // PostgreSQL 3.0 protocol
        
        // Initialize connection state for Extended Query Protocol and SCRAM authentication
        let mut connection_state = ConnectionState {
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            scram_context: None,
        };
        debug!("✅ PostgreSQL 3.0 protocol detected");
        
        // Extract parameters (user, database, etc.)
        if data.len() > 8 {
            let params_data = &data[8..];
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
                debug!("🔍 Raw parameter data: {:?}", String::from_utf8_lossy(params_data));
            } else {
                info!("📊 Total parameters received: {}", params.len());
            }
        }
        
        // Extract username from startup parameters for authentication
        let username = if data.len() > 8 {
            let params_data = &data[8..];
            let params = parse_startup_parameters(params_data);
            debug!("🔍 All startup parameters: {:?}", params);
            
            let user = params.get("user").cloned().unwrap_or_else(|| {
                error!("❌ No 'user' parameter found in startup message from {}", peer_addr);
                error!("🔍 Available parameters: {:?}", params.keys().collect::<Vec<_>>());
                error!("🔍 This might be a Grafana or other client configuration issue");
                "unknown".to_string()
            });
            
            if user == "unknown" {
                error!("🔍 Startup message hex dump (first 128 bytes): {}", hex::encode(&data[..data.len().min(128)]));
                error!("💡 Check client configuration - ensure username is specified");
            }
            
            user
        } else {
            warn!("⚠️  Startup message too short from {}: {} bytes", peer_addr, data.len());
            "unknown".to_string()
        };
        
        info!("🔐 PostgreSQL client {} requesting authentication for user: {}", peer_addr, username);
        
        // Check if no-auth mode is enabled
        if let Some((no_auth_username, no_auth_password)) = &no_auth_config {
            info!("🔓 No-auth mode: bypassing PostgreSQL authentication for client {}", peer_addr);
            info!("🔓 Using configured credentials: username='{}' for GraphQL authentication", no_auth_username);
            
            // Skip PostgreSQL authentication and directly authenticate with GraphQL
            let authenticated_session = match session_manager.authenticate(no_auth_username, no_auth_password).await {
                Ok(session) => {
                    info!("✅ No-auth GraphQL authentication successful for configured user '{}' from {}", no_auth_username, peer_addr);
                    
                    // Send authentication OK response immediately
                    let auth_ok_response = create_postgres_auth_ok_response();
                    debug!("📤 Sending authentication OK to {} (no-auth mode)", peer_addr);
                    if let Err(e) = socket.write_all(&auth_ok_response).await {
                        error!("❌ Failed to send auth OK to {}: {}", peer_addr, e);
                        return Ok(());
                    }
                    
                    session
                }
                Err(e) => {
                    error!("❌ No-auth GraphQL authentication failed for user '{}' from {}: {}", no_auth_username, peer_addr, e);
                    let error_response = create_postgres_error_response("28P01", &format!("GraphQL authentication failed: {}", e));
                    socket.write_all(&error_response).await?;
                    return Ok(());
                }
            };
            
            // Skip to query processing loop
            info!("🔄 Starting PostgreSQL query loop for {} (no-auth mode)", peer_addr);
            let mut buffer = [0; 4096];
            
            loop {
                debug!("📖 Waiting for PostgreSQL query from {}", peer_addr);
                
                let n = socket.read(&mut buffer).await?;
                if n == 0 {
                    info!("🔌 PostgreSQL connection closed by client {}", peer_addr);
                    break;
                }
                
                debug!("📊 Received {} bytes from PostgreSQL client {}", n, peer_addr);
                
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
            (create_postgres_scram_sha256_request(), AuthContext::Scram)
        } else {
            info!("🔐 Sending MD5 authentication request");
            let (auth_request, salt) = create_postgres_md5_request();
            debug!("🧂 Generated salt for MD5 auth: {:02x}{:02x}{:02x}{:02x}", salt[0], salt[1], salt[2], salt[3]);
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
        
        debug!("📊 Received {} bytes authentication response from {}", auth_n, peer_addr);
        
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
                                    warn!("⚠️  Client {} disconnected during MD5 fallback", peer_addr);
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
                                let (scram_username, client_nonce) = match parse_scram_client_first(&initial_response) {
                                    Ok((u, n)) => (u, n),
                                    Err(e) => {
                                        error!("❌ Failed to parse SCRAM client-first from {}: {}", peer_addr, e);
                                        let error_response = create_postgres_error_response("28P01", &format!("Invalid SCRAM client-first: {}", e));
                                        socket.write_all(&error_response).await?;
                                        return Ok(());
                                    }
                                };
                                
                                info!("👤 SCRAM username: '{}', client nonce: '{}'", scram_username, client_nonce);
                                
                                // Generate server-first message
                                let (server_first, mut scram_context) = scram_sha256_server_first_message(&client_nonce, &scram_username);
                                scram_context.client_first_bare = format!("n={},r={}", scram_username, client_nonce);
                                scram_context.stage = ScramStage::Continue;
                                
                                // We'll handle SCRAM context storage after authentication completes
                                
                                debug!("📨 Sending SCRAM server-first: {}", server_first);
                                
                                // Send SASL Continue with server-first
                                let continue_response = create_postgres_sasl_continue_response(&server_first);
                                socket.write_all(&continue_response).await?;
                                
                                // Wait for client-final message
                                let mut client_final_buffer = [0; 1024];
                                let client_final_n = socket.read(&mut client_final_buffer).await?;
                                if client_final_n == 0 {
                                    warn!("⚠️  Client {} disconnected during SCRAM client-final", peer_addr);
                                    return Ok(());
                                }
                                
                                debug!("📊 Received {} bytes SCRAM client-final from {}", client_final_n, peer_addr);
                                
                                // Parse SASL Response (client-final)
                                let client_final_data = match parse_sasl_response(&client_final_buffer[..client_final_n]) {
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
                                let (client_final_without_proof, client_proof) = match parse_scram_client_final(&client_final_data) {
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
                                        warn!("⚠️  Unknown user '{}' for SCRAM authentication", scram_username);
                                        let error_response = create_postgres_error_response("28000", "Authentication failed");
                                        socket.write_all(&error_response).await?;
                                        return Ok(());
                                    }
                                };
                                
                                // Verify client proof and generate server-final
                                // Use the scram_context directly since we're in the same scope
                                match scram_sha256_verify_client_proof(&scram_context, &client_final_without_proof, &client_proof, known_password) {
                                    Ok(server_final) => {
                                        info!("✅ SCRAM-SHA-256 authentication successful for user '{}'", scram_username);
                                        debug!("📨 Sending SCRAM server-final: {}", server_final);
                                        
                                        // Send SASL Final
                                        let final_response = create_postgres_sasl_final_response(&server_final);
                                        socket.write_all(&final_response).await?;
                                        
                                        // Authentication successful - use the SCRAM username and a dummy password for GraphQL
                                        (scram_username, known_password.to_string())
                                    }
                                    Err(e) => {
                                        error!("❌ SCRAM-SHA-256 verification failed for user '{}' from {}: {}", scram_username, peer_addr, e);
                                        let error_response = create_postgres_error_response("28P01", "Authentication failed");
                                        socket.write_all(&error_response).await?;
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("❌ Failed to parse SASL Initial Response from {}: {}", peer_addr, e);
                            error!("🔍 SASL message hex dump: {}", hex::encode(&auth_buffer[..auth_n.min(64)]));
                            let error_response = create_postgres_error_response("28P01", &format!("Invalid SASL message: {}", e));
                            socket.write_all(&error_response).await?;
                            return Ok(());
                        }
                    }
                } else {
                    error!("❌ Expected SASL Initial Response from SCRAM client {}", peer_addr);
                    error!("🔍 Received message hex dump: {}", hex::encode(&auth_buffer[..auth_n.min(64)]));
                    let error_response = create_postgres_error_response("28P01", "Expected SASL Initial Response");
                    socket.write_all(&error_response).await?;
                    return Ok(());
                }
            }
            AuthContext::Md5(_salt) => {
                // Parse password from response (handles both cleartext and MD5)
                let password = parse_postgres_password(&auth_buffer[..auth_n]);
                if password.is_none() {
                    error!("❌ Invalid password format from {}", peer_addr);
                    error!("🔍 Password message hex dump: {}", hex::encode(&auth_buffer[..auth_n.min(64)]));
                    if auth_n > 0 {
                        error!("🔍 First byte: 0x{:02x} (expected 'p' = 0x70)", auth_buffer[0]);
                    }
                    let error_response = create_postgres_error_response("28P01", "Invalid password format");
                    socket.write_all(&error_response).await?;
                    return Ok(());
                }
                (username.clone(), password.unwrap())
            }
        };
        
        info!("🔑 Authenticating user '{}' from {} via GraphQL", username_final, peer_addr);
        
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
                    warn!("⚠️  Unknown user '{}' for MD5 authentication", username_final);
                    ""
                }
            };
            
            if known_password.is_empty() {
                (false, String::new())
            } else {
                let is_valid = match &auth_context {
                    AuthContext::Md5(salt) => {
                        let expected_hash = compute_postgres_md5_hash(&username_final, known_password, salt);
                        debug!("🔍 Expected MD5 hash: {}", expected_hash);
                        debug!("🔍 Received MD5 hash: {}", password_final);
                        let valid = verify_postgres_md5_auth(&username_final, known_password, salt, &password_final);
                        info!("🔍 MD5 verification for user '{}': {}", username_final, if valid { "✅ PASSED" } else { "❌ FAILED" });
                        valid
                    }
                    AuthContext::Scram => {
                        info!("🔍 SCRAM-SHA-256 verification for user '{}' (not fully implemented)", username_final);
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
            error!("❌ Authentication failed for user '{}' from {}", username_final, peer_addr);
            let error_response = create_postgres_error_response("28P01", "MD5 authentication failed");
            socket.write_all(&error_response).await?;
            return Ok(());
        }
        
        // Authenticate with GraphQL using the actual password
        let authenticated_session = match session_manager.authenticate(&username_final, &actual_password).await {
            Ok(session) => {
                info!("✅ Authentication successful for user '{}' from {}", username_final, peer_addr);
                
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
                error!("❌ Authentication failed for user '{}' from {}: {}", username_final, peer_addr, e);
                let error_response = create_postgres_error_response("28P01", &format!("Authentication failed: {}", e));
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
            
            debug!("📊 Received {} bytes from PostgreSQL client {}", n, peer_addr);
            
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
            &format!("Unsupported protocol version: 0x{:08x}. Expected PostgreSQL v3.0 (0x00030000).", version)
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

fn create_postgres_error_response(code: &str, message: &str) -> Vec<u8> {
    let mut response = Vec::new();
    
    // Error message format:
    // 'E' + length(4 bytes) + severity + code + message + null terminators
    
    response.push(b'E'); // Error message type
    
    // Build the error fields
    let mut fields = Vec::new();
    
    // Severity
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR\0");
    
    // SQLSTATE code
    fields.push(b'C');
    fields.extend_from_slice(code.as_bytes());
    fields.push(0);
    
    // Message
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);
    
    // End of fields
    fields.push(0);
    
    // Length field (4 bytes) = fields length + length field size
    let length = fields.len() + 4;
    response.extend_from_slice(&(length as u32).to_be_bytes());
    
    // Add the fields
    response.extend_from_slice(&fields);
    
    response
}

async fn handle_simple_text_protocol(mut socket: TcpStream, session_manager: Arc<SessionManager>, initial_data: String, _no_auth_config: Option<(String, String)>) -> Result<()> {
    let peer_addr = socket.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
    info!("📝 Using simple text protocol with {}", peer_addr);
    
    // Parse authentication from initial data
    debug!("🔐 Processing auth data: {:?}", initial_data.trim());
    
    let parts: Vec<&str> = initial_data.trim().split(':').collect();
    if parts.len() != 2 {
        warn!("❌ Invalid auth format from {}: expected 'username:password'", peer_addr);
        socket.write_all(b"ERROR: Invalid auth format. Expected 'username:password'\n").await?;
        return Ok(());
    }
    
    let username = parts[0];
    let password = parts[1];
    
    info!("🔑 Authentication attempt: user='{}' from {}", username, peer_addr);
    
    // Authenticate
    match session_manager.authenticate(username, password).await {
        Ok(session) => {
            info!("✅ Authentication successful for user '{}' from {}", username, peer_addr);
            socket.write_all(b"OK: Authentication successful\n").await?;
            
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
                    match handle_simple_query(&query, &session).await {
                        Ok(response) => {
                            debug!("📤 Sending response to {} ({} bytes)", peer_addr, response.len());
                            socket.write_all(response.as_bytes()).await?;
                        }
                        Err(e) => {
                            error!("❌ Query processing error for {}: {}", peer_addr, e);
                            let error_msg = format!("ERROR: Query failed: {}\n", e);
                            socket.write_all(error_msg.as_bytes()).await?;
                        }
                    }
                } else {
                    warn!("❌ Unsupported query type from {}: {}", peer_addr, query.trim());
                    socket.write_all(b"ERROR: Only SELECT queries are supported\n").await?;
                }
            }
        }
        Err(e) => {
            error!("❌ Authentication failed for user '{}' from {}: {}", username, peer_addr, e);
            let error_msg = format!("ERROR: Authentication failed: {}\n", e);
            socket.write_all(error_msg.as_bytes()).await?;
        }
    }
    
    info!("🔌 Connection with {} ended", peer_addr);
    Ok(())
}

async fn handle_postgres_message(
    data: &[u8], 
    connection_state: &mut ConnectionState, 
    session: &crate::auth::AuthenticatedSession
) -> Result<Vec<u8>> {
    if data.len() < 5 {
        return Err(anyhow::anyhow!("Message too short"));
    }
    
    let message_type = data[0];
    let length = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
    
    if data.len() < length + 1 {
        // Incomplete message - this is normal and not an error, just return early
        return Err(anyhow::anyhow!("INCOMPLETE_MESSAGE"));
    }
    
    let payload = &data[5..5 + length - 4];
    
    debug!("📨 Processing PostgreSQL message type: '{}' (0x{:02X}), length: {}", 
           if message_type.is_ascii_graphic() { message_type as char } else { '?' }, 
           message_type, length);
    
    match message_type {
        b'Q' => handle_simple_query_message(payload, session).await,
        b'P' => handle_parse_message(payload, connection_state).await,
        b'B' => handle_bind_message(payload, connection_state).await,
        b'E' => handle_execute_message(payload, connection_state, session).await,
        b'D' => handle_describe_message(payload, connection_state).await,
        b'C' => handle_close_message(payload, connection_state).await,
        b'S' => handle_sync_message().await,
        b'X' => handle_terminate_message().await,
        _ => {
            warn!("❓ Unsupported PostgreSQL message type: '{}' (0x{:02X})", 
                  if message_type.is_ascii_graphic() { message_type as char } else { '?' }, 
                  message_type);
            Err(anyhow::anyhow!("Unsupported message type: 0x{:02X}", message_type))
        }
    }
}

async fn handle_simple_query_message(payload: &[u8], session: &crate::auth::AuthenticatedSession) -> Result<Vec<u8>> {
    let query_str = std::str::from_utf8(payload)
        .map_err(|_| anyhow::anyhow!("Invalid UTF-8 in query"))?
        .trim_end_matches('\0');
    
    info!("📥 Simple Query: {}", query_str.trim());
    
    match handle_simple_query(query_str, session).await {
        Ok(response) => Ok(format_as_postgres_result(&response)),
        Err(e) => Err(e),
    }
}

async fn handle_parse_message(payload: &[u8], connection_state: &mut ConnectionState) -> Result<Vec<u8>> {
    let mut pos = 0;
    
    // Extract statement name (null-terminated string)
    let statement_name = extract_null_terminated_string(payload, &mut pos)?;
    
    // Extract query string (null-terminated string)
    let query = extract_null_terminated_string(payload, &mut pos)?;
    
    // Extract parameter count
    if pos + 2 > payload.len() {
        return Err(anyhow::anyhow!("Incomplete parse message"));
    }
    let param_count = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;
    
    // Extract parameter types (OIDs)
    let mut parameter_types = Vec::new();
    for _ in 0..param_count {
        if pos + 4 > payload.len() {
            return Err(anyhow::anyhow!("Incomplete parameter types"));
        }
        let oid = u32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
        parameter_types.push(oid);
        pos += 4;
    }
    
    info!("📋 Parse: statement='{}', query='{}', params={}", 
          statement_name, query.trim(), param_count);
    
    let trimmed_query = query.trim().to_uppercase();
    
    // Check if this is a transaction control or utility statement that should be allowed
    if is_transaction_control_statement(&trimmed_query) || is_utility_statement(&trimmed_query) {
        info!("📋 Parse: accepting transaction/utility statement: {}", query.trim());
        
        // Store the prepared statement even though it's a utility statement
        let prepared_stmt = PreparedStatement {
            name: statement_name.clone(),
            query: query.clone(),
            parameter_types,
        };
        connection_state.prepared_statements.insert(statement_name, prepared_stmt);
        
        return Ok(create_parse_complete_response());
    }
    
    // Validate the SQL query using the same parser as the query handler
    match crate::sql_handler::SqlHandler::parse_query(&query) {
        Ok(_query_info) => {
            // Query is valid, store the prepared statement
            let prepared_stmt = PreparedStatement {
                name: statement_name.clone(),
                query: query.clone(),
                parameter_types,
            };
            connection_state.prepared_statements.insert(statement_name, prepared_stmt);
            
            // Send ParseComplete response
            Ok(create_parse_complete_response())
        }
        Err(e) => {
            // Query is invalid or unsupported
            warn!("❌ Parse failed for statement '{}': {}", statement_name, e);
            warn!("❌ Unsupported query: {}", query.trim());
            
            // Return a more descriptive error for common unsupported statements
            let error_msg = if query.trim().to_uppercase().starts_with("SET ") {
                format!("SET statements are not supported. Query: {}", query.trim())
            } else {
                format!("Unsupported or invalid SQL statement: {}", e)
            };
            
            Err(anyhow::anyhow!("{}", error_msg))
        }
    }
}

async fn handle_bind_message(payload: &[u8], connection_state: &mut ConnectionState) -> Result<Vec<u8>> {
    let mut pos = 0;
    
    // Extract portal name
    let portal_name = extract_null_terminated_string(payload, &mut pos)?;
    
    // Extract statement name
    let statement_name = extract_null_terminated_string(payload, &mut pos)?;
    
    // Parameter format codes count
    if pos + 2 > payload.len() {
        return Err(anyhow::anyhow!("Incomplete bind message"));
    }
    let format_count = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;
    
    // Skip format codes for now (we'll assume text format)
    pos += format_count * 2;
    
    // Parameter values count
    if pos + 2 > payload.len() {
        return Err(anyhow::anyhow!("Incomplete parameter count"));
    }
    let param_count = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;
    
    // Extract parameter values
    let mut parameters = Vec::new();
    for _ in 0..param_count {
        if pos + 4 > payload.len() {
            return Err(anyhow::anyhow!("Incomplete parameter length"));
        }
        let param_length = i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
        pos += 4;
        
        if param_length == -1 {
            // NULL parameter
            parameters.push(None);
        } else {
            let param_length = param_length as usize;
            if pos + param_length > payload.len() {
                return Err(anyhow::anyhow!("Incomplete parameter value"));
            }
            let param_value = std::str::from_utf8(&payload[pos..pos + param_length])
                .map_err(|_| anyhow::anyhow!("Invalid UTF-8 in parameter"))?
                .to_string();
            parameters.push(Some(param_value));
            pos += param_length;
        }
    }
    
    info!("🔗 Bind: portal='{}', statement='{}', params={:?}", 
          portal_name, statement_name, parameters);
    
    // Store the portal
    let portal = Portal {
        name: portal_name.clone(),
        statement_name: statement_name.clone(),
        parameters,
    };
    connection_state.portals.insert(portal_name, portal);
    
    // Send BindComplete response
    Ok(create_bind_complete_response())
}

async fn handle_execute_message(
    payload: &[u8], 
    connection_state: &ConnectionState, 
    session: &crate::auth::AuthenticatedSession
) -> Result<Vec<u8>> {
    let mut pos = 0;
    
    // Extract portal name
    let portal_name = extract_null_terminated_string(payload, &mut pos)?;
    
    // Extract max rows (we'll ignore this for now)
    if pos + 4 > payload.len() {
        return Err(anyhow::anyhow!("Incomplete execute message"));
    }
    let _max_rows = u32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
    
    info!("⚡ Execute: portal='{}'", portal_name);
    
    // Get the portal
    let portal = connection_state.portals.get(&portal_name)
        .ok_or_else(|| anyhow::anyhow!("Portal '{}' not found", portal_name))?;
    
    // Get the prepared statement
    let statement = connection_state.prepared_statements.get(&portal.statement_name)
        .ok_or_else(|| anyhow::anyhow!("Statement '{}' not found", portal.statement_name))?;
    
    // Substitute parameters in the query
    let final_query = substitute_parameters(&statement.query, &portal.parameters)?;
    
    info!("🔍 Executing parameterized query: {}", final_query.trim());
    
    // Execute the query
    match handle_simple_query(&final_query, session).await {
        Ok(response) => {
            let mut result = format_as_postgres_result(&response);
            // Add CommandComplete message
            result.extend_from_slice(&create_command_complete_response("SELECT"));
            Ok(result)
        }
        Err(e) => Err(e),
    }
}

async fn handle_describe_message(payload: &[u8], connection_state: &ConnectionState) -> Result<Vec<u8>> {
    if payload.is_empty() {
        return Err(anyhow::anyhow!("Empty describe message"));
    }
    
    let object_type = payload[0];
    let object_name = std::str::from_utf8(&payload[1..])
        .map_err(|_| anyhow::anyhow!("Invalid UTF-8 in describe message"))?
        .trim_end_matches('\0');
    
    info!("📄 Describe: type='{}', name='{}'", object_type as char, object_name);
    
    match object_type {
        b'S' => {
            // Describe statement
            if let Some(_statement) = connection_state.prepared_statements.get(object_name) {
                Ok(create_parameter_description_response(&[]))
            } else {
                Err(anyhow::anyhow!("Statement '{}' not found", object_name))
            }
        }
        b'P' => {
            // Describe portal - return row description
            if connection_state.portals.contains_key(object_name) {
                Ok(create_empty_row_description_response())
            } else {
                Err(anyhow::anyhow!("Portal '{}' not found", object_name))
            }
        }
        _ => Err(anyhow::anyhow!("Invalid describe type: {}", object_type as char))
    }
}

async fn handle_close_message(payload: &[u8], connection_state: &mut ConnectionState) -> Result<Vec<u8>> {
    if payload.is_empty() {
        return Err(anyhow::anyhow!("Empty close message"));
    }
    
    let object_type = payload[0];
    let object_name = std::str::from_utf8(&payload[1..])
        .map_err(|_| anyhow::anyhow!("Invalid UTF-8 in close message"))?
        .trim_end_matches('\0');
    
    info!("🔒 Close: type='{}', name='{}'", object_type as char, object_name);
    
    match object_type {
        b'S' => {
            // Close statement
            connection_state.prepared_statements.remove(object_name);
        }
        b'P' => {
            // Close portal
            connection_state.portals.remove(object_name);
        }
        _ => return Err(anyhow::anyhow!("Invalid close type: {}", object_type as char))
    }
    
    Ok(create_close_complete_response())
}

async fn handle_sync_message() -> Result<Vec<u8>> {
    info!("🔄 Sync");
    Ok(create_ready_for_query_response())
}

async fn handle_terminate_message() -> Result<Vec<u8>> {
    info!("🔚 Terminate: Client requested graceful connection termination");
    // Return a special marker that signals the connection should be closed
    // We'll use an error with a specific message that the caller can check
    Err(anyhow::anyhow!("TERMINATE_CONNECTION"))
}

async fn handle_simple_query(query: &str, session: &crate::auth::AuthenticatedSession) -> Result<String> {
    debug!("🔍 Processing query: {}", query.trim());
    
    // Handle empty queries (just whitespace and/or semicolons)
    let cleaned_query = query.trim().trim_end_matches(';').trim();
    if cleaned_query.is_empty() {
        info!("⚪ Empty query received, returning empty query response");
        return Ok(create_empty_query_response());
    }
    
    let trimmed_query = query.trim().to_uppercase();
    
    // Handle transaction control statements that can be safely acknowledged
    if is_transaction_control_statement(&trimmed_query) {
        info!("📋 Transaction control statement (acknowledged): {}", query.trim());
        return Ok(create_command_complete_response_text(&get_transaction_command_tag(&trimmed_query)));
    }
    
    // Handle other utility statements
    if is_utility_statement(&trimmed_query) {
        info!("🔧 Utility statement: {}", query.trim());
        
        // Handle SELECT statements with actual data (CSV format: header line, then data lines)
        // Remove trailing semicolons for comparison
        let query_without_semicolon = trimmed_query.trim_end_matches(';').trim();
        if query_without_semicolon == "SELECT 1" {
            info!("🔍 Returning data for SELECT 1");
            return Ok("?column?\n1".to_string());
        } else if query_without_semicolon == "SELECT TRUE" {
            info!("🔍 Returning data for SELECT TRUE");
            return Ok("?column?\nt".to_string());
        } else if query_without_semicolon == "SELECT FALSE" {
            info!("🔍 Returning data for SELECT FALSE");
            return Ok("?column?\nf".to_string());
        } else if query_without_semicolon == "SELECT VERSION()" {
            info!("🔍 Returning data for SELECT VERSION()");
            return Ok("version\nWinCC Unified PostgreSQL Interface 1.0".to_string());
        } else if query_without_semicolon == "SELECT CURRENT_DATABASE()" {
            info!("🔍 Returning data for SELECT CURRENT_DATABASE()");
            return Ok("current_database\nsystem".to_string());
        }
        
        // For other utility statements, just acknowledge
        return Ok(create_command_complete_response_text(&get_utility_command_tag(&trimmed_query)));
    }
    
    // Use the new query handler for all SQL processing
    crate::query_handler::QueryHandler::execute_query(query, session).await
}

fn is_transaction_control_statement(query: &str) -> bool {
    // Transaction control statements that can be safely ignored
    let transaction_keywords = [
        "BEGIN",
        "START TRANSACTION", 
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE SAVEPOINT",
        "ROLLBACK TO SAVEPOINT",
        "SET TRANSACTION",
        "SET SESSION CHARACTERISTICS AS TRANSACTION",
    ];
    
    for keyword in &transaction_keywords {
        if query.starts_with(keyword) {
            return true;
        }
    }
    
    false
}

fn is_utility_statement(query: &str) -> bool {
    // Common utility statements that PostgreSQL clients send
    let utility_patterns = [
        // Session configuration - very common with Grafana
        "SET ",
        "RESET ",
        "SHOW ",
        
        // Client compatibility queries - Grafana sends these frequently
        "SELECT VERSION()",
        "SELECT CURRENT_DATABASE()",
        "SELECT CURRENT_USER",
        "SELECT CURRENT_SCHEMA",
        "SELECT SESSION_USER",
        "SELECT CURRENT_SETTING(",
        "SELECT PG_BACKEND_PID()",
        "SELECT PG_IS_IN_RECOVERY()",
        
        // Information schema queries (common with BI tools like Grafana)
        "SELECT * FROM INFORMATION_SCHEMA",
        "SELECT * FROM PG_",
        "SELECT SCHEMANAME FROM PG_",
        "SELECT TABLENAME FROM PG_",
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA",
        
        // Grafana-specific queries for database introspection
        "SELECT N.NSPNAME",
        "SELECT C.RELNAME",
        "SELECT A.ATTNAME",
        "SELECT T.TYPNAME",
        
        // DISCARD statements
        "DISCARD ALL",
        "DISCARD PLANS",
        "DISCARD SEQUENCES", 
        "DISCARD TEMPORARY",
        
        // Listen/Notify (not supported but should not error)
        "LISTEN ",
        "UNLISTEN ",
        "NOTIFY ",
        
        // Vacuum and maintenance (not applicable)
        "VACUUM",
        "ANALYZE",
        "REINDEX",
        
        // User/Role management (not supported)
        "CREATE USER",
        "CREATE ROLE", 
        "ALTER USER",
        "ALTER ROLE",
        "DROP USER",
        "DROP ROLE",
        "GRANT ",
        "REVOKE ",
        
        // Common compatibility checks
        "SELECT 1",
        "SELECT TRUE",
        "SELECT FALSE",
        
        // Timezone and encoding queries
        "SELECT * FROM PG_TIMEZONE_",
        "SELECT * FROM PG_ENCODING_",
    ];
    
    for pattern in &utility_patterns {
        if query.starts_with(pattern) {
            return true;
        }
    }
    
    false
}

fn get_transaction_command_tag(query: &str) -> String {
    if query.starts_with("BEGIN") || query.starts_with("START TRANSACTION") {
        "BEGIN".to_string()
    } else if query.starts_with("COMMIT") {
        "COMMIT".to_string()
    } else if query.starts_with("ROLLBACK") {
        "ROLLBACK".to_string()
    } else if query.starts_with("SAVEPOINT") {
        "SAVEPOINT".to_string()
    } else if query.starts_with("RELEASE SAVEPOINT") {
        "RELEASE".to_string()
    } else if query.starts_with("ROLLBACK TO SAVEPOINT") {
        "ROLLBACK".to_string()
    } else if query.starts_with("SET TRANSACTION") {
        "SET".to_string()
    } else {
        "OK".to_string()
    }
}

fn get_utility_command_tag(query: &str) -> String {
    if query.starts_with("SET ") {
        "SET".to_string()
    } else if query.starts_with("RESET ") {
        "RESET".to_string()
    } else if query.starts_with("SHOW ") {
        // For SHOW commands, we should return the actual result, but for now just acknowledge
        "SHOW".to_string()
    } else if query.starts_with("SELECT VERSION()") || query.starts_with("SELECT CURRENT_") || query.starts_with("SELECT SESSION_USER") {
        // These should ideally return actual values, but for compatibility just acknowledge
        "SELECT 1".to_string()
    } else if query.starts_with("DISCARD") {
        "DISCARD".to_string()
    } else if query.starts_with("LISTEN ") {
        "LISTEN".to_string()
    } else if query.starts_with("UNLISTEN ") {
        "UNLISTEN".to_string()
    } else if query.starts_with("NOTIFY ") {
        "NOTIFY".to_string()
    } else if query.starts_with("VACUUM") {
        "VACUUM".to_string()
    } else if query.starts_with("ANALYZE") {
        "ANALYZE".to_string()
    } else if query.starts_with("REINDEX") {
        "REINDEX".to_string()
    } else {
        "OK".to_string()
    }
}

fn create_command_complete_response_text(command_tag: &str) -> String {
    // For Simple Query protocol, we return a text response that will be formatted later
    // The actual PostgreSQL CommandComplete message will be created by format_as_postgres_result
    format!("COMMAND_COMPLETE:{}", command_tag)
}

fn create_empty_query_response() -> String {
    // Return a special marker for empty query that will be handled in the response formatting
    "EMPTY_QUERY_RESPONSE".to_string()
}

#[allow(dead_code)]
fn create_postgres_password_request() -> Vec<u8> {
    let mut response = Vec::new();
    
    // Authentication request - cleartext password
    // Message type 'R' (Authentication) + length (4 bytes) + auth type (4 bytes, 3 = cleartext password)
    response.push(b'R');
    response.extend_from_slice(&8u32.to_be_bytes()); // Length: 4 (length) + 4 (auth type) = 8
    response.extend_from_slice(&3u32.to_be_bytes()); // Auth type 3 = cleartext password
    
    response
}

fn create_postgres_md5_request_with_salt(salt: [u8; 4]) -> Vec<u8> {
    let mut response = Vec::new();
    
    // Authentication request - MD5 password
    // Message type 'R' (Authentication) + length (4 bytes) + auth type (4 bytes, 5 = MD5) + salt (4 bytes)
    response.push(b'R');
    response.extend_from_slice(&12u32.to_be_bytes()); // Length: 4 (length) + 4 (auth type) + 4 (salt) = 12
    response.extend_from_slice(&5u32.to_be_bytes()); // Auth type 5 = MD5 password
    
    // Add the salt
    response.extend_from_slice(&salt);
    
    response
}

fn create_postgres_md5_request() -> (Vec<u8>, [u8; 4]) {
    // Generate a random 4-byte salt
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    
    let mut hasher = DefaultHasher::new();
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().hash(&mut hasher);
    let hash = hasher.finish();
    
    let salt = [
        (hash >> 24) as u8,
        (hash >> 16) as u8, 
        (hash >> 8) as u8,
        hash as u8,
    ];
    
    (create_postgres_md5_request_with_salt(salt), salt)
}

fn compute_postgres_md5_hash(username: &str, password: &str, salt: &[u8; 4]) -> String {
    // PostgreSQL MD5 authentication: MD5(MD5(password + username) + salt)
    
    // Step 1: MD5(password + username)
    let mut input1 = Vec::new();
    input1.extend_from_slice(password.as_bytes());
    input1.extend_from_slice(username.as_bytes());
    let inner_hash = md5::compute(&input1);
    let inner_hex = hex::encode(inner_hash.as_ref());
    
    // Step 2: MD5(inner_hex + salt)
    let mut input2 = Vec::new();
    input2.extend_from_slice(inner_hex.as_bytes());
    input2.extend_from_slice(salt);
    let final_hash = md5::compute(&input2);
    let final_hex = hex::encode(final_hash.as_ref());
    
    // PostgreSQL prefixes the result with "md5"
    format!("md5{}", final_hex)
}

fn verify_postgres_md5_auth(username: &str, password: &str, salt: &[u8; 4], client_response: &str) -> bool {
    let expected_hash = compute_postgres_md5_hash(username, password, salt);
    client_response == expected_hash
}

// Authentication context for different auth methods
enum AuthContext {
    Md5([u8; 4]),                   // MD5 with salt
    Scram,                          // SCRAM-SHA-256 (placeholder for now)
}

// SCRAM-SHA-256 Implementation
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct ScramSha256Context {
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

fn create_postgres_scram_sha256_request() -> Vec<u8> {
    let mut response = Vec::new();
    
    // AuthenticationSASL message
    // Message type 'R' + length + auth type (10 = SASL) + mechanism list
    response.push(b'R');
    
    // SASL mechanism: "SCRAM-SHA-256" + null terminator + empty string + null terminator
    let mechanism = b"SCRAM-SHA-256\0\0";
    let total_length = 4 + 4 + mechanism.len(); // length field + auth type + mechanism
    
    response.extend_from_slice(&(total_length as u32).to_be_bytes());
    response.extend_from_slice(&10u32.to_be_bytes()); // Auth type 10 = SASL
    response.extend_from_slice(mechanism);
    
    response
}

#[allow(dead_code)]
fn generate_scram_server_nonce() -> String {
    use rand::Rng;
    use base64::{Engine, engine::general_purpose::STANDARD};
    let mut rng = rand::thread_rng();
    let nonce_bytes: [u8; 18] = rng.gen();
    STANDARD.encode(nonce_bytes)
}

#[allow(dead_code)]
fn scram_sha256_server_first_message(client_nonce: &str, username: &str) -> (String, ScramSha256Context) {
    let server_nonce = generate_scram_server_nonce();
    let combined_nonce = format!("{}{}", client_nonce, server_nonce);
    
    // Generate random salt
    use rand::Rng;
    use base64::{Engine, engine::general_purpose::STANDARD};
    let mut rng = rand::thread_rng();
    let salt: [u8; 16] = rng.gen();
    let salt_base64 = STANDARD.encode(salt);
    
    let iteration_count = 4096; // Standard iteration count
    
    let server_first = format!("r={},s={},i={}", combined_nonce, salt_base64, iteration_count);
    
    let context = ScramSha256Context {
        username: username.to_string(),
        client_nonce: client_nonce.to_string(),
        server_nonce: server_nonce,
        salt: salt.to_vec(),
        iteration_count,
        client_first_bare: String::new(), // Will be set later
        server_first: server_first.clone(),
        stored_key: Vec::new(), // Will be computed later
        server_key: Vec::new(), // Will be computed later
        stage: ScramStage::Initial,
    };
    
    (server_first, context)
}

#[allow(dead_code)]
fn scram_sha256_derive_keys(password: &str, salt: &[u8], iterations: u32) -> (Vec<u8>, Vec<u8>) {
    use pbkdf2::pbkdf2;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    
    type HmacSha256 = Hmac<Sha256>;
    
    // Derive salted password using PBKDF2
    let mut salted_password = [0u8; 32];
    pbkdf2::<HmacSha256>(password.as_bytes(), salt, iterations, &mut salted_password)
        .expect("PBKDF2 derivation failed");
    
    // Client Key = HMAC(SaltedPassword, "Client Key")
    let mut client_key_hmac = HmacSha256::new_from_slice(&salted_password)
        .expect("HMAC creation failed");
    client_key_hmac.update(b"Client Key");
    let client_key = client_key_hmac.finalize().into_bytes();
    
    // Server Key = HMAC(SaltedPassword, "Server Key") 
    let mut server_key_hmac = HmacSha256::new_from_slice(&salted_password)
        .expect("HMAC creation failed");
    server_key_hmac.update(b"Server Key");
    let server_key = server_key_hmac.finalize().into_bytes();
    
    // Stored Key = SHA256(Client Key)
    use sha2::Digest;
    let stored_key = Sha256::digest(&client_key);
    
    (stored_key.to_vec(), server_key.to_vec())
}

#[allow(dead_code)]
fn scram_sha256_verify_client_proof(
    context: &ScramSha256Context,
    client_final_without_proof: &str,
    client_proof: &[u8],
    password: &str,
) -> Result<String, String> {
    use hmac::{Hmac, Mac};
    use sha2::{Sha256, Digest};
    
    type HmacSha256 = Hmac<Sha256>;
    
    // Derive keys from password
    let (stored_key, server_key) = scram_sha256_derive_keys(password, &context.salt, context.iteration_count);
    
    // Build auth message
    let auth_message = format!("{},{},{}", 
        context.client_first_bare, 
        context.server_first, 
        client_final_without_proof
    );
    
    // Client Signature = HMAC(StoredKey, AuthMessage)
    let mut client_sig_hmac = HmacSha256::new_from_slice(&stored_key)
        .map_err(|e| format!("HMAC creation failed: {}", e))?;
    client_sig_hmac.update(auth_message.as_bytes());
    let client_signature = client_sig_hmac.finalize().into_bytes();
    
    // Client Key = Client Signature XOR Client Proof
    if client_proof.len() != client_signature.len() {
        return Err("Client proof length mismatch".to_string());
    }
    
    let mut client_key = vec![0u8; client_signature.len()];
    for i in 0..client_signature.len() {
        client_key[i] = client_signature[i] ^ client_proof[i];
    }
    
    // Verify: SHA256(Client Key) should equal Stored Key
    let computed_stored_key = Sha256::digest(&client_key);
    if computed_stored_key.as_slice() != stored_key {
        return Err("Authentication verification failed".to_string());
    }
    
    // Server Signature = HMAC(ServerKey, AuthMessage)
    let mut server_sig_hmac = HmacSha256::new_from_slice(&server_key)
        .map_err(|e| format!("Server HMAC creation failed: {}", e))?;
    server_sig_hmac.update(auth_message.as_bytes());
    let server_signature = server_sig_hmac.finalize().into_bytes();
    
    // Server final message
    use base64::{Engine, engine::general_purpose::STANDARD};
    let server_final = format!("v={}", STANDARD.encode(server_signature));
    Ok(server_final)
}

#[allow(dead_code)]
fn create_postgres_sasl_continue_response(server_message: &str) -> Vec<u8> {
    let mut response = Vec::new();
    
    // AuthenticationSASLContinue message
    // Message type 'R' + length + auth type (11 = SASL Continue) + SASL data
    response.push(b'R');
    
    let sasl_data = server_message.as_bytes();
    let total_length = 4 + 4 + sasl_data.len(); // length field + auth type + data
    
    response.extend_from_slice(&(total_length as u32).to_be_bytes());
    response.extend_from_slice(&11u32.to_be_bytes()); // Auth type 11 = SASL Continue
    response.extend_from_slice(sasl_data);
    
    response
}

#[allow(dead_code)]
fn create_postgres_sasl_final_response(server_message: &str) -> Vec<u8> {
    let mut response = Vec::new();
    
    // AuthenticationSASLFinal message
    // Message type 'R' + length + auth type (12 = SASL Final) + SASL data
    response.push(b'R');
    
    let sasl_data = server_message.as_bytes();
    let total_length = 4 + 4 + sasl_data.len(); // length field + auth type + data
    
    response.extend_from_slice(&(total_length as u32).to_be_bytes());
    response.extend_from_slice(&12u32.to_be_bytes()); // Auth type 12 = SASL Final
    response.extend_from_slice(sasl_data);
    
    response
}

// Parse SASL Initial Response from client
fn parse_sasl_initial_response(buffer: &[u8]) -> Result<(String, String), String> {
    // SASL Initial Response format:
    // Message type 'p' + length + mechanism + initial_response_length + initial_response
    
    if buffer.len() < 9 || buffer[0] != b'p' {
        return Err("Invalid SASL Initial Response format".to_string());
    }
    
    let mut pos = 5; // Skip 'p' + length (4 bytes)
    
    // Extract mechanism name (null-terminated)
    let mechanism_start = pos;
    while pos < buffer.len() && buffer[pos] != 0 {
        pos += 1;
    }
    if pos >= buffer.len() {
        return Err("Missing null terminator for mechanism".to_string());
    }
    
    let mechanism = String::from_utf8_lossy(&buffer[mechanism_start..pos]).to_string();
    pos += 1; // Skip null terminator
    
    // Extract initial response length (4 bytes)
    if pos + 4 > buffer.len() {
        return Err("Missing initial response length".to_string());
    }
    
    let response_length = u32::from_be_bytes([
        buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3]
    ]) as usize;
    pos += 4;
    
    // Extract initial response
    if pos + response_length > buffer.len() {
        return Err("Initial response length exceeds buffer".to_string());
    }
    
    let initial_response = String::from_utf8_lossy(&buffer[pos..pos + response_length]).to_string();
    
    Ok((mechanism, initial_response))
}

// Parse SASL Response from client (subsequent messages)
fn parse_sasl_response(buffer: &[u8]) -> Result<String, String> {
    // SASL Response format:
    // Message type 'p' + length + response_data
    
    if buffer.len() < 5 || buffer[0] != b'p' {
        return Err("Invalid SASL Response format".to_string());
    }
    
    let response_data = String::from_utf8_lossy(&buffer[5..]).to_string();
    Ok(response_data)
}

// Parse SCRAM client-first message
fn parse_scram_client_first(client_first: &str) -> Result<(String, String), String> {
    // Format: "n,,n=username,r=client_nonce"
    // or: "n=username,r=client_nonce" (without GS2 header)
    
    let client_first_bare = if client_first.starts_with("n,,") {
        &client_first[3..] // Remove GS2 header "n,,"
    } else {
        client_first
    };
    
    let mut username = String::new();
    let mut client_nonce = String::new();
    
    for part in client_first_bare.split(',') {
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "n" => username = value.to_string(),
                "r" => client_nonce = value.to_string(),
                _ => {} // Ignore unknown attributes
            }
        }
    }
    
    if username.is_empty() || client_nonce.is_empty() {
        return Err("Missing username or client nonce in SCRAM client-first".to_string());
    }
    
    Ok((username, client_nonce))
}

// Parse SCRAM client-final message
fn parse_scram_client_final(client_final: &str) -> Result<(String, Vec<u8>), String> {
    // Format: "c=biws,r=client_nonce_server_nonce,p=client_proof"
    
    let mut client_final_without_proof = String::new();
    let mut client_proof_b64 = String::new();
    
    for part in client_final.split(',') {
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "p" => client_proof_b64 = value.to_string(),
                _ => {
                    if !client_final_without_proof.is_empty() {
                        client_final_without_proof.push(',');
                    }
                    client_final_without_proof.push_str(part);
                }
            }
        }
    }
    
    if client_proof_b64.is_empty() {
        return Err("Missing client proof in SCRAM client-final".to_string());
    }
    
    use base64::{Engine, engine::general_purpose::STANDARD};
    let client_proof = STANDARD.decode(client_proof_b64)
        .map_err(|e| format!("Invalid base64 in client proof: {}", e))?;
    
    Ok((client_final_without_proof, client_proof))
}

fn create_postgres_auth_ok_response() -> Vec<u8> {
    let mut response = Vec::new();
    
    // Authentication OK message
    // Message type 'R' (Authentication) + length (4 bytes) + auth type (4 bytes, 0 = OK)
    response.push(b'R');
    response.extend_from_slice(&8u32.to_be_bytes()); // Length: 4 (length) + 4 (auth type) = 8
    response.extend_from_slice(&0u32.to_be_bytes()); // Auth type 0 = OK
    
    // BackendKeyData message - CRITICAL for Grafana compatibility
    // Message type 'K' + length (4 bytes) + process_id (4 bytes) + secret_key (4 bytes)
    response.push(b'K');
    response.extend_from_slice(&12u32.to_be_bytes()); // Length: 4 + 4 + 4 = 12
    response.extend_from_slice(&12345u32.to_be_bytes()); // Dummy process ID
    response.extend_from_slice(&67890u32.to_be_bytes()); // Dummy secret key
    
    // Parameter status messages for required parameters
    let params = [
        ("server_version", "14.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("application_name", ""),
        ("is_superuser", "off"),
        ("session_authorization", "operator"),
        ("DateStyle", "ISO"),
        ("TimeZone", "UTC"),
        ("standard_conforming_strings", "on"),
        ("integer_datetimes", "on"),
    ];
    
    for (name, value) in params {
        // Parameter status message: 'S' + length + name + null + value + null
        response.push(b'S');
        let content = format!("{}\0{}\0", name, value);
        let length = 4 + content.len(); // 4 bytes for length field + content
        response.extend_from_slice(&(length as u32).to_be_bytes());
        response.extend_from_slice(content.as_bytes());
    }
    
    // Ready for query message: 'Z' + length + status
    response.push(b'Z');
    response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
    response.push(b'I'); // Status: 'I' = idle
    
    response
}

fn parse_postgres_password(data: &[u8]) -> Option<String> {
    if data.len() < 5 {
        return None;
    }
    
    // Check for PasswordMessage (type 'p')
    if data[0] == b'p' {
        // Password message: 'p' + length (4 bytes) + password string + null terminator
        let length = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
        if data.len() >= length + 1 && length > 4 {
            let password_bytes = &data[5..5 + length - 4]; // Exclude length and null terminator
            if let Ok(password) = std::str::from_utf8(password_bytes) {
                return Some(password.trim_end_matches('\0').to_string());
            }
        }
    }
    
    None
}

#[allow(dead_code)]
fn parse_postgres_query(data: &[u8]) -> Option<String> {
    if data.len() < 5 {
        return None;
    }
    
    // Check for Query message (Simple Query Protocol)
    if data[0] == b'Q' {
        // Query message: 'Q' + length (4 bytes) + query string + null terminator
        let length = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
        if data.len() >= length + 1 && length > 4 {
            let query_bytes = &data[5..5 + length - 4]; // Exclude length and null terminator
            if let Ok(query) = std::str::from_utf8(query_bytes) {
                return Some(query.trim_end_matches('\0').to_string());
            }
        }
    }
    
    None
}

fn format_as_postgres_result(csv_data: &str) -> Vec<u8> {
    let mut response = Vec::new();
    
    // Check if this is a command complete response (for transaction control/utility statements)
    if csv_data.starts_with("COMMAND_COMPLETE:") {
        let command_tag = csv_data.strip_prefix("COMMAND_COMPLETE:").unwrap_or("OK");
        
        // Command complete message: 'C' + length + tag
        response.push(b'C');
        let tag_length = 4 + command_tag.len() + 1; // 4 bytes for length + tag + null terminator
        response.extend_from_slice(&(tag_length as u32).to_be_bytes());
        response.extend_from_slice(command_tag.as_bytes());
        response.push(0); // Null terminator
        
        // Ready for query message: 'Z' + length + status
        response.push(b'Z');
        response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
        response.push(b'I'); // Status: 'I' = idle
        
        return response;
    }
    
    // Handle empty query response
    if csv_data.trim() == "EMPTY_QUERY_RESPONSE" {
        // For empty queries, send EmptyQueryResponse followed by ReadyForQuery
        // EmptyQueryResponse message: 'I' + length (4 bytes only)
        response.push(b'I');
        response.extend_from_slice(&4u32.to_be_bytes()); // Length: 4 bytes (just the length field)
        
        // Ready for query message: 'Z' + length + status
        response.push(b'Z');
        response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
        response.push(b'I'); // Status: 'I' = idle (not in transaction)
        
        return response;
    }
    
    let lines: Vec<&str> = csv_data.trim().split('\n').collect();
    if lines.is_empty() {
        return response;
    }
    
    // Parse CSV header
    let headers: Vec<&str> = lines[0].split(',').collect();
    let row_count = lines.len() - 1; // Exclude header
    
    // Row description message: 'T' + length + field count + field descriptions
    response.push(b'T');
    let mut row_desc = Vec::new();
    row_desc.extend_from_slice(&(headers.len() as i16).to_be_bytes()); // Field count
    
    for header in &headers {
        row_desc.extend_from_slice(header.as_bytes());
        row_desc.push(0); // Null terminator for field name
        row_desc.extend_from_slice(&0u32.to_be_bytes()); // Table OID
        row_desc.extend_from_slice(&0i16.to_be_bytes());  // Column attribute number
        row_desc.extend_from_slice(&25u32.to_be_bytes()); // Type OID (25 = text)
        row_desc.extend_from_slice(&(-1i16).to_be_bytes()); // Type size (-1 = variable)
        row_desc.extend_from_slice(&(-1i32).to_be_bytes()); // Type modifier
        row_desc.extend_from_slice(&0i16.to_be_bytes());   // Format code (0 = text)
    }
    
    let row_desc_length = 4 + row_desc.len(); // 4 bytes for length + content
    response.extend_from_slice(&(row_desc_length as u32).to_be_bytes());
    response.extend_from_slice(&row_desc);
    
    // Data rows
    for line in lines.iter().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        
        let values: Vec<&str> = line.split(',').collect();
        
        // Data row message: 'D' + length + field count + field values
        response.push(b'D');
        let mut row_data = Vec::new();
        row_data.extend_from_slice(&(values.len() as i16).to_be_bytes()); // Field count
        
        for value in &values {
            if *value == "NULL" {
                row_data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL value
            } else {
                row_data.extend_from_slice(&(value.len() as i32).to_be_bytes()); // Value length
                row_data.extend_from_slice(value.as_bytes()); // Value data
            }
        }
        
        let row_data_length = 4 + row_data.len(); // 4 bytes for length + content
        response.extend_from_slice(&(row_data_length as u32).to_be_bytes());
        response.extend_from_slice(&row_data);
    }
    
    // Command complete message: 'C' + length + tag
    response.push(b'C');
    let tag = format!("SELECT {}", row_count);
    let tag_length = 4 + tag.len() + 1; // 4 bytes for length + tag + null terminator
    response.extend_from_slice(&(tag_length as u32).to_be_bytes());
    response.extend_from_slice(tag.as_bytes());
    response.push(0); // Null terminator
    
    // Ready for query message: 'Z' + length + status
    response.push(b'Z');
    response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
    response.push(b'I'); // Status: 'I' = idle
    
    response
}

// Helper functions for Extended Query Protocol

fn extract_null_terminated_string(data: &[u8], pos: &mut usize) -> Result<String> {
    let start = *pos;
    while *pos < data.len() && data[*pos] != 0 {
        *pos += 1;
    }
    
    if *pos >= data.len() {
        return Err(anyhow::anyhow!("Unterminated string"));
    }
    
    let result = std::str::from_utf8(&data[start..*pos])
        .map_err(|_| anyhow::anyhow!("Invalid UTF-8 in string"))?
        .to_string();
    
    *pos += 1; // Skip null terminator
    Ok(result)
}

fn substitute_parameters(query: &str, parameters: &[Option<String>]) -> Result<String> {
    let mut result = query.to_string();
    
    // Simple parameter substitution: replace $1, $2, etc. with actual values
    for (i, param) in parameters.iter().enumerate() {
        let placeholder = format!("${}", i + 1);
        let value = match param {
            Some(val) => format!("'{}'", val.replace('\'', "''")).to_string(), // Escape single quotes
            None => "NULL".to_string(),
        };
        result = result.replace(&placeholder, &value);
    }
    
    Ok(result)
}

fn create_parse_complete_response() -> Vec<u8> {
    let mut response = Vec::new();
    response.push(b'1'); // ParseComplete message type
    response.extend_from_slice(&4u32.to_be_bytes()); // Length (just the length field)
    response
}

fn create_bind_complete_response() -> Vec<u8> {
    let mut response = Vec::new();
    response.push(b'2'); // BindComplete message type
    response.extend_from_slice(&4u32.to_be_bytes()); // Length
    response
}

fn create_command_complete_response(command: &str) -> Vec<u8> {
    let mut response = Vec::new();
    response.push(b'C'); // CommandComplete message type
    let command_bytes = command.as_bytes();
    let length = 4 + command_bytes.len() + 1; // length + command + null terminator
    response.extend_from_slice(&(length as u32).to_be_bytes());
    response.extend_from_slice(command_bytes);
    response.push(0); // Null terminator
    response
}

fn create_parameter_description_response(types: &[u32]) -> Vec<u8> {
    let mut response = Vec::new();
    response.push(b't'); // ParameterDescription message type
    let length = 4 + 2 + (types.len() * 4); // length + count + types
    response.extend_from_slice(&(length as u32).to_be_bytes());
    response.extend_from_slice(&(types.len() as u16).to_be_bytes());
    for &type_oid in types {
        response.extend_from_slice(&type_oid.to_be_bytes());
    }
    response
}

fn create_empty_row_description_response() -> Vec<u8> {
    let mut response = Vec::new();
    response.push(b'T'); // RowDescription message type
    response.extend_from_slice(&6u32.to_be_bytes()); // Length: 4 + 2 = 6
    response.extend_from_slice(&0u16.to_be_bytes()); // 0 fields
    response
}

fn create_close_complete_response() -> Vec<u8> {
    let mut response = Vec::new();
    response.push(b'3'); // CloseComplete message type
    response.extend_from_slice(&4u32.to_be_bytes()); // Length
    response
}

fn create_ready_for_query_response() -> Vec<u8> {
    let mut response = Vec::new();
    response.push(b'Z'); // ReadyForQuery message type
    response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
    response.push(b'I'); // Status: 'I' = idle
    response
}
