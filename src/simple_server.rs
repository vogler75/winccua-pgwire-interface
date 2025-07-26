use crate::auth::SessionManager;
use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, error, debug, warn, trace};

pub struct SimpleServer {
    session_manager: Arc<SessionManager>,
}

impl SimpleServer {
    pub fn new(graphql_url: String) -> Self {
        Self {
            session_manager: Arc::new(SessionManager::new(graphql_url)),
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
            tokio::spawn(async move {
                debug!("🚀 Starting connection handler for {}", client_addr);
                
                if let Err(e) = handle_connection(socket, session_manager).await {
                    error!("💥 Error handling connection from {}: {}", client_addr, e);
                } else {
                    debug!("✅ Connection handler completed successfully for {}", client_addr);
                }
                
                info!("👋 Connection from {} closed", client_addr);
            });
        }
    }
}

async fn handle_connection(mut socket: TcpStream, session_manager: Arc<SessionManager>) -> Result<()> {
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
        warn!("🔒 SSL connection request detected from {}!", peer_addr);
        info!("📝 Rejecting SSL request - SSL not supported");
        
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
        return handle_postgres_startup(socket, session_manager, &startup_buffer[..startup_n]).await;
    }
    
    // Check if this looks like PostgreSQL wire protocol (non-SSL)
    else if n >= 8 && is_postgres_wire_protocol(&peek_buffer[..n]) {
        warn!("🐘 PostgreSQL wire protocol detected from {}! Current server only supports simple TCP protocol.", peer_addr);
        warn!("💡 DBeaver and other SQL clients expect full PostgreSQL wire protocol.");
        warn!("📝 Try connecting with netcat instead: nc localhost 5433");
        
        // For now, attempt to handle it as PostgreSQL startup
        return handle_postgres_startup(socket, session_manager, &peek_buffer[..n]).await;
    }
    
    // Try to interpret as simple text protocol
    let initial_data = String::from_utf8_lossy(&peek_buffer[..n]);
    debug!("📄 Initial data as text: {:?}", initial_data);
    
    // Check if this looks like an authentication attempt
    if initial_data.contains(':') {
        info!("🔐 Processing authentication attempt from {}", peer_addr);
        return handle_simple_text_protocol(socket, session_manager, initial_data.to_string()).await;
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

async fn handle_postgres_startup(mut socket: TcpStream, session_manager: Arc<SessionManager>, data: &[u8]) -> Result<()> {
    let peer_addr = socket.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
    info!("🐘 Handling PostgreSQL startup from {}", peer_addr);
    
    if data.len() < 8 {
        error!("❌ Invalid startup message length from {}: {} bytes", peer_addr, data.len());
        return Ok(());
    }
    
    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    
    info!("📋 Startup message: length={}, version={} (0x{:08x})", length, version, version);
    
    // Parse startup parameters if this is a v3.0 protocol message
    if version == 196608 { // PostgreSQL 3.0 protocol
        debug!("✅ PostgreSQL 3.0 protocol detected");
        
        // Extract parameters (user, database, etc.)
        if data.len() > 8 {
            let params_data = &data[8..];
            let params = parse_startup_parameters(params_data);
            debug!("📋 Startup parameters: {:?}", params);
        }
        
        // Extract username from startup parameters for authentication
        let username = if data.len() > 8 {
            let params_data = &data[8..];
            let params = parse_startup_parameters(params_data);
            params.get("user").cloned().unwrap_or_else(|| "unknown".to_string())
        } else {
            "unknown".to_string()
        };
        
        info!("🔐 PostgreSQL client {} requesting authentication for user: {}", peer_addr, username);
        
        // Send password authentication request (MD5 or cleartext)
        let auth_request = create_postgres_password_request();
        
        debug!("📤 Sending password authentication request to {}", peer_addr);
        if let Err(e) = socket.write_all(&auth_request).await {
            error!("❌ Failed to send auth request to {}: {}", peer_addr, e);
            return Ok(());
        }
        
        // Wait for password response
        debug!("📖 Waiting for password response from {}", peer_addr);
        let mut password_buffer = [0; 1024];
        let password_n = socket.read(&mut password_buffer).await?;
        if password_n == 0 {
            warn!("⚠️  Client {} disconnected during password authentication", peer_addr);
            return Ok(());
        }
        
        debug!("📊 Received {} bytes password response from {}", password_n, peer_addr);
        
        // Parse password from response
        let password = parse_postgres_password(&password_buffer[..password_n]);
        if password.is_none() {
            error!("❌ Invalid password format from {}", peer_addr);
            let error_response = create_postgres_error_response("28P01", "Invalid password format");
            socket.write_all(&error_response).await?;
            return Ok(());
        }
        
        let password = password.unwrap();
        info!("🔑 Authenticating user '{}' from {} via GraphQL", username, peer_addr);
        
        // Authenticate with GraphQL
        let authenticated_session = match session_manager.authenticate(&username, &password).await {
            Ok(session) => {
                info!("✅ Authentication successful for user '{}' from {}", username, peer_addr);
                
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
                error!("❌ Authentication failed for user '{}' from {}: {}", username, peer_addr, e);
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
            
            // Try to parse as PostgreSQL query message
            if let Some(query) = parse_postgres_query(&buffer[..n]) {
                info!("📥 PostgreSQL query received from {}: {}", peer_addr, query.trim());
                
                // Use the already authenticated session
                match handle_simple_query(&query, &authenticated_session).await {
                    Ok(response) => {
                        // Convert to PostgreSQL result format
                        let pg_response = format_as_postgres_result(&response);
                        debug!("📤 Sending PostgreSQL response to {} ({} bytes)", peer_addr, pg_response.len());
                        socket.write_all(&pg_response).await?;
                    }
                    Err(e) => {
                        error!("❌ Query processing error for {}: {}", peer_addr, e);
                        let mut error_response = create_postgres_error_response("42000", &format!("Query failed: {}", e));
                        
                        // Add ready-for-query message after error to prevent client hang
                        error_response.push(b'Z');
                        error_response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
                        error_response.push(b'I'); // Status: 'I' = idle
                        
                        socket.write_all(&error_response).await?;
                    }
                }
            } else {
                warn!("❓ Unable to parse PostgreSQL message from {}", peer_addr);
                warn!("🔍 Message details: {} bytes received", n);
                warn!("📦 Raw message bytes: {:02X?}", &buffer[..n.min(32)]); // Log first 32 bytes max
                if n > 0 {
                    warn!("🔤 Message type byte: 0x{:02X} ('{}')", buffer[0], 
                          if buffer[0].is_ascii_graphic() { buffer[0] as char } else { '?' });
                }
                if n >= 5 {
                    let length = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]);
                    warn!("📏 Declared length: {} bytes (actual received: {} bytes)", length, n);
                }
                // Send a generic error response with ready-for-query message
                let mut error_response = create_postgres_error_response("08P01", "Invalid message format");
                
                // Add ready-for-query message after error to prevent client hang
                error_response.push(b'Z');
                error_response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
                error_response.push(b'I'); // Status: 'I' = idle
                
                socket.write_all(&error_response).await?;
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

async fn handle_simple_text_protocol(mut socket: TcpStream, session_manager: Arc<SessionManager>, initial_data: String) -> Result<()> {
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

async fn handle_simple_query(query: &str, session: &crate::auth::AuthenticatedSession) -> Result<String> {
    debug!("🔍 Processing query: {}", query.trim());
    
    // Use the new query handler for all SQL processing
    crate::query_handler::QueryHandler::execute_query(query, session).await
}

fn create_postgres_password_request() -> Vec<u8> {
    let mut response = Vec::new();
    
    // Authentication request - cleartext password
    // Message type 'R' (Authentication) + length (4 bytes) + auth type (4 bytes, 3 = cleartext password)
    response.push(b'R');
    response.extend_from_slice(&8u32.to_be_bytes()); // Length: 4 (length) + 4 (auth type) = 8
    response.extend_from_slice(&3u32.to_be_bytes()); // Auth type 3 = cleartext password
    
    response
}

fn create_postgres_auth_ok_response() -> Vec<u8> {
    let mut response = Vec::new();
    
    // Authentication OK message
    // Message type 'R' (Authentication) + length (4 bytes) + auth type (4 bytes, 0 = OK)
    response.push(b'R');
    response.extend_from_slice(&8u32.to_be_bytes()); // Length: 4 (length) + 4 (auth type) = 8
    response.extend_from_slice(&0u32.to_be_bytes()); // Auth type 0 = OK
    
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