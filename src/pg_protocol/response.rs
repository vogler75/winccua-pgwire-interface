pub(super) fn create_postgres_error_response(code: &str, message: &str) -> Vec<u8> {
    let mut response = Vec::new();

    // Error message format:
    // 'E' + length(4 bytes) + severity + code + message + null terminators

    response.push(b'E'); // 'E' = ErrorResponse message type

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

pub(super) fn create_postgres_auth_ok_response() -> Vec<u8> {
    let mut response = Vec::new();

    // Authentication OK message
    // Message type 'R' (Authentication) + length (4 bytes) + auth type (4 bytes, 0 = OK)
    response.push(b'R'); // 'R' = Authentication message
    response.extend_from_slice(&8u32.to_be_bytes()); // Length: 4 (length) + 4 (auth type) = 8
    response.extend_from_slice(&0u32.to_be_bytes()); // Auth type 0 = OK

    // BackendKeyData message - CRITICAL for Grafana compatibility
    // Message type 'K' (BackendKeyData) + length (4 bytes) + process_id (4 bytes) + secret_key (4 bytes)
    response.push(b'K'); // 'K' = BackendKeyData message
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
        // Parameter status message: 'S' (ParameterStatus) + length + name + null + value + null
        response.push(b'S'); // 'S' = ParameterStatus message
        let content = format!("{}\0{}\0", name, value);
        let length = 4 + content.len(); // 4 bytes for length field + content
        response.extend_from_slice(&(length as u32).to_be_bytes());
        response.extend_from_slice(content.as_bytes());
    }

    // Ready for query message: 'Z' (ReadyForQuery) + length + status
    response.push(b'Z'); // 'Z' = ReadyForQuery message
    response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
    response.push(b'I'); // Status: 'I' = idle (not in transaction)

    response
}

#[allow(dead_code)]
pub(super) fn format_as_postgres_result(csv_data: &str) -> Vec<u8> {
    let mut response = Vec::new();

    // Check if this is a command complete response (for transaction control/utility statements)
    if csv_data.starts_with("COMMAND_COMPLETE:") {
        let command_tag = csv_data.strip_prefix("COMMAND_COMPLETE:").unwrap_or("OK");

        // Command complete message: 'C' (CommandComplete) + length + tag
        response.push(b'C'); // 'C' = CommandComplete message
        let tag_length = 4 + command_tag.len() + 1; // 4 bytes for length + tag + null terminator
        response.extend_from_slice(&(tag_length as u32).to_be_bytes());
        response.extend_from_slice(command_tag.as_bytes());
        response.push(0); // Null terminator

        // Ready for query message: 'Z' (ReadyForQuery) + length + status
        response.push(b'Z'); // 'Z' = ReadyForQuery message
        response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
        response.push(b'I'); // Status: 'I' = idle (not in transaction)

        return response;
    }

    // Handle empty query response
    if csv_data.trim() == "EMPTY_QUERY_RESPONSE" {
        // For empty queries, send EmptyQueryResponse followed by ReadyForQuery
        // EmptyQueryResponse message: 'I' (EmptyQueryResponse) + length (4 bytes only)
        response.push(b'I'); // 'I' = EmptyQueryResponse message
        response.extend_from_slice(&4u32.to_be_bytes()); // Length: 4 bytes (just the length field)

        // Ready for query message: 'Z' (ReadyForQuery) + length + status
        response.push(b'Z'); // 'Z' = ReadyForQuery message
        response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
        response.push(b'I'); // Status: 'I' = idle (not in transaction)

        return response;
    }

    let lines: Vec<&str> = csv_data.trim().split('\n').collect();
    if lines.is_empty() {
        return response;
    }

    // Parse CSV header with potential type information, handling quoted fields
    let headers = parse_csv_line(lines[0]);
    tracing::debug!("CSV headers parsed: {:?}", headers);
    let mut column_types: std::collections::HashMap<String, &str> =
        std::collections::HashMap::new();
    let mut clean_headers: Vec<String> = Vec::new();

    for header in &headers {
        if header.contains(':') {
            // Header format: "column:TYPE"
            let parts: Vec<&str> = header.splitn(2, ':').collect();
            if parts.len() == 2 {
                clean_headers.push(parts[0].to_string());
                column_types.insert(parts[0].to_string(), parts[1]);
            } else {
                clean_headers.push(header.clone());
            }
        } else {
            clean_headers.push(header.clone());
        }
    }

    // RowDescription message: 'T' + length + field_count + fields
    response.push(b'T');

    let mut fields_data = Vec::new();
    fields_data.extend_from_slice(&(clean_headers.len() as u16).to_be_bytes());

    for (i, header) in clean_headers.iter().enumerate() {
        fields_data.extend_from_slice(header.as_bytes());
        fields_data.push(0); // Null terminator for name

        // Add dummy table/column IDs
        fields_data.extend_from_slice(&0u32.to_be_bytes()); // Table OID
        fields_data.extend_from_slice(&(i as u16).to_be_bytes()); // Column index

        // Determine data type OID based on column name/type hint
        let type_oid: u32 = match column_types.get(header.as_str()).unwrap_or(&"TEXT") {
            &"NUMERIC" => 1700, // NUMERIC
            &"TIMESTAMP" => 1114, // TIMESTAMP
            &"TEXT" => 25,      // TEXT
            _ => 25,            // Default to TEXT
        };

        fields_data.extend_from_slice(&type_oid.to_be_bytes()); // Data type OID

        // Add type size (-1 for variable size)
        let type_size: i16 = -1;
        fields_data.extend_from_slice(&type_size.to_be_bytes());

        // Add type modifier (-1 for default)
        let type_modifier: i32 = -1;
        fields_data.extend_from_slice(&type_modifier.to_be_bytes());

        // Add format code (0 for text)
        let format_code: i16 = 0;
        fields_data.extend_from_slice(&format_code.to_be_bytes());
    }

    let length = 4 + fields_data.len();
    response.extend_from_slice(&(length as u32).to_be_bytes());
    response.extend_from_slice(&fields_data);

    // DataRow messages: 'D' + length + column_count + columns
    let mut row_count = 0;
    for (line_num, line) in lines.iter().skip(1).enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        
        // Debug log first data row
        if line_num == 0 {
            tracing::debug!("First data row: {}", line);
        }

        response.push(b'D');

        let values = parse_csv_line(line);
        
        // Ensure the number of values matches the number of headers
        if values.len() != clean_headers.len() {
            tracing::warn!(
                "Column count mismatch: headers={}, values={} in line: {}",
                clean_headers.len(),
                values.len(),
                line
            );
        }
        
        let mut row_data = Vec::new();
        // IMPORTANT: Always send the number of columns from the header, not the actual values
        // This ensures consistency with the RowDescription
        row_data.extend_from_slice(&(clean_headers.len() as u16).to_be_bytes());

        // Process values, padding with NULL if we have fewer values than headers
        for i in 0..clean_headers.len() {
            if i < values.len() {
                let value = &values[i];
                if value == "NULL" {
                    row_data.extend_from_slice(&(-1i32).to_be_bytes());
                } else {
                    row_data.extend_from_slice(&(value.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(value.as_bytes());
                }
            } else {
                // Pad with NULL if we don't have enough values
                row_data.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }

        let length = 4 + row_data.len();
        response.extend_from_slice(&(length as u32).to_be_bytes());
        response.extend_from_slice(&row_data);
        row_count += 1;
    }

    // CommandComplete message: 'C' + length + tag
    response.push(b'C');
    let tag = format!("SELECT {}", row_count);
    let tag_length = 4 + tag.len() + 1; // 4 bytes for length + tag + null terminator
    response.extend_from_slice(&(tag_length as u32).to_be_bytes());
    response.extend_from_slice(tag.as_bytes());
    response.push(0); // Null terminator

    // ReadyForQuery message: 'Z' + length + status
    response.push(b'Z');
    response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
    response.push(b'I'); // Status: 'I' = idle

    response
}

#[allow(dead_code)]
pub(super) fn format_as_extended_query_result(csv_data: &str, query_info: &crate::tables::QueryInfo) -> Vec<u8> {
    let mut response = Vec::new();

    // Handle command complete for non-query statements
    if csv_data.starts_with("COMMAND_COMPLETE:") {
        let command_tag = csv_data.strip_prefix("COMMAND_COMPLETE:").unwrap_or("OK");
        response.extend_from_slice(&create_command_complete_response(command_tag));
        return response;
    }

    // Handle empty query response
    if csv_data.trim() == "EMPTY_QUERY_RESPONSE" {
        response.extend_from_slice(&create_command_complete_response(""));
        return response;
    }

    let lines: Vec<&str> = csv_data.trim().split('\n').collect();
    if lines.is_empty() {
        response.extend_from_slice(&create_command_complete_response(""));
        return response;
    }

    // --- 1. RowDescription ---
    response.extend_from_slice(&create_row_description_response(query_info));

    // --- 2. DataRows ---
    let mut row_count = 0;
    for line in lines.iter().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        response.push(b'D'); // DataRow message type
        let values = parse_csv_line(line);
        
        // Check for column count mismatch
        if values.len() != query_info.columns.len() {
            tracing::warn!(
                "Extended query column count mismatch: expected={}, actual={} in line: {}",
                query_info.columns.len(),
                values.len(),
                line
            );
        }
        
        let mut row_data = Vec::new();
        // Always use the column count from query_info for consistency
        row_data.extend_from_slice(&(query_info.columns.len() as u16).to_be_bytes());
        
        // Process values, padding with NULL if needed
        for i in 0..query_info.columns.len() {
            if i < values.len() {
                let value = &values[i];
                if value == "NULL" {
                    row_data.extend_from_slice(&(-1i32).to_be_bytes());
                } else {
                    row_data.extend_from_slice(&(value.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(value.as_bytes());
                }
            } else {
                // Pad with NULL if we don't have enough values
                row_data.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }
        let length = 4 + row_data.len();
        response.extend_from_slice(&(length as u32).to_be_bytes());
        response.extend_from_slice(&row_data);
        row_count += 1;
    }

    // --- 3. CommandComplete ---
    let tag = format!("SELECT {}", row_count);
    response.extend_from_slice(&create_command_complete_response(&tag));

    response
}

pub(super) fn create_row_description_response(query_info: &crate::tables::QueryInfo) -> Vec<u8> {
    let mut response = Vec::new();
    tracing::info!("🚀 create_row_description_response() CALLED with {} columns", query_info.columns.len());
    tracing::info!("🚀 Query info columns: {:?}", query_info.columns);
    response.push(b'T'); // 'T' = RowDescription message type

    let mut fields_data = Vec::new();
    fields_data.extend_from_slice(&(query_info.columns.len() as u16).to_be_bytes());

    for (i, header) in query_info.columns.iter().enumerate() {
        fields_data.extend_from_slice(header.as_bytes());
        fields_data.push(0); // Null terminator for name

        // Add dummy table/column IDs
        fields_data.extend_from_slice(&0u32.to_be_bytes()); // Table OID
        fields_data.extend_from_slice(&(i as u16).to_be_bytes()); // Column index

        // Determine data type OID based on column name/type hint
        let type_oid: u32 = 25; // Default to TEXT
        tracing::info!("🚀 create_row_description_response: Column '{}' -> PostgreSQL OID {} ({})", 
            header, type_oid, postgres_type_name(type_oid));
        fields_data.extend_from_slice(&type_oid.to_be_bytes()); // Data type OID

        // Add type size (-1 for variable size)
        let type_size: i16 = -1;
        fields_data.extend_from_slice(&type_size.to_be_bytes());

        // Add type modifier (-1 for default)
        let type_modifier: i32 = -1;
        fields_data.extend_from_slice(&type_modifier.to_be_bytes());

        // Add format code (0 for text)
        let format_code: i16 = 0;
        fields_data.extend_from_slice(&format_code.to_be_bytes());
    }

    let length = 4 + fields_data.len();
    response.extend_from_slice(&(length as u32).to_be_bytes());
    response.extend_from_slice(&fields_data);
    response
}

pub(super) fn create_parse_complete_response() -> Vec<u8> {
    vec![b'1', 0, 0, 0, 4]
}

pub(super) fn create_bind_complete_response() -> Vec<u8> {
    vec![b'2', 0, 0, 0, 4]
}

pub(super) fn create_close_complete_response() -> Vec<u8> {
    vec![b'3', 0, 0, 0, 4]
}

pub(super) fn create_ready_for_query_response() -> Vec<u8> {
    vec![b'Z', 0, 0, 0, 5, b'I']
}

#[allow(dead_code)]
pub(super) fn create_command_complete_response(tag: &str) -> Vec<u8> {
    let mut response = vec![b'C'];
    let tag_bytes = tag.as_bytes();
    let length = (4 + tag_bytes.len() + 1) as u32;
    response.extend_from_slice(&length.to_be_bytes());
    response.extend_from_slice(tag_bytes);
    response.push(0);
    response
}

pub(super) fn create_parameter_description_response(param_oids: &[u32]) -> Vec<u8> {
    let mut response = vec![b't'];
    let mut data = Vec::new();
    data.extend_from_slice(&(param_oids.len() as u16).to_be_bytes());
    for oid in param_oids {
        data.extend_from_slice(&oid.to_be_bytes());
    }
    let length = (4 + data.len()) as u32;
    response.extend_from_slice(&length.to_be_bytes());
    response.extend_from_slice(&data);
    response
}

pub(super) fn create_empty_row_description_response() -> Vec<u8> {
    vec![b'n', 0, 0, 0, 4]
}

/// Format QueryResult directly to PostgreSQL wire protocol
pub(super) fn format_query_result_as_postgres_result(result: &crate::query_handler::QueryResult) -> Vec<u8> {
    let mut response = Vec::new();
    
    tracing::info!("🚀 format_query_result_as_postgres_result() CALLED with {} columns, {} rows", result.columns.len(), result.rows.len());
    tracing::info!("🚀 Columns: {:?}", result.columns);
    tracing::info!("🚀 Column types: {:?}", result.column_types);
    
    // RowDescription message: 'T' (RowDescription) + length + field_count + fields
    tracing::info!("🚀 Creating RowDescription message ('T') with {} columns", result.columns.len());
    response.push(b'T'); // 'T' = RowDescription message
    
    let mut fields_data = Vec::new();
    fields_data.extend_from_slice(&(result.columns.len() as u16).to_be_bytes());
    
    for (i, column_name) in result.columns.iter().enumerate() {
        fields_data.extend_from_slice(column_name.as_bytes());
        fields_data.push(0); // Null terminator for name
        
        // Add dummy table/column IDs
        fields_data.extend_from_slice(&0u32.to_be_bytes()); // Table OID
        fields_data.extend_from_slice(&(i as u16).to_be_bytes()); // Column index
        
        // Use provided type OID or default to TEXT
        let type_oid = if i < result.column_types.len() {
            result.column_types[i]
        } else {
            25 // TEXT
        };
        tracing::info!("🚀 RowDescription: Column '{}' -> PostgreSQL OID {} ({})", 
            column_name, type_oid, postgres_type_name(type_oid));
        fields_data.extend_from_slice(&type_oid.to_be_bytes());
        
        // Add type size (-1 for variable size)
        let type_size: i16 = -1;
        fields_data.extend_from_slice(&type_size.to_be_bytes());
        
        // Add type modifier (-1 for default)
        let type_modifier: i32 = -1;
        fields_data.extend_from_slice(&type_modifier.to_be_bytes());
        
        // Add format code (0 for text)
        let format_code: i16 = 0;
        fields_data.extend_from_slice(&format_code.to_be_bytes());
    }
    
    // Write length immediately after message type
    let length = 4 + fields_data.len();
    response.extend_from_slice(&(length as u32).to_be_bytes());
    response.extend_from_slice(&fields_data);
    
    tracing::debug!("🔧 RowDescription ('T') message: {} bytes total", response.len());
    
    // DataRow messages: 'D' (DataRow) + length + column_count + columns
    for row in &result.rows {
        response.push(b'D'); // 'D' = DataRow message
        
        let mut row_data = Vec::new();
        row_data.extend_from_slice(&(row.len() as u16).to_be_bytes());
        
        for (col_idx, value) in row.iter().enumerate() {
            // Get the column type to determine format
            let _type_oid = if col_idx < result.column_types.len() {
                result.column_types[col_idx]
            } else {
                25 // TEXT
            };
            
            match value {
                crate::query_handler::QueryValue::Null => {
                    row_data.extend_from_slice(&(-1i32).to_be_bytes());
                }
                crate::query_handler::QueryValue::Text(s) => {
                    row_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(s.as_bytes());
                }
                crate::query_handler::QueryValue::Integer(i) => {
                    let s = i.to_string();
                    tracing::debug!("🔧 Sending integer {} as text: '{}'", i, s);
                    row_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(s.as_bytes());
                }
                crate::query_handler::QueryValue::Float(f) => {
                    let s = f.to_string();
                    tracing::debug!("🔧 Sending float {} as text: '{}'", f, s);
                    row_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(s.as_bytes());
                }
                crate::query_handler::QueryValue::Timestamp(s) => {
                    row_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(s.as_bytes());
                }
                crate::query_handler::QueryValue::Boolean(b) => {
                    let s = if *b { "true" } else { "false" };
                    row_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(s.as_bytes());
                }
            }
        }
        
        let length = 4 + row_data.len();
        response.extend_from_slice(&(length as u32).to_be_bytes());
        response.extend_from_slice(&row_data);
    }
    
    tracing::debug!("🔧 Added {} DataRow ('D') messages", result.rows.len());
    
    // CommandComplete message: 'C' (CommandComplete) + length + tag
    response.push(b'C'); // 'C' = CommandComplete message
    let tag = format!("SELECT {}", result.rows.len());
    let tag_length = 4 + tag.len() + 1; // 4 bytes for length + tag + null terminator
    response.extend_from_slice(&(tag_length as u32).to_be_bytes());
    response.extend_from_slice(tag.as_bytes());
    response.push(0); // Null terminator
    
    // ReadyForQuery message: 'Z' (ReadyForQuery) + length + status
    response.push(b'Z'); // 'Z' = ReadyForQuery message
    response.extend_from_slice(&5u32.to_be_bytes()); // Length: 4 + 1 = 5
    response.push(b'I'); // Status: 'I' = idle (not in transaction)
    
    tracing::debug!("🔧 Complete PostgreSQL response: {} bytes total", response.len());
    
    response
}

/// Format QueryResult for Extended Query protocol (DataRow + CommandComplete only, no RowDescription)
pub(super) fn format_query_result_as_extended_query_result(result: &crate::query_handler::QueryResult) -> Vec<u8> {
    let mut response = Vec::new();
    
    tracing::info!("🚀 format_query_result_as_extended_query_result() CALLED: {} columns, {} rows", result.columns.len(), result.rows.len());
    
    // DataRow messages only (no RowDescription - that was sent by Describe)
    for row in &result.rows {
        response.push(b'D'); // 'D' = DataRow message
        
        let mut row_data = Vec::new();
        row_data.extend_from_slice(&(row.len() as u16).to_be_bytes());
        
        for (col_idx, value) in row.iter().enumerate() {
            // Get the column type to determine format
            let _type_oid = if col_idx < result.column_types.len() {
                result.column_types[col_idx]
            } else {
                25 // TEXT
            };
            
            match value {
                crate::query_handler::QueryValue::Null => {
                    row_data.extend_from_slice(&(-1i32).to_be_bytes());
                }
                crate::query_handler::QueryValue::Text(s) => {
                    row_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(s.as_bytes());
                }
                crate::query_handler::QueryValue::Integer(i) => {
                    let s = i.to_string();
                    tracing::debug!("🔧 Sending integer {} as text: '{}'", i, s);
                    row_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(s.as_bytes());
                }
                crate::query_handler::QueryValue::Float(f) => {
                    let s = f.to_string();
                    tracing::debug!("🔧 Sending float {} as text: '{}'", f, s);
                    row_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(s.as_bytes());
                }
                crate::query_handler::QueryValue::Timestamp(s) => {
                    row_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(s.as_bytes());
                }
                crate::query_handler::QueryValue::Boolean(b) => {
                    let s = if *b { "true" } else { "false" };
                    row_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    row_data.extend_from_slice(s.as_bytes());
                }
            }
        }
        
        let length = 4 + row_data.len();
        response.extend_from_slice(&(length as u32).to_be_bytes());
        response.extend_from_slice(&row_data);
    }
    
    tracing::debug!("🔧 Added {} DataRow ('D') messages for Extended Query", result.rows.len());
    
    // CommandComplete message: 'C' (CommandComplete) + length + tag
    response.push(b'C'); // 'C' = CommandComplete message
    let tag = format!("SELECT {}", result.rows.len());
    let tag_length = 4 + tag.len() + 1; // 4 bytes for length + tag + null terminator
    response.extend_from_slice(&(tag_length as u32).to_be_bytes());
    response.extend_from_slice(tag.as_bytes());
    response.push(0); // Null terminator
    
    tracing::debug!("🔧 Complete Extended Query response: {} bytes total", response.len());
    
    response
}

fn postgres_type_name(oid: u32) -> &'static str {
    match oid {
        16 => "bool",
        20 => "int8", 
        21 => "int2",
        23 => "int4",
        25 => "text",
        700 => "float4",
        701 => "float8", 
        1114 => "timestamp",
        _ => "unknown",
    }
}

// Parse CSV line handling quoted fields properly
pub(crate) fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current_field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    
    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes {
                    // Check if this is an escaped quote
                    if chars.peek() == Some(&'"') {
                        current_field.push('"');
                        chars.next(); // consume the second quote
                    } else {
                        in_quotes = false;
                    }
                } else {
                    in_quotes = true;
                }
            }
            ',' => {
                if in_quotes {
                    current_field.push(',');
                } else {
                    fields.push(current_field.clone());
                    current_field.clear();
                }
            }
            _ => {
                current_field.push(ch);
            }
        }
    }
    
    // Don't forget the last field
    fields.push(current_field);
    
    fields
}
