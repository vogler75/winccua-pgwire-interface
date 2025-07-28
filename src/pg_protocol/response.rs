pub(super) fn create_postgres_error_response(code: &str, message: &str) -> Vec<u8> {
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

pub(super) fn create_postgres_auth_ok_response() -> Vec<u8> {
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

pub(super) fn format_as_postgres_result(csv_data: &str) -> Vec<u8> {
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

    // Parse CSV header with potential type information
    let headers: Vec<&str> = lines[0].split(',').collect();
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
                clean_headers.push(header.to_string());
            }
        } else {
            clean_headers.push(header.to_string());
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
    for line in lines.iter().skip(1) {
        if line.trim().is_empty() {
            continue;
        }

        response.push(b'D');

        let values: Vec<&str> = line.split(',').collect();
        let mut row_data = Vec::new();
        row_data.extend_from_slice(&(values.len() as u16).to_be_bytes());

        for value in values {
            if value == "NULL" {
                row_data.extend_from_slice(&(-1i32).to_be_bytes());
            } else {
                row_data.extend_from_slice(&(value.len() as u32).to_be_bytes());
                row_data.extend_from_slice(value.as_bytes());
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
