use crate::auth::SessionManager;
use crate::sql_handler::SqlHandler;
use crate::tables::SqlResult;
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::{
    response::{
        create_bind_complete_response, create_close_complete_response,
        create_empty_row_description_response,
        create_parameter_description_response, create_parse_complete_response,
        create_ready_for_query_response, create_row_description_response,
        create_row_description_response_with_types,
    },
    ConnectionState, Portal, PreparedStatement,
};

pub(super) async fn handle_postgres_message(
    data: &[u8],
    connection_state: &mut ConnectionState,
    session: &crate::auth::AuthenticatedSession,
    session_manager: Arc<SessionManager>,
    connection_id: Option<u32>,
    quiet_connections: bool,
) -> Result<Vec<u8>> {
    if data.len() < 5 {
        return Err(anyhow!("Message too short"));
    }

    let message_type = data[0];
    let length = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;

    if data.len() < length + 1 {
        // Incomplete message - this is normal and not an error, just return early
        return Err(anyhow!("INCOMPLETE_MESSAGE"));
    }

    let payload = &data[5..5 + length - 4];

    debug!(
        "📨 Processing PostgreSQL message type: '{}' (0x{:02X}), length: {}",
        if message_type.is_ascii_graphic() {
            message_type as char
        } else {
            '?'
        },
        message_type,
        length
    );

    let result = match message_type {
        b'Q' => handle_simple_query_message(payload, session, session_manager.clone(), connection_id).await,
        b'P' => handle_parse_message(payload, connection_state).await,
        b'B' => handle_bind_message(payload, connection_state).await,
        b'E' => handle_execute_message(payload, connection_state, session, session_manager.clone(), connection_id).await,
        b'D' => handle_describe_message(payload, connection_state, session, session_manager.clone(), connection_id).await,
        b'C' => handle_close_message(payload, connection_state).await,
        b'S' => handle_sync_message().await,
        b'X' => handle_terminate_message(quiet_connections).await,
        _ => {
            warn!(
                "❓ Unsupported PostgreSQL message type: '{}' (0x{:02X})",
                if message_type.is_ascii_graphic() {
                    message_type as char
                } else {
                    '?'
                },
                message_type
            );
            Err(anyhow!("Unsupported message type: 0x{:02X}", message_type))
        }
    };
    
    // Log response details
    if let Ok(ref response) = result {
        if !response.is_empty() {
            debug!("📤 Response for message type '{}': {} bytes", 
                if message_type.is_ascii_graphic() { message_type as char } else { '?' },
                response.len()
            );
        }
    }
    
    result
}

async fn handle_simple_query_message(
    payload: &[u8],
    session: &crate::auth::AuthenticatedSession,
    session_manager: Arc<SessionManager>,
    connection_id: Option<u32>,
) -> Result<Vec<u8>> {
    let query_str = std::str::from_utf8(payload)
        .map_err(|_| anyhow!("Invalid UTF-8 in query"))?
        .trim_end_matches('\0');

    // Handle empty queries - PostgreSQL returns CommandComplete with empty tag for empty queries
    if query_str.trim().is_empty() {
        debug!("📥 Empty simple query received, returning CommandComplete");
        let mut response = Vec::new();
        
        // Send CommandComplete with empty tag
        response.extend_from_slice(&super::response::create_command_complete_response(""));
        // Send ReadyForQuery
        response.extend_from_slice(&super::response::create_ready_for_query_response());
        
        return Ok(response);
    }

    if crate::LOG_SQL.load(std::sync::atomic::Ordering::Relaxed) {
        info!("📥 SQL Query: {}", query_str.trim().replace('\n', " ").replace('\r', ""));
    } else {
        debug!("📥 SQL Query: {}", query_str.trim().replace('\n', " ").replace('\r', ""));
    }

    // Start query tracking
    if let Some(conn_id) = connection_id {
        session_manager.start_query(conn_id, query_str).await;
    }

    let result = match super::query_execution::handle_simple_query_with_connection(query_str, session, session_manager.clone(), connection_id).await {
        Ok(response) => {
            if let Some(conn_id) = connection_id {
                // End query tracking - overall time will be calculated automatically
                session_manager.end_query(conn_id).await;
            }
            Ok(response)
        },
        Err(e) => {
            // End query tracking on error
            if let Some(conn_id) = connection_id {
                session_manager.end_query(conn_id).await;
            }
            Err(e)
        }
    };

    result
}

async fn handle_parse_message(
    payload: &[u8],
    connection_state: &mut ConnectionState,
) -> Result<Vec<u8>> {
    let mut pos = 0;

    // Extract statement name (null-terminated string)
    let statement_name = extract_null_terminated_string(payload, &mut pos)?;

    // Extract query string (null-terminated string)
    let query = extract_null_terminated_string(payload, &mut pos)?;
    
    // Handle empty queries - PostgreSQL expects a ParseComplete response for empty queries
    if query.trim().is_empty() {
        debug!("📋 Parse: received empty query, returning ParseComplete");
        let prepared_stmt = PreparedStatement {
            name: statement_name.clone(),
            query: query.clone(),
            parameter_types: Vec::new(),
        };
        connection_state
            .prepared_statements
            .insert(statement_name, prepared_stmt);
        
        return Ok(create_parse_complete_response());
    }

    // Extract parameter count
    if pos + 2 > payload.len() {
        return Err(anyhow!("Incomplete parse message"));
    }
    let param_count = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;

    // Extract parameter types (OIDs)
    let mut parameter_types = Vec::new();
    for _ in 0..param_count {
        if pos + 4 > payload.len() {
            return Err(anyhow!("Incomplete parameter types"));
        }
        let oid = u32::from_be_bytes([
            payload[pos],
            payload[pos + 1],
            payload[pos + 2],
            payload[pos + 3],
        ]);
        parameter_types.push(oid);
        pos += 4;
    }

    debug!(
        "📋 Parse: statement='{}', query='{}', params={}",
        statement_name,
        query.trim(),
        param_count
    );

    // For SET statements, we don't need to validate further.
    if query.trim().to_uppercase().starts_with("SET") {
        debug!("📋 Parse: accepting SET statement: {}", query.trim());
        let prepared_stmt = PreparedStatement {
            name: statement_name.clone(),
            query: query.clone(),
            parameter_types,
        };
        connection_state
            .prepared_statements
            .insert(statement_name, prepared_stmt);
        return Ok(create_parse_complete_response());
    }

    let trimmed_query = query.trim().to_uppercase();

    // Check if this is a transaction control or utility statement that should be allowed
    if super::query_execution::is_transaction_control_statement(&trimmed_query)
        || super::query_execution::is_utility_statement(&trimmed_query)
    {
        debug!(
            "📋 Parse: accepting transaction/utility statement: {}",
            query.trim()
        );

        // Store the prepared statement even though it's a utility statement
        let prepared_stmt = PreparedStatement {
            name: statement_name.clone(),
            query: query.clone(),
            parameter_types,
        };
        connection_state
            .prepared_statements
            .insert(statement_name, prepared_stmt);

        return Ok(create_parse_complete_response());
    }

    // Validate the SQL query using the same parser as the query handler
    match crate::sql_handler::SqlHandler::parse_query(&query) {
        Ok(_sql_result) => {
            // Query is valid, store the prepared statement
            let prepared_stmt = PreparedStatement {
                name: statement_name.clone(),
                query: query.clone(),
                parameter_types,
            };
            connection_state
                .prepared_statements
                .insert(statement_name, prepared_stmt);

            // Send ParseComplete response
            Ok(create_parse_complete_response())
        }
        Err(e) => {
            Err(anyhow!("{}", format!("Unsupported or invalid SQL statement: {}: Query: {}", e, query.trim())))
        }
    }
}

async fn handle_bind_message(
    payload: &[u8],
    connection_state: &mut ConnectionState,
) -> Result<Vec<u8>> {
    let mut pos = 0;

    // Extract portal name
    let portal_name = extract_null_terminated_string(payload, &mut pos)?;

    // Extract statement name
    let statement_name = extract_null_terminated_string(payload, &mut pos)?;

    // Parameter format codes count
    if pos + 2 > payload.len() {
        return Err(anyhow!("Incomplete bind message"));
    }
    let format_count = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;

    // Skip format codes for now (we'll assume text format)
    pos += format_count * 2;

    // Parameter values count
    if pos + 2 > payload.len() {
        return Err(anyhow!("Incomplete parameter count"));
    }
    let param_count = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;

    // Extract parameter values
    let mut parameters = Vec::new();
    for _ in 0..param_count {
        if pos + 4 > payload.len() {
            return Err(anyhow!("Incomplete parameter length"));
        }
        let param_length = i32::from_be_bytes([
            payload[pos],
            payload[pos + 1],
            payload[pos + 2],
            payload[pos + 3],
        ]);
        pos += 4;

        if param_length == -1 {
            // NULL parameter
            parameters.push(None);
        } else {
            let param_length = param_length as usize;
            if pos + param_length > payload.len() {
                return Err(anyhow!("Incomplete parameter value"));
            }
            let param_value = std::str::from_utf8(&payload[pos..pos + param_length])
                .map_err(|_| anyhow!("Invalid UTF-8 in parameter"))?
                .to_string();
            parameters.push(Some(param_value));
            pos += param_length;
        }
    }

    debug!(
        "🔗 Bind: portal='{}', statement='{}', params={:?}",
        portal_name, statement_name, parameters
    );

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
    session: &crate::auth::AuthenticatedSession,
    session_manager: Arc<SessionManager>,
    connection_id: Option<u32>,
) -> Result<Vec<u8>> {
    let mut pos = 0;

    // Extract portal name
    let portal_name = extract_null_terminated_string(payload, &mut pos)?;

    // Extract max rows (we'll ignore this for now)
    if pos + 4 > payload.len() {
        return Err(anyhow!("Incomplete execute message"));
    }
    let _max_rows = u32::from_be_bytes([
        payload[pos],
        payload[pos + 1],
        payload[pos + 2],
        payload[pos + 3],
    ]);

    debug!("⚡ Execute: portal='{}'", portal_name);

    // Get the portal
    let portal = connection_state
        .portals
        .get(&portal_name)
        .ok_or_else(|| anyhow!("Portal '{}' not found", portal_name))?;

    // Get the prepared statement
    let statement = connection_state
        .prepared_statements
        .get(&portal.statement_name)
        .ok_or_else(|| anyhow!("Statement '{}' not found", portal.statement_name))?;

    // Substitute parameters in the query
    let final_query = substitute_parameters(&statement.query, &portal.parameters)?;

    // Handle empty queries in extended query protocol
    if final_query.trim().is_empty() {
        debug!("🔍 Empty extended query received, returning CommandComplete");
        let mut response = Vec::new();
        
        // Send CommandComplete with empty tag
        response.extend_from_slice(&super::response::create_command_complete_response(""));
        
        return Ok(response);
    }

    debug!("🔍 Executing parameterized query: {}", final_query.trim());

    // Start query tracking with timing
    let query_start = std::time::Instant::now();
    if let Some(conn_id) = connection_id {
        session_manager.start_query(conn_id, &final_query).await;
    }

    // Execute the query - for Extended Query protocol, we need a different response format
    let result = match super::query_execution::handle_extended_query_with_connection(&final_query, session, session_manager.clone(), connection_id).await {
        Ok(response) => {
            debug!("📤 Extended query result: {} bytes", response.len());
            // Log the message types in the response
            let mut pos = 0;
            while pos < response.len() && pos + 5 <= response.len() {
                let msg_type = response[pos] as char;
                let msg_len = u32::from_be_bytes([response[pos+1], response[pos+2], response[pos+3], response[pos+4]]) as usize;
                debug!("   Message type '{}' ({} bytes)", msg_type, msg_len);
                pos += 1 + msg_len;
            }
            
            // Capture timing and end query tracking
            let overall_time_ms = query_start.elapsed().as_millis() as u64;
            if let Some(conn_id) = connection_id {
                session_manager.end_query(conn_id).await;
                debug!("🕐 Extended query completed in {}ms for connection {}", overall_time_ms, conn_id);
            }
            Ok(response)
        }
        Err(e) => {
            // End query tracking on error
            if let Some(conn_id) = connection_id {
                session_manager.end_query(conn_id).await;
            }
            Err(e)
        }
    };

    result
}

async fn handle_describe_message(
    payload: &[u8],
    connection_state: &ConnectionState,
    session: &crate::auth::AuthenticatedSession,
    session_manager: Arc<SessionManager>,
    connection_id: Option<u32>,
) -> Result<Vec<u8>> {
    if payload.is_empty() {
        return Err(anyhow!("Empty describe message"));
    }

    let object_type = payload[0];
    let object_name = std::str::from_utf8(&payload[1..])
        .map_err(|_| anyhow!("Invalid UTF-8 in describe message"))?
        .trim_end_matches('\0');

    debug!(
        "📄 Describe: type='{}', name='{}'",
        object_type as char, object_name
    );

    match object_type {
        b'S' => {
            // Describe statement - return both ParameterDescription and RowDescription
            if let Some(statement) = connection_state.prepared_statements.get(object_name) {
                let mut response = Vec::new();
                
                // First send ParameterDescription
                response.extend_from_slice(&create_parameter_description_response(&statement.parameter_types));
                
                // For SELECT queries, also send RowDescription
                let trimmed_query = statement.query.trim().to_uppercase();
                if !trimmed_query.starts_with("SET")
                    && !super::query_execution::is_transaction_control_statement(&trimmed_query)
                    && !super::query_execution::is_utility_statement(&trimmed_query)
                {
                    // Execute the query to get proper column types (like Execute message does)
                    tracing::debug!("🚀 Describe message: Executing query to get proper column types");
                    match crate::query_handler::QueryHandler::execute_query_with_connection(&statement.query, session, session_manager.clone(), connection_id).await {
                        Ok(query_result) => {
                            tracing::debug!("🚀 Describe message: Generated QueryResult with {} columns and proper OIDs", query_result.columns.len());
                            response.extend_from_slice(&create_row_description_response_with_types(&query_result));
                        }
                        Err(e) => {
                            tracing::warn!("🚀 Describe message: Query execution failed, falling back to schema-based types: {}", e);
                            // Fallback to parsing only if execution fails
                            match SqlHandler::parse_query(&statement.query) {
                                Ok(SqlResult::Query(query_info)) => {
                                    response.extend_from_slice(&create_row_description_response(&query_info));
                                }
                                _ => {
                                    // For non-SELECT statements, send NoData
                                    response.push(b'n'); // NoData message
                                    response.extend_from_slice(&4u32.to_be_bytes()); // Length: 4
                                }
                            }
                        }
                    }
                } else {
                    // For SET/utility statements, send NoData
                    response.push(b'n'); // NoData message
                    response.extend_from_slice(&4u32.to_be_bytes()); // Length: 4
                }
                
                Ok(response)
            } else {
                Err(anyhow!("Statement '{}' not found", object_name))
            }
        }
        b'P' => {
            // Describe portal - return proper row description based on the query
            if let Some(portal) = connection_state.portals.get(object_name) {
                if let Some(statement) = connection_state.prepared_statements.get(&portal.statement_name) {
                    // For SET statements and utility statements, return empty row description
                    let trimmed_query = statement.query.trim().to_uppercase();
                    if trimmed_query.starts_with("SET")
                        || super::query_execution::is_transaction_control_statement(&trimmed_query)
                        || super::query_execution::is_utility_statement(&trimmed_query)
                    {
                        return Ok(create_empty_row_description_response());
                    }
                    
                    // For SELECT queries, execute the query to get proper column types
                    tracing::debug!("🚀 Portal Describe: Executing query to get proper column types");
                    match crate::query_handler::QueryHandler::execute_query_with_connection(&statement.query, session, session_manager.clone(), connection_id).await {
                        Ok(query_result) => {
                            tracing::debug!("🚀 Portal Describe: Generated QueryResult with {} columns and proper OIDs", query_result.columns.len());
                            Ok(create_row_description_response_with_types(&query_result))
                        }
                        Err(e) => {
                            tracing::warn!("🚀 Portal Describe: Query execution failed, falling back to schema-based types: {}", e);
                            // Fallback to parsing only if execution fails
                            match SqlHandler::parse_query(&statement.query) {
                                Ok(SqlResult::Query(query_info)) => {
                                    Ok(create_row_description_response(&query_info))
                                }
                                Ok(SqlResult::SetStatement(_)) => {
                                    Ok(create_empty_row_description_response())
                                }
                                Err(_) => {
                                    // Fallback to empty row description if parsing fails
                                    Ok(create_empty_row_description_response())
                                }
                            }
                        }
                    }
                } else {
                    Err(anyhow!("Statement '{}' not found for portal '{}'", portal.statement_name, object_name))
                }
            } else {
                Err(anyhow!("Portal '{}' not found", object_name))
            }
        }
        _ => Err(anyhow!("Invalid describe type: {}", object_type as char)),
    }
}

async fn handle_close_message(
    payload: &[u8],
    connection_state: &mut ConnectionState,
) -> Result<Vec<u8>> {
    if payload.is_empty() {
        return Err(anyhow!("Empty close message"));
    }

    let object_type = payload[0];
    let object_name = std::str::from_utf8(&payload[1..])
        .map_err(|_| anyhow!("Invalid UTF-8 in close message"))?
        .trim_end_matches('\0');

    debug!(
        "🔒 Close: type='{}', name='{}'",
        object_type as char, object_name
    );

    match object_type {
        b'S' => {
            // Close statement
            connection_state.prepared_statements.remove(object_name);
        }
        b'P' => {
            // Close portal
            connection_state.portals.remove(object_name);
        }
        _ => return Err(anyhow!("Invalid close type: {}", object_type as char)),
    }

    Ok(create_close_complete_response())
}

async fn handle_sync_message() -> Result<Vec<u8>> {
    debug!("🔄 Sync");
    Ok(create_ready_for_query_response())
}

async fn handle_terminate_message(quiet_connections: bool) -> Result<Vec<u8>> {
    if !quiet_connections {
        debug!("🔚 Terminate: Client requested graceful connection termination");
    }
    // Return a special marker that signals the connection should be closed
    // We'll use an error with a specific message that the caller can check
    Err(anyhow!("TERMINATE_CONNECTION"))
}

fn substitute_parameters(query: &str, params: &[Option<String>]) -> Result<String> {
    let mut final_query = query.to_string();
    for (i, param) in params.iter().enumerate() {
        let placeholder = format!("${}", i + 1);
        let value = match param {
            Some(val) => format!("'{}'", val.replace('\'', "''")), // Quote and escape strings
            None => "NULL".to_string(),
        };
        final_query = final_query.replace(&placeholder, &value);
    }
    Ok(final_query)
}

fn extract_null_terminated_string<'a>(payload: &'a [u8], pos: &mut usize) -> Result<String> {
    let start = *pos;
    while *pos < payload.len() && payload[*pos] != 0 {
        *pos += 1;
    }
    if *pos >= payload.len() {
        return Err(anyhow!("Incomplete null-terminated string"));
    }
    let result = std::str::from_utf8(&payload[start..*pos])
        .map_err(|_| anyhow!("Invalid UTF-8 in string"))?
        .to_string();
    *pos += 1; // Skip null terminator
    Ok(result)
}

