use crate::auth::SessionManager;
use anyhow::Result;
use std::sync::Arc;
use tracing::{debug, info};

// Helper function to create a simple single-row QueryResult
#[allow(dead_code)]
fn create_simple_query_result(column_name: &str, values: Vec<crate::query_handler::QueryValue>) -> crate::query_handler::QueryResult {
    use crate::query_handler::QueryResult;
    
    let mut result = QueryResult::new(vec![column_name.to_string()], vec![25]); // TEXT type
    result.add_row(values);
    result
}

// Helper function to create CommandComplete wire response
fn create_command_complete_wire_response(tag: &str) -> Vec<u8> {
    let mut response = Vec::new();
    
    // CommandComplete message: 'C' + length + tag + null
    response.push(b'C');
    let tag_length = 4 + tag.len() + 1;
    response.extend_from_slice(&(tag_length as u32).to_be_bytes());
    response.extend_from_slice(tag.as_bytes());
    response.push(0);
    
    // ReadyForQuery message: 'Z' + length + status
    response.push(b'Z');
    response.extend_from_slice(&5u32.to_be_bytes());
    response.push(b'I'); // Idle
    
    response
}

#[allow(dead_code)]
fn create_command_complete_response_text(command_tag: &str) -> String {
    // For Simple Query protocol, we return a text response that will be formatted later
    // The actual PostgreSQL CommandComplete message will be created by format_as_postgres_result
    format!("COMMAND_COMPLETE:{}", command_tag)
}

#[allow(dead_code)]
fn create_empty_query_response() -> String {
    // Return a special marker for empty query that will be handled in the response formatting
    "EMPTY_QUERY_RESPONSE".to_string()
}

#[allow(dead_code)]
pub(super) async fn handle_extended_query(
    query: &str,
    session: &crate::auth::AuthenticatedSession,
    session_manager: Arc<SessionManager>,
) -> Result<Vec<u8>> {
    handle_extended_query_with_connection(query, session, session_manager, None).await
}

pub(super) async fn handle_extended_query_with_connection(
    query: &str,
    session: &crate::auth::AuthenticatedSession,
    session_manager: Arc<SessionManager>,
    connection_id: Option<u32>,
) -> Result<Vec<u8>> {
    debug!("🔍 Processing extended query: {}", query.trim());

    // Handle empty queries (just whitespace and/or semicolons)
    let cleaned_query = query.trim().trim_end_matches(';').trim();
    if cleaned_query.is_empty() {
        info!("⚪ Empty extended query received, returning CommandComplete");
        return Ok(create_command_complete_wire_response(""));
    }

    let trimmed_query = query.trim().to_uppercase();

    // Handle transaction control statements that can be safely acknowledged
    if is_transaction_control_statement(&trimmed_query) {
        info!(
            "📋 Transaction control statement (acknowledged): {}",
            query.trim()
        );
        return Ok(create_command_complete_wire_response(
            &get_transaction_command_tag(&trimmed_query),
        ));
    }

    // Handle other utility statements
    if is_utility_statement(&trimmed_query) {
        info!("🔧 Utility statement: {}", query.trim());

        // Check if this is a SET statement - if so, use QueryHandler for proper parsing
        if trimmed_query.starts_with("SET ") {
            info!(
                "🔧 SET statement detected, routing to QueryHandler: {}",
                query.trim()
            );
            let result = crate::query_handler::QueryHandler::execute_query_with_connection(query, session, session_manager.clone(), connection_id).await?;
            return Ok(super::response::format_query_result_as_extended_query_result(&result));
        }


        // For other utility statements, just acknowledge
        return Ok(create_command_complete_wire_response(
            &get_utility_command_tag(&trimmed_query),
        ));
    }

    // Use the new query handler for all SQL processing
    let result = crate::query_handler::QueryHandler::execute_query_with_connection(query, session, session_manager.clone(), connection_id).await?;
    Ok(super::response::format_query_result_as_extended_query_result(&result))
}

pub(super) async fn handle_simple_query(
    query: &str,
    session: &crate::auth::AuthenticatedSession,
    session_manager: Arc<SessionManager>,
) -> Result<Vec<u8>> {
    handle_simple_query_with_connection(query, session, session_manager, None).await
}

pub(super) async fn handle_simple_query_with_connection(
    query: &str,
    session: &crate::auth::AuthenticatedSession,
    session_manager: Arc<SessionManager>,
    connection_id: Option<u32>,
) -> Result<Vec<u8>> {
    debug!("🔍 Processing query: {}", query.trim());

    // Handle empty queries (just whitespace and/or semicolons)
    let cleaned_query = query.trim().trim_end_matches(';').trim();
    if cleaned_query.is_empty() {
        info!("⚪ Empty query received, returning empty query response");
        // Return empty query response
        let mut response = Vec::new();
        response.push(b'I'); // EmptyQueryResponse
        response.extend_from_slice(&4u32.to_be_bytes());
        response.push(b'Z'); // ReadyForQuery
        response.extend_from_slice(&5u32.to_be_bytes());
        response.push(b'I'); // Idle
        return Ok(response);
    }

    let trimmed_query = query.trim().to_uppercase();

    // Handle transaction control statements that can be safely acknowledged
    if is_transaction_control_statement(&trimmed_query) {
        info!(
            "📋 Transaction control statement (acknowledged): {}",
            query.trim()
        );
        return Ok(create_command_complete_wire_response(
            &get_transaction_command_tag(&trimmed_query),
        ));
    }

    // Handle other utility statements
    if is_utility_statement(&trimmed_query) {
        info!("🔧 Utility statement: {}", query.trim());

        // Check if this is a SET statement - if so, use QueryHandler for proper parsing
        if trimmed_query.starts_with("SET ") {
            info!(
                "🔧 SET statement detected, routing to QueryHandler: {}",
                query.trim()
            );
            let result = crate::query_handler::QueryHandler::execute_query_with_connection(query, session, session_manager.clone(), connection_id).await?;
            return Ok(super::response::format_query_result_as_postgres_result(&result));
        }


        // For other utility statements, just acknowledge
        return Ok(create_command_complete_wire_response(
            &get_utility_command_tag(&trimmed_query),
        ));
    }

    // Use the new query handler for all SQL processing
    let result = crate::query_handler::QueryHandler::execute_query_with_connection(query, session, session_manager.clone(), connection_id).await?;
    tracing::info!("🚀 Received result from QueryHandler");
    Ok(super::response::format_query_result_as_postgres_result(&result))
}

pub(super) fn is_transaction_control_statement(query: &str) -> bool {
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

pub(super) fn is_utility_statement(query: &str) -> bool {
    // Only handle truly non-SQL statements that can't be parsed by DataFusion
    let utility_patterns = [
        // Session configuration
        "SET ",
        "RESET ",
        "SHOW ",
        // DISCARD statements (PostgreSQL-specific)
        "DISCARD ALL",
        "DISCARD PLANS", 
        "DISCARD SEQUENCES",
        "DISCARD TEMPORARY",
        // Listen/Notify (PostgreSQL-specific)
        "LISTEN ",
        "UNLISTEN ",
        "NOTIFY ",
        // Maintenance commands (PostgreSQL-specific)
        "VACUUM",
        "ANALYZE", 
        "REINDEX",
        // User/Role management (PostgreSQL-specific)
        "CREATE USER",
        "CREATE ROLE",
        "ALTER USER", 
        "ALTER ROLE",
        "DROP USER",
        "DROP ROLE",
        "GRANT ",
        "REVOKE ",
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
        "SHOW".to_string()
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
    } else if query.starts_with("CREATE USER") || query.starts_with("CREATE ROLE") {
        "CREATE".to_string()
    } else if query.starts_with("ALTER USER") || query.starts_with("ALTER ROLE") {
        "ALTER".to_string() 
    } else if query.starts_with("DROP USER") || query.starts_with("DROP ROLE") {
        "DROP".to_string()
    } else if query.starts_with("GRANT ") {
        "GRANT".to_string()
    } else if query.starts_with("REVOKE ") {
        "REVOKE".to_string()
    } else {
        "OK".to_string()
    }
}
