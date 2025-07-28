use anyhow::Result;
use tracing::{debug, info, warn};

// Helper function to create a simple single-row QueryResult
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

fn create_command_complete_response_text(command_tag: &str) -> String {
    // For Simple Query protocol, we return a text response that will be formatted later
    // The actual PostgreSQL CommandComplete message will be created by format_as_postgres_result
    format!("COMMAND_COMPLETE:{}", command_tag)
}

fn create_empty_query_response() -> String {
    // Return a special marker for empty query that will be handled in the response formatting
    "EMPTY_QUERY_RESPONSE".to_string()
}

pub(super) async fn handle_simple_query(
    query: &str,
    session: &crate::auth::AuthenticatedSession,
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
            let result = crate::query_handler::QueryHandler::execute_query(query, session).await?;
            return Ok(super::response::format_query_result_as_postgres_result(&result));
        }

        // Handle SELECT statements with actual data (CSV format: header line, then data lines)
        // Remove trailing semicolons for comparison
        let query_without_semicolon = trimmed_query.trim_end_matches(';').trim();
        if query_without_semicolon == "SELECT 1" {
            info!("🔍 Returning data for SELECT 1");
            // This is often used as a keep-alive, so we'll extend the session.
            match session.client.extend_session(&session.token).await {
                Ok(_) => debug!("Session extended successfully"),
                Err(e) => warn!("Failed to extend session: {}", e),
            }
            let result = create_simple_query_result("?column?", vec![crate::query_handler::QueryValue::Text("1".to_string())]);
            return Ok(super::response::format_query_result_as_postgres_result(&result));
        } else if query_without_semicolon == "SELECT TRUE" {
            info!("🔍 Returning data for SELECT TRUE");
            let result = create_simple_query_result("?column?", vec![crate::query_handler::QueryValue::Boolean(true)]);
            return Ok(super::response::format_query_result_as_postgres_result(&result));
        } else if query_without_semicolon == "SELECT FALSE" {
            info!("🔍 Returning data for SELECT FALSE");
            let result = create_simple_query_result("?column?", vec![crate::query_handler::QueryValue::Boolean(false)]);
            return Ok(super::response::format_query_result_as_postgres_result(&result));
        } else if query_without_semicolon == "SELECT VERSION()" {
            info!("🔍 Returning data for SELECT VERSION()");
            let result = create_simple_query_result("version", vec![crate::query_handler::QueryValue::Text("WinCC Unified PostgreSQL Interface 1.0".to_string())]);
            return Ok(super::response::format_query_result_as_postgres_result(&result));
        } else if query_without_semicolon == "SELECT CURRENT_DATABASE()" {
            info!("🔍 Returning data for SELECT CURRENT_DATABASE()");
            let result = create_simple_query_result("current_database", vec![crate::query_handler::QueryValue::Text("system".to_string())]);
            return Ok(super::response::format_query_result_as_postgres_result(&result));
        }

        // For other utility statements, just acknowledge
        return Ok(create_command_complete_wire_response(
            &get_utility_command_tag(&trimmed_query),
        ));
    }

    // Use the new query handler for all SQL processing
    let result = crate::query_handler::QueryHandler::execute_query(query, session).await?;
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
    } else if query.starts_with("SELECT VERSION()")
        || query.starts_with("SELECT CURRENT_")
        || query.starts_with("SELECT SESSION_USER")
    {
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
