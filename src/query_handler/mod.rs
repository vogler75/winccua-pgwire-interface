
pub mod active_alarms_handler;
pub mod logged_alarms_handler;
pub mod logged_tag_values_handler;
pub mod tag_list_handler;
pub mod tag_values_handler;

mod filter;
mod util;

use crate::auth::AuthenticatedSession;
use crate::sql_handler::SqlHandler;
use crate::tables::{SqlResult, VirtualTable};
use anyhow::Result;
use tracing::{debug, info, warn};

pub struct QueryHandler;

impl QueryHandler {
    pub async fn execute_query(sql: &str, session: &AuthenticatedSession) -> Result<String> {
        info!("🔍 Executing SQL query: {}", sql.trim());

        // Parse the SQL query
        let sql_result = match SqlHandler::parse_query(sql) {
            Ok(result) => result,
            Err(e) => {
                // Check if this is an unknown table error and log the SQL statement
                let error_msg = e.to_string();
                if error_msg.starts_with("Unknown table:") {
                    warn!("❌ Unknown table in SQL query: {}", sql.trim());
                    warn!("❌ {}", error_msg);
                    warn!("📋 Available tables: tagvalues, loggedtagvalues, activealarms, loggedalarms, taglist");
                }
                return Err(e);
            }
        };
        debug!("📋 Parsed SQL result: {:?}", sql_result);

        // Handle based on result type
        match sql_result {
            SqlResult::Query(query_info) => {
                // Execute based on table type
                match query_info.table {
                    VirtualTable::TagValues => Self::execute_tag_values_query(&query_info, session).await,
                    VirtualTable::LoggedTagValues => Self::execute_logged_tag_values_query(&query_info, session).await,
                    VirtualTable::ActiveAlarms => Self::execute_active_alarms_query(&query_info, session).await,
                    VirtualTable::LoggedAlarms => Self::execute_logged_alarms_query(&query_info, session).await,
                    VirtualTable::TagList => Self::execute_tag_list_query(&query_info, session).await,
                }
            }
            SqlResult::SetStatement(set_command) => {
                info!("✅ Successfully executed SET statement: {}", set_command);
                // Return a command complete response for SET statements
                Ok("COMMAND_COMPLETE:SET".to_string())
            }
        }
    }
}
