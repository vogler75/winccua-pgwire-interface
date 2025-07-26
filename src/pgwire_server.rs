use crate::auth::SessionManager;
use crate::query_handler::QueryHandler;
use anyhow::Result;
use async_trait::async_trait;
use pgwire::api::auth::StartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, Type, PgWireHandlerFactory};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::startup::PgWireFrontendMessage;
use pgwire::tokio::process_socket;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

pub struct PgWireServer {
    session_manager: Arc<SessionManager>,
}

impl PgWireServer {
    pub fn new(graphql_url: String) -> Self {
        Self {
            session_manager: Arc::new(SessionManager::new(graphql_url)),
        }
    }

    pub async fn start(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("PostgreSQL wire protocol server listening on {}", addr);

        loop {
            let (socket, client_addr) = listener.accept().await?;
            info!("🔌 New PostgreSQL client connection from {}", client_addr);

            let session_manager = self.session_manager.clone();
            tokio::spawn(async move {
                let handler_factory = WinCCHandlerFactory::new(session_manager);
                
                if let Err(e) = process_socket(
                    socket,
                    None, // No TLS support for now
                    Arc::new(handler_factory),
                ).await {
                    error!("❌ Error processing PostgreSQL connection from {}: {}", client_addr, e);
                } else {
                    info!("✅ PostgreSQL connection from {} completed successfully", client_addr);
                }
            });
        }
    }
}

// Handler factory that creates handlers for each connection
pub struct WinCCHandlerFactory {
    session_manager: Arc<SessionManager>,
}

impl WinCCHandlerFactory {
    pub fn new(session_manager: Arc<SessionManager>) -> Self {
        Self { session_manager }
    }
}

impl PgWireHandlerFactory for WinCCHandlerFactory {
    type StartupHandler = WinCCStartupHandler;
    type SimpleQueryHandler = WinCCQueryHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        Arc::new(WinCCQueryHandler::new(self.session_manager.clone()))
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(WinCCStartupHandler::new(self.session_manager.clone()))
    }
    
    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }
    
    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

// Startup handler for authentication
pub struct WinCCStartupHandler {
    session_manager: Arc<SessionManager>,
}

impl WinCCStartupHandler {
    pub fn new(session_manager: Arc<SessionManager>) -> Self {
        Self { session_manager }
    }
}

#[async_trait]
impl StartupHandler for WinCCStartupHandler {
    async fn on_startup<C>(&self, _client: &mut C, message: PgWireFrontendMessage) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // For now, allow all connections
        // Authentication will be handled later in the query handler
        info!("✅ Client startup completed with message: {:?}", message);
        Ok(())
    }
}

// Query handler for PostgreSQL wire protocol
pub struct WinCCQueryHandler {
    session_manager: Arc<SessionManager>,
}

impl WinCCQueryHandler {
    pub fn new(session_manager: Arc<SessionManager>) -> Self {
        Self { session_manager }
    }
}

#[async_trait]
impl SimpleQueryHandler for WinCCQueryHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response<'_>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        info!("📥 Received SQL query: {}", query.trim());
        
        // For testing, use default credentials
        let session = match self.session_manager.authenticate("operator", "secret123").await {
            Ok(session) => session,
            Err(e) => {
                error!("❌ Failed to authenticate: {}", e);
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "28000".to_owned(), // Invalid authorization specification
                    format!("Authentication required: {}", e),
                ))));
            }
        };

        // Handle special PostgreSQL system queries
        if query.trim().to_lowercase().starts_with("select version()") {
            return Ok(vec![create_version_response()]);
        }

        if query.trim().to_lowercase().starts_with("show ") {
            return Ok(vec![create_show_response(query)]);
        }

        // Handle our virtual table queries
        match QueryHandler::execute_query(query, &session).await {
            Ok(csv_response) => {
                debug!("✅ Query executed successfully, parsing CSV response");
                Ok(vec![parse_csv_to_pgwire_response(csv_response)?])
            }
            Err(e) => {
                error!("❌ Query execution failed: {}", e);
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42000".to_owned(), // Syntax error or access rule violation
                    format!("Query failed: {}", e),
                ))))
            }
        }
    }
}

fn create_version_response<'a>() -> Response<'a> {
    let field = FieldInfo::new(
        "version".into(),
        None,
        None,
        Type::TEXT,
        FieldFormat::Text,
    );

    let mut encoder = DataRowEncoder::new(Arc::new([field]));
    encoder.encode_field(&Some("WinCC UA PostgreSQL Wire Protocol Server v0.1.0")).unwrap();

    Response::Query(QueryResponse::new(
        Arc::new([FieldInfo::new(
            "version".into(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        )]),
        vec![encoder.finish()],
    ))
}

fn create_show_response<'a>(query: &str) -> Response<'a> {
    let field = FieldInfo::new(
        "setting".into(),
        None,
        None,
        Type::TEXT,
        FieldFormat::Text,
    );

    let mut encoder = DataRowEncoder::new(Arc::new([field]));
    
    // Handle common SHOW commands
    let value = if query.to_lowercase().contains("timezone") {
        "UTC"
    } else if query.to_lowercase().contains("client_encoding") {
        "UTF8"
    } else if query.to_lowercase().contains("server_version") {
        "14.0 (WinCC UA Proxy)"
    } else {
        "unknown"
    };
    
    encoder.encode_field(&Some(value)).unwrap();

    Response::Query(QueryResponse::new(
        Arc::new([FieldInfo::new(
            "setting".into(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        )]),
        vec![encoder.finish()],
    ))
}

fn parse_csv_to_pgwire_response(csv_data: String) -> PgWireResult<Response<'static>> {
    let lines: Vec<&str> = csv_data.trim().split('\n').collect();
    
    if lines.is_empty() {
        return Ok(Response::Query(QueryResponse::new(Arc::new([]), vec![])));
    }

    // Parse header to create field info
    let headers: Vec<&str> = lines[0].split(',').collect();
    let fields: Vec<FieldInfo> = headers
        .iter()
        .map(|&header| {
            let field_type = match header {
                "tag_name" | "string_value" | "name" | "state" | "event_text" | "info_text" 
                | "origin" | "area" | "host_name" | "user_name" | "duration" => Type::TEXT,
                "timestamp" | "raise_time" | "acknowledgment_time" | "clear_time" 
                | "reset_time" | "modification_time" => Type::TIMESTAMPTZ,
                "numeric_value" => Type::NUMERIC,
                "instance_id" | "alarm_group_id" | "priority" => Type::INT4,
                "value" => Type::TEXT, // Could be numeric or text
                _ => Type::TEXT,
            };

            FieldInfo::new(
                header.to_owned().into(),
                None,
                None,
                field_type,
                FieldFormat::Text,
            )
        })
        .collect();

    let fields_arc = Arc::new(fields.into_boxed_slice());
    let mut rows = Vec::new();

    // Parse data rows
    for line in lines.iter().skip(1) {
        if line.trim().is_empty() {
            continue;
        }

        let values: Vec<&str> = line.split(',').collect();
        let mut encoder = DataRowEncoder::new(fields_arc.clone());

        for (i, &value) in values.iter().enumerate() {
            if i >= headers.len() {
                break; // Skip extra columns
            }

            let field_value = if value == "NULL" { None } else { Some(value) };
            encoder.encode_field(&field_value).map_err(|e| {
                error!("❌ Error encoding field {}: {}", headers.get(i).unwrap_or(&"unknown"), e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22000".to_owned(), // Data exception
                    format!("Failed to encode field: {}", e),
                )))
            })?;
        }

        rows.push(encoder.finish());
    }

    let row_count = rows.len();
    debug!("📊 Formatted {} rows for PostgreSQL response", row_count);

    Ok(Response::Query(QueryResponse::new(fields_arc, rows)))
}