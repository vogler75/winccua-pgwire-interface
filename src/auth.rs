use crate::graphql::{GraphQLClient, Session};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

static CONNECTION_ID_COUNTER: AtomicU32 = AtomicU32::new(1);

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConnectionInfo {
    pub connection_id: u32,                 // Unique connection ID (simulates PID)
    pub session_id: Option<String>,         // Links to AuthenticatedSession (None if not authenticated)
    pub username: Option<String>,           // Username (None if not authenticated)
    pub database_name: Option<String>,      // Database name (None if not specified)
    pub client_addr: SocketAddr,            // Client IP and port
    pub application_name: Option<String>,   // Client application name (None if not provided)
    pub backend_start: DateTime<Utc>,       // Connection start time
    pub query_start: Option<DateTime<Utc>>, // Current query start time
    pub query_stop: Option<DateTime<Utc>>,  // Query completion time
    pub state: ConnectionState,             // Connection state
    pub last_query: String,                 // Last or current query
    pub graphql_time_ms: Option<u64>,       // GraphQL execution time in milliseconds
    pub datafusion_time_ms: Option<u64>,    // DataFusion execution time in milliseconds
    pub overall_time_ms: Option<u64>,       // Overall query execution time in milliseconds
    pub last_alive_sent: Option<DateTime<Utc>>, // Last time a keep-alive was successfully sent
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ConnectionState {
    Active,
    Idle,
    IdleInTransaction,
    IdleInTransactionAborted,
}

impl ConnectionState {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            ConnectionState::Active => "active",
            ConnectionState::Idle => "idle",
            ConnectionState::IdleInTransaction => "idle in transaction",
            ConnectionState::IdleInTransactionAborted => "idle in transaction (aborted)",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthenticatedSession {
    pub session_id: String,
    #[allow(dead_code)]
    pub username: String,
    pub token: String,
    #[allow(dead_code)]
    pub expires: String,
    pub client: Arc<GraphQLClient>,
}

impl AuthenticatedSession {
    pub fn new(username: String, session: Session, client: Arc<GraphQLClient>) -> Self {
        Self {
            session_id: Uuid::new_v4().to_string(),
            username,
            token: session.token,
            expires: session.expires,
            client,
        }
    }

    /// Extend the session using the GraphQL client
    pub async fn extend_session(&mut self) -> Result<()> {
        debug!("🔄 Extending session {} for user {}", self.session_id, self.username);
        
        match self.client.extend_session(&self.token).await {
            Ok(new_session) => {
                self.token = new_session.token;
                self.expires = new_session.expires;
                debug!("✅ Session {} extended successfully for user {}", self.session_id, self.username);
                Ok(())
            }
            Err(e) => {
                error!("❌ Failed to extend session {} for user {}: {}", self.session_id, self.username, e);
                Err(e)
            }
        }
    }
}

#[derive(Debug)]
pub struct SessionManager {
    sessions: Arc<RwLock<HashMap<String, AuthenticatedSession>>>,
    connections: Arc<RwLock<HashMap<u32, ConnectionInfo>>>,
    graphql_url: String,
    extension_task_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    extension_interval_secs: u64,
    quiet_connections: bool,
}

impl SessionManager {
    #[allow(dead_code)]
    pub fn new(graphql_url: String) -> Self {
        Self::with_extension_interval(graphql_url, 600) // Default to 10 minutes
    }

    pub fn with_extension_interval(graphql_url: String, extension_interval_secs: u64) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            connections: Arc::new(RwLock::new(HashMap::new())),
            graphql_url,
            extension_task_handle: Arc::new(RwLock::new(None)),
            extension_interval_secs,
            quiet_connections: false,
        }
    }

    pub fn with_quiet_connections(mut self, quiet: bool) -> Self {
        self.quiet_connections = quiet;
        self
    }

    pub fn graphql_url(&self) -> &str {
        &self.graphql_url
    }

    pub fn extension_interval_secs(&self) -> u64 {
        self.extension_interval_secs
    }

    pub async fn authenticate(&self, username: &str, password: &str) -> Result<AuthenticatedSession> {
        debug!("Authenticating user: {}", username);
        
        let client = Arc::new(GraphQLClient::new(self.graphql_url.clone()));
        let session = client.login(username, password).await?;
        
        let auth_session = AuthenticatedSession::new(username.to_string(), session, client);
        
        // Store the session
        let mut sessions = self.sessions.write().await;
        sessions.insert(auth_session.session_id.clone(), auth_session.clone());
        
        // Start the session extension task if this is the first session
        if sessions.len() == 1 {
            drop(sessions); // Release the lock before starting the task
            self.start_session_extension_task().await;
        }
        
        debug!("User {} authenticated successfully with session {}", username, auth_session.session_id);
        Ok(auth_session)
    }

    #[allow(dead_code)]
    pub async fn get_session(&self, session_id: &str) -> Option<AuthenticatedSession> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).cloned()
    }

    #[allow(dead_code)]
    pub async fn remove_session(&self, session_id: &str) {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.remove(session_id) {
            if !self.quiet_connections {
                info!("🛑 Removed session {} for user {}", session_id, session.username);
            }
        }
        
        // Stop the extension task if no sessions remain
        if sessions.is_empty() {
            drop(sessions); // Release the lock before stopping the task
            self.stop_session_extension_task().await;
        }
    }


    #[allow(dead_code)]
    pub async fn session_count(&self) -> usize {
        self.sessions.read().await.len()
    }

    /// Start the background task that extends all active sessions periodically
    async fn start_session_extension_task(&self) {
        let sessions_clone = Arc::clone(&self.sessions);
        let mut handle_guard = self.extension_task_handle.write().await;
        let extension_interval_secs = self.extension_interval_secs;
        let quiet_connections = self.quiet_connections;
        
        // Don't start a new task if one is already running
        if handle_guard.is_some() {
            return;
        }
        
        if !quiet_connections {
            info!("🚀 Starting session extension background task ({}-second intervals)", extension_interval_secs);
        }
        
        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(extension_interval_secs));
            // Skip the first tick so we don't immediately extend newly created sessions
            interval.tick().await;
            
            loop {
                interval.tick().await;
                
                debug!("⏰ Session extension interval triggered");
                
                // Get all current sessions
                let sessions_to_extend = {
                    let sessions = sessions_clone.read().await;
                    sessions.values().cloned().collect::<Vec<_>>()
                };
                
                if sessions_to_extend.is_empty() {
                    debug!("📝 No active sessions to extend");
                    break; // Exit the loop if no sessions remain
                }
                
                if !quiet_connections {
                    info!("🔄 Extending {} active session(s)", sessions_to_extend.len());
                }
                
                // Extend each session
                for mut session in sessions_to_extend {
                    match session.extend_session().await {
                        Ok(()) => {
                            // Update the session in the map with the new token and expires
                            let mut sessions = sessions_clone.write().await;
                            sessions.insert(session.session_id.clone(), session);
                        }
                        Err(e) => {
                            error!("❌ Failed to extend session {}: {}. Removing session.", session.session_id, e);
                            // Remove failed session
                            let mut sessions = sessions_clone.write().await;
                            sessions.remove(&session.session_id);
                        }
                    }
                }
                
                // Check if we still have sessions after extension attempts
                let remaining_sessions = sessions_clone.read().await.len();
                if remaining_sessions == 0 {
                    debug!("📝 No sessions remaining after extension attempts. Stopping extension task.");                    
                    break;
                }
            }
            
            debug!("🛑 Session extension background task stopped");            
        });
        
        *handle_guard = Some(handle);
    }
    
    /// Stop the background session extension task
    async fn stop_session_extension_task(&self) {
        let mut handle_guard = self.extension_task_handle.write().await;
        
        if let Some(handle) = handle_guard.take() {
            handle.abort();
            if !self.quiet_connections {
                info!("🛑 Stopped session extension background task");
            }
        }
    }


    /// Register a new connection (after authentication)
    pub async fn register_connection(
        &self,
        session_id: &str,
        client_addr: SocketAddr,
        application_name: String,
    ) -> Result<u32> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id)
            .ok_or_else(|| anyhow::anyhow!("Session not found"))?;
        
        let connection_id = CONNECTION_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        
        let connection_info = ConnectionInfo {
            connection_id,
            session_id: Some(session_id.to_string()),
            username: Some(session.username.clone()),
            database_name: Some("winccua".to_string()),
            client_addr,
            application_name: Some(application_name),
            backend_start: Utc::now(),
            query_start: None,
            query_stop: None,
            state: ConnectionState::Idle,
            last_query: String::new(),
            graphql_time_ms: None,
            datafusion_time_ms: None,
            overall_time_ms: None,
            last_alive_sent: None,
        };
        
        let mut connections = self.connections.write().await;
        connections.insert(connection_id, connection_info);
        
        if !self.quiet_connections {
            info!("📊 Registered connection {} for user {} from {}", 
                connection_id, session.username, client_addr);
        }
        
        Ok(connection_id)
    }
    
    /// Unregister a connection and remove the session if no other connections are using it
    pub async fn unregister_connection(&self, connection_id: u32) {
        let session_id_to_check = {
            let mut connections = self.connections.write().await;
            if let Some(conn) = connections.remove(&connection_id) {
                if !self.quiet_connections {
                    info!("📊 Unregistered connection {} for user {:?} from {}", 
                        connection_id, conn.username, conn.client_addr);
                }
                conn.session_id
            } else {
                None
            }
        };
        
        // Check if any other connections are using this session
        if let Some(session_id) = session_id_to_check {
            let connections = self.connections.read().await;
            let session_still_in_use = connections.values()
                .any(|conn| conn.session_id.as_ref() == Some(&session_id));
            drop(connections);
            
            if !session_still_in_use {
                // No other connections are using this session, remove it
                self.remove_session(&session_id).await;
            }
        }
    }
    
    /// Update connection state for query execution
    #[allow(dead_code)]
    pub async fn start_query(&self, connection_id: u32, query: &str) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(&connection_id) {
            conn.state = ConnectionState::Active;
            conn.query_start = Some(Utc::now());
            conn.query_stop = None;
            conn.last_query = query.to_string();
            conn.graphql_time_ms = None;
            conn.datafusion_time_ms = None;
            conn.overall_time_ms = None;
            debug!("📊 Connection {} started query: {}", connection_id, query);
        }
    }
    
    /// Update connection state after query completion
    #[allow(dead_code)]
    pub async fn end_query(&self, connection_id: u32) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(&connection_id) {
            conn.state = ConnectionState::Idle;
            conn.query_stop = Some(Utc::now());
            
            // Calculate overall time if query_start is available
            if let Some(start_time) = conn.query_start {
                if let Some(stop_time) = conn.query_stop {
                    let duration = stop_time.signed_duration_since(start_time);
                    conn.overall_time_ms = Some(duration.num_milliseconds().max(0) as u64);
                }
            }
            
            debug!("📊 Connection {} ended query", connection_id);
        }
    }

    /// Update query timing metrics
    #[allow(dead_code)]
    pub async fn set_query_timings(&self, connection_id: u32, graphql_time_ms: Option<u64>, datafusion_time_ms: Option<u64>) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(&connection_id) {
            conn.graphql_time_ms = graphql_time_ms;
            conn.datafusion_time_ms = datafusion_time_ms;
            info!("📊 Updated connection {} timing - GraphQL: {:?}ms, DataFusion: {:?}ms", 
                connection_id, graphql_time_ms, datafusion_time_ms);
        } else {
            warn!("📊 Could not find connection {} to update timing", connection_id);
        }
    }

    /// Update query timing metrics including overall time
    #[allow(dead_code)]
    pub async fn set_all_query_timings(&self, connection_id: u32, graphql_time_ms: Option<u64>, datafusion_time_ms: Option<u64>, overall_time_ms: Option<u64>) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(&connection_id) {
            conn.graphql_time_ms = graphql_time_ms;
            conn.datafusion_time_ms = datafusion_time_ms;
            conn.overall_time_ms = overall_time_ms;
            debug!("📊 Updated connection {} timing - GraphQL: {:?}ms, DataFusion: {:?}ms, Overall: {:?}ms", 
                connection_id, graphql_time_ms, datafusion_time_ms, overall_time_ms);
        } else {
            warn!("📊 Could not find connection {} to update timing", connection_id);
        }
    }
    
    /// Clean up connections and sessions for a specific client address (used for abrupt disconnections)
    pub async fn cleanup_connections_by_address(&self, client_addr: SocketAddr) {
        let mut connections_to_remove = Vec::new();
        
        // Find all connections from this client address
        {
            let connections = self.connections.read().await;
            for (conn_id, conn_info) in connections.iter() {
                if conn_info.client_addr == client_addr {
                    connections_to_remove.push(*conn_id);
                    if !self.quiet_connections {
                        debug!("🧹 Found orphaned connection {} from {} for cleanup", conn_id, client_addr);
                    }
                }
            }
        }
        
        // Remove each connection (this will also remove associated sessions)
        for conn_id in connections_to_remove {
            self.unregister_connection(conn_id).await;
        }
    }
    
    /// Get all active connections
    #[allow(dead_code)]
    pub async fn get_connections(&self) -> Vec<ConnectionInfo> {
        let connections = self.connections.read().await;
        connections.values().cloned().collect()
    }
    
    /// Update last keep-alive sent time for a connection
    pub async fn update_last_alive_sent(&self, connection_id: u32) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(&connection_id) {
            conn.last_alive_sent = Some(Utc::now());
            debug!("💓 Updated last keep-alive time for connection {}", connection_id);
        }
    }
    
    /// Update connection state for transactions
    #[allow(dead_code)]
    pub async fn set_transaction_state(&self, connection_id: u32, in_transaction: bool, aborted: bool) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(&connection_id) {
            conn.state = if in_transaction {
                if aborted {
                    ConnectionState::IdleInTransactionAborted
                } else {
                    ConnectionState::IdleInTransaction
                }
            } else {
                ConnectionState::Idle
            };
        }
    }
}