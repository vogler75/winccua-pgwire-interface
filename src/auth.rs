use crate::graphql::{GraphQLClient, Session};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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
                info!("✅ Session {} extended successfully for user {}", self.session_id, self.username);
                Ok(())
            }
            Err(e) => {
                error!("❌ Failed to extend session {} for user {}: {}", self.session_id, self.username, e);
                Err(e)
            }
        }
    }

    #[allow(dead_code)]
    pub fn is_expired(&self) -> bool {
        // Parse the expires timestamp and check if it's past current time
        // For now, we'll assume sessions don't expire during the connection
        // In a production system, you'd want to parse the timestamp and check
        false
    }
}

#[derive(Debug)]
pub struct SessionManager {
    sessions: Arc<RwLock<HashMap<String, AuthenticatedSession>>>,
    graphql_url: String,
    extension_task_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl SessionManager {
    pub fn new(graphql_url: String) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            graphql_url,
            extension_task_handle: Arc::new(RwLock::new(None)),
        }
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
        
        info!("User {} authenticated successfully with session {}", username, auth_session.session_id);
        Ok(auth_session)
    }

    #[allow(dead_code)]
    pub async fn get_session(&self, session_id: &str) -> Option<AuthenticatedSession> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id)?;
        
        if session.is_expired() {
            warn!("Session {} for user {} has expired", session_id, session.username);
            return None;
        }
        
        Some(session.clone())
    }

    #[allow(dead_code)]
    pub async fn remove_session(&self, session_id: &str) {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.remove(session_id) {
            info!("Removed session {} for user {}", session_id, session.username);
        }
        
        // Stop the extension task if no sessions remain
        if sessions.is_empty() {
            drop(sessions); // Release the lock before stopping the task
            self.stop_session_extension_task().await;
        }
    }

    #[allow(dead_code)]
    pub async fn cleanup_expired_sessions(&self) {
        let mut sessions = self.sessions.write().await;
        let mut expired_sessions = Vec::new();
        
        for (id, session) in sessions.iter() {
            if session.is_expired() {
                expired_sessions.push(id.clone());
            }
        }
        
        for id in expired_sessions {
            if let Some(session) = sessions.remove(&id) {
                warn!("Cleaned up expired session {} for user {}", id, session.username);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn session_count(&self) -> usize {
        self.sessions.read().await.len()
    }

    /// Start the background task that extends all active sessions every 10 minutes
    async fn start_session_extension_task(&self) {
        let sessions_clone = Arc::clone(&self.sessions);
        let mut handle_guard = self.extension_task_handle.write().await;
        
        // Don't start a new task if one is already running
        if handle_guard.is_some() {
            return;
        }
        
        info!("🚀 Starting session extension background task (10-minute intervals)");
        
        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10 * 60)); // 10 minutes
            
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
                
                info!("🔄 Extending {} active session(s)", sessions_to_extend.len());
                
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
                    info!("📝 No sessions remaining after extension attempts. Stopping extension task.");
                    break;
                }
            }
            
            info!("🛑 Session extension background task stopped");
        });
        
        *handle_guard = Some(handle);
    }
    
    /// Stop the background session extension task
    async fn stop_session_extension_task(&self) {
        let mut handle_guard = self.extension_task_handle.write().await;
        
        if let Some(handle) = handle_guard.take() {
            handle.abort();
            info!("🛑 Stopped session extension background task");
        }
    }
}