use crate::graphql::{GraphQLClient, Session};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
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
}

impl SessionManager {
    pub fn new(graphql_url: String) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            graphql_url,
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
}