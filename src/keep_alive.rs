use anyhow::Result;
use std::io::ErrorKind;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::{debug, trace, warn};

/// Send a TCP keep-alive probe to check if the connection is still alive
/// Returns Ok(true) if the probe was sent successfully, Ok(false) if the connection is dead
pub async fn send_keep_alive_probe<S>(socket: &mut S) -> Result<bool> 
where
    S: AsyncWrite + Unpin,
{
    // Try to send an empty packet with MSG_DONTWAIT and MSG_NOSIGNAL flags
    // This is a low-level TCP probe that doesn't interfere with the PostgreSQL protocol
    match socket.write(&[]).await {
        Ok(_) => {
            trace!("💓 Keep-alive probe sent successfully");
            Ok(true)
        }
        Err(e) if e.kind() == ErrorKind::BrokenPipe || 
                  e.kind() == ErrorKind::ConnectionAborted ||
                  e.kind() == ErrorKind::ConnectionReset => {
            warn!("💔 Keep-alive probe failed: connection is dead ({})", e);
            Ok(false)
        }
        Err(e) => {
            debug!("⚠️ Keep-alive probe error: {}", e);
            // Other errors might be temporary, so we don't consider the connection dead
            Ok(true)
        }
    }
}


/// PostgreSQL-specific keep-alive using ParameterStatus message
/// This sends a harmless ParameterStatus message that clients will ignore
pub fn create_parameter_status_keepalive() -> Vec<u8> {
    let key = "server_keepalive";
    let value = "1";
    
    let mut message = Vec::new();
    message.push(b'S'); // ParameterStatus message type
    
    // Calculate message length (4 bytes length + key + null + value + null)
    let length = 4 + key.len() + 1 + value.len() + 1;
    message.extend_from_slice(&(length as u32).to_be_bytes());
    
    // Add key and value
    message.extend_from_slice(key.as_bytes());
    message.push(0); // null terminator
    message.extend_from_slice(value.as_bytes());
    message.push(0); // null terminator
    
    message
}