use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use super::{ScramSha256Context, ScramStage};

// Authentication context for different auth methods
pub(super) enum AuthContext {
    Md5([u8; 4]), // MD5 with salt
    Scram,        // SCRAM-SHA-256 (placeholder for now)
}

pub(super) fn create_postgres_md5_request() -> (Vec<u8>, [u8; 4]) {
    // Generate a random 4-byte salt
    let mut hasher = DefaultHasher::new();
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .hash(&mut hasher);
    let hash = hasher.finish();

    let salt = [
        (hash >> 24) as u8,
        (hash >> 16) as u8,
        (hash >> 8) as u8,
        hash as u8,
    ];

    (create_postgres_md5_request_with_salt(salt), salt)
}

fn create_postgres_md5_request_with_salt(salt: [u8; 4]) -> Vec<u8> {
    let mut response = Vec::new();

    // Authentication request - MD5 password
    // Message type 'R' (Authentication) + length (4 bytes) + auth type (4 bytes, 5 = MD5) + salt (4 bytes)
    response.push(b'R');
    response.extend_from_slice(&12u32.to_be_bytes()); // Length: 4 (length) + 4 (auth type) + 4 (salt) = 12
    response.extend_from_slice(&5u32.to_be_bytes()); // Auth type 5 = MD5 password

    // Add the salt
    response.extend_from_slice(&salt);

    response
}

pub(super) fn compute_postgres_md5_hash(username: &str, password: &str, salt: &[u8; 4]) -> String {
    // PostgreSQL MD5 authentication: MD5(MD5(password + username) + salt)

    // Step 1: MD5(password + username)
    let mut input1 = Vec::new();
    input1.extend_from_slice(password.as_bytes());
    input1.extend_from_slice(username.as_bytes());
    let inner_hash = md5::compute(&input1);
    let inner_hex = hex::encode(inner_hash.as_ref());

    // Step 2: MD5(inner_hex + salt)
    let mut input2 = Vec::new();
    input2.extend_from_slice(inner_hex.as_bytes());
    input2.extend_from_slice(salt);
    let final_hash = md5::compute(&input2);
    let final_hex = hex::encode(final_hash.as_ref());

    // PostgreSQL prefixes the result with "md5"
    format!("md5{}", final_hex)
}

pub(super) fn verify_postgres_md5_auth(
    username: &str,
    password: &str,
    salt: &[u8; 4],
    client_response: &str,
) -> bool {
    let expected_hash = compute_postgres_md5_hash(username, password, salt);
    client_response == expected_hash
}

pub(super) fn create_postgres_scram_sha256_request() -> Vec<u8> {
    let mut response = Vec::new();

    // AuthenticationSASL message
    // Message type 'R' + length + auth type (10 = SASL) + mechanism list
    response.push(b'R');

    // SASL mechanism: "SCRAM-SHA-256" + null terminator + empty string + null terminator
    let mechanism = b"SCRAM-SHA-256\0\0";
    let total_length = 4 + 4 + mechanism.len(); // length field + auth type + mechanism

    response.extend_from_slice(&(total_length as u32).to_be_bytes());
    response.extend_from_slice(&10u32.to_be_bytes()); // Auth type 10 = SASL
    response.extend_from_slice(mechanism);

    response
}

pub(super) fn parse_sasl_initial_response(buffer: &[u8]) -> Result<(String, String), String> {
    // SASL Initial Response format:
    // Message type 'p' + length + mechanism + initial_response_length + initial_response

    if buffer.len() < 9 || buffer[0] != b'p' {
        return Err("Invalid SASL Initial Response format".to_string());
    }

    let mut pos = 5; // Skip 'p' + length (4 bytes)

    // Extract mechanism name (null-terminated)
    let mechanism_start = pos;
    while pos < buffer.len() && buffer[pos] != 0 {
        pos += 1;
    }
    if pos >= buffer.len() {
        return Err("Missing null terminator for mechanism".to_string());
    }

    let mechanism = String::from_utf8_lossy(&buffer[mechanism_start..pos]).to_string();
    pos += 1; // Skip null terminator

    // Extract initial response length (4 bytes)
    if pos + 4 > buffer.len() {
        return Err("Missing initial response length".to_string());
    }

    let response_length =
        u32::from_be_bytes([buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3]])
            as usize;
    pos += 4;

    // Extract initial response
    if pos + response_length > buffer.len() {
        return Err("Initial response length exceeds buffer".to_string());
    }

    let initial_response =
        String::from_utf8_lossy(&buffer[pos..pos + response_length]).to_string();

    Ok((mechanism, initial_response))
}

pub(super) fn parse_scram_client_first(client_first: &str) -> Result<(String, String), String> {
    // Format: "n,,n=username,r=client_nonce"
    // or: "n=username,r=client_nonce" (without GS2 header)

    let client_first_bare = if client_first.starts_with("n,,") {
        &client_first[3..] // Remove GS2 header "n,,"
    } else {
        client_first
    };

    let mut username = String::new();
    let mut client_nonce = String::new();

    for part in client_first_bare.split(',') {
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "n" => username = value.to_string(),
                "r" => client_nonce = value.to_string(),
                _ => {} // Ignore unknown attributes
            }
        }
    }

    if username.is_empty() || client_nonce.is_empty() {
        return Err("Missing username or client nonce in SCRAM client-first".to_string());
    }

    Ok((username, client_nonce))
}

pub(super) fn scram_sha256_server_first_message(
    client_nonce: &str,
    username: &str,
) -> (String, ScramSha256Context) {
    let server_nonce = generate_scram_server_nonce();
    let combined_nonce = format!("{}{}", client_nonce, server_nonce);

    // Generate random salt
    let mut rng = rand::rng();
    let salt: [u8; 16] = rng.random();
    let salt_base64 = STANDARD.encode(salt);

    let iteration_count = 4096; // Standard iteration count

    let server_first = format!("r={},s={},i={}", combined_nonce, salt_base64, iteration_count);

    let context = ScramSha256Context {
        username: username.to_string(),
        client_nonce: client_nonce.to_string(),
        server_nonce,
        salt: salt.to_vec(),
        iteration_count,
        client_first_bare: String::new(), // Will be set later
        server_first: server_first.clone(),
        stored_key: Vec::new(), // Will be computed later
        server_key: Vec::new(), // Will be computed later
        stage: ScramStage::Initial,
    };

    (server_first, context)
}

fn generate_scram_server_nonce() -> String {
    let mut rng = rand::rng();
    let nonce_bytes: [u8; 18] = rng.random();
    STANDARD.encode(nonce_bytes)
}

pub(super) fn create_postgres_sasl_continue_response(server_message: &str) -> Vec<u8> {
    let mut response = Vec::new();

    // AuthenticationSASLContinue message
    // Message type 'R' + length + auth type (11 = SASL Continue) + SASL data
    response.push(b'R');

    let sasl_data = server_message.as_bytes();
    let total_length = 4 + 4 + sasl_data.len(); // length field + auth type + data

    response.extend_from_slice(&(total_length as u32).to_be_bytes());
    response.extend_from_slice(&11u32.to_be_bytes()); // Auth type 11 = SASL Continue
    response.extend_from_slice(sasl_data);

    response
}

pub(super) fn parse_sasl_response(buffer: &[u8]) -> Result<String, String> {
    // SASL Response format:
    // Message type 'p' + length + response_data

    if buffer.len() < 5 || buffer[0] != b'p' {
        return Err("Invalid SASL Response format".to_string());
    }

    let response_data = String::from_utf8_lossy(&buffer[5..]).to_string();
    Ok(response_data)
}

pub(super) fn parse_scram_client_final(client_final: &str) -> Result<(String, Vec<u8>), String> {
    // Format: "c=biws,r=client_nonce_server_nonce,p=client_proof"

    let mut client_final_without_proof = String::new();
    let mut client_proof_b64 = String::new();

    for part in client_final.split(',') {
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "p" => client_proof_b64 = value.to_string(),
                _ => {
                    if !client_final_without_proof.is_empty() {
                        client_final_without_proof.push(',');
                    }
                    client_final_without_proof.push_str(part);
                }
            }
        }
    }

    if client_proof_b64.is_empty() {
        return Err("Missing client proof in SCRAM client-final".to_string());
    }

    let client_proof = STANDARD
        .decode(client_proof_b64)
        .map_err(|e| format!("Invalid base64 in client proof: {}", e))?;

    Ok((client_final_without_proof, client_proof))
}

pub(super) fn scram_sha256_verify_client_proof(
    context: &ScramSha256Context,
    client_final_without_proof: &str,
    client_proof: &[u8],
    password: &str,
) -> Result<String, String> {
    type HmacSha256 = Hmac<Sha256>;

    // Derive keys from password
    let (stored_key, server_key) =
        scram_sha256_derive_keys(password, &context.salt, context.iteration_count);

    // Build auth message
    let auth_message = format!(
        "{},{},{}",
        context.client_first_bare, context.server_first, client_final_without_proof
    );

    // Client Signature = HMAC(StoredKey, AuthMessage)
    let mut client_sig_hmac = HmacSha256::new_from_slice(&stored_key)
        .map_err(|e| format!("HMAC creation failed: {}", e))?;
    client_sig_hmac.update(auth_message.as_bytes());
    let client_signature = client_sig_hmac.finalize().into_bytes();

    // Client Key = Client Signature XOR Client Proof
    if client_proof.len() != client_signature.len() {
        return Err("Client proof length mismatch".to_string());
    }

    let mut client_key = vec![0u8; client_signature.len()];
    for i in 0..client_signature.len() {
        client_key[i] = client_signature[i] ^ client_proof[i];
    }

    // Verify: SHA256(Client Key) should equal Stored Key
    let computed_stored_key = Sha256::digest(&client_key);
    if computed_stored_key.as_slice() != stored_key {
        return Err("Authentication verification failed".to_string());
    }

    // Server Signature = HMAC(ServerKey, AuthMessage)
    let mut server_sig_hmac = HmacSha256::new_from_slice(&server_key)
        .map_err(|e| format!("Server HMAC creation failed: {}", e))?;
    server_sig_hmac.update(auth_message.as_bytes());
    let server_signature = server_sig_hmac.finalize().into_bytes();

    // Server final message
    let server_final = format!("v={}", STANDARD.encode(server_signature));
    Ok(server_final)
}

fn scram_sha256_derive_keys(password: &str, salt: &[u8], iterations: u32) -> (Vec<u8>, Vec<u8>) {
    use pbkdf2::pbkdf2;
    type HmacSha256 = Hmac<Sha256>;

    // Derive salted password using PBKDF2
    let mut salted_password = [0u8; 32];
    pbkdf2::<HmacSha256>(password.as_bytes(), salt, iterations, &mut salted_password)
        .expect("PBKDF2 derivation failed");

    // Client Key = HMAC(SaltedPassword, "Client Key")
    let mut client_key_hmac =
        HmacSha256::new_from_slice(&salted_password).expect("HMAC creation failed");
    client_key_hmac.update(b"Client Key");
    let client_key = client_key_hmac.finalize().into_bytes();

    // Server Key = HMAC(SaltedPassword, "Server Key")
    let mut server_key_hmac =
        HmacSha256::new_from_slice(&salted_password).expect("HMAC creation failed");
    server_key_hmac.update(b"Server Key");
    let server_key = server_key_hmac.finalize().into_bytes();

    // Stored Key = SHA256(Client Key)
    let stored_key = Sha256::digest(&client_key);

    (stored_key.to_vec(), server_key.to_vec())
}

pub(super) fn create_postgres_sasl_final_response(server_message: &str) -> Vec<u8> {
    let mut response = Vec::new();

    // AuthenticationSASLFinal message
    // Message type 'R' + length + auth type (12 = SASL Final) + SASL data
    response.push(b'R');

    let sasl_data = server_message.as_bytes();
    let total_length = 4 + 4 + sasl_data.len(); // length field + auth type + data

    response.extend_from_slice(&(total_length as u32).to_be_bytes());
    response.extend_from_slice(&12u32.to_be_bytes()); // Auth type 12 = SASL Final
    response.extend_from_slice(sasl_data);

    response
}

pub(super) fn parse_postgres_password(data: &[u8]) -> Option<String> {
    if data.len() < 5 {
        return None;
    }

    // Check for PasswordMessage (type 'p')
    if data[0] == b'p' {
        // Password message: 'p' + length (4 bytes) + password string + null terminator
        let length = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
        if data.len() >= length + 1 && length > 4 {
            let password_bytes = &data[5..5 + length - 4]; // Exclude length and null terminator
            if let Ok(password) = std::str::from_utf8(password_bytes) {
                return Some(password.trim_end_matches('\0').to_string());
            }
        }
    }

    None
}
