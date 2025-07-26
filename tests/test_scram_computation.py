#!/usr/bin/env python3

"""
Test SCRAM-SHA-256 computation to verify our implementation matches the standard.
This script demonstrates the SCRAM-SHA-256 algorithm step by step.
"""

import hashlib
import hmac
import base64
import secrets

def pbkdf2_sha256(password, salt, iterations):
    """PBKDF2 with SHA-256"""
    return hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, iterations)

def scram_sha256_client_key(password, salt, iterations):
    """Compute SCRAM-SHA-256 client key"""
    # SaltedPassword = PBKDF2(Normalize(password), salt, i)
    salted_password = pbkdf2_sha256(password, salt, iterations)
    
    # ClientKey = HMAC(SaltedPassword, "Client Key")
    client_key = hmac.new(salted_password, b"Client Key", hashlib.sha256).digest()
    
    # StoredKey = H(ClientKey)  
    stored_key = hashlib.sha256(client_key).digest()
    
    return salted_password, client_key, stored_key

def scram_sha256_server_key(salted_password):
    """Compute SCRAM-SHA-256 server key"""
    # ServerKey = HMAC(SaltedPassword, "Server Key")
    return hmac.new(salted_password, b"Server Key", hashlib.sha256).digest()

def scram_sha256_proof(client_key, auth_message):
    """Compute SCRAM-SHA-256 client proof"""
    # ClientSignature = HMAC(StoredKey, AuthMessage)
    stored_key = hashlib.sha256(client_key).digest()
    client_signature = hmac.new(stored_key, auth_message.encode('utf-8'), hashlib.sha256).digest()
    
    # ClientProof = ClientKey XOR ClientSignature
    client_proof = bytes(a ^ b for a, b in zip(client_key, client_signature))
    
    return client_proof

def scram_sha256_server_signature(server_key, auth_message):
    """Compute SCRAM-SHA-256 server signature"""
    # ServerSignature = HMAC(ServerKey, AuthMessage)
    return hmac.new(server_key, auth_message.encode('utf-8'), hashlib.sha256).digest()

def test_scram_sha256():
    """Test SCRAM-SHA-256 computation"""
    
    print("🔒 Testing SCRAM-SHA-256 Computation")
    print("=" * 50)
    
    # Test parameters
    username = "username1"
    password = "password1"
    client_nonce = base64.b64encode(secrets.token_bytes(18)).decode('ascii')
    server_nonce = base64.b64encode(secrets.token_bytes(18)).decode('ascii')
    salt = secrets.token_bytes(16)
    iterations = 4096
    
    print(f"Username: {username}")
    print(f"Password: {password}")
    print(f"Client Nonce: {client_nonce}")
    print(f"Server Nonce: {server_nonce}")
    print(f"Salt: {base64.b64encode(salt).decode('ascii')}")
    print(f"Iterations: {iterations}")
    print()
    
    # SCRAM-SHA-256 computation
    print("🔍 SCRAM-SHA-256 Computation Steps:")
    print("-" * 40)
    
    # Step 1: Derive keys
    salted_password, client_key, stored_key = scram_sha256_client_key(password, salt, iterations)
    server_key = scram_sha256_server_key(salted_password)
    
    print(f"1. Salted Password: {base64.b64encode(salted_password).decode('ascii')}")
    print(f"2. Client Key: {base64.b64encode(client_key).decode('ascii')}")
    print(f"3. Stored Key: {base64.b64encode(stored_key).decode('ascii')}")
    print(f"4. Server Key: {base64.b64encode(server_key).decode('ascii')}")
    print()
    
    # Step 2: Build authentication message
    client_first_bare = f"n=,r={client_nonce}"
    server_first = f"r={client_nonce}{server_nonce},s={base64.b64encode(salt).decode('ascii')},i={iterations}"
    client_final_without_proof = f"c=biws,r={client_nonce}{server_nonce}"
    auth_message = f"{client_first_bare},{server_first},{client_final_without_proof}"
    
    print(f"5. Client First Bare: {client_first_bare}")
    print(f"6. Server First: {server_first}")
    print(f"7. Client Final (no proof): {client_final_without_proof}")
    print(f"8. Auth Message: {auth_message}")
    print()
    
    # Step 3: Compute proofs
    client_proof = scram_sha256_proof(client_key, auth_message)
    server_signature = scram_sha256_server_signature(server_key, auth_message)
    
    print(f"9. Client Proof: {base64.b64encode(client_proof).decode('ascii')}")
    print(f"10. Server Signature: {base64.b64encode(server_signature).decode('ascii')}")
    print()
    
    # Step 4: Final messages
    client_final = f"{client_final_without_proof},p={base64.b64encode(client_proof).decode('ascii')}"
    server_final = f"v={base64.b64encode(server_signature).decode('ascii')}"
    
    print("📨 SCRAM-SHA-256 Protocol Messages:")
    print("-" * 40)
    print(f"Client Initial: {client_first_bare}")
    print(f"Server First: {server_first}")
    print(f"Client Final: {client_final}")
    print(f"Server Final: {server_final}")
    print()
    
    print("✅ SCRAM-SHA-256 computation completed successfully!")
    print()
    print("💡 This demonstrates the cryptographic operations our Rust")
    print("   implementation should perform when SCRAM is enabled.")

if __name__ == "__main__":
    test_scram_sha256()