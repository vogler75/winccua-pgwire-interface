use anyhow::{anyhow, Result};
use rustls::pki_types::CertificateDer;
use rustls::{ServerConfig, RootCertStore};
use rustls_pemfile::{certs, private_key};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: Option<String>,
    pub require_client_cert: bool,
}

impl TlsConfig {
    pub fn new(cert_path: String, key_path: String) -> Self {
        Self {
            cert_path,
            key_path,
            ca_cert_path: None,
            require_client_cert: false,
        }
    }

    pub fn with_ca_cert(mut self, ca_cert_path: String) -> Self {
        self.ca_cert_path = Some(ca_cert_path);
        self
    }

    pub fn require_client_cert(mut self, require: bool) -> Self {
        self.require_client_cert = require;
        self
    }
}

pub fn create_server_config(tls_config: &TlsConfig) -> Result<Arc<ServerConfig>> {
    // Install default crypto provider for rustls
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    
    info!("🔒 Loading TLS configuration");
    debug!("   📜 Certificate: {}", tls_config.cert_path);
    debug!("   🔑 Private key: {}", tls_config.key_path);

    // Load certificate chain
    let cert_file = File::open(&tls_config.cert_path)
        .map_err(|e| anyhow!("Failed to open certificate file '{}': {}", tls_config.cert_path, e))?;
    let mut cert_reader = BufReader::new(cert_file);
    let cert_chain: Vec<CertificateDer> = certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow!("Failed to parse certificate file: {}", e))?;

    if cert_chain.is_empty() {
        return Err(anyhow!("No certificates found in file '{}'", tls_config.cert_path));
    }

    info!("✅ Loaded {} certificate(s)", cert_chain.len());

    // Load private key
    let key_file = File::open(&tls_config.key_path)
        .map_err(|e| anyhow!("Failed to open private key file '{}': {}", tls_config.key_path, e))?;
    let mut key_reader = BufReader::new(key_file);
    let private_key = private_key(&mut key_reader)
        .map_err(|e| anyhow!("Failed to parse private key file: {}", e))?
        .ok_or_else(|| anyhow!("No private key found in file '{}'", tls_config.key_path))?;

    info!("✅ Loaded private key");

    // Create server config based on client certificate requirements
    let server_config = if tls_config.require_client_cert {
        info!("🔒 Client certificate verification enabled");
        
        let mut root_store = RootCertStore::empty();
        
        if let Some(ca_cert_path) = &tls_config.ca_cert_path {
            debug!("   📜 CA certificate: {}", ca_cert_path);
            
            let ca_file = File::open(ca_cert_path)
                .map_err(|e| anyhow!("Failed to open CA certificate file '{}': {}", ca_cert_path, e))?;
            let mut ca_reader = BufReader::new(ca_file);
            let ca_certs: Vec<CertificateDer> = certs(&mut ca_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| anyhow!("Failed to parse CA certificate file: {}", e))?;
            
            for ca_cert in ca_certs {
                root_store.add(ca_cert)
                    .map_err(|e| anyhow!("Failed to add CA certificate: {}", e))?;
            }
            
            info!("✅ Loaded CA certificates for client verification");
        } else {
            warn!("⚠️  Client certificate verification enabled but no CA certificate provided");
            warn!("   This means client certificates will not be validated against a trusted CA");
        }

        let client_cert_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| anyhow!("Failed to create client cert verifier: {}", e))?;

        ServerConfig::builder()
            .with_client_cert_verifier(client_cert_verifier)
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| anyhow!("Failed to configure TLS server with client cert verification: {}", e))?
    } else {
        info!("🔓 Client certificate verification disabled");
        
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| anyhow!("Failed to configure TLS server: {}", e))?
    };

    info!("✅ TLS server configuration created successfully");
    Ok(Arc::new(server_config))
}

