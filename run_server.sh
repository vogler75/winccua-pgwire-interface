export RUST_LOG=trace
export DEBUG=--debug
cargo run --bin winccua-pgwire-protocol -- --graphql-url http://DESKTOP-KHLB071:4000/graphql --bind-addr 0.0.0.0:5432 --tls-enabled --tls-cert certs/server.crt --tls-key certs/server.key --tls-ca-cert certs/ca.crt $DEBUG
