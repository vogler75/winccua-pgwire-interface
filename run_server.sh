export RUST_LOG=trace
#export DEBUG=--debug
cargo run --bin winccua-pgwire-protocol -- --graphql-url http://DESKTOP-KHLB071:4000/graphql --bind-addr 0.0.0.0:5432 $DEBUG
