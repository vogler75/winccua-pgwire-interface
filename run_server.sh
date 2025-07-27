export RUST_LOG=debug
cargo run -- --graphql-url http://DESKTOP-KHLB071:4000/graphql --bind-addr 0.0.0.0:5432 --no-auth-username username1 --no-auth-password password1
