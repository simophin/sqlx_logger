FROM messense/rust-musl-cross:x86_64-musl as builder

WORKDIR /src

# Build an empty one first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && touch src/lib.rs && cargo build --release --target=x86_64-unknown-linux-musl || true && rm src/lib.rs

# Install metadata reader
RUN apt-get -y update && apt-get -y install jq

# Build the src folder now
COPY src ./src
RUN cargo build --release --target=x86_64-unknown-linux-musl

# Rename the app 
RUN cp -v target/x86_64-unknown-linux-musl/release/`cargo metadata --no-deps --format-version=1 | jq -r ".packages[0].name"` app


# The runtime image

FROM scratch

COPY --from=builder /src/app /

ENTRYPOINT ["/app"]