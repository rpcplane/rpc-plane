# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1-slim AS builder

WORKDIR /build

# Cache dependency compilation separately from source changes.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo/config.toml            .cargo/config.toml
COPY rpc-plane/Cargo.toml          rpc-plane/Cargo.toml
COPY rpc-plane-core/Cargo.toml     rpc-plane-core/Cargo.toml
COPY tools/dummy-rpc/Cargo.toml    tools/dummy-rpc/Cargo.toml

# Stub sources so cargo can compile dependencies without the real code.
RUN mkdir -p rpc-plane/src rpc-plane-core/src tools/dummy-rpc/src \
 && echo 'fn main() {}' > rpc-plane/src/main.rs \
 && touch rpc-plane-core/src/lib.rs \
 && echo 'fn main() {}' > tools/dummy-rpc/src/main.rs \
 && cargo build --release -p rpc-plane \
 && rm -rf rpc-plane/src rpc-plane-core/src tools/dummy-rpc/src

# Build the real binary.
COPY rpc-plane/src       rpc-plane/src
COPY rpc-plane-core/src  rpc-plane-core/src
# Embedded by `rpc-plane init` (include_str! at compile time).
COPY config.example.toml config.example.toml

# Touch sources so cargo knows they changed after the stub build.
RUN touch rpc-plane/src/main.rs rpc-plane-core/src/lib.rs \
 && cargo build --release -p rpc-plane

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/rpc-plane /usr/local/bin/rpc-plane

# Config is expected to be mounted at /etc/rpc-plane.toml.
VOLUME ["/etc"]

EXPOSE 9400 9401

ENTRYPOINT ["rpc-plane"]
CMD ["-c", "/etc/rpc-plane.toml", "run"]
