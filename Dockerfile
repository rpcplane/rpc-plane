# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1-slim AS builder

WORKDIR /build

# Cache dependency compilation separately from source changes.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo/config.toml            .cargo/config.toml
COPY tools/dummy-rpc/Cargo.toml    tools/dummy-rpc/Cargo.toml
COPY tools/load-test/Cargo.toml    tools/load-test/Cargo.toml
COPY build.rs build.rs

# Stub sources so cargo can compile dependencies without the real code.
# The manifest declares a `proxy_bench` bench, so its stub must exist too.
RUN mkdir -p src benches tools/dummy-rpc/src tools/load-test/src \
 && echo 'fn main() {}' > src/main.rs \
 && echo 'fn main() {}' > benches/proxy_bench.rs \
 && echo 'fn main() {}' > tools/dummy-rpc/src/main.rs \
 && echo 'fn main() {}' > tools/load-test/src/main.rs \
 && cargo build --release -p rpc-plane \
 && rm -rf src benches tools/dummy-rpc/src tools/load-test/src

# Build the real binary.
COPY src      src
# Not compiled by `-p rpc-plane`, but the manifest must be able to find it.
COPY benches  benches
# Embedded by `rpc-plane init` (include_str! at compile time).
COPY config.example.toml config.example.toml

# Touch sources so cargo knows they changed after the stub build.
RUN touch src/main.rs \
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
