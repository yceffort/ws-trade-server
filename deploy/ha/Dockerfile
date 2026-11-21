FROM rust:1.95-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --jobs 2 --locked --release --features ha --bin gateway --bin ha-feed --bin ha-oracle

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl jq tini \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/gateway /build/target/release/ha-feed /build/target/release/ha-oracle /usr/local/bin/
COPY --chmod=755 deploy/container/probe.sh /usr/local/bin/container-probe
USER 10001:10001
ENV WTS_LOG_FORMAT=json RUST_LOG=ws_trade_server=info
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=2s --start-period=10s --retries=3 CMD ["container-probe", "ha-live"]
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["gateway", "--http-addr", "127.0.0.1:8081"]
