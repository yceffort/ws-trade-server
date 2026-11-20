FROM rust:1.95-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --locked --release --bin gateway --bin feed

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl jq tini \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data && chown 10001:10001 /data
COPY --from=build /build/target/release/gateway /build/target/release/feed /usr/local/bin/
COPY --chmod=755 deploy/container/probe.sh /usr/local/bin/container-probe
USER 10001:10001
ENV WTS_LOG_FORMAT=json RUST_LOG=ws_trade_server=info
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["container-probe", "live"]
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["gateway", "--http-addr", "127.0.0.1:8081", "--ring", "/data/feed.ring"]
