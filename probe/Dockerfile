ARG HA_IMAGE=ws-trade-ha:acceptance
ARG EDGE_IMAGE=ws-trade-edge:acceptance
FROM ${HA_IMAGE} AS binaries
FROM ${EDGE_IMAGE}
USER 0
COPY --from=binaries /usr/local/bin/ha-oracle /app/ha-oracle
RUN mkdir -p /app/edge && ln -s /app/node_modules /app/edge/node_modules
COPY examples/ha-client.js examples/ha-state.js examples/latest-state.js /app/examples/
COPY bench/soak/client.js /app/bench/soak/
COPY bench/ha/oracle.js /app/bench/ha/
COPY probe/*.js /app/probe/
USER 10001:10001
CMD ["node", "--max-old-space-size=128", "/app/probe/server.js", "/config/probe.json"]
