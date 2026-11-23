FROM node:24-bookworm-slim AS dependencies
WORKDIR /app
COPY edge/package.json edge/package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts

FROM node:24-bookworm-slim
WORKDIR /app
COPY --from=dependencies /app/node_modules ./node_modules
COPY edge/package.json edge/run.js edge/proxy.js edge/tcp-probe.js ./
USER 10001:10001
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD ["node", "tcp-probe.js"]
CMD ["node", "run.js"]
