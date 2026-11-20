# Railway builds this from telemetry-server/ (deploy-telemetry.yml uploads
# it with `railway up --path-as-root`). The workflow writes latest-version
# (the latest release) and build-id (the commit) first; a build without them
# fails rather than ship a server that announces no release.
FROM rust:1.100.0-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN test -s latest-version && test -s build-id \
 && cargo build --release --locked

# sqlite3 is here so `railway ssh` can query the database.
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates sqlite3 \
 && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /data
WORKDIR /app
COPY --from=build /src/target/release/bloodraven-telemetry /usr/local/bin/bloodraven-telemetry
COPY --from=build /src/latest-version /src/build-id /app/
ENV DATABASE_PATH=/data/telemetry.db
CMD ["bloodraven-telemetry"]
