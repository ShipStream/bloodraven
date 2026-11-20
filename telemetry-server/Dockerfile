# Railway builds this from telemetry-server/ (railway.json). The release
# workflow writes the latest release into latest-version before uploading.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked && touch latest-version

# sqlite3 is here so `railway ssh` can query the database.
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates sqlite3 \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/target/release/bloodraven-telemetry /usr/local/bin/bloodraven-telemetry
COPY --from=build /src/latest-version /app/latest-version
ENV DATABASE_PATH=/data/telemetry.db
CMD ["bloodraven-telemetry"]
