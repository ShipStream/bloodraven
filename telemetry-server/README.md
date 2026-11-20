# Bloodraven usage report server

Receives the operator's daily anonymous usage report
(`internal/telemetry`), stores it in SQLite and answers with the latest
release. What is sent and kept is public at
<https://bloodraven.dev/docs/configuration/usage-reporting>. A change to what
this server stores is a change to that page.

`POST /v1/report` takes `{"installId","version","groups","sites","nodes"}`
(unknown fields are ignored) and answers `{"latestVersion":"1.9.0"}`. The
sending address is looked up at ipinfo.io for its network operator and
country, then dropped: it is never stored or logged. `GET /healthz` is the
health check. `GET /v1/latest` answers the latest release and stores
nothing; the deploy workflow checks it.

## Running

```bash
cargo test
LATEST_VERSION=1.8.0 DATABASE_PATH=/tmp/telemetry.db cargo run
```

| Variable | Default | |
|---|---|---|
| `PORT` | `8080` | Listen port (Railway sets it). |
| `DATABASE_PATH` | `telemetry.db` (`/data/telemetry.db` in the image) | SQLite file. On Railway, a volume is mounted at `/data`. |
| `LATEST_VERSION` | the `latest-version` file | Overrides the release the server answers with. |
| `IPINFO_TOKEN` | unset | With a token the unlimited [ipinfo Lite](https://ipinfo.io/lite) API is used; without one the free endpoint, which is rate limited. |
| `RETENTION_DAYS` | `400` | Reports older than this are deleted daily. |

## Deploying

Service `bloodraven-telemetry` in the Railway project that also runs
bloodraven.dev, at `telemetry.bloodraven.dev`, with a volume at `/data`.
`.github/workflows/deploy-telemetry.yml` builds and deploys it: when this
directory changes on `main`, after every release (`release.yml`), and by hand
(`gh workflow run deploy-telemetry.yml`). Each run writes the latest plain
release into `latest-version`, which the image serves.

## Querying

```bash
railway ssh --service bloodraven-telemetry -- sqlite3 /data/telemetry.db
```

```sql
-- Installs active in the last 7 days, by version.
SELECT version, count(DISTINCT install_id) AS installs
FROM reports WHERE received_at > strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-7 days')
GROUP BY version ORDER BY installs DESC;

-- The latest report of each active install.
SELECT install_id, version, groups, sites, nodes, as_name, country, max(received_at) AS last_seen
FROM reports WHERE received_at > strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-7 days')
GROUP BY install_id ORDER BY last_seen DESC;

-- Weekly active installs.
SELECT strftime('%Y-%W', received_at) AS week, count(DISTINCT install_id) AS installs
FROM reports GROUP BY week ORDER BY week;
```

Each operator reports about once a day, so a 7-day window counts every
running install, including one whose cluster missed a few reports.
