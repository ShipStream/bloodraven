# Bloodraven usage report server

Receives the operator's daily anonymous usage report
(`internal/telemetry`), stores it in SQLite and answers with the latest
release. What is sent and kept is public at
<https://bloodraven.dev/docs/configuration/usage-reporting>. A change to what
this server stores is a change to that page.

`POST /v1/report` takes `{"installId","version","groups","sites","nodes"}`
(unknown fields are ignored) and answers `{"latestVersion":"1.9.0"}`. One row
is kept per install per UTC day. The sending address is looked up at ipinfo.io
for its network operator and country and is never stored or logged; the
in-memory lookup cache and rate limiter key on a keyed hash of it. Forwarding
headers are trusted only from a private peer (Railway's edge), so keep the DNS
record unproxied (no CDN in front), or every report gets the CDN's network.
`GET /healthz` is the health check. `GET /v1/latest` answers the latest release and stores
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
| `BUILD_ID` | the `build-id` file | The commit, answered by `/v1/latest` so the deploy check can tell a new deployment from the old one. |

Limits: 60 reports an hour from one address (one /64 for IPv6), 20,000 an hour
in total, 50,000 sources tracked an hour. Past them the server answers 429.

## Deploying

Service `bloodraven-telemetry` in the Railway project that also runs
bloodraven.dev, at `telemetry.bloodraven.dev`, with a volume at `/data`.
`.github/workflows/deploy-telemetry.yml` builds and deploys it: when this
directory changes on `main`, after every release (`release.yml`), and by hand
(`gh workflow run deploy-telemetry.yml`). Each run writes the latest plain
release into `latest-version` and the commit into `build-id`; the image build
fails without them.

## Querying

```bash
railway ssh --service bloodraven-telemetry -- sqlite3 /data/telemetry.db
```

```sql
-- The latest report of each install active in the last 7 days.
SELECT install_id, version, groups, sites, nodes, as_name, country, max(received_at) AS last_seen
FROM reports WHERE day > date('now', '-7 days')
GROUP BY install_id ORDER BY last_seen DESC;

-- Active installs by the version they last reported.
SELECT version, count(*) AS installs FROM (
  SELECT version, max(received_at) FROM reports WHERE day > date('now', '-7 days') GROUP BY install_id
) GROUP BY version ORDER BY installs DESC;

-- Weekly active installs.
SELECT strftime('%Y-%W', day) AS week, count(DISTINCT install_id) AS installs
FROM reports GROUP BY week ORDER BY week;
```

Each operator reports about once a day, so a 7-day window counts every
running install, including one whose cluster missed a few reports.
