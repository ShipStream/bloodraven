//! Receives the Bloodraven operator's daily anonymous usage report, stores it
//! in SQLite and answers with the latest release.
//!
//! The sending address is used once, to look up its network operator and
//! country at ipinfo.io, and is never written to disk or to a log. What is
//! stored is documented at
//! https://bloodraven.dev/docs/configuration/usage-reporting; keep that page
//! and this file in step.

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

const MAX_BODY_BYTES: usize = 4 * 1024;
const MAX_COUNT: u32 = 1_000_000;
const GEO_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const GEO_TIMEOUT: Duration = Duration::from_secs(3);
const CACHE_CAP: usize = 10_000;
const RATE_WINDOW: Duration = Duration::from_secs(60 * 60);
const RATE_LIMIT: u32 = 60;

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    latest: Arc<String>,
    geo: Arc<Geo>,
    limiter: Arc<Mutex<HashMap<IpAddr, (Instant, u32)>>>,
}

/// The report the operator sends (internal/telemetry.Report). Unknown fields
/// are ignored so a newer operator still reports to this server.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    install_id: String,
    version: String,
    groups: u32,
    sites: u32,
    nodes: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Answer<'a> {
    latest_version: &'a str,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct GeoInfo {
    asn: Option<String>,
    as_name: Option<String>,
    country: Option<String>,
}

struct Geo {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
    cache: Mutex<HashMap<IpAddr, (Instant, GeoInfo)>>,
}

impl Geo {
    async fn lookup(&self, ip: IpAddr) -> GeoInfo {
        if !is_public(ip) {
            return GeoInfo::default();
        }
        if let Some((at, info)) = self.cache.lock().unwrap().get(&ip)
            && at.elapsed() < GEO_TTL
        {
            return info.clone();
        }
        let info = match self.fetch(ip).await {
            Ok(info) => info,
            Err(err) => {
                eprintln!("ipinfo lookup failed: {err}");
                return GeoInfo::default();
            }
        };
        let mut cache = self.cache.lock().unwrap();
        if cache.len() >= CACHE_CAP {
            cache.clear();
        }
        cache.insert(ip, (Instant::now(), info.clone()));
        info
    }

    async fn fetch(&self, ip: IpAddr) -> Result<GeoInfo, String> {
        // With a token the Lite API (unlimited, ASN and country only);
        // without one the free endpoint, which is rate limited.
        let url = match &self.token {
            Some(_) => format!("{}/lite/{ip}", self.base),
            None => format!("{}/{ip}/json", self.base),
        };
        let mut req = self.http.get(url).timeout(GEO_TIMEOUT);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.map_err(|e| e.without_url().to_string())?;
        if !resp.status().is_success() {
            return Err(format!("status {}", resp.status()));
        }
        let body: serde_json::Value = resp.json().await.map_err(|e| e.without_url().to_string())?;
        Ok(parse_geo(&body))
    }
}

/// Reads both ipinfo shapes: Lite (`asn`, `as_name`, `country_code`) and the
/// free endpoint (`org` as "AS15169 Google LLC", `country` as the ISO code).
fn parse_geo(body: &serde_json::Value) -> GeoInfo {
    let s = |k: &str| {
        body.get(k)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| v.chars().take(200).collect::<String>())
    };
    if s("asn").is_some() || s("country_code").is_some() {
        return GeoInfo {
            asn: s("asn"),
            as_name: s("as_name"),
            country: s("country_code"),
        };
    }
    let (asn, as_name) = match s("org") {
        Some(org) => match org.split_once(' ') {
            Some((asn, name)) if asn.starts_with("AS") => {
                (Some(asn.to_string()), Some(name.to_string()))
            }
            _ => (None, Some(org)),
        },
        None => (None, None),
    };
    GeoInfo {
        asn,
        as_name,
        country: s("country"),
    }
}

fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                // 100.64.0.0/10, carrier-grade NAT and Railway's private network.
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 64))
        }
        IpAddr::V6(v6) => {
            !(v6.is_loopback()
                || v6.is_unspecified()
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                || (v6.segments()[0] & 0xffc0) == 0xfe80)
        }
    }
}

/// Railway's edge overwrites X-Real-IP and appends the connecting address to
/// X-Forwarded-For, whose leftmost values the caller controls.
fn client_ip(headers: &HeaderMap, peer: IpAddr) -> IpAddr {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if let Some(ip) = header("x-real-ip").and_then(|v| v.trim().parse().ok()) {
        return ip;
    }
    if let Some(ip) = header("x-forwarded-for")
        .and_then(|v| v.rsplit(',').next())
        .and_then(|v| v.trim().parse().ok())
    {
        return ip;
    }
    peer
}

fn valid_install_id(id: &str) -> bool {
    id.len() == 36
        && id.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_digit() || ('a'..='f').contains(&c),
        })
}

fn valid_version(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 64
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || ".+-_".contains(c))
}

impl Report {
    fn validate(&self) -> Result<(), &'static str> {
        if !valid_install_id(&self.install_id) {
            return Err("installId must be a lowercase UUID");
        }
        if !valid_version(&self.version) {
            return Err("version is not a version");
        }
        if self.groups > MAX_COUNT || self.sites > MAX_COUNT || self.nodes > MAX_COUNT {
            return Err("count out of range");
        }
        Ok(())
    }
}

fn open_db(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA busy_timeout = 5000;
         CREATE TABLE IF NOT EXISTS reports (
           id          INTEGER PRIMARY KEY,
           received_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
           install_id  TEXT    NOT NULL,
           version     TEXT    NOT NULL,
           groups      INTEGER NOT NULL,
           sites       INTEGER NOT NULL,
           nodes       INTEGER NOT NULL,
           asn         TEXT,
           as_name     TEXT,
           country     TEXT
         );
         CREATE INDEX IF NOT EXISTS reports_received_at ON reports (received_at);
         CREATE INDEX IF NOT EXISTS reports_install_id ON reports (install_id, received_at);",
    )?;
    Ok(conn)
}

fn insert(conn: &Connection, r: &Report, geo: &GeoInfo) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO reports (install_id, version, groups, sites, nodes, asn, as_name, country)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            r.install_id,
            r.version,
            r.groups,
            r.sites,
            r.nodes,
            geo.asn,
            geo.as_name,
            geo.country
        ],
    )?;
    Ok(())
}

fn prune(conn: &Connection, retention_days: u32) -> rusqlite::Result<usize> {
    conn.execute(
        "DELETE FROM reports WHERE received_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?1)",
        params![format!("-{retention_days} days")],
    )
}

fn rate_limited(limiter: &Mutex<HashMap<IpAddr, (Instant, u32)>>, ip: IpAddr) -> bool {
    let mut map = limiter.lock().unwrap();
    if map.len() >= CACHE_CAP {
        map.retain(|_, (start, _)| start.elapsed() < RATE_WINDOW);
    }
    let entry = map.entry(ip).or_insert((Instant::now(), 0));
    if entry.0.elapsed() >= RATE_WINDOW {
        *entry = (Instant::now(), 0);
    }
    entry.1 += 1;
    entry.1 > RATE_LIMIT
}

async fn report(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Report>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    let Ok(Json(report)) = body else {
        return (StatusCode::BAD_REQUEST, "invalid report").into_response();
    };
    if let Err(why) = report.validate() {
        return (StatusCode::BAD_REQUEST, why).into_response();
    }
    let ip = client_ip(&headers, peer.ip());
    if rate_limited(&state.limiter, ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "too many reports").into_response();
    }
    let geo = state.geo.lookup(ip).await;
    let db = state.db.clone();
    let stored = {
        let geo = geo.clone();
        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            insert(&conn, &report, &geo).map(|_| report)
        })
        .await
    };
    match stored {
        Ok(Ok(r)) => {
            println!(
                "report version={} groups={} sites={} nodes={} asn={} country={}",
                r.version,
                r.groups,
                r.sites,
                r.nodes,
                geo.asn.as_deref().unwrap_or("-"),
                geo.country.as_deref().unwrap_or("-")
            );
            Json(Answer {
                latest_version: &state.latest,
            })
            .into_response()
        }
        Ok(Err(err)) => {
            eprintln!("store report: {err}");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        Err(err) => {
            eprintln!("store report: {err}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn index() -> &'static str {
    "Bloodraven usage report endpoint. What is sent and kept: https://bloodraven.dev/docs/configuration/usage-reporting\n"
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/report", post(report))
        .route(
            "/v1/latest",
            get(|State(state): State<AppState>| async move {
                Json(serde_json::json!({ "latestVersion": *state.latest }))
            }),
        )
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

/// The latest release: LATEST_VERSION, else the file the release workflow
/// writes into the image (LATEST_VERSION_FILE, default ./latest-version).
fn latest_version() -> String {
    if let Ok(v) = std::env::var("LATEST_VERSION") {
        return v.trim().trim_start_matches('v').to_string();
    }
    let path = std::env::var("LATEST_VERSION_FILE").unwrap_or_else(|_| "latest-version".into());
    std::fs::read_to_string(path)
        .map(|v| v.trim().trim_start_matches('v').to_string())
        .unwrap_or_default()
}

#[tokio::main]
async fn main() {
    let db_path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "telemetry.db".into());
    let retention_days: u32 = std::env::var("RETENTION_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8080);
    let conn = open_db(Path::new(&db_path)).unwrap_or_else(|e| panic!("open {db_path}: {e}"));
    let latest = latest_version();

    let token = std::env::var("IPINFO_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty());
    // The Lite API lives on api.ipinfo.io; the free endpoint on ipinfo.io.
    let default_base = if token.is_some() {
        "https://api.ipinfo.io"
    } else {
        "https://ipinfo.io"
    };
    let state = AppState {
        db: Arc::new(Mutex::new(conn)),
        latest: Arc::new(latest.clone()),
        geo: Arc::new(Geo {
            http: reqwest::Client::builder()
                .user_agent("bloodraven-telemetry")
                .build()
                .expect("http client"),
            base: std::env::var("IPINFO_URL").unwrap_or_else(|_| default_base.into()),
            token,
            cache: Mutex::new(HashMap::new()),
        }),
        limiter: Arc::new(Mutex::new(HashMap::new())),
    };

    let db = state.db.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(24 * 60 * 60));
        loop {
            tick.tick().await;
            let db = db.clone();
            let result =
                tokio::task::spawn_blocking(move || prune(&db.lock().unwrap(), retention_days))
                    .await;
            match result {
                Ok(Ok(n)) => println!("pruned {n} reports older than {retention_days} days"),
                Ok(Err(e)) => eprintln!("prune: {e}"),
                Err(e) => eprintln!("prune: {e}"),
            }
        }
    });

    let listener = tokio::net::TcpListener::bind(("::", port))
        .await
        .expect("bind");
    println!(
        "listening on :{port}, database {db_path}, latest version {:?}, ipinfo token {}",
        latest,
        if state.geo.token.is_some() {
            "set"
        } else {
            "not set"
        }
    );
    axum::serve(
        listener,
        app(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
    .expect("serve");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn state(base: &str, token: Option<&str>) -> AppState {
        AppState {
            db: Arc::new(Mutex::new(open_db(Path::new(":memory:")).unwrap())),
            latest: Arc::new("1.9.0".into()),
            geo: Arc::new(Geo {
                http: reqwest::Client::builder().no_proxy().build().unwrap(),
                base: base.into(),
                token: token.map(Into::into),
                cache: Mutex::new(HashMap::new()),
            }),
            limiter: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    const ID: &str = "5f0c1e9a-3b7d-4c2e-9a41-6d8f2b7e0c13";

    fn post_report(body: String, headers: &[(&str, &str)]) -> Request<Body> {
        let mut req = Request::post("/v1/report").header("content-type", "application/json");
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let mut req = req.body(Body::from(body)).unwrap();
        req.extensions_mut()
            .insert(ConnectInfo::<SocketAddr>("10.0.0.1:1234".parse().unwrap()));
        req
    }

    fn body(id: &str) -> String {
        format!(r#"{{"installId":"{id}","version":"1.8.0","groups":2,"sites":4,"nodes":5}}"#)
    }

    type Row = (
        String,
        String,
        u32,
        u32,
        u32,
        Option<String>,
        Option<String>,
        Option<String>,
    );

    fn rows(state: &AppState) -> Vec<Row> {
        let conn = state.db.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT install_id, version, groups, sites, nodes, asn, as_name, country FROM reports ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
            ))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    /// A stand-in for ipinfo that answers in either shape and records paths.
    async fn fake_ipinfo() -> (String, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s = seen.clone();
        let router = Router::new().fallback(move |req: Request<Body>| {
            let s = s.clone();
            async move {
                let path = req.uri().path().to_string();
                let auth = req.headers().get("authorization").map(|v| v.to_str().unwrap().to_string());
                s.lock().unwrap().push(format!("{path} {}", auth.unwrap_or_default()));
                if path.starts_with("/lite/") {
                    Json(serde_json::json!({"ip":"8.8.8.8","asn":"AS15169","as_name":"Google LLC","country_code":"US","country":"United States"}))
                } else {
                    Json(serde_json::json!({"ip":"1.1.1.1","org":"AS13335 Cloudflare, Inc.","country":"AU","city":"Sydney"}))
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    #[tokio::test]
    async fn stores_report_with_network_and_never_the_address() {
        let (base, seen) = fake_ipinfo().await;
        let st = state(&base, Some("tok"));
        let resp = app(st.clone())
            .oneshot(post_report(
                body(ID),
                &[
                    ("x-forwarded-for", "6.6.6.6, 8.8.8.8"),
                    ("x-real-ip", "8.8.8.8"),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&bytes[..], br#"{"latestVersion":"1.9.0"}"#);
        assert_eq!(
            rows(&st),
            vec![(
                ID.into(),
                "1.8.0".into(),
                2,
                4,
                5,
                Some("AS15169".into()),
                Some("Google LLC".into()),
                Some("US".into())
            )]
        );
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            ["/lite/8.8.8.8 Bearer tok"]
        );

        // No column holds the address.
        let conn = st.db.lock().unwrap();
        let dump: String = conn
            .query_row(
                "SELECT group_concat(received_at || install_id || version || coalesce(asn, '') || coalesce(as_name, '') || coalesce(country, '')) FROM reports",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!dump.contains("8.8.8.8"));
    }

    #[tokio::test]
    async fn free_endpoint_and_cache() {
        let (base, seen) = fake_ipinfo().await;
        let st = state(&base, None);
        for _ in 0..2 {
            let resp = app(st.clone())
                .oneshot(post_report(body(ID), &[("x-real-ip", "1.1.1.1")]))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }
        let r = rows(&st);
        assert_eq!(r.len(), 2);
        assert_eq!(
            (r[0].5.as_deref(), r[0].6.as_deref(), r[0].7.as_deref()),
            (Some("AS13335"), Some("Cloudflare, Inc."), Some("AU"))
        );
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "the second report must come from the cache"
        );
    }

    #[tokio::test]
    async fn private_address_skips_lookup() {
        let (base, seen) = fake_ipinfo().await;
        let st = state(&base, None);
        let resp = app(st.clone())
            .oneshot(post_report(body(ID), &[]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(rows(&st)[0].5, None);
    }

    #[tokio::test]
    async fn latest_stores_nothing() {
        let st = state("http://127.0.0.1:9", None);
        let resp = app(st.clone())
            .oneshot(Request::get("/v1/latest").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&bytes[..], br#"{"latestVersion":"1.9.0"}"#);
        assert!(rows(&st).is_empty());
    }

    #[tokio::test]
    async fn ignores_unknown_fields() {
        let st = state("http://127.0.0.1:9", None);
        let b = format!(
            r#"{{"installId":"{ID}","version":"2.0.0","groups":1,"sites":2,"nodes":3,"future":true}}"#
        );
        let resp = app(st.clone()).oneshot(post_report(b, &[])).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rejects_bad_reports() {
        let st = state("http://127.0.0.1:9", None);
        for b in [
            "not json".to_string(),
            body("not-a-uuid"),
            body(&ID.to_uppercase()),
            format!(
                r#"{{"installId":"{ID}","version":"1.8.0; drop","groups":1,"sites":1,"nodes":1}}"#
            ),
            format!(r#"{{"installId":"{ID}","version":"1.8.0","groups":-1,"sites":1,"nodes":1}}"#),
            format!(
                r#"{{"installId":"{ID}","version":"1.8.0","groups":1000001,"sites":1,"nodes":1}}"#
            ),
            format!(r#"{{"installId":"{ID}","version":"1.8.0","groups":1}}"#),
            format!(
                r#"{{"installId":"{ID}","version":"{}","groups":1,"sites":1,"nodes":1}}"#,
                "1".repeat(65)
            ),
        ] {
            let resp = app(st.clone())
                .oneshot(post_report(b.clone(), &[]))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{b}");
        }
        let big = format!(
            r#"{{"installId":"{ID}","version":"1.8.0","groups":1,"sites":1,"nodes":1,"pad":"{}"}}"#,
            "x".repeat(5000)
        );
        let resp = app(st.clone())
            .oneshot(post_report(big, &[]))
            .await
            .unwrap();
        assert!(resp.status().is_client_error());
        assert!(rows(&st).is_empty());
    }

    #[tokio::test]
    async fn rate_limits_one_address() {
        let st = state("http://127.0.0.1:9", None);
        for i in 0..RATE_LIMIT {
            let resp = app(st.clone())
                .oneshot(post_report(body(ID), &[]))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "report {i}");
        }
        let resp = app(st.clone())
            .oneshot(post_report(body(ID), &[]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(rows(&st).len(), RATE_LIMIT as usize);
    }

    #[test]
    fn client_ip_trusts_the_edge() {
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let mut h = HeaderMap::new();
        assert_eq!(client_ip(&h, peer), peer);
        h.insert("x-forwarded-for", "6.6.6.6, 9.9.9.9".parse().unwrap());
        assert_eq!(client_ip(&h, peer), "9.9.9.9".parse::<IpAddr>().unwrap());
        h.insert("x-real-ip", "8.8.4.4".parse().unwrap());
        assert_eq!(client_ip(&h, peer), "8.8.4.4".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn public_addresses() {
        for ip in [
            "10.1.2.3",
            "192.168.1.1",
            "127.0.0.1",
            "100.64.0.1",
            "169.254.1.1",
            "::1",
            "fd00::1",
            "fe80::1",
        ] {
            assert!(!is_public(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["8.8.8.8", "100.128.0.1", "2001:4860:4860::8888"] {
            assert!(is_public(ip.parse().unwrap()), "{ip}");
        }
    }

    #[test]
    fn prune_keeps_recent() {
        let conn = open_db(Path::new(":memory:")).unwrap();
        conn.execute(
            "INSERT INTO reports (received_at, install_id, version, groups, sites, nodes) VALUES ('2020-01-01T00:00:00Z', ?1, '1.0.0', 1, 1, 1)",
            [ID],
        )
        .unwrap();
        let r = Report {
            install_id: ID.into(),
            version: "1.8.0".into(),
            groups: 1,
            sites: 1,
            nodes: 1,
        };
        insert(&conn, &r, &GeoInfo::default()).unwrap();
        assert_eq!(prune(&conn, 400).unwrap(), 1);
        let left: i64 = conn
            .query_row("SELECT count(*) FROM reports", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 1);
    }
}
