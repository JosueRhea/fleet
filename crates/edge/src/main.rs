//! Edge proxy v0. Pulls the edge doc, routes by Host header, and keeps serving the last-known-good
//! routes when the doc is missing or broken. It reports which version it applied (the deploy
//! workflow's "edge ack") and per-backend in-flight/request/error counts (drain and bake signals).

use anyhow::Result;
use bytes::Bytes;
use ccloud_proto::{self as proto, Backend, BackendState, EdgeDoc, Route};
use clap::Parser;
use http_body_util::{combinators::BoxBody, BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::collections::HashMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(about = "ccloud edge proxy")]
struct Cli {
    /// Edge desired-state doc to pull.
    #[arg(long)]
    doc: PathBuf,
    /// Where to write the status report (applied version + backend stats).
    #[arg(long)]
    status: PathBuf,
    #[arg(long, default_value = "/var/lib/ccloud/edge")]
    state_dir: PathBuf,
    #[arg(long, default_value = "0.0.0.0:8000")]
    listen: String,
}

const EJECT_AFTER: u32 = 5;
const EJECT_FOR: Duration = Duration::from_secs(30);
const MAX_BODY: usize = 16 << 20;

#[derive(Default)]
struct Stats {
    deployment: Mutex<String>,
    in_flight: AtomicU64,
    requests: AtomicU64,
    errors: AtomicU64,
    consecutive_failures: AtomicU32,
    ejected_until: Mutex<Option<Instant>>,
}

impl Stats {
    fn ejected(&self) -> bool {
        self.ejected_until.lock().unwrap().is_some_and(|t| Instant::now() < t)
    }
    fn result(&self, ok: bool) {
        if ok {
            self.consecutive_failures.store(0, Ordering::Relaxed);
            return;
        }
        self.errors.fetch_add(1, Ordering::Relaxed);
        if self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1 >= EJECT_AFTER {
            *self.ejected_until.lock().unwrap() = Some(Instant::now() + EJECT_FOR);
            self.consecutive_failures.store(0, Ordering::Relaxed);
        }
    }
}

/// Decrements in-flight when the response body is fully sent or dropped.
struct InFlight(Arc<Stats>);
impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Edge {
    routes: RwLock<HashMap<String, Route>>,
    applied: AtomicU64,
    stats: Mutex<HashMap<String, Arc<Stats>>>,
    rr: AtomicUsize,
    client: Client<HttpConnector, Full<Bytes>>,
}

type Body = BoxBody<Bytes, hyper::Error>;

fn text(code: StatusCode, msg: &str) -> Response<Body> {
    let body = Full::new(Bytes::from(format!("{msg}\n"))).map_err(|n: Infallible| match n {}).boxed();
    Response::builder().status(code).body(body).unwrap()
}

fn log(msg: impl AsRef<str>) {
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    eprintln!("[{}.{:03}] {}", t.as_secs(), t.subsec_millis(), msg.as_ref());
}

impl Edge {
    fn stats(&self, b: &Backend) -> Arc<Stats> {
        let s = self.stats.lock().unwrap().entry(b.instance.clone()).or_default().clone();
        *s.deployment.lock().unwrap() = b.deployment.clone();
        s
    }

    fn apply(&self, doc: EdgeDoc) {
        let mut routes = self.routes.write().unwrap();
        for r in doc.routes {
            routes.insert(r.host.clone(), r);
        }
        for t in &doc.tombstones {
            routes.remove(t);
        }
        self.applied.store(doc.version, Ordering::SeqCst);
        log(format!("applied edge doc v{} ({} routes)", doc.version, routes.len()));
    }

    /// Weighted round robin over active, non-ejected backends; never eject the last one.
    fn pick(&self, host: &str, skip: &[String]) -> Option<(Backend, Arc<Stats>)> {
        let routes = self.routes.read().unwrap();
        let route = routes.get(host)?;
        let live: Vec<&Backend> = route
            .backends
            .iter()
            .filter(|b| b.state == BackendState::Active && b.weight > 0 && !skip.contains(&b.instance))
            .collect();
        let healthy: Vec<&Backend> = live.iter().copied().filter(|b| !self.stats(b).ejected()).collect();
        let pool = if healthy.is_empty() { live } else { healthy };
        let total: u32 = pool.iter().map(|b| b.weight).sum();
        if total == 0 {
            return None;
        }
        let mut n = (self.rr.fetch_add(1, Ordering::Relaxed) % total as usize) as u32;
        for b in pool {
            if n < b.weight {
                return Some((b.clone(), self.stats(b)));
            }
            n -= b.weight;
        }
        None
    }

    async fn handle(self: Arc<Self>, req: Request<Incoming>) -> Result<Response<Body>, Infallible> {
        let host = req
            .headers()
            .get(hyper::header::HOST)
            .and_then(|h| h.to_str().ok())
            .map(|h| h.split(':').next().unwrap_or(h).to_ascii_lowercase())
            .unwrap_or_default();
        if !self.routes.read().unwrap().contains_key(&host) {
            return Ok(text(StatusCode::NOT_FOUND, "unknown host"));
        }
        let (parts, body) = req.into_parts();
        // Buffered so a request that never reached a backend can be retried on another one.
        let body = match Limited::new(body, MAX_BODY).collect().await {
            Ok(b) => b.to_bytes(),
            Err(_) => return Ok(text(StatusCode::PAYLOAD_TOO_LARGE, "body too large")),
        };
        let idempotent = matches!(parts.method, Method::GET | Method::HEAD | Method::OPTIONS);
        let mut tried = Vec::new();
        for _ in 0..3 {
            let Some((b, stats)) = self.pick(&host, &tried) else { break };
            tried.push(b.instance.clone());
            stats.requests.fetch_add(1, Ordering::Relaxed);
            stats.in_flight.fetch_add(1, Ordering::SeqCst);
            let guard = InFlight(stats.clone());
            let pq = parts.uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
            let mut up = Request::builder().method(parts.method.clone()).uri(format!("http://{}{pq}", b.addr));
            for (k, v) in &parts.headers {
                up = up.header(k, v);
            }
            let up = up.header("x-ccloud-instance", &b.instance).body(Full::new(body.clone())).unwrap();
            match self.client.request(up).await {
                Ok(resp) => {
                    stats.result(!resp.status().is_server_error());
                    let (p, body) = resp.into_parts();
                    let body = body
                        .map_frame(move |f| {
                            let _ = &guard;
                            f
                        })
                        .boxed();
                    return Ok(Response::from_parts(p, body));
                }
                Err(e) => {
                    stats.result(false);
                    let retry = e.is_connect() || idempotent;
                    log(format!("{host}: {} failed ({e:?}){}", b.instance, if retry { ", retrying" } else { "" }));
                    if !retry {
                        return Ok(text(StatusCode::BAD_GATEWAY, "upstream error"));
                    }
                }
            }
        }
        Ok(text(StatusCode::SERVICE_UNAVAILABLE, "no healthy backend"))
    }

    fn status(&self) -> proto::EdgeStatus {
        let backends = self
            .stats
            .lock()
            .unwrap()
            .iter()
            .map(|(id, s)| proto::BackendStats {
                instance: id.clone(),
                deployment: s.deployment.lock().unwrap().clone(),
                in_flight: s.in_flight.load(Ordering::SeqCst),
                requests: s.requests.load(Ordering::Relaxed),
                errors: s.errors.load(Ordering::Relaxed),
                ejected: s.ejected(),
            })
            .collect();
        proto::EdgeStatus { applied_version: self.applied.load(Ordering::SeqCst), backends }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    std::fs::create_dir_all(&cli.state_dir)?;
    let last_good = cli.state_dir.join("last-good-edge.json");
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(Duration::from_secs(2)));
    let edge = Arc::new(Edge {
        routes: Default::default(),
        applied: AtomicU64::new(0),
        stats: Default::default(),
        rr: AtomicUsize::new(0),
        client: Client::builder(TokioExecutor::new()).pool_idle_timeout(Duration::from_secs(5)).build(connector),
    });
    if let Some(doc) = proto::load_last_good::<EdgeDoc>(&last_good) {
        log("restoring last-known-good edge doc");
        edge.apply(doc);
    }
    tokio::spawn({
        let edge = edge.clone();
        async move {
            let mut last_err = String::new();
            loop {
                match proto::load_newer::<EdgeDoc>(&cli.doc, &last_good, edge.applied.load(Ordering::SeqCst)) {
                    Ok(Some(doc)) => edge.apply(doc),
                    Ok(None) => last_err.clear(),
                    Err(e) => {
                        let e = format!("{e:#}");
                        if e != last_err {
                            log(format!("edge doc unusable ({e}); keeping v{}", edge.applied.load(Ordering::SeqCst)));
                            last_err = e;
                        }
                    }
                }
                if let Err(e) = proto::write_atomic(&cli.status, &edge.status()) {
                    log(format!("writing status: {e:#}"));
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    });
    let listener = tokio::net::TcpListener::bind(&cli.listen).await?;
    log(format!("edge listening on {}", cli.listen));
    loop {
        let (conn, _) = listener.accept().await?;
        let edge = edge.clone();
        tokio::spawn(async move {
            let svc = hyper::service::service_fn(move |req| edge.clone().handle(req));
            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(conn), svc).await;
        });
    }
}
