use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{DefaultBodyLimit, Request, State},
    http::{header, HeaderValue, Method, StatusCode, Uri},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use oauth2::CsrfToken;
use reqwest::Url;
use serde::Serialize;
use tokio::sync::{Mutex, Notify};

use crate::{
    daikincloudconnection::{AuthenticationState, DaikinCloudConnection},
    MqttBridgeError,
};

#[derive(Clone)]
pub(crate) struct WebState {
    connection: Arc<Mutex<DaikinCloudConnection>>,
    refresh: Arc<Notify>,
    redirect_uri: String,
    csrf_token: CsrfToken,
    allowed_hosts: Vec<String>,
}

impl WebState {
    pub(crate) fn new(
        connection: Arc<Mutex<DaikinCloudConnection>>,
        refresh: Arc<Notify>,
        redirect_uri: String,
        listen: SocketAddr,
    ) -> Result<Self, MqttBridgeError> {
        let redirect = Url::parse(&redirect_uri).map_err(|_| invalid_redirect())?;
        if !matches!(redirect.scheme(), "http" | "https")
            || redirect.host_str().is_none()
            || !redirect.username().is_empty()
            || redirect.password().is_some()
            || redirect.path() != "/oauth/callback"
            || redirect.query().is_some()
            || redirect.fragment().is_some()
        {
            return Err(invalid_redirect());
        }
        let uri: axum::http::Uri = redirect.as_str().parse().map_err(|_| invalid_redirect())?;
        let authority = uri.authority().ok_or_else(invalid_redirect)?.as_str().to_ascii_lowercase();
        let mut allowed_hosts = vec![authority.clone(), listen.to_string()];
        if redirect.port().is_none() {
            if let Some(port) = redirect.port_or_known_default() {
                allowed_hosts.push(format!("{authority}:{port}"));
            }
        }
        if listen.ip().is_loopback() || listen.ip().is_unspecified() {
            allowed_hosts.extend([
                format!("localhost:{}", listen.port()),
                format!("127.0.0.1:{}", listen.port()),
                format!("[::1]:{}", listen.port()),
            ]);
        }
        if matches!(listen.port(), 80 | 443) {
            let suffix = format!(":{}", listen.port());
            let unqualified: Vec<_> = allowed_hosts.iter().filter_map(|host| host.strip_suffix(&suffix).map(str::to_owned)).collect();
            allowed_hosts.extend(unqualified);
        }
        Ok(Self {
            connection,
            refresh,
            redirect_uri,
            csrf_token: CsrfToken::new_random(),
            allowed_hosts,
        })
    }
}

fn invalid_redirect() -> MqttBridgeError {
    MqttBridgeError::RuntimeError(
        "The web OAuth redirect URI must be an HTTP(S) URL ending in /oauth/callback, without credentials, query, or fragment; register that exact URI with Daikin".into(),
    )
}

pub(crate) fn router(state: WebState) -> Router {
    Router::new()
        .route("/", get(|| async { Html(include_str!("../resources/web/index.html")) }))
        .route(
            "/style.css",
            get(|| async { ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], include_str!("../resources/web/style.css")) }),
        )
        .route(
            "/app.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                    include_str!("../resources/web/app.js"),
                )
            }),
        )
        .route("/api/auth/status", get(status))
        .route("/api/auth/start", post(start))
        .route("/api/auth/revoke", post(revoke))
        .route("/oauth/callback", get(callback))
        .layer(DefaultBodyLimit::max(1024))
        .layer(middleware::from_fn_with_state(state.clone(), protect_requests))
        .with_state(state)
}

// The UI is a local administration endpoint, not a multi-user login system.
// Host validation prevents DNS rebinding; a per-process secret blocks cross-site actions.
async fn protect_requests(State(state): State<WebState>, request: Request, next: Next) -> Response {
    let host = request.headers().get(header::HOST).and_then(|value| value.to_str().ok());
    let host_allowed = host.is_some_and(|host| state.allowed_hosts.iter().any(|allowed| allowed.eq_ignore_ascii_case(host)));
    let mut response = if !host_allowed {
        message(
            StatusCode::FORBIDDEN,
            "Unrecognized web host; use the configured callback host or local listen address",
        )
    } else if request.method() == Method::POST && !valid_csrf(&state, &request) {
        message(StatusCode::FORBIDDEN, "Invalid request origin or CSRF token; reload the page and try again")
    } else {
        next.run(request).await
    };
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    headers.insert("x-content-type-options", HeaderValue::from_static("nosniff"));
    headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
    headers.insert("content-security-policy", HeaderValue::from_static(
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
    ));
    response
}

fn valid_csrf(state: &WebState, request: &Request) -> bool {
    let Some(token) = request.headers().get("x-csrf-token").and_then(|value| value.to_str().ok()) else {
        return false;
    };
    if CsrfToken::new(token.to_owned()) != state.csrf_token {
        return false;
    }
    if let Some(origin) = request.headers().get(header::ORIGIN) {
        let Some(origin) = origin.to_str().ok().and_then(|value| Url::parse(value).ok()) else {
            return false;
        };
        let Some(host) = request.headers().get(header::HOST).and_then(|value| value.to_str().ok()) else {
            return false;
        };
        // Both schemes are accepted for a TLS-terminating reverse proxy, but not other hosts.
        let same_origin = ["http", "https"]
            .iter()
            .any(|scheme| Url::parse(&format!("{scheme}://{host}")).is_ok_and(|expected| origin.origin() == expected.origin()));
        if !same_origin {
            return false;
        }
    }
    true
}

#[derive(Serialize)]
struct Status {
    state: AuthenticationState,
    redirect_uri: String,
    csrf_token: String,
}

async fn status(State(state): State<WebState>) -> Json<Status> {
    let authentication = state.connection.lock().await.authentication_state();
    Json(Status {
        state: authentication,
        redirect_uri: state.redirect_uri,
        csrf_token: state.csrf_token.secret().clone(),
    })
}

async fn start(State(state): State<WebState>) -> Response {
    match state.connection.lock().await.begin_authorization() {
        Ok(url) => Json(serde_json::json!({ "authorization_url": url.as_str() })).into_response(),
        Err(error) => message(StatusCode::CONFLICT, &safe_error(&error)),
    }
}

async fn callback(State(state): State<WebState>, uri: Uri) -> Redirect {
    // Reconstruct using the registered destination, never untrusted Host/Forwarded headers.
    // The cloud connection checks state, duplicates, provider errors and one-time PKCE use.
    let callback = format!("{}?{}", state.redirect_uri, uri.query().unwrap_or_default());
    let mut connection = state.connection.lock().await;
    let result = match connection.set_authorization_code(callback) {
        Ok(()) => {
            let result = connection.exchange_pending_code().await;
            // Accepted callbacks trigger immediate polling/retry. Invalid callbacks must
            // not bypass the configured polling interval or consume provider API quota.
            state.refresh.notify_one();
            result
        }
        Err(error) => Err(error),
    };
    drop(connection);
    match result {
        Ok(()) => Redirect::to("/?auth=success"),
        Err(error) => {
            log::warn!("Web authorization failed: {}", safe_error(&error));
            Redirect::to("/?auth=error")
        }
    }
}

async fn revoke(State(state): State<WebState>) -> Response {
    let result = state.connection.lock().await.revoke_authorization().await;
    // On failure the bridge must also mark itself offline: revocation intent suspends polling.
    state.refresh.notify_one();
    match result {
        Ok(()) => message(StatusCode::OK, "Daikin access has been revoked. You can connect again at any time."),
        Err(error) => message(StatusCode::BAD_GATEWAY, &safe_error(&error)),
    }
}

fn safe_error(error: &MqttBridgeError) -> String {
    match error {
        MqttBridgeError::AuthorizationRequired(_) => "Authorization expired or was rejected; connect to Daikin again".into(),
        _ => error.to_string(),
    }
}

fn message(status: StatusCode, text: &str) -> Response {
    (status, Json(serde_json::json!({ "message": text }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Server {
        address: SocketAddr,
        task: tokio::task::JoinHandle<()>,
        _directory: tempfile::TempDir,
    }

    impl Server {
        async fn new() -> Self {
            let directory = tempfile::tempdir().expect("create private data directory");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind test UI");
            let address = listener.local_addr().expect("get test address");
            let redirect = format!("http://{address}/oauth/callback");
            let connection = DaikinCloudConnection::new("client".into(), "secret".into(), redirect.clone(), directory.path()).expect("create connection");
            let state = WebState::new(Arc::new(Mutex::new(connection)), Arc::new(Notify::new()), redirect, address).expect("create web state");
            let task = tokio::spawn(async move { axum::serve(listener, router(state)).await.expect("serve test UI") });
            Self {
                address,
                task,
                _directory: directory,
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{path}", self.address)
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[tokio::test]
    async fn serves_embedded_ui_and_safe_status_without_mqtt() {
        let server = Server::new().await;
        let client = reqwest::Client::new();
        for (path, content_type, content) in [
            ("/", "text/html", "Connect your Daikin account"),
            ("/style.css", "text/css", ".connection-card"),
            ("/app.js", "text/javascript", "/api/auth/status"),
        ] {
            let response = client.get(server.url(path)).send().await.expect("fetch asset");
            assert_eq!(response.status(), StatusCode::OK);
            assert!(response.headers()[header::CONTENT_TYPE]
                .to_str()
                .expect("content type")
                .starts_with(content_type));
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
            assert_eq!(response.headers()["referrer-policy"], "no-referrer");
            assert!(response.headers().contains_key("content-security-policy"));
            assert!(response.text().await.expect("read asset").contains(content));
        }
        let status: serde_json::Value = client
            .get(server.url("/api/auth/status"))
            .send()
            .await
            .expect("fetch status")
            .json()
            .await
            .expect("read status");
        assert_eq!(status["state"], "unauthorized");
        assert_eq!(status["redirect_uri"], server.url("/oauth/callback"));
        assert!(!status["csrf_token"].as_str().expect("CSRF token").is_empty());
        assert_eq!(status.as_object().expect("status object").len(), 3);
    }

    #[tokio::test]
    async fn actions_require_csrf_and_same_origin_and_reject_dns_rebinding() {
        let server = Server::new().await;
        let client = reqwest::Client::new();
        for path in ["/api/auth/start", "/api/auth/revoke"] {
            let response = client
                .post(server.url(path))
                .json(&serde_json::json!({}))
                .send()
                .await
                .expect("post without CSRF");
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
        let response = client
            .get(server.url("/api/auth/status"))
            .header(header::HOST, "attacker.example")
            .send()
            .await
            .expect("request with hostile host");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let status: serde_json::Value = client
            .get(server.url("/api/auth/status"))
            .send()
            .await
            .expect("fetch status")
            .json()
            .await
            .expect("read status");
        let csrf = status["csrf_token"].as_str().expect("csrf token");
        for (token, origin) in [("invalid", server.url("")), (csrf, "https://attacker.example".into())] {
            let response = client
                .post(server.url("/api/auth/start"))
                .header("x-csrf-token", token)
                .header(header::ORIGIN, origin)
                .json(&serde_json::json!({}))
                .send()
                .await
                .expect("post protected action");
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
        let response = client
            .post(server.url("/api/auth/start"))
            .header("x-csrf-token", csrf)
            .header(header::ORIGIN, server.url(""))
            .json(&serde_json::json!({}))
            .send()
            .await
            .expect("begin authorization");
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await.expect("authorization response");
        let url = Url::parse(body["authorization_url"].as_str().expect("authorization URL")).expect("valid URL");
        let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query["redirect_uri"], server.url("/oauth/callback"));
        assert_eq!(query["response_type"], "code");
        assert_eq!(query["code_challenge_method"], "S256");
        assert!(!query["state"].is_empty());
    }

    #[tokio::test]
    async fn invalid_callback_redirects_without_leaking_oauth_data() {
        let server = Server::new().await;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("create client");
        for query in ["code=private-code&state=wrong", "error=access_denied&error_description=private-error", ""] {
            let response = client
                .get(server.url(&format!("/oauth/callback?{query}")))
                .send()
                .await
                .expect("send invalid callback");
            assert_eq!(response.status(), StatusCode::SEE_OTHER);
            assert_eq!(response.headers()[header::LOCATION], "/?auth=error");
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
            assert!(!response.text().await.expect("read response").contains("private-"));
        }
    }

    #[test]
    fn normalizes_default_ports_for_local_hosts_and_proxy_origins() {
        let directory = tempfile::tempdir().expect("create directory");
        let redirect = "https://bridge.example.com/oauth/callback";
        let connection = DaikinCloudConnection::new("client".into(), "secret".into(), redirect.into(), directory.path()).expect("create connection");
        let state = WebState::new(
            Arc::new(Mutex::new(connection)),
            Arc::new(Notify::new()),
            redirect.into(),
            "127.0.0.1:80".parse().expect("listen address"),
        )
        .expect("create state");
        for (host, origin) in [
            ("localhost", "http://localhost"),
            ("localhost:80", "http://localhost"),
            ("127.0.0.1", "http://127.0.0.1"),
            ("bridge.example.com:443", "https://bridge.example.com"),
        ] {
            assert!(state.allowed_hosts.iter().any(|allowed| allowed == host));
            let request = Request::builder()
                .method(Method::POST)
                .uri("/api/auth/start")
                .header(header::HOST, host)
                .header(header::ORIGIN, origin)
                .header("x-csrf-token", state.csrf_token.secret())
                .body(axum::body::Body::empty())
                .expect("build request");
            assert!(valid_csrf(&state, &request), "origin should match {host}");
        }
    }

    #[tokio::test]
    async fn requires_an_automatic_http_callback_destination() {
        let directory = tempfile::tempdir().expect("create private directory");
        for redirect in [
            "onectaintegration://callback/",
            "https://example.com/other",
            "https://example.com/oauth/callback?query=1",
            "https://example.com/oauth/callback#fragment",
            "https://user:pass@example.com/oauth/callback",
        ] {
            let connection = DaikinCloudConnection::new(
                "client".into(),
                "secret".into(),
                "http://localhost:8076/oauth/callback".into(),
                directory.path(),
            )
            .expect("create connection");
            assert!(
                WebState::new(
                    Arc::new(Mutex::new(connection)),
                    Arc::new(Notify::new()),
                    redirect.into(),
                    "127.0.0.1:8076".parse().expect("listen address")
                )
                .is_err(),
                "invalid redirect: {redirect}"
            );
        }
    }
}
