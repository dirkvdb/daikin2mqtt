use super::*;

use std::{
    collections::BTreeMap,
    io::{self, Read},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Instant,
};

const CLIENT_ID: &str = "test-client+id";
const CLIENT_SECRET: &str = "test-secret&=+";
const REDIRECT_URI: &str = "https://example.invalid/custom/oauth/callback";
const DEVICES: &str = r#"[{"id":"test-gateway"}]"#;

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "daikin2mqtt-auth-{}-{}",
            std::process::id(),
            CsrfToken::new_random().secret()
        ));
        fs::create_dir(&path).expect("create isolated authentication directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn authentication_file(&self) -> PathBuf {
        self.0.join("openid_authentication.json")
    }

    fn saved(&self) -> serde_json::Value {
        serde_json::from_slice(&fs::read(self.authentication_file()).expect("read persisted authentication"))
            .expect("authentication file is valid JSON")
    }

    fn seed_token(&self, access: &str, refresh: Option<&str>, expires_at: u64) {
        fs::write(
            self.authentication_file(),
            serde_json::to_vec(&serde_json::json!({
                "token": {
                    "access_token": access,
                    "refresh_token": refresh,
                    "expires_at": expires_at,
                }
            }))
            .expect("serialize test token"),
        )
        .expect("seed persisted authentication");
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct MockResponse {
    status: u16,
    body: String,
}

impl MockResponse {
    fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }

    fn devices() -> Self {
        Self::json(200, DEVICES)
    }

    fn token(access: &str, refresh: Option<&str>) -> Self {
        let mut body = serde_json::json!({
            "access_token": access,
            "token_type": "Bearer",
            "expires_in": 3600,
        });
        if let Some(refresh) = refresh {
            body["refresh_token"] = refresh.into();
        }
        Self::json(200, &body.to_string())
    }

    fn success() -> Self {
        Self::json(200, "")
    }

    fn unavailable() -> Self {
        Self::json(503, r#"{"error":"temporarily_unavailable"}"#)
    }

    fn invalid_grant() -> Self {
        Self::json(400, r#"{"error":"invalid_grant"}"#)
    }

    fn unauthorized() -> Self {
        Self::json(401, r#"{"error":"unauthorized"}"#)
    }
}

#[derive(Debug)]
struct RecordedRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: String,
}

impl RecordedRequest {
    fn form(&self) -> BTreeMap<String, String> {
        let mut url = Url::parse("http://localhost/").expect("valid form parser URL");
        url.set_query(Some(&self.body));
        let pairs: Vec<_> = url.query_pairs().into_owned().collect();
        let fields: BTreeMap<_, _> = pairs.iter().cloned().collect();
        assert_eq!(fields.len(), pairs.len(), "duplicate form parameters");
        fields
    }

    fn assert_client_form(&self, path: &str) -> BTreeMap<String, String> {
        assert_eq!(self.method, "POST");
        assert_eq!(self.path, path);
        assert_eq!(
            self.headers.get("content-type").map(String::as_str),
            Some("application/x-www-form-urlencoded")
        );
        assert!(
            !self.headers.contains_key("authorization"),
            "client credentials must be in the form, not HTTP Basic auth"
        );
        let form = self.form();
        assert_eq!(form["client_id"], CLIENT_ID);
        assert_eq!(form["client_secret"], CLIENT_SECRET);
        form
    }

    fn assert_bearer_api(&self, access: &str) {
        assert_eq!(self.method, "GET");
        assert_eq!(self.path, "/v1/gateway-devices");
        assert_eq!(self.headers.get("authorization"), Some(&format!("Bearer {access}")));
        assert_eq!(self.headers.get("accept").map(String::as_str), Some("application/json"));
        assert!(self.body.is_empty());
    }
}

struct MockServer {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<io::Result<Vec<RecordedRequest>>>>,
    expected_requests: usize,
}

impl MockServer {
    fn new(responses: Vec<MockResponse>) -> Self {
        Self::new_with_observer(responses, |_| {})
    }

    fn new_with_observer(
        responses: Vec<MockResponse>,
        mut observe: impl FnMut(&RecordedRequest) + Send + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback mock server");
        let address = listener.local_addr().expect("mock server address");
        listener.set_nonblocking(true).expect("make mock listener nonblocking");
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let expected_requests = responses.len();
        let worker = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(20);
            let mut responses = responses.into_iter();
            let mut requests = Vec::new();
            while !worker_stop.load(Ordering::Acquire) {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "mock server deadline"));
                }
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                stream.set_write_timeout(Some(Duration::from_secs(2)))?;
                let request = read_request(&mut stream)?;
                observe(&request);
                let response = responses.next().unwrap_or_else(MockResponse::unavailable);
                requests.push(request);
                write!(
                    stream,
                    "HTTP/1.1 {} Mock\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response.status,
                    response.body.len(),
                    response.body,
                )?;
                stream.flush()?;
            }
            Ok(requests)
        });
        Self {
            address,
            stop,
            worker: Some(worker),
            expected_requests,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.address)
    }

    fn revocation_url(&self) -> String {
        // oauth2 v5 requires HTTPS even on loopback; loopback_oauth_request maps
        // the constructed request to HTTP inside the parent's test-only adapter.
        format!("https://{}/revoke", self.address)
    }

    fn finish(mut self) -> Vec<RecordedRequest> {
        self.stop.store(true, Ordering::Release);
        let requests = self
            .worker
            .take()
            .expect("mock worker exists")
            .join()
            .expect("mock server must not panic")
            .expect("mock server IO succeeds");
        assert_eq!(
            requests.len(),
            self.expected_requests,
            "unexpected number of requests: {requests:?}"
        );
        requests
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn read_request(stream: &mut TcpStream) -> io::Result<RecordedRequest> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    let headers_end = loop {
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        let count = stream.read(&mut buffer)?;
        if count == 0 || bytes.len() + count > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incomplete or oversized HTTP headers",
            ));
        }
        bytes.extend_from_slice(&buffer[..count]);
    };
    let headers = std::str::from_utf8(&bytes[..headers_end])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut lines = headers.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split_whitespace();
    let method = request_line.next().unwrap_or_default().to_owned();
    let path = request_line.next().unwrap_or_default().to_owned();
    let headers: BTreeMap<_, _> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        .unwrap_or(0);
    if length > 64 * 1024 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "oversized HTTP body"));
    }
    while bytes.len() < headers_end + length {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete HTTP body"));
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    let body = String::from_utf8(bytes[headers_end..headers_end + length].to_vec())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(RecordedRequest {
        method,
        path,
        headers,
        body,
    })
}

fn connection(directory: &TempDirectory) -> DaikinCloudConnection {
    connection_with_redirect(directory, REDIRECT_URI)
}

fn connection_with_redirect(directory: &TempDirectory, redirect: &str) -> DaikinCloudConnection {
    DaikinCloudConnection::new(
        CLIENT_ID.into(),
        CLIENT_SECRET.into(),
        redirect.into(),
        directory.path(),
    )
    .expect("construct test connection")
}

// Called by oauth_http under cfg(test), after oauth2's HTTPS endpoint validation.
// Restrict the plaintext transport to the numeric loopback revocation endpoint.
pub(super) fn loopback_oauth_request(mut request: HttpRequest) -> HttpRequest {
    if request.uri().scheme_str() == Some("https")
        && request.uri().host() == Some("127.0.0.1")
        && request.uri().path() == "/revoke"
        && request.uri().port_u16().is_some()
    {
        *request.uri_mut() = request
            .uri()
            .to_string()
            .replacen("https://", "http://", 1)
            .parse()
            .expect("valid loopback HTTP request URI");
    }
    request
}

fn use_mock(connection: &mut DaikinCloudConnection, server: &MockServer) {
    connection.oauth_client = connection
        .oauth_client
        .clone()
        .set_token_uri(TokenUrl::new(server.url("/token")).expect("valid mock token URL"))
        .set_revocation_url(RevocationUrl::new(server.revocation_url()).expect("valid mock revocation URL"));
    connection.api = server.url("/v1/");
    connection.http_client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(3))
        .build()
        .expect("build bounded local test client");
}

fn query(url: &Url) -> BTreeMap<String, String> {
    let pairs: Vec<_> = url.query_pairs().into_owned().collect();
    let result: BTreeMap<_, _> = pairs.iter().cloned().collect();
    assert_eq!(result.len(), pairs.len(), "duplicate authorization parameters");
    result
}

fn callback(redirect: &str, pairs: &[(&str, &str)]) -> String {
    let mut url = Url::parse(redirect).expect("valid callback base URL");
    url.query_pairs_mut().extend_pairs(pairs.iter().copied());
    url.into()
}

fn authorization_required(error: MqttBridgeError) -> Url {
    match error {
        MqttBridgeError::AuthorizationRequired(url) => url,
        other => panic!("expected authorization request, got {other:?}"),
    }
}

fn assert_cleared(connection: &DaikinCloudConnection, directory: &TempDirectory) {
    assert!(connection.authentication_data.token.is_none());
    assert!(!connection.authentication_data.revocation_pending);
    assert!(connection.pending_code.is_none());
    assert!(!connection.persistence_pending);
    assert!(!connection.deletion_pending);
    assert!(!directory.authentication_file().exists());
}

fn assert_revocation_pending(
    connection: &DaikinCloudConnection,
    directory: &TempDirectory,
    expected_token: &serde_json::Value,
) {
    assert!(connection.authentication_data.revocation_pending);
    assert!(!connection.persistence_pending);
    assert!(!connection.deletion_pending);
    let retained = connection
        .authentication_data
        .token
        .as_ref()
        .expect("retry token retained");
    assert_eq!(
        &serde_json::to_value(retained).expect("serialize retained token"),
        expected_token
    );
    let saved = directory.saved();
    assert_eq!(&saved["token"], expected_token);
    assert_eq!(saved["revocation_pending"], true);
}

fn assert_refresh(request: &RecordedRequest, refresh: &str) {
    let form = request.assert_client_form("/token");
    assert_eq!(form["grant_type"], "refresh_token");
    assert_eq!(form["refresh_token"], refresh);
}

fn assert_revocations(requests: &[RecordedRequest]) {
    assert_eq!(requests.len(), 2);
    let mut revoked = BTreeMap::new();
    for request in requests {
        let form = request.assert_client_form("/revoke");
        assert_eq!(form.len(), 4);
        assert!(revoked
            .insert(form["token_type_hint"].clone(), form["token"].clone())
            .is_none());
    }
    assert_eq!(revoked["access_token"], "stored-access");
    assert_eq!(revoked["refresh_token"], "stored-refresh");
}

#[test]
fn authorization_url_caches_random_state_scopes_and_s256_pkce() {
    let directory = TempDirectory::new();
    let mut first = connection(&directory);
    let first_url = first.auth_url();
    assert_eq!(first_url, first.auth_url());
    let fields = query(&first_url);
    assert_eq!(fields["response_type"], "code");
    assert_eq!(fields["client_id"], CLIENT_ID);
    assert_eq!(fields["redirect_uri"], REDIRECT_URI);
    assert!(!fields["state"].is_empty());
    assert_eq!(fields["code_challenge_method"], "S256");
    let mut scopes: Vec<_> = fields["scope"].split_whitespace().collect();
    scopes.sort_unstable();
    assert_eq!(scopes, ["offline_access", "onecta:basic.integration", "openid"]);
    let pending = first.pending_authorization.as_ref().expect("pending authorization");
    assert_eq!(pending.state.secret(), &fields["state"]);
    assert_eq!(
        PkceCodeChallenge::from_code_verifier_sha256(&pending.verifier).as_str(),
        fields["code_challenge"]
    );
    let mut second = connection(&directory);
    let second_fields = query(&second.auth_url());
    assert_ne!(fields["state"], second_fields["state"]);
    assert_ne!(fields["code_challenge"], second_fields["code_challenge"]);
    assert!(first.authentication_data.token.is_none());
    assert!(!directory.authentication_file().exists());
}

#[test]
fn invalid_callbacks_do_not_consume_pending_authorization() {
    let directory = TempDirectory::new();
    let mut connection = connection(&directory);
    let authorization = connection.auth_url();
    let state = query(&authorization)["state"].clone();
    let valid = callback(REDIRECT_URI, &[("code", "valid-code"), ("state", &state)]);
    let mut invalid = vec![
        ("bare code", "valid-code".into()),
        ("malformed URL", "://not-a-url".into()),
        (
            "wrong state",
            callback(REDIRECT_URI, &[("code", "valid-code"), ("state", "wrong")]),
        ),
        ("missing state", callback(REDIRECT_URI, &[("code", "valid-code")])),
        (
            "empty state",
            callback(REDIRECT_URI, &[("code", "valid-code"), ("state", "")]),
        ),
        ("missing code", callback(REDIRECT_URI, &[("state", &state)])),
        ("empty code", callback(REDIRECT_URI, &[("code", ""), ("state", &state)])),
        (
            "duplicate state",
            callback(
                REDIRECT_URI,
                &[("code", "valid-code"), ("state", &state), ("state", &state)],
            ),
        ),
        (
            "duplicate code",
            callback(
                REDIRECT_URI,
                &[("code", "valid-code"), ("code", "other-code"), ("state", &state)],
            ),
        ),
        (
            "provider error",
            callback(REDIRECT_URI, &[("error", "access_denied"), ("state", &state)]),
        ),
        (
            "error with code",
            callback(
                REDIRECT_URI,
                &[("code", "valid-code"), ("state", &state), ("error", "access_denied")],
            ),
        ),
        ("fragment", format!("{valid}#unexpected")),
    ];
    for redirect in [
        "http://example.invalid/custom/oauth/callback",
        "https://other.invalid/custom/oauth/callback",
        "https://example.invalid:8443/custom/oauth/callback",
        "https://example.invalid/custom/oauth/other",
        "https://example.invalid/custom/oauth/callback/",
        "https://intruder@example.invalid/custom/oauth/callback",
    ] {
        invalid.push((
            "wrong redirect",
            callback(redirect, &[("code", "valid-code"), ("state", &state)]),
        ));
    }
    for (case, url) in invalid {
        assert!(connection.set_authorization_code(url).is_err(), "accepted {case}");
        assert!(connection.pending_code.is_none(), "stored code after {case}");
        assert!(
            connection.pending_authorization.is_some(),
            "consumed authorization after {case}"
        );
        assert_eq!(
            connection.auth_url(),
            authorization,
            "changed authorization after {case}"
        );
    }
    connection
        .set_authorization_code(valid)
        .expect("valid callback remains usable");
    assert!(connection.pending_authorization.is_none());
    assert_eq!(
        connection.pending_code.as_ref().expect("pending code").code,
        "valid-code"
    );
}

#[test]
fn callbacks_require_pending_authorization_and_cannot_be_replayed() {
    let directory = TempDirectory::new();
    let mut connection = connection(&directory);
    assert!(connection
        .set_authorization_code(callback(REDIRECT_URI, &[("code", "code"), ("state", "unrequested")]))
        .is_err());
    assert!(connection.pending_authorization.is_none());
    assert!(connection.pending_code.is_none());
    let authorization = connection.auth_url();
    let state = query(&authorization)["state"].clone();
    let callback = callback(REDIRECT_URI, &[("code", "accepted-code"), ("state", &state)]);
    connection
        .set_authorization_code(callback.clone())
        .expect("accept callback once");
    let verifier = connection
        .pending_code
        .as_ref()
        .expect("pending code")
        .verifier
        .secret()
        .clone();
    assert!(connection.set_authorization_code(callback.clone()).is_err());
    assert_eq!(
        connection.pending_code.as_ref().expect("code retained").code,
        "accepted-code"
    );
    assert_eq!(
        connection
            .pending_code
            .as_ref()
            .expect("code retained")
            .verifier
            .secret(),
        &verifier
    );
    let next_authorization = connection.auth_url();
    assert_ne!(query(&next_authorization)["state"], state);
    assert!(connection.set_authorization_code(callback).is_err());
    assert_eq!(connection.auth_url(), next_authorization);
    assert_eq!(
        connection.pending_code.as_ref().expect("code retained").code,
        "accepted-code"
    );
}

#[tokio::test]
async fn unauthenticated_update_returns_the_cached_authorization_url() {
    let directory = TempDirectory::new();
    let mut connection = connection(&directory);
    let first = authorization_required(connection.update().await.expect_err("authorization needed"));
    let second = authorization_required(connection.update().await.expect_err("authorization still needed"));
    assert_eq!(first, second);
    assert_eq!(first, connection.auth_url());
    assert_cleared(&connection, &directory);
}

#[tokio::test]
async fn code_exchange_uses_exact_redirect_body_credentials_pkce_and_bearer_api() {
    let directory = TempDirectory::new();
    let server = MockServer::new(vec![
        // ONECTA's documented token response omits token_type.
        MockResponse::json(
            200,
            r#"{"access_token":"code-access","refresh_token":"code-refresh","expires_in":3600}"#,
        ),
        MockResponse::devices(),
    ]);
    let redirect = "https://example.invalid:8443/custom%20oauth/callback/";
    let mut connection = connection_with_redirect(&directory, redirect);
    use_mock(&mut connection, &server);
    let authorization = query(&connection.auth_url());
    assert_eq!(authorization["redirect_uri"], redirect);
    let code = "authorization+code/with &=?";
    connection
        .set_authorization_code(callback(
            redirect,
            &[("code", code), ("state", &authorization["state"])],
        ))
        .expect("accept callback");
    let before = now();
    assert_eq!(
        connection.update().await.expect("exchange code and fetch devices"),
        DEVICES
    );
    let after = now();
    assert!(connection.pending_code.is_none());
    assert!(connection.pending_authorization.is_none());
    assert!(!connection.persistence_pending);
    let saved = directory.saved();
    assert_eq!(saved["token"]["access_token"], "code-access");
    assert_eq!(saved["token"]["refresh_token"], "code-refresh");
    let expires_at = saved["token"]["expires_at"].as_u64().expect("absolute expiry");
    assert!((before + 3600..=after + 3600).contains(&expires_at));
    assert!(saved.get("auth_code").is_none());
    assert!(saved["token"].get("expires_in").is_none());
    let requests = server.finish();
    let form = requests[0].assert_client_form("/token");
    assert_eq!(form["grant_type"], "authorization_code");
    assert_eq!(form["code"], code);
    assert_eq!(form["redirect_uri"], redirect);
    let verifier = PkceCodeVerifier::new(form["code_verifier"].clone());
    assert_eq!(
        PkceCodeChallenge::from_code_verifier_sha256(&verifier).as_str(),
        authorization["code_challenge"]
    );
    assert!(!form.contains_key("state"));
    requests[1].assert_bearer_api("code-access");
}

#[tokio::test]
async fn transient_code_exchange_failure_retains_code_and_pkce_for_retry() {
    let directory = TempDirectory::new();
    let server = MockServer::new(vec![
        MockResponse::unavailable(),
        MockResponse::token("retried-access", Some("retried-refresh")),
        MockResponse::devices(),
    ]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    let authorization = query(&connection.auth_url());
    connection
        .set_authorization_code(callback(
            REDIRECT_URI,
            &[("code", "retry-code"), ("state", &authorization["state"])],
        ))
        .expect("accept callback");
    let verifier = connection
        .pending_code
        .as_ref()
        .expect("pending code")
        .verifier
        .secret()
        .clone();
    assert!(matches!(
        connection.update().await,
        Err(MqttBridgeError::NetworkError(_))
    ));
    assert!(connection.authentication_data.token.is_none());
    assert!(connection.pending_authorization.is_none());
    assert!(!directory.authentication_file().exists());
    assert_eq!(
        connection.pending_code.as_ref().expect("code retained").code,
        "retry-code"
    );
    assert_eq!(
        connection
            .pending_code
            .as_ref()
            .expect("verifier retained")
            .verifier
            .secret(),
        &verifier
    );
    assert_eq!(connection.update().await.expect("retry code exchange"), DEVICES);
    assert!(connection.pending_code.is_none());
    let requests = server.finish();
    for request in &requests[..2] {
        let form = request.assert_client_form("/token");
        assert_eq!(form["code"], "retry-code");
        assert_eq!(form["code_verifier"], verifier);
    }
    requests[2].assert_bearer_api("retried-access");
}

#[tokio::test]
async fn invalid_grant_during_code_exchange_requires_a_new_authorization() {
    let directory = TempDirectory::new();
    let server = MockServer::new(vec![MockResponse::invalid_grant()]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    let authorization = query(&connection.auth_url());
    connection
        .set_authorization_code(callback(
            REDIRECT_URI,
            &[("code", "expired-code"), ("state", &authorization["state"])],
        ))
        .expect("accept callback");
    let next = authorization_required(
        connection
            .update()
            .await
            .expect_err("expired code needs reauthorization"),
    );
    assert_ne!(query(&next)["state"], authorization["state"]);
    assert_ne!(query(&next)["code_challenge"], authorization["code_challenge"]);
    assert_eq!(next, connection.auth_url());
    assert_cleared(&connection, &directory);
    let requests = server.finish();
    assert_eq!(requests[0].assert_client_form("/token")["code"], "expired-code");
}

#[tokio::test]
async fn refresh_rotation_is_saved_and_absolute_expiry_is_stable_on_restart() {
    let directory = TempDirectory::new();
    directory.seed_token("old-access", Some("old-refresh"), now().saturating_sub(60));
    let server = MockServer::new(vec![
        MockResponse::token("rotated-access", Some("rotated-refresh")),
        MockResponse::devices(),
        MockResponse::devices(),
    ]);
    let mut first = connection(&directory);
    use_mock(&mut first, &server);
    let before = now();
    assert_eq!(first.update().await.expect("refresh expired token"), DEVICES);
    let after = now();
    let saved = directory.saved();
    assert_eq!(saved["token"]["access_token"], "rotated-access");
    assert_eq!(saved["token"]["refresh_token"], "rotated-refresh");
    let expires_at = saved["token"]["expires_at"].as_u64().expect("absolute expiry");
    assert!((before + 3600..=after + 3600).contains(&expires_at));
    drop(first);
    let mut restarted = connection(&directory);
    use_mock(&mut restarted, &server);
    let restored = restarted.authentication_data.token.as_ref().expect("restored token");
    assert_eq!(restored.expires_at, expires_at);
    assert_eq!(restored.refresh_token.as_deref(), Some("rotated-refresh"));
    assert_eq!(restarted.update().await.expect("use restored access token"), DEVICES);
    assert_eq!(directory.saved()["token"]["expires_at"], expires_at);
    let requests = server.finish();
    assert_refresh(&requests[0], "old-refresh");
    requests[1].assert_bearer_api("rotated-access");
    requests[2].assert_bearer_api("rotated-access");
}

#[tokio::test]
async fn refresh_response_without_rotation_retains_the_existing_refresh_token() {
    let directory = TempDirectory::new();
    directory.seed_token("old-access", Some("reusable-refresh"), now().saturating_sub(60));
    let server = MockServer::new(vec![
        MockResponse::json(200, r#"{"access_token":"new-access","expires_in":3600}"#),
        MockResponse::devices(),
    ]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    assert_eq!(
        connection.update().await.expect("refresh without token rotation"),
        DEVICES
    );
    assert_eq!(directory.saved()["token"]["refresh_token"], "reusable-refresh");
    assert_eq!(
        connection
            .authentication_data
            .token
            .as_ref()
            .expect("refreshed token")
            .refresh_token
            .as_deref(),
        Some("reusable-refresh")
    );
    let requests = server.finish();
    assert_refresh(&requests[0], "reusable-refresh");
    requests[1].assert_bearer_api("new-access");
}

#[tokio::test]
async fn legacy_absolute_expires_in_is_migrated_and_saved_auth_code_is_removed() {
    let directory = TempDirectory::new();
    let expires_at = now() + 7200;
    fs::write(
        directory.authentication_file(),
        serde_json::json!({
            "auth_code": "legacy-single-use-code",
            "token": {
                "access_token": "legacy-access",
                "refresh_token": "legacy-refresh",
                "token_type": "bearer",
                "expires_in": expires_at,
                "scope": "openid onecta:basic.integration",
            }
        })
        .to_string(),
    )
    .expect("write legacy authentication file");
    let server = MockServer::new(vec![MockResponse::devices()]);
    let mut first = connection(&directory);
    use_mock(&mut first, &server);
    assert!(first.pending_code.is_none());
    assert!(first.pending_authorization.is_none());
    assert_eq!(
        first
            .authentication_data
            .token
            .as_ref()
            .expect("legacy token")
            .expires_at,
        expires_at
    );
    let migrated = directory.saved();
    assert_eq!(migrated["token"]["expires_at"], expires_at);
    assert!(migrated["token"].get("expires_in").is_none());
    assert!(migrated.get("auth_code").is_none());
    assert_eq!(
        first.update().await.expect("legacy access token remains valid"),
        DEVICES
    );
    drop(first);
    let restarted = connection(&directory);
    assert_eq!(
        restarted
            .authentication_data
            .token
            .as_ref()
            .expect("restored migrated token")
            .expires_at,
        expires_at
    );
    assert_eq!(directory.saved()["token"]["expires_at"], expires_at);
    server.finish()[0].assert_bearer_api("legacy-access");
}

#[tokio::test]
async fn transient_refresh_failure_retains_memory_and_disk_state_for_retry() {
    let directory = TempDirectory::new();
    let expires_at = now().saturating_sub(60);
    directory.seed_token("old-access", Some("old-refresh"), expires_at);
    let server = MockServer::new(vec![
        MockResponse::unavailable(),
        MockResponse::token("recovered-access", Some("recovered-refresh")),
        MockResponse::devices(),
    ]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    let saved_before = fs::read(directory.authentication_file()).expect("saved state before refresh");
    assert!(matches!(
        connection.update().await,
        Err(MqttBridgeError::NetworkError(_))
    ));
    let retained = connection
        .authentication_data
        .token
        .as_ref()
        .expect("token retained after transient error");
    assert_eq!(retained.access_token, "old-access");
    assert_eq!(retained.refresh_token.as_deref(), Some("old-refresh"));
    assert_eq!(retained.expires_at, expires_at);
    assert!(connection.pending_authorization.is_none());
    assert!(connection.pending_code.is_none());
    assert_eq!(
        fs::read(directory.authentication_file()).expect("state remains saved"),
        saved_before
    );
    assert_eq!(
        connection.update().await.expect("retry transient refresh failure"),
        DEVICES
    );
    assert_eq!(directory.saved()["token"]["refresh_token"], "recovered-refresh");
    let requests = server.finish();
    assert_refresh(&requests[0], "old-refresh");
    assert_refresh(&requests[1], "old-refresh");
    requests[2].assert_bearer_api("recovered-access");
}

#[tokio::test]
async fn invalid_refresh_grant_deletes_local_state_and_requires_reauthorization() {
    let directory = TempDirectory::new();
    directory.seed_token("old-access", Some("invalid-refresh"), now().saturating_sub(60));
    let server = MockServer::new(vec![MockResponse::invalid_grant()]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    let authorization = authorization_required(connection.update().await.expect_err("invalid refresh grant"));
    assert_cleared(&connection, &directory);
    assert_eq!(authorization, connection.auth_url());
    assert_eq!(
        authorization,
        authorization_required(connection.update().await.expect_err("still needs authorization"))
    );
    assert_refresh(&server.finish()[0], "invalid-refresh");
    let restarted = self::connection(&directory);
    assert!(restarted.authentication_data.token.is_none());
}

#[tokio::test]
async fn missing_or_empty_refresh_token_requests_authorization_without_network_io() {
    for refresh in [None, Some("")] {
        let directory = TempDirectory::new();
        directory.seed_token("expired-access", refresh, now().saturating_sub(60));
        let server = MockServer::new(vec![]);
        let mut connection = connection(&directory);
        use_mock(&mut connection, &server);
        let authorization = authorization_required(connection.update().await.expect_err("no usable refresh token"));
        assert_cleared(&connection, &directory);
        assert_eq!(authorization, connection.auth_url());
        server.finish();
    }
}

#[tokio::test]
async fn api_401_forces_refresh_even_when_the_access_token_has_not_expired() {
    let directory = TempDirectory::new();
    directory.seed_token("rejected-access", Some("stored-refresh"), now() + 7200);
    let server = MockServer::new(vec![
        MockResponse::unauthorized(),
        MockResponse::token("accepted-access", Some("rotated-refresh")),
        MockResponse::devices(),
    ]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    assert_eq!(connection.update().await.expect("refresh after API 401"), DEVICES);
    assert_eq!(directory.saved()["token"]["refresh_token"], "rotated-refresh");
    let requests = server.finish();
    requests[0].assert_bearer_api("rejected-access");
    assert_refresh(&requests[1], "stored-refresh");
    requests[2].assert_bearer_api("accepted-access");
}

#[tokio::test]
async fn repeated_api_401_is_bounded_and_clears_authentication_for_reauthorization() {
    let directory = TempDirectory::new();
    directory.seed_token("rejected-access", Some("stored-refresh"), now() + 7200);
    let server = MockServer::new(vec![
        MockResponse::unauthorized(),
        MockResponse::token("also-rejected-access", Some("rotated-refresh")),
        MockResponse::unauthorized(),
    ]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    let authorization = authorization_required(
        connection
            .update()
            .await
            .expect_err("second 401 requires authorization"),
    );
    assert_cleared(&connection, &directory);
    assert_eq!(authorization, connection.auth_url());
    assert_eq!(
        authorization,
        authorization_required(connection.update().await.expect_err("no unbounded retry"))
    );
    let requests = server.finish();
    requests[0].assert_bearer_api("rejected-access");
    assert_refresh(&requests[1], "stored-refresh");
    requests[2].assert_bearer_api("also-rejected-access");
}

#[tokio::test]
async fn api_401_with_invalid_refresh_grant_requires_authorization_without_api_retry() {
    let directory = TempDirectory::new();
    directory.seed_token("rejected-access", Some("invalid-refresh"), now() + 7200);
    let server = MockServer::new(vec![MockResponse::unauthorized(), MockResponse::invalid_grant()]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    authorization_required(connection.update().await.expect_err("invalid forced refresh"));
    assert_cleared(&connection, &directory);
    let requests = server.finish();
    requests[0].assert_bearer_api("rejected-access");
    assert_refresh(&requests[1], "invalid-refresh");
}

#[tokio::test]
async fn api_401_with_transient_refresh_failure_retains_authorization_for_retry() {
    let directory = TempDirectory::new();
    directory.seed_token("rejected-access", Some("stored-refresh"), now() + 7200);
    let server = MockServer::new(vec![MockResponse::unauthorized(), MockResponse::unavailable()]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    let saved_before = fs::read(directory.authentication_file()).expect("saved state");
    assert!(matches!(
        connection.update().await,
        Err(MqttBridgeError::NetworkError(_))
    ));
    assert!(connection.pending_authorization.is_none());
    assert_eq!(
        connection
            .authentication_data
            .token
            .as_ref()
            .expect("token retained")
            .refresh_token
            .as_deref(),
        Some("stored-refresh")
    );
    assert_eq!(
        fs::read(directory.authentication_file()).expect("saved state retained"),
        saved_before
    );
    let requests = server.finish();
    requests[0].assert_bearer_api("rejected-access");
    assert_refresh(&requests[1], "stored-refresh");
}

#[tokio::test]
async fn api_401_without_a_refresh_token_requires_authorization_without_retry() {
    let directory = TempDirectory::new();
    directory.seed_token("rejected-access", None, now() + 7200);
    let server = MockServer::new(vec![MockResponse::unauthorized()]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    authorization_required(connection.update().await.expect_err("401 without refresh token"));
    assert_cleared(&connection, &directory);
    server.finish()[0].assert_bearer_api("rejected-access");
}

#[tokio::test]
async fn revocation_posts_both_token_types_and_removes_all_local_authorization() {
    let directory = TempDirectory::new();
    directory.seed_token("stored-access", Some("stored-refresh"), now() + 7200);
    let server = MockServer::new(vec![MockResponse::success(), MockResponse::success()]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    connection.auth_url();
    connection.revoke_authorization().await.expect("revoke both tokens");
    assert_cleared(&connection, &directory);
    assert!(connection.pending_authorization.is_none());
    assert_revocations(&server.finish());
    let restarted = self::connection(&directory);
    assert!(restarted.authentication_data.token.is_none());
}

#[tokio::test]
async fn revocation_failures_attempt_both_tokens_and_preserve_local_state_for_retry() {
    for (access_status, access_body, refresh_status, refresh_body) in [
        (503, "", 200, ""),
        (200, "", 503, ""),
        (503, "", 503, ""),
        (200, r#"{"errorCode":17}"#, 200, r#"{"errorCode":0}"#),
        (200, r#"{"errorCode":0}"#, 200, r#"{"errorCode":17}"#),
    ] {
        for expires_at in [now() + 7200, now().saturating_sub(60)] {
            let directory = TempDirectory::new();
            directory.seed_token("stored-access", Some("stored-refresh"), expires_at);
            let mut connection = connection(&directory);
            let saved_before = directory.saved();
            assert_eq!(saved_before["revocation_pending"], false);
            let expected_token = saved_before["token"].clone();
            let observed_token = expected_token.clone();
            let authentication_file = directory.authentication_file();
            let server = MockServer::new_with_observer(
                vec![
                    MockResponse::json(access_status, access_body),
                    MockResponse::json(refresh_status, refresh_body),
                    MockResponse::success(),
                    MockResponse::success(),
                ],
                move |request| {
                    assert_eq!(request.path, "/revoke");
                    let saved: serde_json::Value = serde_json::from_slice(
                        &fs::read(&authentication_file).expect("revocation intent saved before network"),
                    )
                    .expect("persisted revocation intent is valid JSON");
                    assert_eq!(saved["revocation_pending"], true);
                    assert_eq!(saved["token"], observed_token);
                },
            );
            use_mock(&mut connection, &server);
            let authorization = connection.auth_url();
            assert!(matches!(
                connection.revoke_authorization().await,
                Err(MqttBridgeError::NetworkError(_))
            ));
            assert_revocation_pending(&connection, &directory, &expected_token);
            for _ in 0..2 {
                assert!(matches!(
                    connection.update().await,
                    Err(MqttBridgeError::RuntimeError(_))
                ));
                assert_revocation_pending(&connection, &directory, &expected_token);
                assert_eq!(connection.auth_url(), authorization);
            }
            drop(connection);
            let mut restarted = self::connection(&directory);
            use_mock(&mut restarted, &server);
            assert_revocation_pending(&restarted, &directory, &expected_token);
            for _ in 0..2 {
                assert!(matches!(
                    restarted.update().await,
                    Err(MqttBridgeError::RuntimeError(_))
                ));
                assert_revocation_pending(&restarted, &directory, &expected_token);
                assert!(restarted.pending_authorization.is_none());
            }
            restarted
                .revoke_authorization()
                .await
                .expect("retry both revocations after restart");
            assert_cleared(&restarted, &directory);
            assert!(restarted.pending_authorization.is_none());
            authorization_required(
                restarted
                    .update()
                    .await
                    .expect_err("explicit retry permits new authorization"),
            );
            let requests = server.finish();
            assert_revocations(&requests[..2]);
            assert_revocations(&requests[2..]);
        }
    }
}

#[tokio::test]
async fn revocation_accepts_numeric_zero_error_code_envelopes_for_both_tokens() {
    let directory = TempDirectory::new();
    directory.seed_token("stored-access", Some("stored-refresh"), now() + 7200);
    let server = MockServer::new(vec![
        MockResponse::json(200, r#"{"errorCode":0}"#),
        MockResponse::json(200, r#"{"errorCode":0,"errorMessage":"","details":null}"#),
    ]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    connection
        .revoke_authorization()
        .await
        .expect("numeric zero means revocation succeeded");
    assert_cleared(&connection, &directory);
    assert_revocations(&server.finish());
}

#[tokio::test]
async fn invalid_revocation_success_envelopes_fail_both_attempts_and_retain_retry_credentials() {
    for body in [
        r#"{"errorCode":1}"#,
        r#"{"errorCode":-1}"#,
        r#"{"errorCode":"0"}"#,
        r#"{"errorCode":false}"#,
        r#"{"errorCode":null}"#,
        r#"{"errorCode":0.0}"#,
        r#"{"success":true}"#,
        "{}",
        "null",
        "[]",
        r#"{"errorCode":0"#,
        "not-json",
        " ",
    ] {
        let directory = TempDirectory::new();
        directory.seed_token("stored-access", Some("stored-refresh"), now() + 7200);
        let server = MockServer::new(vec![
            MockResponse::json(200, body),
            MockResponse::json(200, body),
            MockResponse::json(200, r#"{"errorCode":0}"#),
            MockResponse::success(),
        ]);
        let mut connection = connection(&directory);
        use_mock(&mut connection, &server);
        let expected_token = directory.saved()["token"].clone();
        assert!(
            matches!(
                connection.revoke_authorization().await,
                Err(MqttBridgeError::NetworkError(_))
            ),
            "accepted invalid revocation envelope: {body:?}"
        );
        assert_revocation_pending(&connection, &directory, &expected_token);
        assert!(matches!(
            connection.update().await,
            Err(MqttBridgeError::RuntimeError(_))
        ));
        assert_revocation_pending(&connection, &directory, &expected_token);
        connection
            .revoke_authorization()
            .await
            .expect("retry with valid revocation responses");
        assert_cleared(&connection, &directory);
        let requests = server.finish();
        assert_revocations(&requests[..2]);
        assert_revocations(&requests[2..]);
    }
}

#[tokio::test]
async fn authentication_file_deletion_failure_is_retried_before_authorization_without_network_io() {
    let directory = TempDirectory::new();
    directory.seed_token("stored-access", Some("stored-refresh"), now() + 7200);
    let server = MockServer::new(vec![]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    let previous_authorization = connection.auth_url();
    fs::remove_file(directory.authentication_file()).expect("remove token file before deterministic blocker");
    fs::create_dir(directory.authentication_file()).expect("block file deletion with a directory");
    assert!(matches!(
        connection.clear_authentication_data(),
        Err(MqttBridgeError::RuntimeError(_))
    ));
    assert!(connection.authentication_data.token.is_none());
    assert!(!connection.authentication_data.revocation_pending);
    assert!(connection.pending_code.is_none());
    assert!(connection.pending_authorization.is_none());
    assert!(!connection.persistence_pending);
    assert!(connection.deletion_pending);
    for _ in 0..2 {
        assert!(matches!(
            connection.update().await,
            Err(MqttBridgeError::RuntimeError(_))
        ));
        assert!(connection.deletion_pending);
        assert!(connection.pending_authorization.is_none());
        assert!(directory.authentication_file().is_dir());
    }
    fs::remove_dir(directory.authentication_file()).expect("remove deterministic deletion blocker");
    let next = authorization_required(
        connection
            .update()
            .await
            .expect_err("fresh authorization after cleanup"),
    );
    assert_cleared(&connection, &directory);
    assert_ne!(query(&next)["state"], query(&previous_authorization)["state"]);
    assert_eq!(
        next,
        authorization_required(connection.update().await.expect_err("authorization remains cached"))
    );
    server.finish();
}

#[tokio::test]
async fn refresh_persistence_failure_retries_the_write_without_refreshing_rotated_tokens_again() {
    let directory = TempDirectory::new();
    directory.seed_token("old-access", Some("old-refresh"), now().saturating_sub(60));
    let server = MockServer::new(vec![
        MockResponse::token("rotated-access", Some("rotated-refresh")),
        MockResponse::devices(),
    ]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    fs::remove_file(directory.authentication_file()).expect("remove token file before deterministic blocker");
    fs::create_dir(directory.authentication_file()).expect("block persistence rename with a directory");
    let before = now();
    assert!(matches!(
        connection.update().await,
        Err(MqttBridgeError::RuntimeError(_))
    ));
    let after = now();
    let retained = connection
        .authentication_data
        .token
        .as_ref()
        .expect("rotated token retained in memory");
    assert_eq!(retained.access_token, "rotated-access");
    assert_eq!(retained.refresh_token.as_deref(), Some("rotated-refresh"));
    assert!((before + 3600..=after + 3600).contains(&retained.expires_at));
    let expected_token = serde_json::to_value(retained).expect("serialize rotated token");
    assert!(connection.persistence_pending);
    assert!(!connection.deletion_pending);
    assert!(!connection.authentication_data.revocation_pending);
    assert!(directory.authentication_file().is_dir());
    assert!(matches!(
        connection.update().await,
        Err(MqttBridgeError::RuntimeError(_))
    ));
    assert!(connection.persistence_pending);
    assert_eq!(
        fs::read_dir(directory.path())
            .expect("list authentication directory")
            .count(),
        1
    );
    fs::remove_dir(directory.authentication_file()).expect("remove deterministic persistence blocker");
    assert_eq!(
        connection
            .update()
            .await
            .expect("persist retained rotation and fetch devices"),
        DEVICES
    );
    assert!(!connection.persistence_pending);
    assert_eq!(directory.saved()["token"], expected_token);
    let restarted = self::connection(&directory);
    assert_eq!(
        serde_json::to_value(
            restarted
                .authentication_data
                .token
                .as_ref()
                .expect("restored rotated token")
        )
        .expect("serialize restored token"),
        expected_token
    );
    let requests = server.finish();
    assert_refresh(&requests[0], "old-refresh");
    requests[1].assert_bearer_api("rotated-access");
}

#[tokio::test]
async fn code_exchange_persistence_failure_retries_the_write_without_reusing_the_code() {
    let directory = TempDirectory::new();
    let server = MockServer::new(vec![
        MockResponse::token("code-access", Some("code-refresh")),
        MockResponse::devices(),
    ]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    let authorization = query(&connection.auth_url());
    connection
        .set_authorization_code(callback(
            REDIRECT_URI,
            &[("code", "single-use-code"), ("state", &authorization["state"])],
        ))
        .expect("accept callback before persistence failure");
    fs::create_dir(directory.authentication_file()).expect("block persistence rename with a directory");
    assert!(matches!(
        connection.update().await,
        Err(MqttBridgeError::RuntimeError(_))
    ));
    assert!(connection.pending_code.is_none());
    assert!(connection.pending_authorization.is_none());
    assert!(connection.persistence_pending);
    let retained = connection
        .authentication_data
        .token
        .as_ref()
        .expect("exchanged token retained in memory");
    assert_eq!(retained.access_token, "code-access");
    assert_eq!(retained.refresh_token.as_deref(), Some("code-refresh"));
    let expected_token = serde_json::to_value(retained).expect("serialize exchanged token");
    assert!(matches!(
        connection.update().await,
        Err(MqttBridgeError::RuntimeError(_))
    ));
    assert!(connection.persistence_pending);
    fs::remove_dir(directory.authentication_file()).expect("remove deterministic persistence blocker");
    assert_eq!(
        connection
            .update()
            .await
            .expect("persist exchanged token and fetch devices"),
        DEVICES
    );
    assert!(!connection.persistence_pending);
    assert!(connection.pending_code.is_none());
    assert_eq!(directory.saved()["token"], expected_token);
    let requests = server.finish();
    let form = requests[0].assert_client_form("/token");
    assert_eq!(form["grant_type"], "authorization_code");
    assert_eq!(form["code"], "single-use-code");
    requests[1].assert_bearer_api("code-access");
}

#[tokio::test]
async fn revocation_intent_persistence_failure_blocks_network_until_the_write_can_be_retried() {
    let directory = TempDirectory::new();
    directory.seed_token("stored-access", Some("stored-refresh"), now() + 7200);
    let server = MockServer::new(vec![MockResponse::success(), MockResponse::success()]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    let expected_token = directory.saved()["token"].clone();
    fs::remove_file(directory.authentication_file()).expect("remove token file before deterministic blocker");
    fs::create_dir(directory.authentication_file()).expect("block revocation intent persistence with a directory");
    assert!(matches!(
        connection.revoke_authorization().await,
        Err(MqttBridgeError::RuntimeError(_))
    ));
    assert!(connection.authentication_data.revocation_pending);
    assert!(connection.persistence_pending);
    assert!(!connection.deletion_pending);
    assert!(matches!(
        connection.update().await,
        Err(MqttBridgeError::RuntimeError(_))
    ));
    assert!(connection.persistence_pending);
    fs::remove_dir(directory.authentication_file()).expect("remove deterministic persistence blocker");
    assert!(matches!(
        connection.update().await,
        Err(MqttBridgeError::RuntimeError(_))
    ));
    assert_revocation_pending(&connection, &directory, &expected_token);
    connection
        .revoke_authorization()
        .await
        .expect("explicit retry after intent is persisted");
    assert_cleared(&connection, &directory);
    assert_revocations(&server.finish());
}

#[tokio::test]
async fn revoke_without_tokens_discards_pending_code_without_network_io() {
    let directory = TempDirectory::new();
    let server = MockServer::new(vec![]);
    let mut connection = connection(&directory);
    use_mock(&mut connection, &server);
    let authorization = query(&connection.auth_url());
    connection
        .set_authorization_code(callback(
            REDIRECT_URI,
            &[("code", "unused-code"), ("state", &authorization["state"])],
        ))
        .expect("accept pending code");
    connection
        .revoke_authorization()
        .await
        .expect("clear pending authorization");
    assert_cleared(&connection, &directory);
    assert!(connection.pending_authorization.is_none());
    let next = authorization_required(connection.update().await.expect_err("new authorization needed"));
    assert_ne!(query(&next)["state"], authorization["state"]);
    server.finish();
}
