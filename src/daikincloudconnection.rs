use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use oauth2::{
    basic::{BasicClient, BasicErrorResponseType, BasicTokenResponse, BasicTokenType},
    AccessToken, AuthType, AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointNotSet, EndpointSet, HttpClientError, HttpRequest,
    HttpResponse, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, RefreshToken, RequestTokenError, RevocationUrl, Scope, StandardRevocableToken,
    TokenResponse, TokenUrl,
};
use reqwest::{StatusCode, Url};

use crate::MqttBridgeError;

const API: &str = "https://api.onecta.daikineurope.com/v1/";
const AUTH_URI: &str = "https://idp.onecta.daikineurope.com/v1/oidc/authorize";
const TOKEN_URI: &str = "https://idp.onecta.daikineurope.com/v1/oidc/token";
const REVOKE_URI: &str = "https://idp.onecta.daikineurope.com/v1/oidc/revoke";

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredToken {
    access_token: String,
    refresh_token: Option<String>,
    // Absolute expiry avoids treating a saved timestamp as a fresh token lifetime on restart.
    #[serde(default, alias = "expires_in")]
    expires_at: u64,
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct AuthenticationData {
    token: Option<StoredToken>,
    #[serde(default)]
    revocation_pending: bool,
}

struct PendingAuthorization {
    url: Url,
    state: CsrfToken,
    verifier: PkceCodeVerifier,
}

struct PendingCode {
    code: String,
    verifier: PkceCodeVerifier,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuthenticationState {
    Unauthorized,
    Authorizing,
    Authorized,
    RevocationPending,
}

pub struct DaikinCloudConnection {
    http_client: reqwest::Client,
    oauth_client: BasicClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointSet, EndpointSet>,
    redirect_uri: String,
    api: String,
    data_dir: PathBuf,
    authentication_data: AuthenticationData,
    pending_authorization: Option<PendingAuthorization>,
    pending_code: Option<PendingCode>,
    persistence_pending: bool,
    deletion_pending: bool,
    connected: Arc<AtomicBool>,
}

impl DaikinCloudConnection {
    #[cfg(test)]
    pub(crate) fn set_test_api_endpoint(&mut self, api: String) {
        self.api = api;
    }

    pub fn new(client_id: String, client_secret: String, redirect_uri: String, data_dir: &Path) -> Result<Self, MqttBridgeError> {
        if client_id.is_empty() || client_secret.is_empty() {
            return Err(MqttBridgeError::RuntimeError("OAuth client credentials must not be empty".into()));
        }
        let redirect = RedirectUrl::new(redirect_uri.clone()).map_err(|_| MqttBridgeError::RuntimeError("Invalid OAuth redirect URI".into()))?;
        if redirect.url().query().is_some() || redirect.url().fragment().is_some() {
            return Err(MqttBridgeError::RuntimeError("OAuth redirect URI must not contain a query or fragment".into()));
        }
        let oauth_client = BasicClient::new(ClientId::new(client_id))
            .set_client_secret(ClientSecret::new(client_secret))
            .set_auth_uri(AuthUrl::new(AUTH_URI.into()).map_err(|_| MqttBridgeError::ParseError)?)
            .set_token_uri(TokenUrl::new(TOKEN_URI.into()).map_err(|_| MqttBridgeError::ParseError)?)
            .set_auth_type(AuthType::RequestBody)
            .set_redirect_uri(redirect)
            .set_revocation_url(RevocationUrl::new(REVOKE_URI.into()).map_err(|_| MqttBridgeError::ParseError)?);
        let http_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| MqttBridgeError::NetworkError("Failed to create HTTP client".into()))?;
        let authentication_data = Self::restore_authentication_data(data_dir)?;
        let mut connection = Self {
            http_client,
            oauth_client,
            redirect_uri,
            api: API.into(),
            data_dir: data_dir.to_path_buf(),
            authentication_data,
            pending_authorization: None,
            pending_code: None,
            persistence_pending: false,
            deletion_pending: false,
            connected: Arc::new(AtomicBool::new(false)),
        };
        // Migrate legacy files, remove saved single-use codes, and enforce private file permissions.
        if connection.authentication_data.token.is_some() {
            connection.store_authentication_data()?;
        }
        Ok(connection)
    }

    fn auth_url(&mut self) -> Url {
        let pending = self.pending_authorization.get_or_insert_with(|| {
            let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
            let (url, state) = self
                .oauth_client
                .authorize_url(CsrfToken::new_random)
                .add_scope(Scope::new("openid".into()))
                .add_scope(Scope::new("onecta:basic.integration".into()))
                .add_scope(Scope::new("offline_access".into()))
                .set_pkce_challenge(challenge)
                .url();
            PendingAuthorization { url, state, verifier }
        });
        pending.url.clone()
    }

    pub(crate) fn connection_status(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.connected)
    }

    pub(crate) fn authentication_state(&self) -> AuthenticationState {
        if self.authentication_data.revocation_pending {
            AuthenticationState::RevocationPending
        } else if self.authentication_data.token.is_some() {
            AuthenticationState::Authorized
        } else if self.pending_code.is_some() {
            AuthenticationState::Authorizing
        } else {
            AuthenticationState::Unauthorized
        }
    }

    pub(crate) fn begin_authorization(&mut self) -> Result<Url, MqttBridgeError> {
        if self.deletion_pending {
            self.remove_authentication_file()?;
        }
        if self.authentication_data.token.is_some() || self.authentication_data.revocation_pending {
            return Err(MqttBridgeError::RuntimeError(
                "Disconnect the existing integration before authorizing again".into(),
            ));
        }
        if self.pending_code.is_some() {
            return Err(MqttBridgeError::RuntimeError(
                "Authorization is already being completed; wait for the connection status to update".into(),
            ));
        }
        Ok(self.auth_url())
    }

    pub(crate) async fn exchange_pending_code(&mut self) -> Result<(), MqttBridgeError> {
        let Some(code) = self.pending_code.as_ref() else {
            return Err(MqttBridgeError::AuthorizationRequired(self.auth_url()));
        };
        let result = self
            .oauth_client
            .exchange_code(AuthorizationCode::new(code.code.clone()))
            .set_pkce_verifier(PkceCodeVerifier::new(code.verifier.secret().clone()))
            .request_async(&|request| self.oauth_http(request))
            .await;
        match result {
            Ok(token) => {
                self.pending_code = None;
                self.accept_token(token)
            }
            Err(RequestTokenError::ServerResponse(ref error)) if *error.error() == BasicErrorResponseType::InvalidGrant => Err(self.authorization_required()?),
            Err(_) => Err(MqttBridgeError::NetworkError(
                "Authorization code exchange failed; check OAuth credentials and provider availability".into(),
            )),
        }
    }

    pub fn set_authorization_code(&mut self, callback: String) -> Result<(), MqttBridgeError> {
        let invalid =
            || MqttBridgeError::RuntimeError("Invalid authorization callback: expected the registered redirect URL with code and matching state".into());
        let mut url = Url::parse(callback.trim()).map_err(|_| invalid())?;
        if url.fragment().is_some() {
            return Err(invalid());
        }
        let mut code = None;
        let mut state = None;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "code" if code.is_none() => code = Some(value.into_owned()),
                "state" if state.is_none() => state = Some(value.into_owned()),
                "code" | "state" | "error" => return Err(invalid()),
                _ => {}
            }
        }
        url.set_query(None);
        let redirect = Url::parse(&self.redirect_uri).map_err(|_| invalid())?;
        let pending = self.pending_authorization.as_ref().ok_or_else(invalid)?;
        let returned_state = CsrfToken::new(state.ok_or_else(invalid)?);
        if url != redirect || returned_state != pending.state {
            return Err(invalid());
        }
        let code = code.filter(|value| !value.is_empty()).ok_or_else(invalid)?;
        if self.authentication_data.token.is_some() {
            return Err(MqttBridgeError::RuntimeError("Revoke the existing integration before authorizing again".into()));
        }
        let pending = self.pending_authorization.take().ok_or_else(invalid)?;
        self.pending_code = Some(PendingCode {
            code,
            verifier: pending.verifier,
        });
        Ok(())
    }

    async fn prepare_authentication(&mut self) -> Result<(), MqttBridgeError> {
        if self.deletion_pending {
            self.remove_authentication_file()?;
        }
        if self.persistence_pending {
            self.store_authentication_data()?;
        }
        if self.authentication_data.revocation_pending {
            return Err(MqttBridgeError::RuntimeError(
                "Revocation pending; retry disconnect in the web UI before accessing the cloud".into(),
            ));
        }
        if self.authentication_data.token.is_none() {
            self.exchange_pending_code().await?;
        }
        if self
            .authentication_data
            .token
            .as_ref()
            .is_some_and(|token| token.expires_at <= now().saturating_add(30))
        {
            self.refresh_access_token().await?;
        }
        Ok(())
    }

    pub async fn update(&mut self) -> Result<String, MqttBridgeError> {
        let result = async {
            self.prepare_authentication().await?;
            let response = self.gateway_devices().await?;
            let response = if response.status() == StatusCode::UNAUTHORIZED {
                self.refresh_access_token().await?;
                self.gateway_devices().await?
            } else {
                response
            };
            if response.status() == StatusCode::UNAUTHORIZED {
                return Err(self.authorization_required()?);
            }
            if !response.status().is_success() {
                return Err(MqttBridgeError::NetworkError(format!("Gateway device request failed: {}", response.status())));
            }
            response
                .text()
                .await
                .map_err(|_| MqttBridgeError::NetworkError("Failed to read gateway devices".into()))
        }
        .await;
        self.connected.store(result.is_ok(), Ordering::Relaxed);
        result
    }

    pub(crate) async fn set_characteristic(
        &mut self,
        gateway_id: &str,
        management_point: &str,
        characteristic: &str,
        path: Option<&str>,
        value: &serde_json::Value,
    ) -> Result<(), MqttBridgeError> {
        let result = async {
            self.prepare_authentication().await?;
            // URL path builders normalize dot-only segments instead of encoding them.
            if [gateway_id, management_point, characteristic]
                .iter()
                .any(|segment| matches!(*segment, "." | ".."))
            {
                return Err(MqttBridgeError::RuntimeError(
                    "Characteristic endpoint must not contain dot-only path segments".into(),
                ));
            }
            let mut url = Url::parse(&self.api).map_err(|_| MqttBridgeError::RuntimeError("Invalid cloud API endpoint".into()))?;
            url.path_segments_mut()
                .map_err(|_| MqttBridgeError::RuntimeError("Invalid cloud API endpoint".into()))?
                .pop_if_empty()
                .extend([
                    "gateway-devices",
                    gateway_id,
                    "management-points",
                    management_point,
                    "characteristics",
                    characteristic,
                ]);
            let mut body = serde_json::json!({ "value": value });
            if let Some(path) = path {
                body["path"] = path.into();
            }
            let response = self.patch_characteristic(&url, &body).await?;
            let response = if response.status() == StatusCode::UNAUTHORIZED {
                self.refresh_access_token().await?;
                self.patch_characteristic(&url, &body).await?
            } else {
                response
            };
            if response.status() == StatusCode::UNAUTHORIZED {
                return Err(self.authorization_required()?);
            }
            if !response.status().is_success() {
                return Err(MqttBridgeError::NetworkError(format!("Characteristic update failed: {}", response.status())));
            }
            Ok(())
        }
        .await;
        self.connected.store(result.is_ok(), Ordering::Relaxed);
        result
    }

    async fn patch_characteristic(&self, url: &Url, body: &serde_json::Value) -> Result<reqwest::Response, MqttBridgeError> {
        let token = self.authentication_data.token.as_ref().ok_or(MqttBridgeError::NotAuhrorized)?;
        self.http_client
            .patch(url.clone())
            .header("Accept", "application/json")
            .bearer_auth(&token.access_token)
            .json(body)
            .send()
            .await
            .map_err(|_| MqttBridgeError::NetworkError("Characteristic update request failed".into()))
    }

    async fn refresh_access_token(&mut self) -> Result<(), MqttBridgeError> {
        let refresh = self.authentication_data.token.as_ref().and_then(|token| token.refresh_token.clone());
        let Some(refresh) = refresh.filter(|value| !value.is_empty()) else {
            return Err(self.authorization_required()?);
        };
        let refresh = RefreshToken::new(refresh);
        let result = self
            .oauth_client
            .exchange_refresh_token(&refresh)
            .request_async(&|request| self.oauth_http(request))
            .await;
        match result {
            Ok(mut token) => {
                // RFC 6749 permits omission of an unchanged refresh token.
                if token.refresh_token().is_none() {
                    token.set_refresh_token(Some(refresh));
                }
                self.accept_token(token)
            }
            Err(RequestTokenError::ServerResponse(ref error)) if *error.error() == BasicErrorResponseType::InvalidGrant => Err(self.authorization_required()?),
            Err(_) => Err(MqttBridgeError::NetworkError(
                "Token refresh failed; stored refresh token retained for retry".into(),
            )),
        }
    }

    fn accept_token(&mut self, token: BasicTokenResponse) -> Result<(), MqttBridgeError> {
        if token.token_type() != &BasicTokenType::Bearer || token.access_token().secret().is_empty() {
            return Err(MqttBridgeError::RuntimeError("Provider did not return a Bearer access token".into()));
        }
        let lifetime = token
            .expires_in()
            .ok_or_else(|| MqttBridgeError::RuntimeError("Provider omitted token expiry".into()))?;
        self.authentication_data.token = Some(StoredToken {
            access_token: token.access_token().secret().clone(),
            refresh_token: token.refresh_token().map(|value| value.secret().clone()),
            expires_at: now().saturating_add(lifetime.as_secs()),
        });
        self.persistence_pending = true;
        self.store_authentication_data()
    }

    async fn gateway_devices(&self) -> Result<reqwest::Response, MqttBridgeError> {
        let token = self.authentication_data.token.as_ref().ok_or(MqttBridgeError::NotAuhrorized)?;
        self.http_client
            .get(format!("{}gateway-devices", self.api))
            .header("Accept", "application/json")
            .bearer_auth(&token.access_token)
            .send()
            .await
            .map_err(|_| MqttBridgeError::NetworkError("Gateway device request failed".into()))
    }

    pub async fn revoke_authorization(&mut self) -> Result<(), MqttBridgeError> {
        self.connected.store(false, Ordering::Relaxed);
        if self.authentication_data.token.is_some() {
            // Persist intent before contacting the provider so restarts cannot resume a
            // partly revoked integration or rotate away credentials needed for retry.
            self.authentication_data.revocation_pending = true;
            self.persistence_pending = true;
            self.store_authentication_data()?;
        }
        let mut failed = false;
        if let Some(token) = self.authentication_data.token.as_ref() {
            let mut tokens = vec![StandardRevocableToken::AccessToken(AccessToken::new(token.access_token.clone()))];
            if let Some(refresh) = &token.refresh_token {
                tokens.push(StandardRevocableToken::RefreshToken(RefreshToken::new(refresh.clone())));
            }
            // Attempt both, even if the first fails; retain local tokens on failure to allow retry.
            for token in tokens {
                let request = self
                    .oauth_client
                    .revoke_token(token)
                    .map_err(|_| MqttBridgeError::RuntimeError("Invalid revocation endpoint".into()))?;
                if request.request_async(&|request| self.oauth_http(request)).await.is_err() {
                    failed = true;
                }
            }
        }
        if failed {
            return Err(MqttBridgeError::NetworkError("Token revocation failed; retry disconnect in the web UI".into()));
        }
        self.clear_authentication_data()
    }

    // oauth2 handles protocol requests and responses. This adapter reuses our bounded,
    // non-redirecting HTTP client and accepts ONECTA's documented omission of token_type.
    fn oauth_http(&self, request: HttpRequest) -> impl std::future::Future<Output = Result<HttpResponse, HttpClientError<reqwest::Error>>> + Send + 'static {
        // Own the cheap client handle so HTTP handlers can await this future without
        // borrowing the whole cloud connection through oauth2's higher-ranked adapter.
        let http_client = self.http_client.clone();
        async move {
            #[cfg(test)]
            let request = tests::loopback_oauth_request(request);
            let revocation = request.uri().path().ends_with("/revoke");
            let (parts, body) = request.into_parts();
            let response = http_client
                .request(parts.method, parts.uri.to_string())
                .headers(parts.headers)
                .body(body)
                .send()
                .await
                .map_err(|error| HttpClientError::Reqwest(Box::new(error)))?;
            let status_code = response.status();
            let headers = response.headers().clone();
            let mut body = response.bytes().await.map_err(|error| HttpClientError::Reqwest(Box::new(error)))?.to_vec();
            if status_code.is_success() && revocation && !body.is_empty() {
                let envelope: serde_json::Value = serde_json::from_slice(&body).map_err(|_| HttpClientError::Other("Invalid revocation response".into()))?;
                if envelope.get("errorCode").and_then(serde_json::Value::as_u64) != Some(0) {
                    return Err(HttpClientError::Other("Provider reported token revocation failure".into()));
                }
            }
            if status_code.is_success() && !revocation {
                if let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(&body) {
                    if let Some(object) = json.as_object_mut() {
                        if object.contains_key("access_token") && !object.contains_key("token_type") {
                            object.insert("token_type".into(), serde_json::Value::String("Bearer".into()));
                            if let Ok(normalized) = serde_json::to_vec(&json) {
                                body = normalized;
                            }
                        }
                    }
                }
            }
            let mut result = HttpResponse::new(body);
            *result.status_mut() = status_code;
            *result.headers_mut() = headers;
            Ok(result)
        }
    }

    fn authorization_required(&mut self) -> Result<MqttBridgeError, MqttBridgeError> {
        self.clear_authentication_data()?;
        Ok(MqttBridgeError::AuthorizationRequired(self.auth_url()))
    }

    fn authentication_data_path(base_dir: &Path) -> PathBuf {
        base_dir.join("openid_authentication.json")
    }

    fn clear_authentication_data(&mut self) -> Result<(), MqttBridgeError> {
        self.connected.store(false, Ordering::Relaxed);
        self.authentication_data = AuthenticationData::default();
        self.pending_code = None;
        self.pending_authorization = None;
        self.persistence_pending = false;
        self.deletion_pending = true;
        self.remove_authentication_file()
    }

    fn remove_authentication_file(&mut self) -> Result<(), MqttBridgeError> {
        match fs::remove_file(Self::authentication_data_path(&self.data_dir)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {
                return Err(MqttBridgeError::RuntimeError(
                    "Failed to remove stored authentication data; cleanup will be retried".into(),
                ))
            }
        }
        #[cfg(unix)]
        if self.data_dir.exists() {
            fs::File::open(&self.data_dir)
                .and_then(|directory| directory.sync_all())
                .map_err(|_| MqttBridgeError::RuntimeError("Failed to synchronize authentication cleanup".into()))?;
        }
        self.deletion_pending = false;
        Ok(())
    }

    fn store_authentication_data(&mut self) -> Result<(), MqttBridgeError> {
        fs::create_dir_all(&self.data_dir).map_err(|_| MqttBridgeError::RuntimeError("Failed to create authentication directory".into()))?;
        let temporary = self.data_dir.join(format!(".authentication-{}.tmp", CsrfToken::new_random().secret()));
        let result = (|| -> Result<(), MqttBridgeError> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            file.write_all(serde_json::to_string(&self.authentication_data)?.as_bytes())?;
            file.sync_all()?;
            fs::rename(&temporary, Self::authentication_data_path(&self.data_dir))?;
            #[cfg(unix)]
            fs::File::open(&self.data_dir)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
            return Err(MqttBridgeError::RuntimeError("Failed to securely persist authentication data".into()));
        }
        self.persistence_pending = false;
        Ok(())
    }

    fn restore_authentication_data(data_dir: &Path) -> Result<AuthenticationData, MqttBridgeError> {
        match fs::read_to_string(Self::authentication_data_path(data_dir)) {
            Ok(json) => serde_json::from_str(&json).map_err(|_| MqttBridgeError::RuntimeError("Failed to parse authentication file".into())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(AuthenticationData::default()),
            Err(_) => Err(MqttBridgeError::RuntimeError("Failed to read authentication file".into())),
        }
    }
}

#[cfg(test)]
mod tests;

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}
