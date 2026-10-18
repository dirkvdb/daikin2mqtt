# ONECTA authentication compliance

The authentication implementation is checked against `authentication.txt` and the bearer authentication/scopes in `apispec.json`. Protocol handling uses `oauth2` **5**; HTTP transport uses `reqwest` **0.13**. A small adapter bridges those libraries without adding reqwest 0.12 and accepts ONECTA's documented token responses that omit `token_type` (treated as Bearer).

## Configuration

Configure your own application credentials from the Daikin developer portal. The previously embedded client credentials are no longer used.

| CLI option | Environment variable | Default |
| --- | --- | --- |
| `--daikin-redirect-uri` | `DAIKIN_REDIRECT_URI` | `http://localhost:8076` |
| `--web-listen` | `DAIKIN_WEB_LISTEN` | `0.0.0.0:8076` |
| `--data-dir` | `DAIKIN_DATA_DIR` | `./data` |

`DAIKIN_CLIENT_ID` is the **Daikin OAuth** client ID and is resolved through SecretSpec. The non-secret **MQTT** client ID uses `--mqtt-client-id` / `DAIKIN_MQTT_CLIENT_ID` (default: `daikin2mqtt`).

Configure `--daikin-redirect-uri` / `DAIKIN_REDIRECT_URI` with the browser-reachable **base URL**, for example `https://bridge.example.com` (an optional trailing slash is accepted). The application appends `/oauth/callback` automatically. The base must use HTTP or HTTPS and contain no credentials, query string, fragment, or path prefix. Invalid values are rejected during argument parsing, before secrets are resolved or the bridge starts. Previously configured full `/oauth/callback` URLs remain accepted for compatibility.

Register the resulting **full callback URL** with Daikin, for example `https://bridge.example.com/oauth/callback`. That URL must exactly match the registered URI, including scheme, host, port, and path; a trailing slash after `/oauth/callback` is not accepted. The derived callback is shown in the UI and used consistently for authorization, code exchange, and callback validation. The provider adds `code` and `state` automatically.

The default HTTP localhost callback works only if the provider accepts it for your registered application. Otherwise, configure a registered HTTPS callback behind a reverse proxy, for example `--daikin-redirect-uri https://bridge.example.com`, and forward `/oauth/callback` (and the UI/assets/API routes) to the bridge. The redirect URI must be reachable from the browser used for consent; `localhost` refers to that browser's machine, not a remote bridge or container.

### Web access and deployment

The embedded web server listens on all IPv4 interfaces (`0.0.0.0:8076`) by default. Open `http://localhost:8076` on the bridge host, or use the configured browser-reachable base URL for remote access. To restrict the listener to the local machine, set `--web-listen 127.0.0.1:8076` (or `DAIKIN_WEB_LISTEN=127.0.0.1:8076`). The UI is shadcn-inspired static content embedded in the binary; it requires neither Node.js nor a CDN.

For LAN or Docker access, the default listen address is sufficient; explicitly configure a browser-reachable base URL, such as `--daikin-redirect-uri https://bridge.example.com`; register `https://bridge.example.com/oauth/callback` with Daikin. The listen address controls the socket binding; it does not change the OAuth redirect URI or enable TLS. Terminate HTTPS at a reverse proxy.

The Docker image declares `EXPOSE 8076`, but this does not publish the port. The default `0.0.0.0:8076` listener is suitable inside the container; publish the port, for example with `-p 127.0.0.1:8076:8076` for a reverse proxy on the Docker host. If publishing to a LAN interface, restrict access to trusted clients.

**Security:** Exposing the web UI grants administrative access to authorization and revocation. The bridge has no web login. Restrict it to a trusted network or protect it with reverse-proxy authentication and TLS. Never expose it publicly without protection. The server checks request hosts and CSRF tokens on administrative actions; reverse proxies must preserve the browser's `Host` header (matching the configured callback host). Responses disable caching and referrer forwarding, and enforce a self-only content security policy.

### Health check

`GET /api/health` on the same web server returns plain-text `ok` with HTTP **200** only when both the Daikin cloud connection and MQTT broker connection are established. Otherwise it returns plain-text `unavailable` with HTTP **503**, including during startup before either connection is established.

Cloud health reflects the most recent cloud device poll or command result; failed API requests or authorization revocation mark it unhealthy until a successful API request. MQTT health follows broker connection acknowledgments, disconnects, and connection errors. Health checks use cached connection state without making additional cloud requests or waiting for an in-progress cloud request, so they do not consume Daikin API quota. Outages are reflected when the normal cloud polling or MQTT event loop detects them.

For a local probe using the default port:

```sh
curl --fail http://127.0.0.1:8076/api/health
```

The existing host validation also applies to this endpoint; use the configured callback host or an accepted local listen address.

### Secrets via SecretSpec

The application uses the SecretSpec Rust SDK to resolve credentials from the declarations in `secretspec.toml` before starting the bridge. The manifest is embedded at build time, so installed binaries and Docker containers do not need a runtime copy. Changing the declarations requires rebuilding the application; secret values are never embedded.

| SecretSpec name | Purpose | Requirement |
| --- | --- | --- |
| `DAIKIN_CLIENT_ID` | Daikin OAuth client ID | Required |
| `DAIKIN_CLIENT_SECRET` | Daikin OAuth client secret | Required |
| `MQTT_USERNAME` | MQTT broker username | Required |
| `MQTT_PASSWORD` | MQTT broker password | Required |

The application always selects the sole `default` profile declared in the embedded manifest, ignoring ambient profile settings (including `SECRETSPEC_PROFILE`). Provider selection still uses SecretSpec's configuration and the `SECRETSPEC_PROVIDER` environment variable. This build enables the keyring provider as well as the SDK's always-available providers, including environment variables and dotenv. Optional cloud-provider features are not enabled. All four credentials are validated non-interactively; a missing credential causes startup to fail rather than prompting or connecting anonymously to MQTT. The application resolves the entire profile regardless of `SECRETSPEC_SCOPE`.

For a deployment that injects secrets as environment variables, set `SECRETSPEC_PROVIDER=env` and supply the four names above through your deployment's secret manager. Non-secret options remain CLI/environment settings:

```sh
SECRETSPEC_PROVIDER=env cargo run --locked -- --mqtt-addr localhost
```

For a private dotenv file, set `SECRETSPEC_PROVIDER=dotenv:/absolute/path/to/secrets.env`. Protect the file with restrictive permissions and never commit it. An SDK default audit reason identifies bridge initialization; `SECRETSPEC_REASON` can supply a more specific reason.

**Migration:** `--daikin-client-id`, `--daikin-client-secret`, `--mqtt-user`, and `--mqtt-pass` are no longer accepted. Replace the old `DAIKIN_OAUTH_CLIENT_ID`, `DAIKIN_OAUTH_CLIENT_SECRET`, `DAIKIN_MQTT_USER`, and `DAIKIN_MQTT_PASS` variables with `DAIKIN_CLIENT_ID`, `DAIKIN_CLIENT_SECRET`, `MQTT_USERNAME`, and `MQTT_PASSWORD`, respectively, when using the environment provider. If you previously used `DAIKIN_CLIENT_ID` for the MQTT client ID, move that value to `DAIKIN_MQTT_CLIENT_ID`; `DAIKIN_CLIENT_ID` now identifies the Daikin OAuth application. All four secrets are required, matching the manifest. Secrets are not parsed by Clap or displayed in CLI help.

The OAuth client ID is managed as a configuration secret, but OAuth requires it in the browser's authorization URL as `client_id`. It is therefore visible to the user initiating authentication and to Daikin; unlike the client secret, it cannot be kept confidential throughout this protocol.

## Authorizing the integration

1. Open `http://localhost:8076` on the bridge host, or the protected browser-reachable URL configured for your deployment.
2. Click **Connect**.
3. Log in and grant consent using Daikin's identity provider.
4. The provider redirects the browser to `/oauth/callback`; the bridge validates the callback and exchanges the code automatically. No copying or MQTT command is needed.

The existing OAuth scopes (`openid`, `onecta:basic.integration`, and `offline_access`), random state validation, and PKCE (`S256`) protection are retained. Duplicate `code`/`state`, missing or mismatched state, OAuth error responses, and replayed callbacks are rejected without exposing the payload in logs. State and the PKCE verifier remain in memory; after restarting the bridge, click **Connect** again rather than reusing an older callback.

After a successful token exchange, the single-use code is discarded. Token refresh is automatic after this initial login; repeated consent is not required while the session remains valid. Tokens, codes, client secrets, and PKCE verifiers are not logged.

No authentication topics are generated or accepted over MQTT. Device state continues to be published on MQTT; authorization and revocation use the web UI.

## Revoking the integration

Use the **Disconnect** button in the web UI and confirm revocation.

The bridge sends separate authenticated requests to `/v1/oidc/revoke` for the access token and refresh token, with their corresponding `token_type_hint`. Both are attempted even if one fails. It accepts an empty HTTP 200 response as specified by RFC 7009, or ONECTA's documented JSON envelope with numeric `errorCode: 0`. A malformed envelope or nonzero error code is not treated as success.

Revocation intent is saved before contacting the provider. On failure, normal API polling and token refresh are suspended, including after a restart; the credentials remain available only to retry revocation. Use the web UI's revocation retry button after correcting connectivity/provider errors. Local credentials are removed only once revocation succeeds. Click **Connect** to authorize again; accessing the cloud then requires fresh user consent.

## Requirements checked

| Documentation requirement | Implementation |
| --- | --- |
| Authorization-code flow at the documented identity-provider endpoints | `oauth2` authorization/code-exchange APIs; `response_type=code` |
| Required `openid` and `onecta:basic.integration` scopes | Both requested; `offline_access` also requested for continuous bridge operation |
| Same registered redirect URI in authorize and token requests | One configurable URI used for both; callback destination also checked |
| Code exchange with client ID, secret, code, and redirect URI | Credentials sent in the POST form body, not the URL query |
| Bearer header on cloud API requests | `Authorization: Bearer …` |
| Hour-long access-token expiry | Absolute expiry derived from provider `expires_in`; refresh within 30 seconds of expiry |
| Refresh with the latest refresh token; token rotation | Latest returned token saved immediately and used for the next refresh |
| Revoked/expired session requires reauthorization | `invalid_grant` clears invalid credentials; API 401 triggers at most one refresh/retry before reauthorization |
| Users can revoke both tokens from the application | Web UI revocation/retry buttons and both-token revocation requests |
| Limit token exposure | No credential logging; private, atomic token-file replacement and synchronized file/directory writes on Unix |
| Optional CSRF state protection | Random per-flow state; timing-resistant comparison using `oauth2` |
| Optional PKCE | Random verifier and SHA-256 (`S256`) challenge generated by `oauth2` |

Transient network/provider failures preserve credentials for retry rather than panic or force unnecessary consent. If writing a rotated token fails, it remains in memory and persistence is retried before further cloud requests. Failed credential-file deletion is also retried before proceeding.

Token introspection and JWKS validation are optional mechanisms in the supplied docs. This bridge obtains tokens directly over verified HTTPS and uses them to call the protected API; it does not use access/ID token claims as local proof of a user's identity. It does not perform introspection or local JWT validation.

## Storage and migration

`<data-dir>/openid_authentication.json` remains the filename for compatibility, despite the library change. Tokens are stored with an absolute `expires_at`; the old library's saved absolute `expires_in` timestamp is migrated without extending the token's lifetime, and saved authorization codes are removed. Existing unreadable/corrupt files produce an explicit startup error instead of silently discarding authentication state.

On Unix, token files are created with mode `0600` and replaced atomically. Tokens are **not encrypted at rest**. The docs recommend considering encryption for continuous access, rather than mandating it for this non-mobile bridge. Use a private data directory on a protected/encrypted volume, restrict backups, and never commit token files. Non-Unix deployments must restrict file access using their platform's ACLs.

Rotate the client secret that was previously hardcoded in this project: removing it from the source does not remove it from Git history or previous binaries.

**MQTT authentication migration:** Remove old retained `<base-topic>/authorization_required` URLs and `<base-topic>/cmnd/authorization_code` commands from the broker (default base topic: `daikin`). Remove any retained `<base-topic>/cmnd/revoke_authorization` commands as well. Stop publishing authentication commands or relying on authorization-topic subscriptions: these topics are no longer generated or accepted. Use the web UI instead.

The current MQTT transport is plain TCP, not TLS. Use a protected local connection or encrypted tunnel and broker authentication/topic ACLs. Merely changing the broker port to 8883 does not enable TLS.

`reqwest` 0.13 uses platform trust roots with the configured Rustls backend. The Docker runtime installs `ca-certificates`; other deployments also need a usable trust store.

## Verification

`src/daikincloudconnection/tests.rs` uses local mock endpoints to exercise authorization, redirect/state/PKCE validation, token exchange and Bearer usage, expiry/rotation/restart behavior, legacy migration, network and persistence failure recovery, bounded 401 retries, and both-token revocation including partial failures and provider error envelopes. CLI/SDK tests verify SecretSpec resolution of the required OAuth client ID, rejection of the old client-ID CLI option, distinct MQTT client-ID environment configuration, redirect-base normalization and compatibility with full callback URLs, rejection of invalid bases before startup, redirect and web-listen defaults, custom listen parsing and bridge mapping, invalid listen addresses, environment declarations, absence of secret CLI options, embedded-manifest loading through an isolated dotenv provider, credential-to-bridge mapping, and missing-secret failures without value disclosure. The SDK tests use the application's profile selection rather than overriding it; running them with `SECRETSPEC_PROFILE=development` verifies that an ambient unsupported profile cannot break startup.

`src/web.rs` tests verify embedded UI delivery, no-cache/security headers, CSRF and host/origin checks, default-port normalization, and safe redirects for invalid callbacks. An end-to-end mock-provider test in `src/daikincloudconnection/tests.rs` exercises web login initiation, automatic callback/code exchange with PKCE, private token persistence, callback replay rejection without extra cloud polling, and both-token revocation without MQTT.

These tests verify the implementation against the supplied documentation without contacting a real Daikin account. Live authorization, the registered redirect URI, application scopes, and account-specific provider behavior still require deployment validation.

```sh
cargo test --locked --offline
```
