# ONECTA authentication compliance

The authentication implementation is checked against `authentication.txt` and the bearer authentication/scopes in `apispec.json`. Protocol handling uses `oauth2` **5**; HTTP transport uses `reqwest` **0.13**. A small adapter bridges those libraries without adding reqwest 0.12 and accepts ONECTA's documented token responses that omit `token_type` (treated as Bearer).

## Configuration

Configure your own application credentials from the Daikin developer portal. The previously embedded client credentials are no longer used.

| CLI option | Environment variable | Default |
| --- | --- | --- |
| `--daikin-client-id` | `DAIKIN_OAUTH_CLIENT_ID` | Required |
| `--daikin-client-secret` | `DAIKIN_OAUTH_CLIENT_SECRET` | Required |
| `--daikin-redirect-uri` | `DAIKIN_REDIRECT_URI` | `onectaintegration://callback/` |
| `--data-dir` | `DAIKIN_DATA_DIR` | `./data` |

`DAIKIN_CLIENT_ID` continues to configure the **MQTT** client ID; it is not the OAuth client ID.

The redirect URI must exactly match the URI registered for your application, including its scheme, path, and trailing slash. The configured value is used in both authorization and code-exchange requests. Redirect URIs with a query string or fragment are rejected. The bridge does not host an HTTP callback server or install a custom-scheme handler: submit the final redirected URL manually over MQTT.

Prefer injecting secrets through your deployment's secret manager/environment instead of command-line arguments, shell history, or checked-in configuration. OAuth and MQTT secret environment values are hidden from CLI help.

## Authorizing the integration

1. Subscribe to `<base-topic>/authorization_required` (default: `daikin/authorization_required`).
2. Open the published authorization URL and log in/consent using Daikin's identity provider.
3. Copy the **full final callback URL**, including `code` and `state`.
4. Publish that URL as the payload of `<base-topic>/cmnd/authorization_code`, **without MQTT retain**.

Bare authorization codes are no longer accepted: the callback's state and redirect destination must be checked. Duplicate `code`/`state`, missing or mismatched state, OAuth error responses, fragments, and replayed callbacks are rejected without exposing the payload in logs. State and the PKCE verifier remain in memory; after restarting the bridge, use its newly published authorization URL rather than an older callback.

After a successful token exchange, the single-use code is discarded. The retained authorization URL is cleared. Tokens, codes, client secrets, and PKCE verifiers are not logged.

## Revoking the integration

Publish a message to `<base-topic>/cmnd/revoke_authorization`, **without retain**; its payload is ignored.

The bridge sends separate authenticated requests to `/v1/oidc/revoke` for the access token and refresh token, with their corresponding `token_type_hint`. Both are attempted even if one fails. It accepts an empty HTTP 200 response as specified by RFC 7009, or ONECTA's documented JSON envelope with numeric `errorCode: 0`. A malformed envelope or nonzero error code is not treated as success.

Revocation intent is saved before contacting the provider. On failure, normal API polling and token refresh are suspended, including after a restart; the credentials remain available only to retry revocation. Retry the same command after correcting connectivity/provider errors. Local credentials are removed only once revocation succeeds. A new authorization URL can then be published, but accessing the cloud again requires fresh user consent.

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
| Users can revoke both tokens from the application | MQTT revocation command and both-token revocation requests |
| Limit token exposure | No credential logging; private, atomic token-file replacement and synchronized file/directory writes on Unix |
| Optional CSRF state protection | Random per-flow state; timing-resistant comparison using `oauth2` |
| Optional PKCE | Random verifier and SHA-256 (`S256`) challenge generated by `oauth2` |

Transient network/provider failures preserve credentials for retry rather than panic or force unnecessary consent. If writing a rotated token fails, it remains in memory and persistence is retried before further cloud requests. Failed credential-file deletion is also retried before proceeding.

Token introspection and JWKS validation are optional mechanisms in the supplied docs. This bridge obtains tokens directly over verified HTTPS and uses them to call the protected API; it does not use access/ID token claims as local proof of a user's identity. It does not perform introspection or local JWT validation.

## Storage and migration

`<data-dir>/openid_authentication.json` remains the filename for compatibility, despite the library change. Tokens are stored with an absolute `expires_at`; the old library's saved absolute `expires_in` timestamp is migrated without extending the token's lifetime, and saved authorization codes are removed. Existing unreadable/corrupt files produce an explicit startup error instead of silently discarding authentication state.

On Unix, token files are created with mode `0600` and replaced atomically. Tokens are **not encrypted at rest**. The docs recommend considering encryption for continuous access, rather than mandating it for this non-mobile bridge. Use a private data directory on a protected/encrypted volume, restrict backups, and never commit token files. Non-Unix deployments must restrict file access using their platform's ACLs.

Rotate the client secret that was previously hardcoded in this project: removing it from the source does not remove it from Git history or previous binaries.

The current MQTT transport is plain TCP, not TLS. Use a protected local connection or encrypted tunnel and broker authentication/topic ACLs. Merely changing the broker port to 8883 does not enable TLS. Do not retain command messages containing callbacks or revocation requests; remove any old retained authorization-code commands from the broker.

`reqwest` 0.13 uses platform trust roots with the configured Rustls backend. The Docker runtime installs `ca-certificates`; other deployments also need a usable trust store.

## Verification

`src/daikincloudconnection/tests.rs` uses local mock endpoints to exercise authorization, redirect/state/PKCE validation, token exchange and Bearer usage, expiry/rotation/restart behavior, legacy migration, network and persistence failure recovery, bounded 401 retries, and both-token revocation including partial failures and provider error envelopes. CLI tests verify required credentials, redirect configuration, and secret hiding.

These tests verify the implementation against the supplied documentation without contacting a real Daikin account. Live authorization, the registered redirect URI, application scopes, and account-specific provider behavior still require deployment validation.

```sh
cargo test --locked --offline
```
