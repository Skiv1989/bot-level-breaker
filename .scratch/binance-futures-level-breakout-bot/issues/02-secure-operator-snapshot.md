# 02 — Serve a secured operator snapshot over HTTPS

**What to build:** An authenticated HTTPS application surface that returns one consolidated runtime snapshot and establishes the security contract later level and control operations will use.

**Blocked by:** 01 — Bootstrap a safe live-only application.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§25, 26.3, 27, 28.3, 30.6.

## Acceptance criteria

- [ ] Spring Boot terminates HTTPS directly using a mounted PKCS#12 keystore configured only through environment variables.
- [ ] All API, static-resource, and health-detail endpoints require valid HTTP Basic credentials; invalid or missing credentials are rejected.
- [ ] The consolidated snapshot reports application start time, global state, public-stream readiness, private-stream readiness, and trading readiness as distinct values.
- [ ] Snapshot assembly reads current state directly from the services that own it.
- [ ] CORS is disabled, and the security configuration issues and validates a browser-usable CSRF token for future same-origin mutations.
- [ ] Credentials, keystore passwords, and other secrets are absent from responses, exception bodies, and captured logs.
- [ ] Security tests cover unauthenticated, invalid-credential, and authenticated snapshot access over the configured HTTPS application context.
