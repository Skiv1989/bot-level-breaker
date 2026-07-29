# 02 — Serve a secured operator snapshot over HTTPS

**What to build:** An authenticated HTTPS application surface that returns one consolidated runtime snapshot and establishes the security contract later level and control operations will use.

**Blocked by:** 01 — Bootstrap a safe live-only application.

**Status:** completed

**Source:** `PRD.md` v1.0 §§25, 26.3, 27, 28.3, 30.6.

## Acceptance criteria

- [x] Spring Boot terminates HTTPS directly using a mounted PKCS#12 keystore configured only through environment variables.
- [x] All API, static-resource, and health-detail endpoints require valid HTTP Basic credentials; invalid or missing credentials are rejected.
- [x] The consolidated snapshot reports application start time, public-stream readiness, private-stream readiness, and trading readiness as distinct values.
- [x] Snapshot assembly reads current state directly from the services that own it.
- [x] CORS is disabled, and the security configuration issues and validates a browser-usable CSRF token for future same-origin mutations.
- [x] Credentials, keystore passwords, and other secrets are absent from responses, exception bodies, and captured logs.
- [x] Security tests cover unauthenticated, invalid-credential, and authenticated snapshot access over the configured HTTPS application context.

## Verification

- `gradlew.bat cleanTest test --no-daemon --offline`: 10 tests passed.
- `gradlew.bat build --no-daemon --offline`: successful.
- `liner-starter` and `liner-dto`: clean worktrees.
