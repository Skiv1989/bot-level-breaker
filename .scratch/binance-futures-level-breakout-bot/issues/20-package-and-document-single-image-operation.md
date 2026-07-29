# 20 — Package and document single-image operation

**What to build:** A reproducible packaging and operations slice that stages read-only source snapshots, builds one live-only Docker image, runs the secured bot with mounted TLS/audit storage, and documents safe DigitalOcean operation.

**Blocked by:** 18 — Operate the bot from one web page; 19 — Replay recorded attempts deterministically.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§23.4, 27.2–27.3, 28, 30.7, 33.

## Acceptance criteria

- [ ] A packaging command stages bot, `liner-starter`, and `liner-dto` source snapshots into a temporary Docker build context without modifying the original repositories.
- [ ] Staging excludes `.git`, IDE metadata, build output, credentials, certificates, audit data, and other secret files.
- [ ] A multi-stage Docker build resolves both local dependencies and emits one runtime image containing one Spring Boot process with no Docker Compose requirement.
- [ ] The container listens on HTTPS port 443, reads the PKCS#12 keystore from a read-only mount, writes audit/events to a read-write mount, and obtains all required secrets/configuration from environment variables.
- [ ] No testnet selector or shadow-trading runtime mode exists; production endpoints are live while automated verification remains fake/offline.
- [ ] Container health distinguishes HTTP/process liveness from public, private, and trading readiness.
- [ ] An example configuration contains every required variable but no secret value.
- [ ] The operational README covers private-CA trust, inbound firewall restrictions, Binance outbound-IP allowlisting, dedicated-account guarantees, startup/restart/shutdown behavior, unresolved exchange-state responsibility, and kill/unlock procedures.
