# 21 — Prove release acceptance without live orders

**What to build:** A final integrated verification slice that demonstrates the complete PRD behavior through unit, state-machine, adapter, replay, security, packaging, and container tests while guaranteeing that validation cannot trade on Binance.

**Blocked by:** 20 — Package and document single-image operation.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§2.2, 30–31, 33.

## Acceptance criteria

- [ ] One documented verification command runs the complete automated suite and produces a clear pass/fail result for every PRD §31 acceptance area.
- [ ] Tests prove exact decimal rounding, NPU/gates, mirrored strategy math, stop/TP calculations, planned R, daily risk, reservations, cooldowns, and all stable terminal reasons.
- [ ] Deterministic scenarios prove the full 30/30/40 path, protection before additions, all invalidations/exits, TP setup, hard stop behavior, uncertainty handling, data failures, SAFE_MODE, daily flattening, manual controls, and restart/shutdown boundaries.
- [ ] Security verification proves Basic Auth, CSRF, disabled CORS, secret redaction, and secured static/API access.
- [ ] Packaging verification builds the staged image, starts it with a test certificate and audit mount, reaches HTTPS liveness, and distinguishes non-ready trading state.
- [ ] A transport-level guard fails the suite if any automated test attempts a live Binance trading request.
- [ ] Before-and-after repository checks prove `liner-starter` and `liner-dto` remain unchanged and the staging process introduces no secret material.
- [ ] The release evidence identifies every accepted operational risk from PRD §32 without silently claiming guaranteed profitability or recovery behavior outside the approved scope.
