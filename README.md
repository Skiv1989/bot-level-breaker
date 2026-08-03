# Binance Futures level-breakout bot

This service is delivered as one live-only Docker image containing one Spring
Boot process. It has no Docker Compose dependency, Binance testnet selector, or
shadow-trading mode. Automated verification uses injected fakes and offline
replay; the runtime adapters use Binance production endpoints.

## Prove release acceptance

Run the complete release proof from PowerShell with Docker configured for Linux
containers and both reference repositories available:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
    -File .\scripts\verify-release.ps1 `
    -LinerStarterSource C:\IdeaProjects\liner-starter `
    -LinerDtoSource "$env:USERPROFILE\IdeaProjects\liner-dto"
```

This one command runs the full Gradle suite, verifies that every required test
layer and PRD §31 area produced JUnit evidence, stages and scans the three
source snapshots, builds the image, and starts it with a generated certificate
and mounted audit directory. The container has `--network none`; its HTTPS
liveness must become healthy while trading readiness remains `BLOCKED`. Gradle
test workers also install a transport guard whose persistent marker fails the
test task if live Binance order or account-mutation transport is reached.

The command records before/after fingerprints for `liner-starter` and
`liner-dto`, uses only conspicuous dummy credentials, removes its generated
container and default disposable image, and writes the complete result under
`build/reports/release-acceptance/`. It does not read production credentials.
See [RELEASE_ACCEPTANCE.md](RELEASE_ACCEPTANCE.md) for the evidence map, stable
reason catalog, failure semantics, and accepted operational risks.

## Build the image

Prerequisites are Docker with Linux-container support and readable checkouts of
the bot, `liner-starter`, and `liner-dto`. Run from PowerShell:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
    -File .\scripts\package-image.ps1 `
    -LinerStarterSource C:\IdeaProjects\liner-starter `
    -LinerDtoSource "$env:USERPROFILE\IdeaProjects\liner-dto" `
    -ImageName binance-futures-level-breakout-bot:0.1.0
```

The command creates a new context under the operating-system temporary
directory, copies the three source trees without writing to them, builds the
image, and removes the context. Staging omits VCS and IDE metadata, Gradle and
other build output, local agent metadata, credentials, environment files,
keystores and certificates, logs, databases, and audit/event artifacts. The
multi-stage Dockerfile publishes `liner-dto` only inside the builder, builds
`liner-starter` against that local publication, builds the bot against both
resulting JARs, and copies only the executable bot JAR and liveness probe into
the runtime image.

To inspect or test the sanitized context without invoking Docker, provide a new
directory and `-StageOnly`. The command refuses an existing directory or any
context located inside a source repository.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
    -File .\scripts\package-image.ps1 `
    -LinerStarterSource C:\IdeaProjects\liner-starter `
    -LinerDtoSource "$env:USERPROFILE\IdeaProjects\liner-dto" `
    -ContextDirectory C:\Temp\breakout-bot-context `
    -StageOnly
```

Do not put a keystore, credential file, audit data, or a populated environment
file in any source checkout even though staging rejects common secret formats.
Never pass secrets as Docker build arguments; they are runtime inputs only.

## Required runtime configuration

[deployment/runtime.env.example](deployment/runtime.env.example) lists every
required variable. Copy it to a root-owned file outside the checkout and fill
the blank values there. The example intentionally contains no API key,
password, or keystore password.

| Variable | Requirement |
| --- | --- |
| `BINANCE_API_KEY` | Dedicated-account Binance Futures API key |
| `BINANCE_API_SECRET` | HMAC secret for that key |
| `BOT_BASIC_USERNAME` | HTTPS UI/API Basic-auth username |
| `BOT_BASIC_PASSWORD` | Strong Basic-auth password |
| `TLS_KEYSTORE_PATH` | In-container path of the read-only PKCS#12 mount |
| `TLS_KEYSTORE_PASSWORD` | Password of that PKCS#12 file |
| `AUDIT_DIRECTORY` | In-container path of the read-write audit/event mount |

Protect the populated environment file with mode `0600`. Do not add a testnet,
base-URL, or shadow-mode variable. Production endpoints are fixed in the
application.

## TLS and host preparation

Create a private CA offline, issue a server certificate whose SAN covers the
Droplet hostname or IP used by the operator, and export the server key plus its
full chain as PKCS#12. Keep the CA private key offline. Install only the private
root CA certificate into the trust store of every operator browser/device;
otherwise HTTPS must be treated as untrusted. Never bypass the browser warning
for routine operation and never bake the PKCS#12 file into the image.

On the Droplet, prepare root-owned locations. The container runs as UID/GID
`10001`, so the audit directory must be writable by that identity and the
keystore must be readable by it.

```bash
sudo install -d -m 0700 /etc/breakout-bot
sudo install -d -o 10001 -g 10001 -m 0700 /var/lib/breakout-bot/audit
sudo install -o 10001 -g 10001 -m 0400 breakout-bot.p12 \
  /etc/breakout-bot/breakout-bot.p12
sudo install -o root -g root -m 0600 runtime.env \
  /etc/breakout-bot/runtime.env
```

Set `TLS_KEYSTORE_PATH=/run/tls/breakout-bot.p12` and
`AUDIT_DIRECTORY=/var/lib/breakout-bot/audit` in `runtime.env`.

## DigitalOcean network controls

Create a DigitalOcean Cloud Firewall before starting the container:

- allow inbound TCP 443 only from the operator public IP as a `/32` rule;
- allow inbound TCP 22 only from that same operator IP;
- allow no other inbound traffic;
- update both rules before connecting when the operator IP changes.

The inbound operator address is not the Binance allowlist address. Add the
Droplet's outbound public IPv4 address, and only that address, to the Binance
API-key IP allowlist. Confirm the address from the Droplet and recheck it after
any networking or Droplet change. Do not start the container until the Binance
allowlist is active.

## Start and observe

Load or transfer the image to the Droplet, then run it directly. Compose is not
needed. Automatic restarts are deliberately disabled because a new process
does not adopt or clean up exchange state left by an earlier process.

```bash
docker run --detach \
  --name breakout-bot \
  --restart=no \
  --env-file /etc/breakout-bot/runtime.env \
  --publish 443:443 \
  --mount type=bind,src=/etc/breakout-bot/breakout-bot.p12,dst=/run/tls/breakout-bot.p12,readonly \
  --mount type=bind,src=/var/lib/breakout-bot/audit,dst=/var/lib/breakout-bot/audit \
  binance-futures-level-breakout-bot:0.1.0
```

The authenticated endpoints have intentionally different meanings:

- `GET /api/health/liveness` proves that the process answers HTTPS; Docker's
  `HEALTHCHECK` uses only this endpoint.
- `GET /api/health/readiness` separately reports public data, private stream,
  Binance clock/account, and trading readiness. A `200` response can contain
  `NOT_READY` or `BLOCKED`; HTTP availability never means trading is allowed.

Use the private root CA for operator checks. Supplying only a username makes
`curl` prompt for the Basic-auth password instead of storing it in shell
history.

```bash
curl --cacert private-root-ca.crt --user '<operator-username>' \
  https://<droplet-host>/api/health/liveness
curl --cacert private-root-ca.crt --user '<operator-username>' \
  https://<droplet-host>/api/health/readiness
docker inspect --format '{{json .State.Health}}' breakout-bot
```

Do not wire public/private/trading readiness to an automatic container restart.
A stream outage blocks or closes exposure through application safety logic; an
automatic process restart would instead discard all memory-only levels and
state.

## Account and lifecycle guarantees

This bot is safe to operate only when all of these guarantees remain true:

- the Binance account is dedicated exclusively to this one bot;
- no person, second bot, or external system trades the account;
- only one container instance exists;
- no unmanaged order or position exists at startup;
- exchange-side state left by any earlier process is manually resolved before
  another start.

Startup establishes a new temporary daily-equity anchor and starts with no
levels. It does not inspect or adopt positions/open orders, cancel anything,
flatten exposure, restore attempts, or send a trading order. Every restart is a
fresh startup and resets the in-memory level state and temporary daily budget.

Use `docker stop --time 15 breakout-bot` for graceful shutdown. Shutdown flushes
audit output when possible but sends no Binance command: it does not cancel
entries, stops, or take profits and does not close positions. A crash or forced
stop has the same exchange-state responsibility, with less assurance that the
last audit records were flushed. Hard stops, GTC take profits, open orders, and
positions can outlive the process. Inspect and resolve all of them in Binance
before removing/restarting the container. Back up the mounted audit directory;
it is the persistent operational record.

## Kill and unlock procedures

When the application is reachable, use the web page's global kill control. It
blocks new entries, cancels pending entries/take profits as required, closes
account exposure reduce-only, retains protection until flat is confirmed, and
enters `MANUAL_LOCK`. Verify flat positions and expected order cleanup in both
the UI and Binance. Stopping the container is not a kill operation and never
substitutes for this control.

If the bot is unreachable or an order outcome remains unresolved, manage the
dedicated account directly in Binance: resolve open orders, positions, and
protective orders before any restart. Preserve the audit directory and record
the manual intervention.

Unlock only after the account is confirmed flat, unresolved outcomes are
cleared, public/private/account/clock health is ready, and the operator has
confirmed exclusive account ownership. Unlocking does not recreate levels lost
by a restart. Never unlock merely to clear an alert or force trading readiness.
