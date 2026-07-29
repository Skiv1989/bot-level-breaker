# 18 — Operate the bot from one web page

**What to build:** A single secured static page that polls the consolidated snapshot once per second and lets the operator create/delete levels, inspect strategy and risk state, close positions, kill trading, and unlock the bot.

**Blocked by:** 06 — Calculate NPU, market metrics, and mandatory gates; 09 — Record audit and market evidence; 17 — Add manual close, kill, and unlock controls.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§25–27.

## Acceptance criteria

- [ ] The page uses static HTML, plain CSS, and vanilla JavaScript with no SPA framework, browser WebSocket, or SSE.
- [ ] One consolidated REST snapshot is polled every second; overlapping polls do not corrupt displayed state or issue duplicate commands.
- [ ] System health shows global state, public/private stream health, data age, clock status, SAFE_MODE count, and application start time.
- [ ] Risk/equity shows current equity, normal or temporary anchor, daily limit/drawdown, reservations/capacity, open-symbol count, loss streak, and cooldown.
- [ ] The Add Level form exposes the five operator inputs and displays stable backend validation errors; level rows show configuration, state, NPU, distance, gates, blockers, terminal reason, and allowed delete action.
- [ ] Position/order rows show actual quantity/notional, weighted entry, unrealized PnL, hard stop, TPs, holding deadline, and close action.
- [ ] Controls expose kill and eligible unlock actions, and the recent section shows decisions, completed results, errors, and recovery actions.
- [ ] Browser mutations send the valid CSRF token, credentials rely on the native Basic challenge, and sensitive configuration never appears in page content or client logs.
