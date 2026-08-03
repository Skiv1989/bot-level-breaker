(function () {
    "use strict";

    const POLL_INTERVAL_MILLIS = 1000;
    const CSRF_COOKIE_NAME = "XSRF-TOKEN";
    const CSRF_HEADER_NAME = "X-XSRF-TOKEN";
    const MAX_RECENT_ITEMS = 12;

    const state = {
        snapshot: null,
        pollActive: false,
        pollTimer: null,
        refreshRequested: false,
        requestSequence: 0,
        renderedSequence: 0,
        pendingActions: new Set(),
    };

    const elements = {
        globalState: byId("global-state"),
        pollStatus: byId("poll-status"),
        pageAlert: byId("page-alert"),
        tradingReadiness: byId("trading-readiness"),
        publicStreamHealth: byId("public-stream-health"),
        privateStreamHealth: byId("private-stream-health"),
        marketDataAge: byId("market-data-age"),
        clockHealth: byId("clock-health"),
        safeModeCount: byId("safe-mode-count"),
        applicationStarted: byId("application-started"),
        currentEquity: byId("current-equity"),
        dailyAnchor: byId("daily-anchor"),
        dailyLimitDrawdown: byId("daily-limit-drawdown"),
        riskReservations: byId("risk-reservations"),
        remainingCapacity: byId("remaining-capacity"),
        openSymbolCount: byId("open-symbol-count"),
        lossStreak: byId("loss-streak"),
        entryCooldown: byId("entry-cooldown"),
        riskReason: byId("risk-reason"),
        addLevelForm: byId("add-level-form"),
        levelFormError: byId("level-form-error"),
        killButton: byId("kill-button"),
        unlockButton: byId("unlock-button"),
        unlockEligibility: byId("unlock-eligibility"),
        levelCount: byId("level-count"),
        levelsBody: byId("levels-body"),
        positionsBody: byId("positions-body"),
        ordersBody: byId("orders-body"),
        recentDecisions: byId("recent-decisions"),
        recentResults: byId("recent-results"),
        recentErrors: byId("recent-errors"),
        recentRecovery: byId("recent-recovery"),
    };

    elements.addLevelForm.addEventListener("submit", addLevel);
    elements.levelsBody.addEventListener("click", handleLevelAction);
    elements.positionsBody.addEventListener("click", handlePositionAction);
    elements.killButton.addEventListener("click", killTrading);
    elements.unlockButton.addEventListener("click", unlockTrading);

    schedulePoll(0);

    function byId(id) {
        return document.getElementById(id);
    }

    function schedulePoll(delayMillis) {
        if (state.pollTimer !== null) {
            window.clearTimeout(state.pollTimer);
        }
        state.pollTimer = window.setTimeout(pollSnapshot, delayMillis);
    }

    async function pollSnapshot() {
        if (state.pollActive) {
            state.refreshRequested = true;
            return;
        }

        state.pollActive = true;
        state.pollTimer = null;
        const sequence = ++state.requestSequence;
        setText(elements.pollStatus, "Refreshing…");

        try {
            const response = await window.fetch("/api/snapshot", {
                method: "GET",
                headers: { "Accept": "application/json" },
                credentials: "same-origin",
                cache: "no-store",
            });
            const payload = await readJson(response);
            if (!response.ok) {
                throw apiFailure(response, payload, "SNAPSHOT_FAILED");
            }
            if (sequence >= state.renderedSequence) {
                state.renderedSequence = sequence;
                state.snapshot = payload;
                renderSnapshot(payload);
                setText(elements.pollStatus, `Updated ${formatClockTime(new Date())}`);
            }
        } catch (error) {
            const failure = normalizeFailure(error, "SNAPSHOT_UNAVAILABLE");
            setText(elements.pollStatus, `${failure.code}: ${failure.message}`);
            setBadge(elements.globalState, "STALE", "bad");
        } finally {
            state.pollActive = false;
            const delay = state.refreshRequested ? 0 : POLL_INTERVAL_MILLIS;
            state.refreshRequested = false;
            schedulePoll(delay);
        }
    }

    function requestSnapshotRefresh() {
        if (state.pollActive) {
            state.refreshRequested = true;
        } else {
            schedulePoll(0);
        }
    }

    function renderSnapshot(snapshot) {
        renderHealth(snapshot);
        renderRisk(snapshot.risk || {});
        renderLevels(snapshot.levels || []);
        renderPositions(snapshot);
        renderOrders(snapshot.execution?.orders || []);
        renderControls(snapshot);
        renderRecent(snapshot);
        syncPendingButtons();
    }

    function renderHealth(snapshot) {
        const health = snapshot.health || {};
        const market = snapshot.publicMarketData || [];
        const binance = snapshot.authenticatedBinance || {};
        const privateStream = binance.privateStream || {};
        const clock = binance.clock || {};
        const connectedStreams = market.filter((item) => item.healthy).length;
        const publicDetail = market.length === 0
            ? health.publicDataReadiness || "NOT_READY"
            : `${health.publicDataReadiness || "UNKNOWN"} · ${connectedStreams}/${market.length} healthy`;

        setBadge(
            elements.globalState,
            snapshot.globalTradingState || "UNKNOWN",
            globalStateTone(snapshot.globalTradingState),
        );
        setBadge(
            elements.tradingReadiness,
            health.tradingReadiness || "UNKNOWN",
            health.tradingReadiness === "READY" ? "good" : "bad",
        );
        setText(elements.publicStreamHealth, publicDetail);
        setText(
            elements.privateStreamHealth,
            `${privateStream.readiness || health.privateStreamReadiness || "UNKNOWN"}`
                + ` · ${privateStream.connectionState || "UNKNOWN"}`
                + ` · event ${formatAgeFromInstant(privateStream.lastEventAt)}`,
        );
        setText(elements.marketDataAge, marketAgeSummary(market));
        setText(
            elements.clockHealth,
            `${clock.readiness || health.clockReadiness || "UNKNOWN"}`
                + ` · offset ${formatMillis(clock.serverOffsetMillis)}`
                + ` · RTT ${formatMillis(clock.roundTripMillis)}`,
        );
        setText(elements.safeModeCount, formatNumber(snapshot.risk?.safeModeEventCount, 0));
        setText(elements.applicationStarted, formatInstant(snapshot.startedAt));
    }

    function renderRisk(risk) {
        setText(elements.currentEquity, formatMoney(risk.currentTotalAccountEquity));
        setText(
            elements.dailyAnchor,
            `${formatMoney(risk.dailyAnchorEquity)} · ${label(risk.dailyAnchorKind)}`
                + ` · ${formatInstant(risk.dailyAnchorEstablishedAt)}`,
        );
        setText(
            elements.dailyLimitDrawdown,
            `${formatMoney(risk.dailyLossLimit)} / ${formatMoney(risk.tradingDrawdown)}`,
        );
        setText(
            elements.riskReservations,
            `${formatMoney(risk.totalReservedRisk)} total`
                + ` · ${formatMoney(risk.reservedRiskForOpenPositions)} open`
                + ` · ${formatMoney(risk.reservedRiskForPendingAttempts)} pending`,
        );
        setText(elements.remainingCapacity, formatMoney(risk.remainingDailyCapacity));
        setText(
            elements.openSymbolCount,
            `${formatNumber(risk.openSymbolCount, 0)} / ${formatNumber(risk.activeAttemptSymbolCount, 0)}`,
        );
        setText(elements.lossStreak, formatNumber(risk.consecutiveLossCount, 0));
        setText(elements.entryCooldown, formatDeadline(risk.entryCooldownUntil));
        setText(elements.riskReason, risk.stateReason || "No active risk-state reason");
    }

    function renderLevels(levels) {
        setText(elements.levelCount, levels.length);
        clear(elements.levelsBody);
        if (levels.length === 0) {
            appendEmptyRow(elements.levelsBody, 9, "No configured levels");
            return;
        }

        levels.forEach((level) => {
            const row = document.createElement("tr");
            appendStackCell(row, [
                level.symbol,
                label(level.direction),
                `created ${formatInstant(level.createdAt)}`,
            ]);
            appendStackCell(row, [
                `requested ${formatNumber(level.requestedLevelPrice)}`,
                `normalized ${formatNumber(level.normalizedLevelPrice)}`,
                `${formatMoney(level.positionNotionalUsdt)} · impulse ${formatPercent(level.maxImpulsePct)}`,
                `qty ${formatNumber(level.plannedQuantity)} · ${formatNumber(level.leverage, 0)}x`,
            ]);
            appendNodeCell(row, badge(level.state, levelStateTone(level.state)));
            appendStackCell(row, [
                `abs ${formatNumber(level.signal?.npu?.absolute)}`,
                `pct ${formatPercent(level.signal?.npu?.percentage)}`,
                level.signal?.npu?.frozen ? "frozen" : "live",
            ]);
            appendTextCell(row, formatNumber(level.signal?.distanceToLevel));
            appendNodeCell(row, gateTags(level.signal?.mandatoryGates?.gates || []));
            appendNodeCell(
                row,
                tags([
                    ...(level.blockers || []),
                    ...(level.signal?.mandatoryGates?.blockerReasons || []),
                ]),
            );
            appendTextCell(row, level.terminalReason ? label(level.terminalReason) : "—");

            const actionCell = document.createElement("td");
            if (level.deleteAllowed) {
                actionCell.appendChild(actionButton(
                    "Delete",
                    "button-secondary",
                    `delete:${level.id}`,
                    "delete-level",
                    level.id,
                ));
            } else {
                actionCell.textContent = "Not allowed";
                actionCell.className = "muted";
            }
            row.appendChild(actionCell);
            elements.levelsBody.appendChild(row);
        });
    }

    function renderPositions(snapshot) {
        const positions = (snapshot.execution?.positions || [])
            .filter((position) => isNonZero(position.positionAmount));
        const levels = snapshot.levels || [];
        const orders = snapshot.execution?.orders || [];
        clear(elements.positionsBody);
        if (positions.length === 0) {
            appendEmptyRow(elements.positionsBody, 9, "No active positions");
            return;
        }

        positions.forEach((position) => {
            const level = levels.find((candidate) =>
                candidate.symbol === position.symbol
                && (candidate.ownsExposure || isNonZero(candidate.confirmedPositionQuantity)))
                || levels.find((candidate) => candidate.symbol === position.symbol);
            const ownedOrders = orders.filter((order) =>
                order.symbol === position.symbol
                && (!level || order.levelId === level.id));
            const hardStops = ownedOrders.filter((order) => order.role === "HARD_STOP");
            const takeProfits = ownedOrders.filter((order) => order.role === "TAKE_PROFIT");
            const row = document.createElement("tr");

            appendNodeCell(row, badge(position.symbol, position.positionAmount > 0 ? "good" : "warn"));
            appendTextCell(row, formatNumber(position.positionAmount));
            appendTextCell(
                row,
                formatMoney(position.actualNotional ?? multiplyAbs(
                    position.positionAmount,
                    position.entryPrice,
                )),
            );
            appendTextCell(row, formatNumber(position.entryPrice));
            appendPnlCell(row, position.unrealizedPnl);
            appendNodeCell(row, orderSummary(
                hardStops,
                level?.hardStopPrice,
                "No confirmed hard stop",
            ));
            appendNodeCell(row, orderSummary(takeProfits, null, "No take profits"));
            appendTextCell(row, formatDeadline(level?.maximumHoldingDeadline));

            const actionCell = document.createElement("td");
            actionCell.appendChild(actionButton(
                "Close",
                "button-danger",
                `close:${position.symbol}`,
                "close-position",
                position.symbol,
            ));
            row.appendChild(actionCell);
            elements.positionsBody.appendChild(row);
        });
    }

    function renderOrders(orders) {
        clear(elements.ordersBody);
        if (orders.length === 0) {
            appendEmptyRow(elements.ordersBody, 6, "No runtime orders");
            return;
        }

        [...orders]
            .sort((left, right) => right.intentSequence - left.intentSequence)
            .slice(0, 100)
            .forEach((order) => {
                const row = document.createElement("tr");
                appendTextCell(row, order.symbol);
                appendTextCell(row, `${label(order.role)} · slot ${order.slot}`);
                appendStackCell(row, [
                    `qty ${formatNumber(order.requestedQuantity)}`,
                    order.requestedPrice == null
                        ? `stop ${formatNumber(order.stopPrice)}`
                        : `price ${formatNumber(order.requestedPrice)}`,
                ]);
                appendTextCell(row, formatNumber(order.actualFilledQuantity));
                appendNodeCell(row, badge(
                    order.outcome || "PENDING",
                    orderOutcomeTone(order.outcome),
                ));
                appendStackCell(row, [
                    formatInstant(order.updatedAt),
                    order.reason ? label(order.reason) : "",
                ]);
                elements.ordersBody.appendChild(row);
            });
    }

    function renderControls(snapshot) {
        const killLocked = snapshot.globalTradingState === "MANUAL_LOCK";
        elements.killButton.disabled = killLocked || state.pendingActions.has("kill");

        const eligibility = unlockEligibility(snapshot);
        elements.unlockButton.disabled = !eligibility.allowed
            || state.pendingActions.has("unlock");
        setText(elements.unlockEligibility, eligibility.message);
    }

    function unlockEligibility(snapshot) {
        if (snapshot.globalTradingState !== "MANUAL_LOCK") {
            return { allowed: false, message: "Bot is not in MANUAL_LOCK." };
        }
        const health = snapshot.health || {};
        const readiness = [
            health.publicDataReadiness,
            health.privateStreamReadiness,
            health.clockReadiness,
            health.accountReadiness,
        ];
        if (readiness.some((item) => item !== "READY")) {
            return { allowed: false, message: "Runtime health is not ready for unlock." };
        }
        if ((snapshot.execution?.positions || []).some((position) =>
            isNonZero(position.positionAmount))) {
            return { allowed: false, message: "Account exposure must be flat before unlock." };
        }
        if ((snapshot.execution?.orders || []).some((order) => order.outcome === "UNKNOWN")) {
            return { allowed: false, message: "Unknown order outcomes block unlock." };
        }
        return { allowed: true, message: "Health and flat-state checks allow an unlock attempt." };
    }

    function renderRecent(snapshot) {
        const audit = [...(snapshot.evidence?.recentAudit || [])]
            .sort((left, right) => instantMillis(right.timestamp) - instantMillis(left.timestamp));
        const commands = [...(snapshot.controls?.commands || [])]
            .sort((left, right) => instantMillis(right.requestedAt) - instantMillis(left.requestedAt));
        const levels = snapshot.levels || [];

        renderEventList(
            elements.recentDecisions,
            audit.filter((record) => ["DECISION", "STATE_TRANSITION", "RISK_UPDATED"]
                .includes(record.eventType))
                .map(auditEvent),
            "No recent decisions",
        );

        const completedCommands = commands
            .filter((command) => command.status !== "IN_PROGRESS")
            .map(commandEvent);
        const completedLevels = levels
            .filter((level) => level.state === "TERMINAL")
            .map((level) => ({
                timestamp: level.stateChangedAt,
                text: `${level.symbol} · ${label(level.terminalReason)} · net ${formatMoney(level.netResult?.netPnl)}`,
            }));
        renderEventList(
            elements.recentResults,
            [...completedCommands, ...completedLevels]
                .sort((left, right) => instantMillis(right.timestamp) - instantMillis(left.timestamp)),
            "No completed results",
        );

        const auditErrors = audit
            .filter((record) => record.eventType === "EXCEPTION")
            .map(auditEvent);
        const commandErrors = commands
            .filter((command) => ["FAILED", "BLOCKED"].includes(command.status))
            .map(commandEvent);
        if (snapshot.evidence?.lastWriteError) {
            auditErrors.unshift({
                timestamp: snapshot.evidence?.applicationStartedAt,
                text: `Evidence writer · ${snapshot.evidence.lastWriteError}`,
            });
        }
        renderEventList(
            elements.recentErrors,
            [...auditErrors, ...commandErrors]
                .sort((left, right) => instantMillis(right.timestamp) - instantMillis(left.timestamp)),
            "No recent errors",
        );

        const recoveryAudit = audit
            .filter((record) => record.eventType === "RECOVERY")
            .map(auditEvent);
        const recoveryCommands = commands
            .filter((command) => ["KILL_SWITCH", "MANUAL_UNLOCK"].includes(command.type))
            .map(commandEvent);
        renderEventList(
            elements.recentRecovery,
            [...recoveryAudit, ...recoveryCommands]
                .sort((left, right) => instantMillis(right.timestamp) - instantMillis(left.timestamp)),
            "No recent recovery actions",
        );
    }

    function auditEvent(record) {
        const transition = record.stateBefore || record.stateAfter
            ? ` · ${label(record.stateBefore)} → ${label(record.stateAfter)}`
            : "";
        const blockers = (record.blockerReasons || []).length > 0
            ? ` · blockers: ${record.blockerReasons.map(label).join(", ")}`
            : "";
        return {
            timestamp: record.timestamp,
            text: `${record.symbol} · ${label(record.eventType)}${transition} · ${record.decision}${blockers}`,
        };
    }

    function commandEvent(command) {
        const blockers = (command.blockers || []).length > 0
            ? ` · blockers: ${command.blockers.map(label).join(", ")}`
            : "";
        return {
            timestamp: command.completedAt || command.requestedAt,
            text: `${label(command.type)}${command.symbol ? ` ${command.symbol}` : ""}`
                + ` · ${label(command.status)} · ${command.code}: ${command.message}${blockers}`,
        };
    }

    function renderEventList(container, events, emptyText) {
        clear(container);
        const visible = events.slice(0, MAX_RECENT_ITEMS);
        if (visible.length === 0) {
            const item = document.createElement("li");
            item.textContent = emptyText;
            item.className = "muted";
            container.appendChild(item);
            return;
        }
        visible.forEach((event) => {
            const item = document.createElement("li");
            const time = document.createElement("time");
            time.dateTime = event.timestamp || "";
            time.textContent = formatInstant(event.timestamp);
            const detail = document.createElement("span");
            detail.textContent = event.text;
            item.append(time, detail);
            container.appendChild(item);
        });
    }

    async function addLevel(event) {
        event.preventDefault();
        clearLevelError();
        const form = new FormData(elements.addLevelForm);
        const request = {
            symbol: String(form.get("symbol") || "").trim(),
            direction: String(form.get("direction") || ""),
            levelPrice: String(form.get("levelPrice") || "").trim(),
            positionNotionalUsdt: String(form.get("positionNotionalUsdt") || "").trim(),
            maxImpulsePct: String(form.get("maxImpulsePct") || "").trim(),
        };
        try {
            const created = await mutate("add-level", "/api/levels", "POST", request);
            if (created) {
                elements.addLevelForm.reset();
            }
        } catch (error) {
            showLevelError(normalizeFailure(error, "LEVEL_CREATE_FAILED"));
        }
    }

    async function handleLevelAction(event) {
        const button = event.target.closest("button[data-command='delete-level']");
        if (!button) {
            return;
        }
        const levelId = button.dataset.value;
        if (!window.confirm("Delete this exposure-free level?")) {
            return;
        }
        await runPageMutation(
            `delete:${levelId}`,
            `/api/levels/${encodeURIComponent(levelId)}`,
            "DELETE",
        );
    }

    async function handlePositionAction(event) {
        const button = event.target.closest("button[data-command='close-position']");
        if (!button) {
            return;
        }
        const symbol = button.dataset.value;
        if (!window.confirm(`Close the active ${symbol} position at market?`)) {
            return;
        }
        await runPageMutation(
            `close:${symbol}`,
            `/api/positions/${encodeURIComponent(symbol)}/close`,
            "POST",
            { commandId: crypto.randomUUID() },
        );
    }

    async function killTrading() {
        if (!window.confirm(
            "Kill trading, cancel bot entries and TPs, and close every account position?",
        )) {
            return;
        }
        await runPageMutation(
            "kill",
            "/api/controls/kill",
            "POST",
            { commandId: crypto.randomUUID() },
        );
    }

    async function unlockTrading() {
        if (!window.confirm("Unlock the healthy, flat bot and resume trading?")) {
            return;
        }
        await runPageMutation(
            "unlock",
            "/api/controls/unlock",
            "POST",
            { commandId: crypto.randomUUID() },
        );
    }

    async function runPageMutation(key, path, method, body) {
        try {
            await mutate(key, path, method, body);
        } catch (error) {
            showPageAlert(normalizeFailure(error, "COMMAND_FAILED"), false);
        }
    }

    async function mutate(key, path, method, body) {
        if (state.pendingActions.has(key)) {
            return null;
        }
        state.pendingActions.add(key);
        syncPendingButtons();

        try {
            const csrfToken = readCookie(CSRF_COOKIE_NAME);
            if (!csrfToken) {
                throw new OperatorFailure(
                    "CSRF_TOKEN_UNAVAILABLE",
                    "Refresh the authenticated page before sending a command",
                );
            }
            const headers = {
                "Accept": "application/json",
                [CSRF_HEADER_NAME]: csrfToken,
            };
            if (body !== undefined) {
                headers["Content-Type"] = "application/json";
            }
            const response = await window.fetch(path, {
                method,
                headers,
                credentials: "same-origin",
                cache: "no-store",
                body: body === undefined ? undefined : JSON.stringify(body),
            });
            const payload = await readJson(response);
            if (!response.ok) {
                throw apiFailure(response, payload, "COMMAND_FAILED");
            }
            showPageAlert({
                code: payload?.code || mutationSuccessCode(method, path),
                message: payload?.message || "Command accepted",
            }, true);
            requestSnapshotRefresh();
            return payload;
        } finally {
            state.pendingActions.delete(key);
            syncPendingButtons();
        }
    }

    function syncPendingButtons() {
        document.querySelectorAll("[data-action-key]").forEach((button) => {
            const pending = state.pendingActions.has(button.dataset.actionKey);
            button.setAttribute("aria-busy", pending ? "true" : "false");
            button.disabled = pending;
        });
        if (state.snapshot) {
            renderControls(state.snapshot);
        }
    }

    function mutationSuccessCode(method, path) {
        if (path === "/api/levels" && method === "POST") {
            return "LEVEL_CREATED";
        }
        if (method === "DELETE") {
            return "LEVEL_DELETED";
        }
        return "COMMAND_ACCEPTED";
    }

    function readCookie(name) {
        const prefix = `${name}=`;
        const part = document.cookie
            .split(";")
            .map((item) => item.trim())
            .find((item) => item.startsWith(prefix));
        if (!part) {
            return null;
        }
        try {
            return decodeURIComponent(part.slice(prefix.length));
        } catch (_ignored) {
            return null;
        }
    }

    async function readJson(response) {
        const text = await response.text();
        if (!text) {
            return null;
        }
        try {
            return JSON.parse(text);
        } catch (_ignored) {
            return null;
        }
    }

    function apiFailure(response, payload, fallbackCode) {
        return new OperatorFailure(
            payload?.code || fallbackCode,
            payload?.message || `Request failed with HTTP ${response.status}`,
        );
    }

    function normalizeFailure(error, fallbackCode) {
        if (error instanceof OperatorFailure) {
            return error;
        }
        return new OperatorFailure(fallbackCode, "The request could not be completed");
    }

    function OperatorFailure(code, message) {
        this.name = "OperatorFailure";
        this.code = code;
        this.message = message;
    }
    OperatorFailure.prototype = Object.create(Error.prototype);

    function showPageAlert(result, success) {
        elements.pageAlert.textContent = `${result.code}: ${result.message}`;
        elements.pageAlert.classList.toggle("success", success);
        elements.pageAlert.classList.remove("hidden");
    }

    function showLevelError(error) {
        elements.levelFormError.textContent = `${error.code}: ${error.message}`;
        elements.levelFormError.classList.remove("hidden");
    }

    function clearLevelError() {
        elements.levelFormError.textContent = "";
        elements.levelFormError.classList.add("hidden");
    }

    function setBadge(element, text, tone) {
        element.textContent = text || "UNKNOWN";
        element.className = `badge badge-${tone}`;
    }

    function badge(text, tone) {
        const element = document.createElement("span");
        setBadge(element, label(text), tone);
        return element;
    }

    function tags(values) {
        const container = document.createElement("div");
        container.className = "tag-list";
        const distinct = [...new Set(values.filter(Boolean))];
        if (distinct.length === 0) {
            container.textContent = "—";
            return container;
        }
        distinct.forEach((value) => {
            const item = document.createElement("span");
            item.className = "tag";
            item.textContent = label(value);
            container.appendChild(item);
        });
        return container;
    }

    function gateTags(gates) {
        const container = document.createElement("div");
        container.className = "tag-list";
        if (gates.length === 0) {
            container.textContent = "—";
            return container;
        }
        gates.forEach((gate) => {
            const item = document.createElement("span");
            item.className = `tag ${gate.passed ? "pass" : "fail"}`;
            item.textContent = `${label(gate.gate)} ${gate.passed ? "PASS" : "FAIL"}`;
            container.appendChild(item);
        });
        return container;
    }

    function orderSummary(orders, fallbackPrice, emptyText) {
        const container = document.createElement("div");
        container.className = "cell-stack";
        if (orders.length === 0) {
            setText(
                container,
                fallbackPrice == null ? emptyText : `${formatNumber(fallbackPrice)} · confirmed`,
            );
            return container;
        }
        orders.forEach((order) => {
            const line = document.createElement("span");
            const price = order.requestedPrice ?? order.stopPrice ?? fallbackPrice;
            line.textContent = `${formatNumber(price)} · ${label(order.outcome || "PENDING")}`
                + ` · filled ${formatNumber(order.actualFilledQuantity)}`;
            container.appendChild(line);
        });
        return container;
    }

    function actionButton(text, kind, key, command, value) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = `button button-small ${kind}`;
        button.textContent = text;
        button.dataset.actionKey = key;
        button.dataset.command = command;
        button.dataset.value = value;
        button.disabled = state.pendingActions.has(key);
        return button;
    }

    function appendTextCell(row, value) {
        const cell = document.createElement("td");
        cell.textContent = value ?? "—";
        row.appendChild(cell);
    }

    function appendNodeCell(row, node) {
        const cell = document.createElement("td");
        cell.appendChild(node);
        row.appendChild(cell);
    }

    function appendStackCell(row, values) {
        const stack = document.createElement("div");
        stack.className = "cell-stack";
        values.filter((value) => value !== "").forEach((value, index) => {
            const line = document.createElement("span");
            line.textContent = value ?? "—";
            if (index > 0) {
                line.className = "secondary";
            }
            stack.appendChild(line);
        });
        appendNodeCell(row, stack);
    }

    function appendPnlCell(row, value) {
        const cell = document.createElement("td");
        cell.textContent = formatMoney(value);
        const numeric = Number(value);
        if (Number.isFinite(numeric) && numeric !== 0) {
            cell.className = numeric > 0 ? "positive" : "negative";
        }
        row.appendChild(cell);
    }

    function appendEmptyRow(body, columnCount, text) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = columnCount;
        cell.className = "empty-cell";
        cell.textContent = text;
        row.appendChild(cell);
        body.appendChild(row);
    }

    function clear(element) {
        element.replaceChildren();
    }

    function setText(element, value) {
        element.textContent = value ?? "—";
    }

    function formatNumber(value, maximumFractionDigits = 8) {
        if (value === null || value === undefined || value === "") {
            return "—";
        }
        const number = Number(value);
        if (!Number.isFinite(number)) {
            return String(value);
        }
        return new Intl.NumberFormat(undefined, {
            maximumFractionDigits,
            minimumFractionDigits: 0,
        }).format(number);
    }

    function formatMoney(value) {
        const formatted = formatNumber(value, 8);
        return formatted === "—" ? formatted : `${formatted} USDT`;
    }

    function formatPercent(value) {
        const formatted = formatNumber(value, 6);
        return formatted === "—" ? formatted : `${formatted}%`;
    }

    function formatMillis(value) {
        const formatted = formatNumber(value, 0);
        return formatted === "—" ? formatted : `${formatted} ms`;
    }

    function formatInstant(value) {
        if (!value) {
            return "—";
        }
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
            return String(value);
        }
        return date.toLocaleString();
    }

    function formatClockTime(date) {
        return date.toLocaleTimeString();
    }

    function formatAgeFromInstant(value) {
        if (!value) {
            return "—";
        }
        const age = Date.now() - instantMillis(value);
        return Number.isFinite(age) ? formatAgeMillis(Math.max(0, age)) : "—";
    }

    function formatAgeMillis(value) {
        if (value === null || value === undefined || !Number.isFinite(Number(value))) {
            return "—";
        }
        const millis = Number(value);
        if (millis < 1000) {
            return `${Math.round(millis)} ms`;
        }
        return `${(millis / 1000).toFixed(1)} s`;
    }

    function formatDeadline(value) {
        if (!value) {
            return "—";
        }
        const remaining = instantMillis(value) - Date.now();
        if (!Number.isFinite(remaining)) {
            return formatInstant(value);
        }
        if (remaining <= 0) {
            return `${formatInstant(value)} · elapsed`;
        }
        const seconds = Math.ceil(remaining / 1000);
        const minutes = Math.floor(seconds / 60);
        return `${formatInstant(value)} · ${minutes}m ${seconds % 60}s`;
    }

    function marketAgeSummary(market) {
        const aggregateAges = market
            .map((item) => item.aggregateTradeAge?.receiveAgeMillis)
            .filter((value) => value !== null && value !== undefined);
        const bookAges = market
            .map((item) => item.bookTickerAge?.receiveAgeMillis)
            .filter((value) => value !== null && value !== undefined);
        const maxAggregate = aggregateAges.length === 0 ? null : Math.max(...aggregateAges);
        const maxBook = bookAges.length === 0 ? null : Math.max(...bookAges);
        return `trade ${formatAgeMillis(maxAggregate)} · book ${formatAgeMillis(maxBook)}`;
    }

    function multiplyAbs(left, right) {
        const result = Math.abs(Number(left)) * Number(right);
        return Number.isFinite(result) ? result : null;
    }

    function instantMillis(value) {
        return value ? new Date(value).getTime() : 0;
    }

    function isNonZero(value) {
        const number = Number(value);
        return Number.isFinite(number) && number !== 0;
    }

    function label(value) {
        if (value === null || value === undefined || value === "") {
            return "—";
        }
        return String(value).replaceAll("_", " ");
    }

    function globalStateTone(value) {
        if (value === "RUNNING") {
            return "good";
        }
        if (["ENTRY_COOLDOWN", "DAILY_LOCKED"].includes(value)) {
            return "warn";
        }
        return value ? "bad" : "neutral";
    }

    function levelStateTone(value) {
        if (["ARMED", "APPROACH", "POSITION_MANAGEMENT"].includes(value)) {
            return "good";
        }
        if (value === "TERMINAL") {
            return "neutral";
        }
        return "warn";
    }

    function orderOutcomeTone(value) {
        if (["FILLED", "ACTIVE"].includes(value)) {
            return "good";
        }
        if (["UNKNOWN", "REJECTED"].includes(value)) {
            return "bad";
        }
        return value ? "warn" : "neutral";
    }
}());
