package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.binance.BinanceExecutionClient
import com.scalpsecta.breakoutbot.binance.BinanceOrderReconciliation
import com.scalpsecta.breakoutbot.binance.BinanceOrderRequest
import com.scalpsecta.breakoutbot.binance.BinanceOrderStatus
import com.scalpsecta.breakoutbot.binance.BinancePositionUpdate
import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.evidence.AuditEventType
import com.scalpsecta.breakoutbot.evidence.AuditRecordDraft
import com.scalpsecta.breakoutbot.evidence.DecisionEvidence
import com.scalpsecta.breakoutbot.evidence.EvidenceRecorder
import com.scalpsecta.breakoutbot.evidence.OrderEvidence
import com.scalpsecta.breakoutbot.evidence.QuantityEvidence
import com.scalpsecta.breakoutbot.evidence.ReconciliationEvidence
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import com.scalpsecta.breakoutbot.service.AuthenticatedBinanceReadinessService
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.Disposables
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

@Service
class ExecutionService internal constructor(
    private val client: BinanceExecutionClient,
    private val privateEvents: Flux<BinanceUserDataEvent>,
    private val symbolCoordinator: SymbolExecutionCoordinator,
    private val riskService: AttemptRiskService,
    private val clientOrderIdFactory: ClientOrderIdFactory,
    private val evidenceRecorder: EvidenceRecorder,
    private val clock: Clock,
    private val scheduler: Scheduler,
    private val requestTimeout: Duration,
    private val reconciliationInterval: Duration,
    private val stopConfirmationTimeout: Duration = Duration.ofSeconds(2),
) : PreEntryOrderExecutor {
    @Autowired
    constructor(
        client: BinanceExecutionClient,
        readinessService: AuthenticatedBinanceReadinessService,
        symbolCoordinator: SymbolExecutionCoordinator,
        riskService: AttemptRiskService,
        clientOrderIdFactory: ClientOrderIdFactory,
        evidenceRecorder: EvidenceRecorder,
        clock: Clock,
        @Value("\${bot.execution.request-timeout:1s}")
        requestTimeout: Duration,
        @Value("\${bot.execution.reconciliation-interval:1s}")
        reconciliationInterval: Duration,
        @Value("\${bot.execution.stop-confirmation-timeout:2s}")
        stopConfirmationTimeout: Duration,
    ) : this(
        client = client,
        privateEvents = readinessService.events(),
        symbolCoordinator = symbolCoordinator,
        riskService = riskService,
        clientOrderIdFactory = clientOrderIdFactory,
        evidenceRecorder = evidenceRecorder,
        clock = clock,
        scheduler = Schedulers.parallel(),
        requestTimeout = requestTimeout,
        reconciliationInterval = reconciliationInterval,
        stopConfirmationTimeout = stopConfirmationTimeout,
    )

    private val pendingOrders = ConcurrentHashMap<String, PendingOrder>()
    private val orders = ConcurrentHashMap<String, OrderExecutionSnapshot>()
    private val positions = ConcurrentHashMap<String, ExecutionPositionSnapshot>()
    private val balances = ConcurrentHashMap<String, ExecutionBalanceSnapshot>()
    private val activeSymbols = ConcurrentHashMap.newKeySet<String>()
    private val subscriptions = Disposables.composite()

    init {
        require(!requestTimeout.isZero && !requestTimeout.isNegative) {
            "requestTimeout must be positive"
        }
        require(
            !reconciliationInterval.isZero &&
                !reconciliationInterval.isNegative,
        ) {
            "reconciliationInterval must be positive"
        }
        require(
            !stopConfirmationTimeout.isZero &&
                !stopConfirmationTimeout.isNegative,
        ) {
            "stopConfirmationTimeout must be positive"
        }
        subscriptions.add(
            privateEvents.subscribe(
                ::routePrivateEvent,
                { /* Readiness owns reconnect and degradation handling. */ },
            ),
        )
    }

    override fun execute(request: OrderIntentRequest): Mono<OrderResolution> {
        val intent = clientOrderIdFactory.create(request)
        return dispatch(intent, closeOnUnknown = true).cache()
    }

    override fun confirmHardStop(
        request: OrderIntentRequest,
    ): Mono<HardStopConfirmation> {
        val intent = clientOrderIdFactory.create(request)
        return Mono.defer {
            val confirmedPositionAmount = checkNotNull(
                intent.confirmedPositionAmount,
            )
            symbolCoordinator
                .recordOwnership(
                    levelId = intent.levelId,
                    ownsActiveAttempt = true,
                    ownsExposure = true,
                    hasUnresolvedOrder = true,
                )
                .then(
                    symbolCoordinator.submit(
                        symbol = intent.symbol,
                        eventId = "hard-stop-intent:${intent.clientOrderId}",
                    ) {
                        recordIntent(intent)
                    },
                )
                .then(placeAndConfirmHardStop(intent))
                .onErrorResume {
                    Mono.just(
                        failedHardStopConfirmation(
                            intent = intent,
                            reconciliationChecks = 0,
                        ),
                    )
                }
                .flatMap { confirmation ->
                    finalizeHardStopConfirmation(
                        confirmation.copy(
                            confirmedPositionAmount = confirmedPositionAmount,
                        ),
                    )
                }
        }.cache()
    }

    fun currentState(): ExecutionSnapshot =
        ExecutionSnapshot(
            observedAt = clock.instant(),
            entriesAndAdditionsBlocked =
                riskService.currentState().globalTradingState !=
                    GlobalTradingState.RUNNING ||
                    orders.values.any { order ->
                        order.outcome == OrderOutcome.UNKNOWN
                    },
            positions = positions.values.sortedBy(ExecutionPositionSnapshot::symbol),
            balances = balances.values.sortedBy(ExecutionBalanceSnapshot::asset),
            orders = orders.values.sortedBy(OrderExecutionSnapshot::intentSequence),
        )

    @PreDestroy
    fun close() {
        subscriptions.dispose()
        pendingOrders.values.forEach { pending ->
            pending.result.tryEmitError(
                IllegalStateException("Execution service is shutting down"),
            )
        }
        pendingOrders.clear()
    }

    private fun dispatch(
        intent: OrderIntent,
        closeOnUnknown: Boolean,
    ): Mono<OrderResolution> =
        Mono.defer {
            if (
                !intent.role.closesExposure &&
                riskService.currentState().globalTradingState !=
                GlobalTradingState.RUNNING
            ) {
                return@defer Mono.error(
                    OrderExecutionException(
                        "Entries and additions are blocked in SAFE_MODE",
                    ),
                )
            }
            val pending = PendingOrder(intent)
            val hasConfirmedExposure =
                intent.confirmedPositionAmount?.signum()?.let { it != 0 } == true
            symbolCoordinator
                .recordOwnership(
                    levelId = intent.levelId,
                    ownsActiveAttempt = true,
                    ownsExposure = hasConfirmedExposure,
                    hasUnresolvedOrder = true,
                )
                .then(
                    symbolCoordinator.submit(
                        symbol = intent.symbol,
                        eventId = "order-intent:${intent.clientOrderId}",
                    ) {
                        register(pending)
                    },
                )
                .then(resolvePlacedOrder(pending))
                .flatMap { resolution ->
                    finalizeResolution(resolution, closeOnUnknown)
                }
        }

    private fun register(pending: PendingOrder) {
        check(pendingOrders.putIfAbsent(pending.intent.clientOrderId, pending) == null) {
            "Duplicate clientOrderId ${pending.intent.clientOrderId}"
        }
        recordIntent(pending.intent)
    }

    private fun recordIntent(intent: OrderIntent) {
        check(!orders.containsKey(intent.clientOrderId)) {
            "Duplicate clientOrderId ${intent.clientOrderId}"
        }
        activeSymbols += intent.symbol
        orders[intent.clientOrderId] = intent.snapshot(clock.instant())
        evidenceRecorder.recordOrderIntent(
            levelId = intent.levelId,
            symbol = intent.symbol,
            timestamp = clock.instant(),
            order = intent.evidence(),
        )
    }

    private fun placeAndConfirmHardStop(
        intent: OrderIntent,
    ): Mono<HardStopConfirmation> {
        val checks = AtomicInteger()
        val lastObservation =
            AtomicReference<HardStopConfirmation?>()
        val placement = client
            .placeOrder(intent.toBinanceRequest())
            .timeout(requestTimeout, scheduler)
            .onErrorResume { Mono.empty() }
            .then()
        return placement
            .then(reconcileHardStop(intent, checks, lastObservation))
            .timeout(stopConfirmationTimeout, scheduler)
            .onErrorResume {
                Mono.just(
                    lastObservation.get()?.copy(confirmed = false)
                        ?: failedHardStopConfirmation(
                            intent = intent,
                            reconciliationChecks = checks.get(),
                        ),
                )
            }
    }

    private fun reconcileHardStop(
        intent: OrderIntent,
        checks: AtomicInteger,
        lastObservation: AtomicReference<HardStopConfirmation?>,
    ): Mono<HardStopConfirmation> {
        val interval = minOf(reconciliationInterval, MAX_STOP_CHECK_INTERVAL)
        val maximumChecks = (
            stopConfirmationTimeout.toMillis() /
                interval.toMillis().coerceAtLeast(1L) +
                1L
            ).coerceAtMost(MAX_STOP_RECONCILIATION_CHECKS.toLong()).toInt()
        return Flux
            .range(1, maximumChecks)
            .concatMap { attempt ->
                val delay = if (attempt == 1) Duration.ZERO else interval
                Mono
                    .delay(delay, scheduler)
                    .then(
                        client.reconcileOrder(
                            symbol = intent.symbol,
                            clientOrderId = intent.clientOrderId,
                        ),
                    )
                    .onErrorResume { error ->
                        Mono.just(
                            BinanceOrderReconciliation(
                                order = null,
                                position = null,
                                openClientOrderIds = emptySet(),
                                safeDetail = error.javaClass.simpleName,
                            ),
                        )
                    }
                    .flatMap { reconciliation ->
                        checks.set(attempt)
                        val evaluation = evaluateHardStop(
                            intent = intent,
                            attempt = attempt,
                            reconciliation = reconciliation,
                        )
                        lastObservation.set(evaluation.confirmation)
                        symbolCoordinator
                            .submit(
                                symbol = intent.symbol,
                                eventId =
                                    "hard-stop-reconciliation:${intent.clientOrderId}:$attempt",
                            ) {
                                recordHardStopReconciliation(
                                    intent = intent,
                                    attempt = attempt,
                                    reconciliation = reconciliation,
                                    evaluation = evaluation,
                                )
                            }
                            .thenReturn(evaluation)
                    }
            }
            .filter(HardStopEvaluation::terminal)
            .next()
            .map(HardStopEvaluation::confirmation)
            .switchIfEmpty(Mono.never())
    }

    private fun evaluateHardStop(
        intent: OrderIntent,
        attempt: Int,
        reconciliation: BinanceOrderReconciliation,
    ): HardStopEvaluation {
        val order = reconciliation.order
        val observedWorkingType = order?.workingType?.let { value ->
            runCatching { TriggerWorkingType.valueOf(value.uppercase()) }
                .getOrNull()
        }
        val confirmed =
            order != null &&
                order.status.equals("NEW", ignoreCase = true) &&
                order.clientOrderId in reconciliation.openClientOrderIds &&
                order.type.equals(OrderType.STOP_MARKET.name, ignoreCase = true) &&
                order.closePosition &&
                order.stopPrice?.compareTo(checkNotNull(intent.stopPrice)) == 0 &&
                observedWorkingType == intent.workingType &&
                order.priceProtect == intent.priceProtect
        val definitivelyFailed = order?.status?.uppercase() in STOP_TERMINAL_STATUSES
        return HardStopEvaluation(
            terminal = confirmed || definitivelyFailed,
            confirmation = HardStopConfirmation(
                intent = intent,
                confirmed = confirmed,
                exchangeOrderId = order?.orderId,
                observedStopPrice = order?.stopPrice,
                observedWorkingType = observedWorkingType,
                observedPriceProtect = order?.priceProtect,
                reconciliationChecks = attempt,
                confirmedPositionAmount =
                    reconciliation.position?.positionAmount
                        ?: checkNotNull(intent.confirmedPositionAmount),
            ),
        )
    }

    private fun recordHardStopReconciliation(
        intent: OrderIntent,
        attempt: Int,
        reconciliation: BinanceOrderReconciliation,
        evaluation: HardStopEvaluation,
    ) {
        reconciliation.position?.let { position ->
            positions[intent.symbol] = ExecutionPositionSnapshot(
                symbol = intent.symbol,
                positionAmount = position.positionAmount,
                entryPrice = position.entryPrice,
                updatedAt = clock.instant(),
            )
        }
        val result = when {
            evaluation.confirmation.confirmed -> OrderOutcome.ACTIVE.name
            evaluation.terminal -> ExecutionReasonCode.STOP_SETUP_FAILED.name
            else -> OrderOutcome.UNKNOWN.name
        }
        evidenceRecorder.recordReconciliation(
            levelId = intent.levelId,
            symbol = intent.symbol,
            timestamp = clock.instant(),
            reconciliation = ReconciliationEvidence(
                clientOrderId = intent.clientOrderId,
                attemptNumber = attempt,
                result = result,
                exchangeOrderId = reconciliation.order?.orderId,
                requestedQuantity = null,
                filledQuantity = reconciliation.order?.executedQuantity,
                safeDetail = reconciliation.safeDetail,
            ),
        )
    }

    private fun failedHardStopConfirmation(
        intent: OrderIntent,
        reconciliationChecks: Int,
    ): HardStopConfirmation =
        HardStopConfirmation(
            intent = intent,
            confirmed = false,
            exchangeOrderId = null,
            observedStopPrice = null,
            observedWorkingType = null,
            observedPriceProtect = null,
            reconciliationChecks = reconciliationChecks,
            confirmedPositionAmount =
                checkNotNull(intent.confirmedPositionAmount),
        )

    private fun finalizeHardStopConfirmation(
        confirmation: HardStopConfirmation,
    ): Mono<HardStopConfirmation> {
        val intent = confirmation.intent
        orders.computeIfPresent(intent.clientOrderId) { _, current ->
            current.copy(
                outcome = if (confirmation.confirmed) {
                    OrderOutcome.ACTIVE
                } else {
                    OrderOutcome.UNKNOWN
                },
                source = OrderResolutionSource.REST_RECONCILIATION,
                exchangeOrderId = confirmation.exchangeOrderId,
                updatedAt = clock.instant(),
                reason = if (confirmation.confirmed) {
                    null
                } else {
                    ExecutionReasonCode.STOP_SETUP_FAILED
                },
            )
        }
        val exposure = confirmation.confirmedPositionAmount.signum() != 0
        return symbolCoordinator
            .recordOwnership(
                levelId = intent.levelId,
                ownsActiveAttempt = true,
                ownsExposure = exposure,
                hasUnresolvedOrder = !confirmation.confirmed,
            )
            .thenReturn(confirmation)
    }

    private fun resolvePlacedOrder(
        pending: PendingOrder,
    ): Mono<OrderResolution> {
        val privateOutcome = pending.result.asMono()
        val afterRequest = client
            .placeOrder(pending.intent.toBinanceRequest())
            .timeout(requestTimeout, scheduler)
            .onErrorResume { Mono.empty() }
            .then(
                Mono.firstWithSignal(
                    privateOutcome,
                    reconcile(pending).then(privateOutcome),
                ),
            )
        return Mono.firstWithSignal(privateOutcome, afterRequest)
    }

    private fun reconcile(pending: PendingOrder): Mono<Void> =
        Flux
            .range(1, MAX_RECONCILIATION_CHECKS)
            .concatMap { attempt ->
                Mono
                    .delay(reconciliationInterval, scheduler)
                    .then(
                        client.reconcileOrder(
                            symbol = pending.intent.symbol,
                            clientOrderId = pending.intent.clientOrderId,
                        ),
                    )
                    .onErrorResume { error ->
                        Mono.just(
                            BinanceOrderReconciliation(
                                order = null,
                                position = null,
                                openClientOrderIds = emptySet(),
                                safeDetail = error.javaClass.simpleName,
                            ),
                        )
                    }
                    .flatMap { reconciliation ->
                        symbolCoordinator
                            .submit(
                                symbol = pending.intent.symbol,
                                eventId =
                                    "reconciliation:${pending.intent.clientOrderId}:$attempt",
                            ) {
                                handleReconciliation(
                                    pending = pending,
                                    attempt = attempt,
                                    reconciliation = reconciliation,
                                )
                            }
                            .then()
                    }
            }
            .takeUntilOther(pending.result.asMono())
            .then()

    private fun handleReconciliation(
        pending: PendingOrder,
        attempt: Int,
        reconciliation: BinanceOrderReconciliation,
    ) {
        if (pendingOrders[pending.intent.clientOrderId] !== pending) {
            return
        }
        val outcome = reconciliation.order?.let(::classifiedOutcome)
        val finalUnknown =
            outcome == null && attempt == MAX_RECONCILIATION_CHECKS
        val result = when {
            outcome != null -> outcome.name
            finalUnknown -> ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN.name
            else -> OrderOutcome.UNKNOWN.name
        }
        evidenceRecorder.recordReconciliation(
            levelId = pending.intent.levelId,
            symbol = pending.intent.symbol,
            timestamp = clock.instant(),
            reconciliation = ReconciliationEvidence(
                clientOrderId = pending.intent.clientOrderId,
                attemptNumber = attempt,
                result = result,
                exchangeOrderId = reconciliation.order?.orderId,
                requestedQuantity = pending.intent.confirmedQuantity,
                filledQuantity = reconciliation.order?.executedQuantity,
                safeDetail = reconciliation.safeDetail,
            ),
        )
        when {
            outcome != null -> complete(
                pending,
                reconciliation.order.toResolution(
                    intent = pending.intent,
                    outcome = outcome,
                    source = OrderResolutionSource.REST_RECONCILIATION,
                    confirmedPositionAmount =
                        reconciliation.position?.positionAmount
                            ?: confirmedPositionAfterFill(
                                pending.intent,
                                reconciliation.order.executedQuantity,
                            ),
                    reconciliationChecks = attempt,
                ),
            )

            finalUnknown -> {
                val resolution = OrderResolution(
                    intent = pending.intent,
                    outcome = OrderOutcome.UNKNOWN,
                    source = OrderResolutionSource.BOUNDED_UNKNOWN,
                    exchangeOrderId = reconciliation.order?.orderId,
                    actualFilledQuantity =
                        reconciliation.order?.executedQuantity
                            ?: BigDecimal.ZERO,
                    averageFilledPrice = reconciliation.order
                        ?.averagePrice
                        ?.takeIf { it.signum() > 0 },
                    confirmedPositionAmount =
                        reconciliation.position?.positionAmount
                            ?: BigDecimal.ZERO,
                    reconciliationChecks = attempt,
                    reason = ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN,
                )
                recordUnknownOutcome(resolution)
                complete(pending, resolution)
            }
        }
    }

    private fun routePrivateEvent(event: BinanceUserDataEvent) {
        when (event) {
            is BinanceUserDataEvent.OrderUpdate -> {
                if (
                    pendingOrders.containsKey(event.clientOrderId) ||
                    orders.containsKey(event.clientOrderId)
                ) {
                    submitPrivateEvent(event.symbol, "private-order:${event.orderId}") {
                        handlePrivateOrder(event)
                    }
                }
            }

            is BinanceUserDataEvent.AccountUpdate -> routeAccountUpdate(event)
            is BinanceUserDataEvent.ListenKeyExpired -> Unit
        }
    }

    private fun routeAccountUpdate(event: BinanceUserDataEvent.AccountUpdate) {
        val updatesBySymbol = event.positions.associateBy { update ->
            update.symbol.trim().uppercase()
        }
        val symbols = if (updatesBySymbol.isEmpty()) {
            activeSymbols.toSet()
        } else {
            updatesBySymbol.keys.filterTo(linkedSetOf()) { symbol ->
                symbol in activeSymbols
            }
        }
        symbols.forEach { symbol ->
            submitPrivateEvent(
                symbol = symbol,
                eventId = "private-account:${event.transactionTime.toEpochMilli()}",
            ) {
                handleAccountUpdate(event, updatesBySymbol[symbol])
            }
        }
    }

    private fun submitPrivateEvent(
        symbol: String,
        eventId: String,
        action: () -> Unit,
    ) {
        val subscription = symbolCoordinator
            .submit(symbol, eventId) {
                action()
                Unit
            }
            .subscribe({}, {})
        subscriptions.add(subscription)
    }

    private fun handlePrivateOrder(event: BinanceUserDataEvent.OrderUpdate) {
        val pending = pendingOrders[event.clientOrderId] ?: return
        val outcome = classifiedOutcome(
            status = event.orderStatus,
            actualFilledQuantity = event.accumulatedFilledQuantity,
        )
        orders.computeIfPresent(event.clientOrderId) { _, current ->
            current.copy(
                actualFilledQuantity = event.accumulatedFilledQuantity,
                outcome = outcome,
                source = outcome?.let { OrderResolutionSource.PRIVATE_STREAM },
                exchangeOrderId = event.orderId,
                updatedAt = event.receivedAt,
            )
        }
        if (outcome == null) {
            return
        }
        complete(
            pending,
            OrderResolution(
                intent = pending.intent,
                outcome = outcome,
                source = OrderResolutionSource.PRIVATE_STREAM,
                exchangeOrderId = event.orderId,
                actualFilledQuantity = event.accumulatedFilledQuantity,
                averageFilledPrice = event.averagePrice
                    .takeIf { it.signum() > 0 }
                    ?: event.lastFilledPrice.takeIf { it.signum() > 0 },
                confirmedPositionAmount =
                    positions[event.symbol]?.positionAmount
                        ?: confirmedPositionAfterFill(
                            intent = pending.intent,
                            actualFilledQuantity = event.accumulatedFilledQuantity,
                        ),
                reconciliationChecks = 0,
            ),
        )
    }

    private fun handleAccountUpdate(
        event: BinanceUserDataEvent.AccountUpdate,
        position: BinancePositionUpdate?,
    ) {
        event.balances.forEach { balance ->
            balances[balance.asset] = ExecutionBalanceSnapshot(
                asset = balance.asset,
                walletBalance = balance.walletBalance,
                updatedAt = event.receivedAt,
            )
        }
        if (position != null) {
            positions[position.symbol] = ExecutionPositionSnapshot(
                symbol = position.symbol,
                positionAmount = position.positionAmount,
                entryPrice = position.entryPrice,
                updatedAt = event.receivedAt,
            )
        }
    }

    private fun complete(
        pending: PendingOrder,
        resolution: OrderResolution,
    ) {
        pendingOrders.remove(pending.intent.clientOrderId, pending)
        orders[pending.intent.clientOrderId] =
            orders.getValue(pending.intent.clientOrderId).copy(
                actualFilledQuantity = resolution.actualFilledQuantity,
                outcome = resolution.outcome,
                source = resolution.source,
                exchangeOrderId = resolution.exchangeOrderId,
                updatedAt = clock.instant(),
                reason = resolution.reason,
            )
        positions[pending.intent.symbol] = ExecutionPositionSnapshot(
            symbol = pending.intent.symbol,
            positionAmount = resolution.confirmedPositionAmount,
            entryPrice = resolution.averageFilledPrice
                ?: positions[pending.intent.symbol]?.entryPrice
                ?: BigDecimal.ZERO,
            updatedAt = clock.instant(),
        )
        pending.result.tryEmitValue(resolution)
    }

    private fun finalizeResolution(
        resolution: OrderResolution,
        closeOnUnknown: Boolean,
    ): Mono<OrderResolution> {
        val unresolved = resolution.outcome == OrderOutcome.UNKNOWN
        val exposure = resolution.confirmedPositionAmount.signum() != 0
        val enterSafeMode = if (unresolved) {
            riskService
                .enterSafeMode(ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN.name)
                .then()
        } else {
            Mono.empty()
        }
        return enterSafeMode
            .then(
                symbolCoordinator.recordOwnership(
                    levelId = resolution.intent.levelId,
                    ownsActiveAttempt = exposure || unresolved,
                    ownsExposure = exposure,
                    hasUnresolvedOrder =
                        unresolved || hasUnresolvedOrder(resolution.intent.levelId),
                ),
            )
            .doOnSuccess {
                if (unresolved && closeOnUnknown && exposure) {
                    startUnknownOutcomeClose(resolution)
                }
            }
            .thenReturn(resolution)
    }

    private fun startUnknownOutcomeClose(resolution: OrderResolution) {
        val positionAmount = resolution.confirmedPositionAmount
        val request = OrderIntentRequest(
            levelId = resolution.intent.levelId,
            attemptNumber = resolution.intent.attemptNumber,
            symbol = resolution.intent.symbol,
            role = OrderRole.UNKNOWN_OUTCOME_CLOSE,
            slot = resolution.intent.slot,
            side = if (positionAmount.signum() > 0) {
                OrderSide.SELL
            } else {
                OrderSide.BUY
            },
            type = OrderType.MARKET,
            confirmedQuantity = positionAmount.abs(),
            reduceOnly = true,
            confirmedPositionAmount = positionAmount,
        )
        val closeIntent = clientOrderIdFactory.create(request)
        val subscription: Disposable = dispatch(
            intent = closeIntent,
            closeOnUnknown = false,
        )
            .flatMap { closeResolution ->
                val remainingQuantity =
                    closeResolution.confirmedPositionAmount.abs()
                when {
                    remainingQuantity.signum() == 0 ->
                        riskService.recordConfirmedFlat(
                            closeResolution.intent.levelId,
                        )

                    remainingQuantity < positionAmount.abs() ->
                        riskService.recordConfirmedReducingFill(
                            levelId = closeResolution.intent.levelId,
                            confirmedRemainingQuantity = remainingQuantity,
                        )

                    else -> Mono.just(riskService.currentState())
                }
            }
            .subscribe({}, {})
        subscriptions.add(subscription)
    }

    private fun hasUnresolvedOrder(levelId: UUID): Boolean =
        pendingOrders.values.any { pending -> pending.intent.levelId == levelId } ||
            orders.values.any { order ->
                order.levelId == levelId && order.outcome == OrderOutcome.UNKNOWN
            }

    private fun recordUnknownOutcome(resolution: OrderResolution) {
        evidenceRecorder.recordAudit(
            AuditRecordDraft(
                timestamp = clock.instant(),
                symbol = resolution.intent.symbol,
                levelId = resolution.intent.levelId,
                stateBefore = null,
                stateAfter = null,
                eventType = AuditEventType.DECISION,
                decision = ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN.name,
                blockerReasons = listOf(
                    ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN.name,
                ),
                recoveryDetail = if (
                    resolution.confirmedPositionAmount.signum() == 0
                ) {
                    "No exposure was confirmed by bounded reconciliation"
                } else {
                    "Closing only exposure confirmed by bounded reconciliation"
                },
                evidence = DecisionEvidence(
                    quantity = QuantityEvidence(
                        requestedQuantity = resolution.intent.confirmedQuantity,
                        filledQuantity = resolution.actualFilledQuantity,
                        remainingQuantity =
                            resolution.confirmedPositionAmount.abs(),
                    ),
                    order = resolution.intent.evidence(
                        status = resolution.outcome.name,
                        filledQuantity = resolution.actualFilledQuantity,
                        exchangeOrderId = resolution.exchangeOrderId,
                    ),
                ),
            ),
        )
    }

    private fun classifiedOutcome(order: BinanceOrderStatus): OrderOutcome? =
        classifiedOutcome(order.status, order.executedQuantity)

    private fun classifiedOutcome(
        status: String,
        actualFilledQuantity: BigDecimal,
    ): OrderOutcome? =
        when (status.uppercase()) {
            "FILLED" -> OrderOutcome.FILLED
            "PARTIALLY_FILLED" -> OrderOutcome.PARTIALLY_FILLED
            "REJECTED" -> if (actualFilledQuantity.signum() > 0) {
                OrderOutcome.PARTIALLY_FILLED
            } else {
                OrderOutcome.REJECTED
            }

            "CANCELED",
            "CANCELLED",
            "EXPIRED",
            "EXPIRED_IN_MATCH",
            -> if (actualFilledQuantity.signum() > 0) {
                OrderOutcome.PARTIALLY_FILLED
            } else {
                OrderOutcome.CANCELED
            }

            else -> null
        }

    private fun BinanceOrderStatus.toResolution(
        intent: OrderIntent,
        outcome: OrderOutcome,
        source: OrderResolutionSource,
        confirmedPositionAmount: BigDecimal,
        reconciliationChecks: Int,
    ): OrderResolution =
        OrderResolution(
            intent = intent,
            outcome = outcome,
            source = source,
            exchangeOrderId = orderId,
            actualFilledQuantity = executedQuantity,
            averageFilledPrice = averagePrice.takeIf { it.signum() > 0 },
            confirmedPositionAmount = confirmedPositionAmount,
            reconciliationChecks = reconciliationChecks,
        )

    private fun confirmedPositionAfterFill(
        intent: OrderIntent,
        actualFilledQuantity: BigDecimal,
    ): BigDecimal {
        if (!intent.role.closesExposure) {
            val signedFill = when (intent.side) {
                OrderSide.BUY -> actualFilledQuantity
                OrderSide.SELL -> actualFilledQuantity.negate()
            }
            return intent.confirmedPositionAmount
                ?.add(signedFill)
                ?: signedFill
        }
        val originalPosition = checkNotNull(intent.confirmedPositionAmount)
        val remaining = originalPosition.abs()
            .subtract(actualFilledQuantity)
            .max(BigDecimal.ZERO)
        return if (originalPosition.signum() > 0) remaining else remaining.negate()
    }

    private fun OrderIntent.toBinanceRequest(): BinanceOrderRequest =
        BinanceOrderRequest(
            symbol = symbol,
            clientOrderId = clientOrderId,
            side = side.name,
            type = type.name,
            timeInForce = timeInForce?.name,
            quantity = confirmedQuantity,
            price = price,
            stopPrice = stopPrice,
            workingType = workingType?.name,
            priceProtect = priceProtect,
            reduceOnly = reduceOnly,
            closePosition = closePosition,
        )

    private fun OrderIntent.snapshot(now: java.time.Instant): OrderExecutionSnapshot =
        OrderExecutionSnapshot(
            intentSequence = intentSequence,
            clientOrderId = clientOrderId,
            levelId = levelId,
            attemptNumber = attemptNumber,
            symbol = symbol,
            role = role,
            slot = slot,
            requestedQuantity = confirmedQuantity,
            stopPrice = stopPrice,
            workingType = workingType,
            priceProtect = priceProtect,
            actualFilledQuantity = BigDecimal.ZERO,
            outcome = null,
            source = null,
            exchangeOrderId = null,
            updatedAt = now,
            reason = null,
        )

    private fun OrderIntent.evidence(
        status: String? = null,
        filledQuantity: BigDecimal? = null,
        exchangeOrderId: Long? = null,
    ): OrderEvidence =
        OrderEvidence(
            intentId = intentSequence.toString(),
            clientOrderId = clientOrderId,
            exchangeOrderId = exchangeOrderId,
            role = role.name,
            side = side.name,
            type = type.name,
            timeInForce = timeInForce?.name,
            requestedPrice = price,
            requestedQuantity = confirmedQuantity,
            filledQuantity = filledQuantity,
            averageFilledPrice = null,
            stopPrice = stopPrice,
            workingType = workingType?.name,
            priceProtect = priceProtect,
            closePosition = closePosition,
            status = status,
            reduceOnly = reduceOnly,
        )
}

private data class PendingOrder(
    val intent: OrderIntent,
    val result: Sinks.One<OrderResolution> = Sinks.one(),
)

private data class HardStopEvaluation(
    val terminal: Boolean,
    val confirmation: HardStopConfirmation,
)

private const val MAX_RECONCILIATION_CHECKS = 3
private const val MAX_STOP_RECONCILIATION_CHECKS = 50
private val MAX_STOP_CHECK_INTERVAL: Duration = Duration.ofMillis(250)
private val STOP_TERMINAL_STATUSES = setOf(
    "CANCELED",
    "CANCELLED",
    "EXPIRED",
    "EXPIRED_IN_MATCH",
    "FILLED",
    "REJECTED",
)
