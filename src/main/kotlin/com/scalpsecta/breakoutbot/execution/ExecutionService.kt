package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.binance.BinanceAccountReconciliation
import com.scalpsecta.breakoutbot.binance.BinanceExecutionClient
import com.scalpsecta.breakoutbot.binance.BinanceOrderReconciliation
import com.scalpsecta.breakoutbot.binance.BinanceOrderRequest
import com.scalpsecta.breakoutbot.binance.BinanceOrderStatus
import com.scalpsecta.breakoutbot.binance.BinancePositionUpdate
import com.scalpsecta.breakoutbot.binance.BinancePositionRisk
import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.evidence.AuditEventType
import com.scalpsecta.breakoutbot.evidence.AuditRecordDraft
import com.scalpsecta.breakoutbot.evidence.DecisionEvidence
import com.scalpsecta.breakoutbot.evidence.EvidenceRecorder
import com.scalpsecta.breakoutbot.evidence.OrderEvidence
import com.scalpsecta.breakoutbot.evidence.QuantityEvidence
import com.scalpsecta.breakoutbot.evidence.ReconciliationEvidence
import com.scalpsecta.breakoutbot.failure.RequiredDataHealthGate
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.level.PositionNetResult
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
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Duration
import java.time.Instant
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
    private val entryDataHealthy: (String) -> Boolean = { true },
) : PreEntryOrderExecutor, BreakoutOrderExecutor {
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
        requiredDataHealthGate: RequiredDataHealthGate,
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
        entryDataHealthy = requiredDataHealthGate::entriesAndAdditionsAllowed,
    )

    private val pendingOrders = ConcurrentHashMap<String, PendingOrder>()
    private val intents = ConcurrentHashMap<String, OrderIntent>()
    private val orders = ConcurrentHashMap<String, OrderExecutionSnapshot>()
    private val positions = ConcurrentHashMap<String, ExecutionPositionSnapshot>()
    private val balances = ConcurrentHashMap<String, ExecutionBalanceSnapshot>()
    private val activeSymbols = ConcurrentHashMap.newKeySet<String>()
    private val activeTakeProfitSets =
        ConcurrentHashMap<UUID, ActiveTakeProfitSet>()
    private val activeHardStops = ConcurrentHashMap<UUID, ActiveHardStop>()
    private val attemptAccounting = ConcurrentHashMap<UUID, AttemptAccounting>()
    private val directCloseOperations =
        ConcurrentHashMap<String, Mono<OrderResolution>>()
    private val positionReductionSink = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer<PositionReduction>()
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
        return dispatch(
            intent,
            closeOnUnknown = !intent.role.closesExposure,
        ).cache()
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
                        activeHardStops[intent.levelId] = ActiveHardStop(intent)
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

    override fun reconcilePosition(
        symbol: String,
        clientOrderId: String,
    ): Mono<BigDecimal> = client
        .reconcileOrder(symbol, clientOrderId)
        .timeout(requestTimeout, scheduler)
        .flatMap { reconciliation ->
            val position = reconciliation.position
                ?: return@flatMap Mono.error(
                    OrderExecutionException(
                        "Binance position reconciliation returned no position",
                    ),
                )
            positions[position.symbol] = position.snapshot(
                observedAt = clock.instant(),
                previous = positions[position.symbol],
            )
            Mono.just(position.positionAmount)
        }

    fun reconcileRuntime(): Mono<ExecutionRuntimeReconciliation> =
        client
            .reconcileAccount()
            .timeout(requestTimeout, scheduler)
            .flatMap(::resolveUnknownOrdersFromReconciliation)

    fun closeAccountPositions(
        reconciledPositions: List<BinancePositionRisk>,
        operationId: String,
    ): Mono<List<OrderResolution>> {
        require(operationId.isNotBlank()) { "operationId must not be blank" }
        return Flux
            .fromIterable(
                reconciledPositions
                    .filter { position -> position.positionAmount.signum() != 0 }
                    .sortedBy(BinancePositionRisk::symbol),
            )
            .concatMap { position ->
                val operationKey = "$operationId:${position.symbol}"
                directCloseOperations.computeIfAbsent(operationKey) {
                    directMarketClose(position, operationId).cache()
                }
            }
            .collectList()
    }

    fun cancelBotOrders(
        openOrders: List<BinanceOrderStatus>,
        retainHardStops: Boolean,
    ): Mono<Boolean> {
        val activeHardStopIds = if (retainHardStops) {
            activeHardStops.values
                .mapTo(mutableSetOf()) { stop -> stop.intent.clientOrderId }
        } else {
            emptySet()
        }
        return Flux
            .fromIterable(
                openOrders
                    .filter { order ->
                        order.clientOrderId.matches(BOT_CLIENT_ORDER_ID) &&
                            order.clientOrderId !in activeHardStopIds
                    }
                    .sortedBy(BinanceOrderStatus::clientOrderId),
            )
            .concatMap { order ->
                client
                    .cancelOrder(order.symbol, order.clientOrderId)
                    .timeout(requestTimeout, scheduler)
                    .thenReturn(true)
                    .onErrorReturn(false)
            }
            .all { canceled -> canceled }
    }

    override fun confirmTakeProfits(
        requests: List<OrderIntentRequest>,
        timeout: Duration,
    ): Mono<TakeProfitSetConfirmation> {
        require(!timeout.isZero && !timeout.isNegative) {
            "Take-profit confirmation timeout must be positive"
        }
        val intents = requests.map(clientOrderIdFactory::create)
        validateTakeProfitIntents(intents)
        return Mono.defer {
            val positionAmount = checkNotNull(
                intents.first().confirmedPositionAmount,
            )
            symbolCoordinator
                .recordOwnership(
                    levelId = intents.first().levelId,
                    ownsActiveAttempt = true,
                    ownsExposure = true,
                    hasUnresolvedOrder = true,
                )
                .then(
                    symbolCoordinator.submit(
                        symbol = intents.first().symbol,
                        eventId =
                            "take-profit-intents:${intents.first().levelId}",
                    ) {
                        intents.forEach(::recordIntent)
                        activeTakeProfitSets[intents.first().levelId] =
                            ActiveTakeProfitSet(
                                levelId = intents.first().levelId,
                                symbol = intents.first().symbol,
                                initialPositionAmount = positionAmount,
                                intents = intents,
                            )
                    },
                )
                .then(
                    placeAndConfirmTakeProfits(
                        intents = intents,
                        positionAmount = positionAmount,
                        timeout = timeout,
                    ),
                )
                .flatMap(::finalizeTakeProfitConfirmation)
        }.cache()
    }

    override fun cancelTakeProfits(
        intents: List<OrderIntent>,
    ): Mono<Boolean> {
        if (intents.isEmpty()) {
            return Mono.just(true)
        }
        return Flux
            .fromIterable(intents)
            .concatMap { intent ->
                client
                    .cancelOrder(intent.symbol, intent.clientOrderId)
                    .timeout(requestTimeout, scheduler)
                    .then(
                        symbolCoordinator.submit(
                            symbol = intent.symbol,
                            eventId =
                                "take-profit-canceled:${intent.clientOrderId}",
                        ) {
                            orders.computeIfPresent(
                                intent.clientOrderId,
                            ) { _, current ->
                                current.copy(
                                    outcome = OrderOutcome.CANCELED,
                                    updatedAt = clock.instant(),
                                    reason = null,
                                )
                            }
                            Unit
                        },
                    )
                    .thenReturn(true)
                    .onErrorReturn(false)
            }
            .collectList()
            .map { results -> results.all { result -> result } }
            .doOnNext { complete ->
                if (complete) {
                    activeTakeProfitSets.remove(intents.first().levelId)
                }
            }
    }

    override fun cancelActiveTakeProfits(levelId: UUID): Mono<Boolean> {
        val activeSet = activeTakeProfitSets[levelId] ?: return Mono.just(true)
        return cancelTakeProfits(activeSet.intents)
    }

    override fun cancelActiveHardStop(levelId: UUID): Mono<Boolean> {
        val activeStop = activeHardStops[levelId] ?: return Mono.just(true)
        return client
            .cancelOrder(activeStop.intent.symbol, activeStop.intent.clientOrderId)
            .timeout(requestTimeout, scheduler)
            .thenReturn(true)
            .onErrorReturn(false)
            .doOnNext { complete ->
                if (complete) {
                    activeHardStops.remove(levelId, activeStop)
                    orders.computeIfPresent(activeStop.intent.clientOrderId) {
                            _, current,
                        ->
                        current.copy(
                            outcome = OrderOutcome.CANCELED,
                            updatedAt = clock.instant(),
                        )
                    }
                }
            }
    }

    override fun reconcilePositionAfter(
        symbol: String,
        clientOrderId: String,
        wait: Duration,
    ): Mono<BigDecimal> {
        require(!wait.isNegative) { "Position reconciliation wait must not be negative" }
        return Mono.delay(wait, scheduler)
            .then(reconcilePosition(symbol, clientOrderId))
    }

    override fun executeNormalExit(
        request: OrderIntentRequest,
        wait: Duration,
    ): Mono<NormalExitResolution> {
        require(!wait.isZero && !wait.isNegative) {
            "Normal exit wait must be positive"
        }
        require(
            request.role == OrderRole.CLOSE &&
                request.type == OrderType.LIMIT &&
                request.timeInForce == OrderTimeInForce.IOC &&
                request.reduceOnly,
        ) {
            "Normal exit requires one reduce-only LIMIT IOC close"
        }
        val intent = clientOrderIdFactory.create(request)
        return Mono.defer {
            val pending = PendingOrder(intent)
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
                        eventId = "normal-exit-intent:${intent.clientOrderId}",
                    ) {
                        register(pending)
                    },
                )
                .then(placeNormalExitAndWait(intent, wait))
                .then(
                    client
                        .reconcileOrder(intent.symbol, intent.clientOrderId)
                        .timeout(requestTimeout, scheduler)
                        .onErrorResume { error ->
                            Mono.just(
                                BinanceOrderReconciliation(
                                    order = null,
                                    position = null,
                                    openClientOrderIds = emptySet(),
                                    safeDetail = error.javaClass.simpleName,
                                ),
                            )
                        },
                )
                .flatMap { reconciliation ->
                    finalizeNormalExit(pending, reconciliation)
                }
        }.cache()
    }

    private fun placeNormalExitAndWait(
        intent: OrderIntent,
        wait: Duration,
    ): Mono<Void> {
        val placementTimeout = minOf(requestTimeout, wait)
        val placement = client
            .placeOrder(intent.toBinanceRequest())
            .timeout(placementTimeout, scheduler)
            .onErrorResume { Mono.empty() }
            .then()
        val waitForFills = Mono.delay(wait, scheduler).then()
        return Mono.`when`(placement, waitForFills)
    }

    private fun finalizeNormalExit(
        pending: PendingOrder,
        reconciliation: BinanceOrderReconciliation,
    ): Mono<NormalExitResolution> {
        val intent = pending.intent
        val reconciledOrder = reconciliation.order
        val terminalOutcome = reconciledOrder?.let(::classifiedOutcome)
        val outcome = terminalOutcome ?: orders[intent.clientOrderId]
            ?.outcome
            ?.takeIf { candidate ->
                candidate != OrderOutcome.ACTIVE
            } ?: OrderOutcome.UNKNOWN
        val confirmedPositionAmount = reconciliation.position
            ?.positionAmount
            ?: reconciledOrder?.let { order ->
                confirmedPositionAfterFill(intent, order.executedQuantity)
            }
            ?: positions[intent.symbol]?.positionAmount
            ?: checkNotNull(intent.confirmedPositionAmount)
        val unresolved =
            outcome == OrderOutcome.UNKNOWN ||
                intent.clientOrderId in reconciliation.openClientOrderIds
        pendingOrders.remove(intent.clientOrderId, pending)
        pending.result.tryEmitValue(
            OrderResolution(
                intent = intent,
                outcome = outcome,
                source = OrderResolutionSource.REST_RECONCILIATION,
                exchangeOrderId = reconciledOrder?.orderId,
                actualFilledQuantity =
                    reconciledOrder?.executedQuantity ?: BigDecimal.ZERO,
                averageFilledPrice = reconciledOrder
                    ?.averagePrice
                    ?.takeIf { it.signum() > 0 },
                confirmedPositionAmount = confirmedPositionAmount,
                reconciliationChecks = 1,
                reason = if (unresolved) {
                    ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN
                } else {
                    null
                },
            ),
        )
        reconciliation.position?.let { position ->
            positions[position.symbol] = position.snapshot(
                observedAt = clock.instant(),
                previous = positions[position.symbol],
            )
        }
        orders.computeIfPresent(intent.clientOrderId) { _, current ->
            current.copy(
                actualFilledQuantity =
                    reconciledOrder?.executedQuantity
                        ?: current.actualFilledQuantity,
                outcome = outcome,
                source = OrderResolutionSource.REST_RECONCILIATION,
                exchangeOrderId = reconciledOrder?.orderId
                    ?: current.exchangeOrderId,
                updatedAt = clock.instant(),
                reason = if (unresolved) {
                    ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN
                } else {
                    null
                },
            )
        }
        evidenceRecorder.recordReconciliation(
            levelId = intent.levelId,
            symbol = intent.symbol,
            timestamp = clock.instant(),
            reconciliation = ReconciliationEvidence(
                clientOrderId = intent.clientOrderId,
                attemptNumber = 1,
                result = if (unresolved) {
                    ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN.name
                } else {
                    outcome.name
                },
                exchangeOrderId = reconciledOrder?.orderId,
                requestedQuantity = intent.confirmedQuantity,
                filledQuantity = reconciledOrder?.executedQuantity,
                safeDetail = reconciliation.safeDetail,
            ),
        )
        val safeMode = if (unresolved) {
            riskService
                .enterSafeMode(ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN.name)
                .then()
        } else {
            Mono.empty()
        }
        val exposure = confirmedPositionAmount.signum() != 0
        return safeMode
            .then(
                symbolCoordinator.recordOwnership(
                    levelId = intent.levelId,
                    ownsActiveAttempt = exposure || unresolved,
                    ownsExposure = exposure,
                    hasUnresolvedOrder = unresolved,
                ),
            )
            .thenReturn(
                NormalExitResolution(
                    intent = intent,
                    outcome = outcome,
                    confirmedPositionAmount = confirmedPositionAmount,
                    hasUnresolvedOrder = unresolved,
                ),
            )
    }

    override fun activateTakeProfits(
        confirmation: TakeProfitSetConfirmation,
    ): Mono<Void> {
        require(confirmation.confirmed) {
            "Only a confirmed take-profit set can be activated"
        }
        val first = confirmation.intents.first()
        return symbolCoordinator
            .submit(
                symbol = first.symbol,
                eventId = "take-profits-active:${first.levelId}",
            ) {
                val activeSet = checkNotNull(
                    activeTakeProfitSets[first.levelId],
                ) {
                    "Confirmed take-profit set is not registered"
                }
                activeSet.activated = true
                publishTakeProfitFill(
                    activeSet = activeSet,
                    clientOrderId = first.clientOrderId,
                    updatedAt = clock.instant(),
                )
            }
            .then()
    }

    override fun positionReductions(): Flux<PositionReduction> =
        positionReductionSink.asFlux()

    override fun positionResult(levelId: UUID): PositionNetResult? =
        attemptAccounting[levelId]?.snapshot()

    fun currentState(): ExecutionSnapshot =
        ExecutionSnapshot(
            observedAt = clock.instant(),
            entriesAndAdditionsBlocked =
                riskService.currentState().globalTradingState !=
                    GlobalTradingState.RUNNING ||
                    activeSymbols.any { symbol -> !entryDataHealthy(symbol) } ||
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
        positionReductionSink.tryEmitComplete()
        activeTakeProfitSets.clear()
        activeHardStops.clear()
        attemptAccounting.clear()
        directCloseOperations.clear()
        intents.clear()
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
                (
                    riskService.currentState().globalTradingState !=
                        GlobalTradingState.RUNNING ||
                        !entryDataHealthy(intent.symbol)
                    )
            ) {
                return@defer Mono.error(
                    OrderExecutionException(
                        "Entries and additions are blocked by runtime health",
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

    private fun resolveUnknownOrdersFromReconciliation(
        account: BinanceAccountReconciliation,
    ): Mono<ExecutionRuntimeReconciliation> {
        val reconciledPositions = ConcurrentHashMap<String, BinancePositionRisk>()
        account.positions.forEach { position ->
            reconciledPositions[position.symbol] = position
            positions[position.symbol] = position.snapshot(
                observedAt = clock.instant(),
                previous = positions[position.symbol],
            )
        }
        val unknownOrders = orders.values
            .filter { order -> order.outcome == OrderOutcome.UNKNOWN }
            .sortedBy(OrderExecutionSnapshot::intentSequence)
        return Flux
            .fromIterable(unknownOrders)
            .concatMap { unknown ->
                client
                    .reconcileOrder(unknown.symbol, unknown.clientOrderId)
                    .timeout(requestTimeout, scheduler)
                    .doOnNext { reconciliation ->
                        reconciliation.position?.let { position ->
                            reconciledPositions[position.symbol] = position
                            positions[position.symbol] = position.snapshot(
                                observedAt = clock.instant(),
                                previous = positions[position.symbol],
                            )
                        }
                        val order = reconciliation.order ?: return@doOnNext
                        val outcome = classifiedOutcome(order) ?: return@doOnNext
                        orders.computeIfPresent(unknown.clientOrderId) { _, current ->
                            current.copy(
                                actualFilledQuantity = order.executedQuantity,
                                outcome = outcome,
                                source = OrderResolutionSource.REST_RECONCILIATION,
                                exchangeOrderId = order.orderId,
                                updatedAt = clock.instant(),
                                reason = null,
                            )
                        }
                    }
                    .onErrorResume { Mono.empty() }
            }
            .then(
                Mono.fromCallable {
                    val openBotOrders = account.openOrders.filter { order ->
                        order.clientOrderId.matches(BOT_CLIENT_ORDER_ID)
                    }
                    ExecutionRuntimeReconciliation(
                        observedAt = clock.instant(),
                        positions = reconciledPositions.values
                            .filter { position -> position.positionAmount.signum() != 0 }
                            .sortedBy(BinancePositionRisk::symbol),
                        openBotOrders = openBotOrders
                            .sortedBy(BinanceOrderStatus::clientOrderId),
                        orphanedBotOrderIds = openBotOrders
                            .map(BinanceOrderStatus::clientOrderId)
                            .filterTo(sortedSetOf()) { clientOrderId ->
                                !intents.containsKey(clientOrderId)
                            },
                        unresolvedOrderIds = buildSet {
                            addAll(pendingOrders.keys)
                            orders.values
                                .filter { order -> order.outcome == OrderOutcome.UNKNOWN }
                                .mapTo(this, OrderExecutionSnapshot::clientOrderId)
                        },
                    )
                },
            )
    }

    private fun directMarketClose(
        position: BinancePositionRisk,
        operationId: String,
    ): Mono<OrderResolution> {
        val levelId = UUID.nameUUIDFromBytes(
            "$operationId:${position.symbol}"
                .toByteArray(StandardCharsets.UTF_8),
        )
        val intent = clientOrderIdFactory.create(
            OrderIntentRequest(
                levelId = levelId,
                attemptNumber = 1,
                symbol = position.symbol,
                role = OrderRole.SAFE_MODE_CLOSE,
                slot = 0,
                side = if (position.positionAmount.signum() > 0) {
                    OrderSide.SELL
                } else {
                    OrderSide.BUY
                },
                type = OrderType.MARKET,
                confirmedQuantity = position.positionAmount.abs(),
                reduceOnly = true,
                confirmedPositionAmount = position.positionAmount,
            ),
        )
        return Mono.fromRunnable<Void> { recordIntent(intent) }
            .then(
                client
                    .placeOrder(intent.toBinanceRequest())
                    .timeout(requestTimeout, scheduler)
                    .onErrorResume { Mono.empty() }
                    .then(),
            )
            .then(reconcileDirectClose(intent, position))
    }

    private fun reconcileDirectClose(
        intent: OrderIntent,
        originalPosition: BinancePositionRisk,
    ): Mono<OrderResolution> =
        Flux
            .range(1, MAX_RECONCILIATION_CHECKS)
            .concatMap { attempt ->
                val delay = if (attempt == 1) Duration.ZERO else reconciliationInterval
                Mono
                    .delay(delay, scheduler)
                    .then(client.reconcileOrder(intent.symbol, intent.clientOrderId))
                    .timeout(requestTimeout, scheduler)
                    .onErrorReturn(
                        BinanceOrderReconciliation(
                            order = null,
                            position = null,
                            openClientOrderIds = emptySet(),
                            safeDetail = "RECONCILIATION_FAILED",
                        ),
                    )
                    .map { reconciliation ->
                        val position = reconciliation.position ?: originalPosition
                        val outcome = reconciliation.order
                            ?.let(::classifiedOutcome)
                            ?: if (position.positionAmount.signum() == 0) {
                                OrderOutcome.FILLED
                            } else {
                                OrderOutcome.UNKNOWN
                            }
                        val terminal =
                            outcome != OrderOutcome.UNKNOWN ||
                                attempt == MAX_RECONCILIATION_CHECKS
                        DirectCloseCheck(
                            terminal = terminal,
                            resolution = OrderResolution(
                                intent = intent,
                                outcome = outcome,
                                source = OrderResolutionSource.REST_RECONCILIATION,
                                exchangeOrderId = reconciliation.order?.orderId,
                                actualFilledQuantity = reconciliation.order
                                    ?.executedQuantity ?: BigDecimal.ZERO,
                                averageFilledPrice = reconciliation.order
                                    ?.averagePrice
                                    ?.takeIf { price -> price.signum() > 0 },
                                confirmedPositionAmount = position.positionAmount,
                                reconciliationChecks = attempt,
                                reason = if (outcome == OrderOutcome.UNKNOWN) {
                                    ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN
                                } else {
                                    null
                                },
                            ),
                            position = position,
                        )
                    }
            }
            .filter(DirectCloseCheck::terminal)
            .next()
            .map { check ->
                val resolution = check.resolution
                positions[intent.symbol] = check.position.snapshot(
                    observedAt = clock.instant(),
                    previous = positions[intent.symbol],
                )
                orders[intent.clientOrderId] = orders.getValue(intent.clientOrderId).copy(
                    actualFilledQuantity = resolution.actualFilledQuantity,
                    outcome = resolution.outcome,
                    source = resolution.source,
                    exchangeOrderId = resolution.exchangeOrderId,
                    updatedAt = clock.instant(),
                    reason = resolution.reason,
                )
                resolution
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
        intents[intent.clientOrderId] = intent
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
            positions[intent.symbol] = position.snapshot(
                observedAt = clock.instant(),
                previous = positions[intent.symbol],
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
        if (!confirmation.confirmed) {
            activeHardStops.remove(intent.levelId)
        }
        return symbolCoordinator
            .recordOwnership(
                levelId = intent.levelId,
                ownsActiveAttempt = true,
                ownsExposure = exposure,
                hasUnresolvedOrder = !confirmation.confirmed,
            )
            .thenReturn(confirmation)
    }

    private fun placeAndConfirmTakeProfits(
        intents: List<OrderIntent>,
        positionAmount: BigDecimal,
        timeout: Duration,
    ): Mono<TakeProfitSetConfirmation> {
        val checks = AtomicInteger()
        val lastObservation =
            AtomicReference<TakeProfitSetConfirmation?>()
        val placement = Flux
            .fromIterable(intents)
            .flatMap(
                { intent ->
                    client
                        .placeOrder(intent.toBinanceRequest())
                        .timeout(requestTimeout, scheduler)
                        .onErrorResume { Mono.empty() }
                },
                TAKE_PROFIT_COUNT,
            )
            .then()
        return placement
            .then(
                reconcileTakeProfits(
                    intents = intents,
                    positionAmount = positionAmount,
                    timeout = timeout,
                    checks = checks,
                    lastObservation = lastObservation,
                ),
            )
            .timeout(timeout, scheduler)
            .onErrorResume {
                Mono.just(
                    lastObservation.get()?.copy(confirmed = false)
                        ?: TakeProfitSetConfirmation(
                            intents = intents,
                            confirmed = false,
                            confirmedPositionAmount = positionAmount,
                            reconciliationChecks = checks.get(),
                        ),
                )
            }
    }

    private fun reconcileTakeProfits(
        intents: List<OrderIntent>,
        positionAmount: BigDecimal,
        timeout: Duration,
        checks: AtomicInteger,
        lastObservation: AtomicReference<TakeProfitSetConfirmation?>,
    ): Mono<TakeProfitSetConfirmation> {
        val interval = minOf(
            reconciliationInterval,
            MAX_TAKE_PROFIT_CHECK_INTERVAL,
        )
        val maximumChecks = (
            timeout.toMillis() /
                interval.toMillis().coerceAtLeast(1L) +
                1L
            ).coerceAtMost(
            MAX_TAKE_PROFIT_RECONCILIATION_CHECKS.toLong(),
        ).toInt()
        return Flux
            .range(1, maximumChecks)
            .concatMap { attempt ->
                val delay = if (attempt == 1) Duration.ZERO else interval
                Mono
                    .delay(delay, scheduler)
                    .thenMany(Flux.fromIterable(intents))
                    .concatMap { intent ->
                        client
                            .reconcileOrder(
                                symbol = intent.symbol,
                                clientOrderId = intent.clientOrderId,
                            )
                            .onErrorResume { error ->
                                Mono.just(
                                    BinanceOrderReconciliation(
                                        order = null,
                                        position = null,
                                        openClientOrderIds = emptySet(),
                                        safeDetail =
                                            error.javaClass.simpleName,
                                    ),
                                )
                            }
                    }
                    .collectList()
                    .flatMap { reconciliations ->
                        checks.set(attempt)
                        val evaluation = evaluateTakeProfits(
                            intents = intents,
                            attempt = attempt,
                            reconciliations = reconciliations,
                            fallbackPositionAmount = positionAmount,
                        )
                        lastObservation.set(evaluation.confirmation)
                        symbolCoordinator
                            .submit(
                                symbol = intents.first().symbol,
                                eventId =
                                    "take-profit-reconciliation:${intents.first().levelId}:$attempt",
                            ) {
                                recordTakeProfitReconciliation(
                                    intents = intents,
                                    attempt = attempt,
                                    reconciliations = reconciliations,
                                    evaluation = evaluation,
                                )
                            }
                            .thenReturn(evaluation)
                    }
            }
            .filter(TakeProfitEvaluation::terminal)
            .next()
            .map(TakeProfitEvaluation::confirmation)
            .switchIfEmpty(Mono.never())
    }

    private fun evaluateTakeProfits(
        intents: List<OrderIntent>,
        attempt: Int,
        reconciliations: List<BinanceOrderReconciliation>,
        fallbackPositionAmount: BigDecimal,
    ): TakeProfitEvaluation {
        val observedPositionAmount = reconciliations
            .asReversed()
            .firstNotNullOfOrNull { reconciliation ->
                reconciliation.position?.positionAmount
            }
        val confirmed = reconciliations.all { reconciliation ->
            reconciliation.position?.positionAmount?.compareTo(
                fallbackPositionAmount,
            ) == 0
        } &&
            intents.size == TAKE_PROFIT_COUNT &&
            reconciliations.size == intents.size &&
            intents.zip(reconciliations).all { (intent, reconciliation) ->
                takeProfitMatches(intent, reconciliation)
            }
        val definitivelyFailed = reconciliations.any { reconciliation ->
            reconciliation.order?.status?.uppercase() in
                TAKE_PROFIT_TERMINAL_STATUSES
        }
        val positionAmount = observedPositionAmount ?: fallbackPositionAmount
        return TakeProfitEvaluation(
            terminal = confirmed || definitivelyFailed,
            confirmation = TakeProfitSetConfirmation(
                intents = intents,
                confirmed = confirmed,
                confirmedPositionAmount = positionAmount,
                reconciliationChecks = attempt,
            ),
        )
    }

    private fun takeProfitMatches(
        intent: OrderIntent,
        reconciliation: BinanceOrderReconciliation,
    ): Boolean {
        val order = reconciliation.order ?: return false
        return order.status.equals("NEW", ignoreCase = true) &&
            order.clientOrderId == intent.clientOrderId &&
            order.clientOrderId in reconciliation.openClientOrderIds &&
            order.type.equals(OrderType.LIMIT.name, ignoreCase = true) &&
            order.side.equals(intent.side.name, ignoreCase = true) &&
            order.timeInForce.equals(
                OrderTimeInForce.GTC.name,
                ignoreCase = true,
            ) &&
            order.originalQuantity.compareTo(
                checkNotNull(intent.confirmedQuantity),
            ) == 0 &&
            order.price?.compareTo(checkNotNull(intent.price)) == 0 &&
            order.reduceOnly &&
            !order.closePosition
    }

    private fun recordTakeProfitReconciliation(
        intents: List<OrderIntent>,
        attempt: Int,
        reconciliations: List<BinanceOrderReconciliation>,
        evaluation: TakeProfitEvaluation,
    ) {
        reconciliations.firstNotNullOfOrNull { reconciliation ->
            reconciliation.position
        }?.let { position ->
            positions[position.symbol] = position.snapshot(
                observedAt = clock.instant(),
                previous = positions[position.symbol],
            )
        }
        intents.zip(reconciliations).forEach { (intent, reconciliation) ->
            val result = when {
                evaluation.confirmation.confirmed -> OrderOutcome.ACTIVE.name
                evaluation.terminal ->
                    ExecutionReasonCode.TP_SETUP_FAILED.name

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
                    requestedQuantity = intent.confirmedQuantity,
                    filledQuantity =
                        reconciliation.order?.executedQuantity,
                    safeDetail = reconciliation.safeDetail,
                ),
            )
        }
    }

    private fun finalizeTakeProfitConfirmation(
        confirmation: TakeProfitSetConfirmation,
    ): Mono<TakeProfitSetConfirmation> {
        confirmation.intents.forEach { intent ->
            orders.computeIfPresent(intent.clientOrderId) { _, current ->
                current.copy(
                    outcome = if (confirmation.confirmed) {
                        OrderOutcome.ACTIVE
                    } else {
                        OrderOutcome.UNKNOWN
                    },
                    source = OrderResolutionSource.REST_RECONCILIATION,
                    updatedAt = clock.instant(),
                    reason = if (confirmation.confirmed) {
                        null
                    } else {
                        ExecutionReasonCode.TP_SETUP_FAILED
                    },
                )
            }
        }
        val first = confirmation.intents.first()
        if (!confirmation.confirmed) {
            activeTakeProfitSets.remove(first.levelId)
        }
        val exposure = confirmation.confirmedPositionAmount.signum() != 0
        return symbolCoordinator
            .recordOwnership(
                levelId = first.levelId,
                ownsActiveAttempt = true,
                ownsExposure = exposure,
                hasUnresolvedOrder = !confirmation.confirmed,
            )
            .thenReturn(confirmation)
    }

    private fun validateTakeProfitIntents(intents: List<OrderIntent>) {
        require(intents.size == TAKE_PROFIT_COUNT) {
            "Exactly three take-profit intents are required"
        }
        val first = intents.first()
        require(intents.map(OrderIntent::levelId).toSet().size == 1) {
            "Take-profit intents must belong to one level"
        }
        require(intents.map(OrderIntent::symbol).toSet().size == 1) {
            "Take-profit intents must belong to one symbol"
        }
        require(intents.map(OrderIntent::slot).toSet() == setOf(1, 2, 3)) {
            "Take-profit slots must be 1, 2, and 3"
        }
        require(intents.all { intent ->
            intent.role == OrderRole.TAKE_PROFIT &&
                intent.type == OrderType.LIMIT &&
                intent.timeInForce == OrderTimeInForce.GTC &&
                intent.reduceOnly &&
                !intent.closePosition &&
                intent.confirmedPositionAmount ==
                first.confirmedPositionAmount
        }) {
            "Take profits must be reduce-only LIMIT GTC orders"
        }
        val totalQuantity = intents.fold(BigDecimal.ZERO) { total, intent ->
            total.add(checkNotNull(intent.confirmedQuantity))
        }
        require(
            totalQuantity <=
                checkNotNull(first.confirmedPositionAmount).abs(),
        ) {
            "Take-profit quantities cannot exceed confirmed exposure"
        }
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
        val currentOrder = orders[event.clientOrderId] ?: return
        recordTradeAccounting(event, currentOrder)
        val pending = pendingOrders[event.clientOrderId]
        if (pending == null) {
            when (currentOrder.role) {
                OrderRole.TAKE_PROFIT -> handleActiveTakeProfit(event)
                OrderRole.HARD_STOP -> handleActiveHardStop(event)
                else -> Unit
            }
            return
        }
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

    private fun handleActiveTakeProfit(
        event: BinanceUserDataEvent.OrderUpdate,
    ) {
        val current = orders[event.clientOrderId] ?: return
        if (current.role != OrderRole.TAKE_PROFIT) {
            return
        }
        val activeSet = activeTakeProfitSets[current.levelId] ?: return
        if (event.clientOrderId !in activeSet.clientOrderIds) {
            return
        }
        val accumulatedFill = maxOf(
            current.actualFilledQuantity,
            event.accumulatedFilledQuantity,
        )
        val outcome = when (event.orderStatus.uppercase()) {
            "NEW" -> OrderOutcome.ACTIVE
            else -> classifiedOutcome(
                status = event.orderStatus,
                actualFilledQuantity = accumulatedFill,
            )
        }
        orders[event.clientOrderId] = current.copy(
            actualFilledQuantity = accumulatedFill,
            outcome = outcome ?: current.outcome,
            source = OrderResolutionSource.PRIVATE_STREAM,
            exchangeOrderId = event.orderId,
            updatedAt = event.receivedAt,
        )
        if (accumulatedFill <= current.actualFilledQuantity) {
            return
        }
        publishTakeProfitFill(
            activeSet = activeSet,
            clientOrderId = event.clientOrderId,
            updatedAt = event.receivedAt,
        )
    }

    private fun publishTakeProfitFill(
        activeSet: ActiveTakeProfitSet,
        clientOrderId: String,
        updatedAt: java.time.Instant,
    ) {
        if (!activeSet.activated) {
            return
        }
        val totalFilled = activeSet.clientOrderIds.fold(BigDecimal.ZERO) {
                total, orderClientId,
            ->
            total.add(
                orders[orderClientId]?.actualFilledQuantity
                    ?: BigDecimal.ZERO,
            )
        }.min(activeSet.initialPositionAmount.abs())
        val remainingQuantity = activeSet.initialPositionAmount
            .abs()
            .subtract(totalFilled)
            .max(BigDecimal.ZERO)
        if (remainingQuantity >= activeSet.lastReportedRemainingQuantity) {
            return
        }
        activeSet.lastReportedRemainingQuantity = remainingQuantity
        val remainingPositionAmount = if (
            activeSet.initialPositionAmount.signum() > 0
        ) {
            remainingQuantity
        } else {
            remainingQuantity.negate()
        }
        positions[activeSet.symbol] = ExecutionPositionSnapshot(
            symbol = activeSet.symbol,
            positionAmount = remainingPositionAmount,
            entryPrice =
                positions[activeSet.symbol]?.entryPrice ?: BigDecimal.ZERO,
            updatedAt = updatedAt,
            unrealizedPnl = positions[activeSet.symbol]?.unrealizedPnl,
        )
        val complete = remainingQuantity.signum() == 0
        positionReductionSink.tryEmitNext(
            PositionReduction(
                levelId = activeSet.levelId,
                symbol = activeSet.symbol,
                clientOrderId = clientOrderId,
                role = OrderRole.TAKE_PROFIT,
                confirmedRemainingQuantity = remainingQuantity,
                terminalReason = if (complete) {
                    LevelReasonCode.TAKE_PROFITS_COMPLETE
                } else {
                    null
                },
                netResult = attemptAccounting[activeSet.levelId]?.snapshot(),
            ),
        )
        if (complete) {
            activeTakeProfitSets.remove(activeSet.levelId, activeSet)
        }
    }

    private fun handleActiveHardStop(
        event: BinanceUserDataEvent.OrderUpdate,
    ) {
        val current = orders[event.clientOrderId] ?: return
        val activeStop = activeHardStops[current.levelId] ?: return
        if (activeStop.intent.clientOrderId != event.clientOrderId) {
            return
        }
        val accumulatedFill = maxOf(
            current.actualFilledQuantity,
            event.accumulatedFilledQuantity,
        )
        val outcome = when (event.orderStatus.uppercase()) {
            "NEW" -> OrderOutcome.ACTIVE
            else -> classifiedOutcome(
                status = event.orderStatus,
                actualFilledQuantity = accumulatedFill,
            )
        }
        orders[event.clientOrderId] = current.copy(
            actualFilledQuantity = accumulatedFill,
            outcome = outcome ?: current.outcome,
            source = OrderResolutionSource.PRIVATE_STREAM,
            exchangeOrderId = event.orderId,
            updatedAt = event.receivedAt,
        )
        if (accumulatedFill <= current.actualFilledQuantity) {
            return
        }
        val initialQuantity = activeStop.positionQuantityAtFirstFill
            ?: maxOf(
                positions[event.symbol]?.positionAmount?.abs()
                    ?: BigDecimal.ZERO,
                accumulatedFill,
            ).also { quantity ->
                activeStop.positionQuantityAtFirstFill = quantity
            }
        val remainingQuantity = initialQuantity
            .subtract(accumulatedFill)
            .max(BigDecimal.ZERO)
        val remainingPositionAmount = if (
            activeStop.intent.confirmedPositionAmount?.signum() == -1
        ) {
            remainingQuantity.negate()
        } else {
            remainingQuantity
        }
        positions[event.symbol] = ExecutionPositionSnapshot(
            symbol = event.symbol,
            positionAmount = remainingPositionAmount,
            entryPrice = positions[event.symbol]?.entryPrice ?: BigDecimal.ZERO,
            updatedAt = event.receivedAt,
            unrealizedPnl = positions[event.symbol]?.unrealizedPnl,
        )
        val complete = remainingQuantity.signum() == 0
        positionReductionSink.tryEmitNext(
            PositionReduction(
                levelId = current.levelId,
                symbol = event.symbol,
                clientOrderId = event.clientOrderId,
                role = OrderRole.HARD_STOP,
                confirmedRemainingQuantity = remainingQuantity,
                terminalReason = if (complete) {
                    LevelReasonCode.HARD_STOP_FILLED
                } else {
                    null
                },
                netResult = attemptAccounting[current.levelId]?.snapshot(),
            ),
        )
        if (complete) {
            activeHardStops.remove(current.levelId, activeStop)
        }
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
                unrealizedPnl = position.unrealizedProfit,
            )
        }
        recordFunding(event)
    }

    private fun recordTradeAccounting(
        event: BinanceUserDataEvent.OrderUpdate,
        current: OrderExecutionSnapshot,
    ) {
        if (
            !event.executionType.equals("TRADE", ignoreCase = true) ||
            event.lastFilledQuantity.signum() <= 0
        ) {
            return
        }
        val intent = intents[event.clientOrderId] ?: return
        attemptAccounting
            .computeIfAbsent(current.levelId) { AttemptAccounting() }
            .recordTrade(intent, event)
    }

    private fun recordFunding(event: BinanceUserDataEvent.AccountUpdate) {
        if (!event.reason.equals("FUNDING_FEE", ignoreCase = true)) {
            return
        }
        val activeLevelIds = event.positions
            .map(BinancePositionUpdate::symbol)
            .distinct()
            .flatMap { symbol ->
                buildList {
                    activeHardStops.values
                        .filter { stop -> stop.intent.symbol == symbol }
                        .mapTo(this) { stop -> stop.intent.levelId }
                    activeTakeProfitSets.values
                        .filter { set -> set.symbol == symbol }
                        .mapTo(this) { set -> set.levelId }
                }
            }
            .distinct()
        if (activeLevelIds.size != 1) {
            return
        }
        val funding = event.balances.fold(BigDecimal.ZERO) { total, balance ->
            total.add(balance.balanceChange)
        }
        attemptAccounting
            .computeIfAbsent(activeLevelIds.single()) { AttemptAccounting() }
            .recordFunding(funding)
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
            unrealizedPnl = positions[pending.intent.symbol]?.unrealizedPnl,
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

    private fun OrderIntent.snapshot(now: Instant): OrderExecutionSnapshot =
        OrderExecutionSnapshot(
            intentSequence = intentSequence,
            clientOrderId = clientOrderId,
            levelId = levelId,
            attemptNumber = attemptNumber,
            symbol = symbol,
            role = role,
            slot = slot,
            requestedQuantity = confirmedQuantity,
            requestedPrice = price,
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

    private fun BinancePositionRisk.snapshot(
        observedAt: Instant,
        previous: ExecutionPositionSnapshot?,
    ): ExecutionPositionSnapshot =
        ExecutionPositionSnapshot(
            symbol = symbol,
            positionAmount = positionAmount,
            entryPrice = entryPrice,
            updatedAt = observedAt,
            actualNotional = notional?.abs()
                ?: positionAmount.abs().multiply(entryPrice),
            unrealizedPnl = unrealizedProfit ?: previous?.unrealizedPnl,
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

private data class DirectCloseCheck(
    val terminal: Boolean,
    val resolution: OrderResolution,
    val position: BinancePositionRisk,
)

private data class PendingOrder(
    val intent: OrderIntent,
    val result: Sinks.One<OrderResolution> = Sinks.one(),
)

private data class HardStopEvaluation(
    val terminal: Boolean,
    val confirmation: HardStopConfirmation,
)

private data class TakeProfitEvaluation(
    val terminal: Boolean,
    val confirmation: TakeProfitSetConfirmation,
)

private data class ActiveTakeProfitSet(
    val levelId: UUID,
    val symbol: String,
    val initialPositionAmount: BigDecimal,
    val intents: List<OrderIntent>,
    var activated: Boolean = false,
    var lastReportedRemainingQuantity: BigDecimal =
        initialPositionAmount.abs(),
) {
    val clientOrderIds: Set<String> = intents.mapTo(
        linkedSetOf(),
        OrderIntent::clientOrderId,
    )
}

private data class ActiveHardStop(
    val intent: OrderIntent,
    var positionQuantityAtFirstFill: BigDecimal? = null,
)

private class AttemptAccounting {
    private val recordedTrades = mutableSetOf<String>()
    private var grossPnl = BigDecimal.ZERO
    private var fees = BigDecimal.ZERO
    private var funding = BigDecimal.ZERO
    private var slippage = BigDecimal.ZERO
    private var grossPnlObserved = false
    private var feesObserved = false
    private var slippageObserved = false

    @Synchronized
    fun recordTrade(
        intent: OrderIntent,
        event: BinanceUserDataEvent.OrderUpdate,
    ) {
        val identity = "${event.clientOrderId}:${event.tradeId}"
        if (!recordedTrades.add(identity)) {
            return
        }
        event.commission?.let { commission ->
            fees = fees.add(commission.abs())
            feesObserved = true
        }
        if (!intent.role.closesExposure) {
            return
        }
        grossPnl = grossPnl.add(event.realizedProfit)
        grossPnlObserved = true
        val referencePrice = when (intent.role) {
            OrderRole.HARD_STOP -> intent.stopPrice
            else -> intent.price
        }
        val fillPrice = event.lastFilledPrice.takeIf { it.signum() > 0 }
        if (referencePrice != null && fillPrice != null) {
            val adversePerUnit = when (intent.side) {
                OrderSide.BUY -> fillPrice.subtract(referencePrice)
                OrderSide.SELL -> referencePrice.subtract(fillPrice)
            }.max(BigDecimal.ZERO)
            slippage = slippage.add(
                adversePerUnit.multiply(event.lastFilledQuantity),
            )
            slippageObserved = true
        }
    }

    @Synchronized
    fun recordFunding(amount: BigDecimal) {
        funding = funding.add(amount)
    }

    @Synchronized
    fun snapshot(): PositionNetResult {
        val observedGrossPnl = grossPnl.takeIf { grossPnlObserved }
        val observedFees = fees.takeIf { feesObserved }
        return PositionNetResult(
            grossPnl = observedGrossPnl,
            fees = observedFees,
            funding = funding,
            slippage = slippage.takeIf { slippageObserved },
            netPnl = if (observedGrossPnl != null && observedFees != null) {
                observedGrossPnl.subtract(observedFees).add(funding)
            } else {
                null
            },
        )
    }
}

private const val MAX_RECONCILIATION_CHECKS = 3
private const val MAX_STOP_RECONCILIATION_CHECKS = 50
private const val MAX_TAKE_PROFIT_RECONCILIATION_CHECKS = 75
private const val TAKE_PROFIT_COUNT = 3
private val MAX_STOP_CHECK_INTERVAL: Duration = Duration.ofMillis(250)
private val MAX_TAKE_PROFIT_CHECK_INTERVAL: Duration = Duration.ofMillis(250)
private val STOP_TERMINAL_STATUSES = setOf(
    "CANCELED",
    "CANCELLED",
    "EXPIRED",
    "EXPIRED_IN_MATCH",
    "FILLED",
    "REJECTED",
)
private val TAKE_PROFIT_TERMINAL_STATUSES = setOf(
    "CANCELED",
    "CANCELLED",
    "EXPIRED",
    "EXPIRED_IN_MATCH",
    "FILLED",
    "REJECTED",
)
