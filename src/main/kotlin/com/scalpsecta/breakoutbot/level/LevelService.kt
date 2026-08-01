package com.scalpsecta.breakoutbot.level

import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceClient
import com.scalpsecta.breakoutbot.binance.BinanceLeverageBracket
import com.scalpsecta.breakoutbot.binance.BinanceLotSizeFilter
import com.scalpsecta.breakoutbot.binance.BinanceMarginType
import com.scalpsecta.breakoutbot.binance.BinanceSymbolLeverageBrackets
import com.scalpsecta.breakoutbot.binance.BinanceSymbolMetadata
import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.evidence.EvidenceRecorder
import com.scalpsecta.breakoutbot.evidence.NoOpEvidenceRecorder
import com.scalpsecta.breakoutbot.execution.BreakoutAdditionRequest
import com.scalpsecta.breakoutbot.execution.BreakoutContinuationDecision
import com.scalpsecta.breakoutbot.execution.BreakoutDispatchDecision
import com.scalpsecta.breakoutbot.execution.BreakoutExecutionRequest
import com.scalpsecta.breakoutbot.execution.BreakoutExitRequest
import com.scalpsecta.breakoutbot.execution.BreakoutLevelCoordinator
import com.scalpsecta.breakoutbot.execution.BreakoutTranche
import com.scalpsecta.breakoutbot.execution.PreEntryLevelCoordinator
import com.scalpsecta.breakoutbot.execution.PreEntryOpportunity
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataService
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSubscription
import com.scalpsecta.breakoutbot.signal.LevelSignalTracker
import com.scalpsecta.breakoutbot.signal.NpuMode
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import com.scalpsecta.breakoutbot.service.AuthenticatedBinanceReadinessService
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.min

@Service
class LevelService internal constructor(
    private val client: AuthenticatedBinanceClient,
    private val publicMarketDataService: PublicMarketDataService,
    private val clock: Clock,
    private val automaticTimers: Boolean,
    private val evidenceRecorder: EvidenceRecorder = NoOpEvidenceRecorder,
    private val privateStreamReadinessProvider: () -> BinanceReadiness = {
        BinanceReadiness.NOT_READY
    },
    private val globalTradingStateProvider: () -> GlobalTradingState = {
        GlobalTradingState.RUNNING
    },
    private val publicMarketDataProvider: (String) -> PublicMarketDataSnapshot? =
        { symbol ->
            publicMarketDataService
                .snapshots()
                .firstOrNull { snapshot -> snapshot.symbol == symbol }
        },
) : PreEntryLevelCoordinator, BreakoutLevelCoordinator {
    @Autowired
    constructor(
        client: AuthenticatedBinanceClient,
        publicMarketDataService: PublicMarketDataService,
        clock: Clock,
        evidenceRecorder: EvidenceRecorder,
        authenticatedBinanceReadinessService:
            AuthenticatedBinanceReadinessService,
        attemptRiskService: AttemptRiskService,
    ) : this(
        client = client,
        publicMarketDataService = publicMarketDataService,
        clock = clock,
        automaticTimers = true,
        evidenceRecorder = evidenceRecorder,
        privateStreamReadinessProvider = {
            authenticatedBinanceReadinessService
                .snapshot()
                .privateStream
                .readiness
        },
        globalTradingStateProvider = {
            attemptRiskService.currentState().globalTradingState
        },
    )

    private val lock = ReentrantLock()
    private val levels = linkedMapOf<UUID, StoredLevel>()
    private val symbolRuntimes = mutableMapOf<String, SymbolRuntime>()
    private val symbolConfigurationPermits =
        ConcurrentHashMap<String, Semaphore>()
    private val eventScheduler: Scheduler = Schedulers.newParallel("level-events")
    private val preEntryOpportunitySink = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer<UUID>()
    private val breakoutRequestSink = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer<BreakoutExecutionRequest>()

    fun create(command: CreateLevelCommand): Mono<LevelSnapshot> =
        Mono.defer {
            val input = validateInput(command)
            withSymbolConfigurationPermit(input.symbol) {
                client
                    .exchangeInfo()
                    .map { exchangeInfo ->
                        exchangeInfo.symbols.firstOrNull { metadata ->
                            metadata.symbol == input.symbol
                        } ?: throw levelError(
                            LevelReasonCode.INVALID_SYMBOL,
                            "Symbol ${input.symbol} is not available on Binance USD-M Futures",
                        )
                    }
                    .onErrorMap { error ->
                        mapBoundaryFailure(
                            error = error,
                            code = LevelReasonCode.INVALID_SYMBOL,
                            message = "Could not validate Binance symbol ${input.symbol}",
                        )
                    }
                    .flatMap { metadata -> createPlan(input, metadata) }
                    .flatMap { plan ->
                        checkCollectionConstraints(plan.key)
                        validateLiquidation(plan)
                        configureSymbol(plan)
                            .then(Mono.defer { store(plan) })
                    }
            }
        }

    fun delete(levelId: UUID): Mono<LevelSnapshot> {
        val symbolRuntime = lock.withLock {
            val symbol = levels[levelId]?.snapshot?.symbol ?: throw levelError(
                LevelReasonCode.LEVEL_NOT_FOUND,
                "Level $levelId does not exist",
            )
            checkNotNull(symbolRuntimes[symbol]) {
                "Event queue for $symbol is missing"
            }.also { it.pendingEvents += 1 }
        }
        val event = SymbolLevelEvent.DeleteLevel(
            levelId = levelId,
            processedAt = clock.instant(),
            publicMarketData = publicMarketData(levelId),
        )
        return submit(symbolRuntime, event)
            .map { result -> result as LevelSnapshot }
    }

    fun currentState(
        privateStreamReadiness: BinanceReadiness = BinanceReadiness.NOT_READY,
        publicMarketData: List<PublicMarketDataSnapshot> =
            publicMarketDataService.snapshots(),
        globalState: GlobalTradingState = GlobalTradingState.RUNNING,
    ): List<LevelSnapshot> {
        val publicMarketDataBySymbol = publicMarketData.associateBy { snapshot ->
            snapshot.symbol
        }
        return lock.withLock {
            levels.values.map { stored ->
                currentSnapshot(
                    stored = stored,
                    publicMarketData =
                        publicMarketDataBySymbol[stored.snapshot.symbol],
                    privateStreamReadiness = privateStreamReadiness,
                    globalState = globalState,
                )
            }
        }
    }

    internal fun recordOwnership(
        levelId: UUID,
        ownsActiveAttempt: Boolean = false,
        ownsExposure: Boolean,
        hasUnresolvedOrder: Boolean,
    ): Mono<LevelSnapshot> {
        val symbolRuntime = lock.withLock {
            val symbol = levels[levelId]?.snapshot?.symbol ?: throw levelError(
                LevelReasonCode.LEVEL_NOT_FOUND,
                "Level $levelId does not exist",
            )
            checkNotNull(symbolRuntimes[symbol]) {
                "Event queue for $symbol is missing"
            }.also { it.pendingEvents += 1 }
        }
        return submit(
            symbolRuntime,
            SymbolLevelEvent.UpdateOwnership(
                levelId = levelId,
                ownsActiveAttempt = ownsActiveAttempt,
                ownsExposure = ownsExposure,
                hasUnresolvedOrder = hasUnresolvedOrder,
                processedAt = clock.instant(),
                publicMarketData = publicMarketData(levelId),
            ),
        )
            .map { result -> result as LevelSnapshot }
    }

    internal fun process(
        event: AggregateTradeEvent,
        marketHealthy: Boolean,
        publicMarketData: PublicMarketDataSnapshot? = null,
    ): Mono<Void> =
        submitSymbolEvent(
            symbol = event.symbol,
            event = SymbolLevelEvent.AggregateTrade(
                event = event,
                processedAt = clock.instant(),
                publicMarketData = publicMarketData,
                marketHealthy = marketHealthy,
            ),
        )

    internal fun process(
        event: BookTickerEvent,
        marketHealthy: Boolean,
        publicMarketData: PublicMarketDataSnapshot? = null,
    ): Mono<Void> =
        submitSymbolEvent(
            symbol = event.symbol,
            event = SymbolLevelEvent.BookTicker(
                event = event,
                processedAt = clock.instant(),
                publicMarketData = publicMarketData,
                marketHealthy = marketHealthy,
            ),
        )

    internal fun processTimer(
        symbol: String,
        marketHealthy: Boolean,
        publicMarketData: PublicMarketDataSnapshot? = null,
    ): Mono<Void> =
        submitSymbolEvent(
            symbol = symbol,
            event = SymbolLevelEvent.Timer(
                processedAt = clock.instant(),
                publicMarketData = publicMarketData,
                marketHealthy = marketHealthy,
            ),
        )

    internal fun processExecutionEvent(
        symbol: String,
        eventId: String,
        action: () -> Any,
    ): Mono<Any> =
        submitSymbolEventResult(
            symbol = symbol,
            event = SymbolLevelEvent.Execution(
                eventId = eventId,
                processedAt = clock.instant(),
                action = action,
            ),
        )

    override fun opportunities(): Flux<UUID> =
        preEntryOpportunitySink.asFlux()

    override fun breakoutRequests(): Flux<BreakoutExecutionRequest> =
        breakoutRequestSink.asFlux()

    override fun prepare(levelId: UUID): Mono<PreEntryOpportunity> =
        submitPreEntryEvent(
            levelId = levelId,
            event = { publicMarketData ->
                SymbolLevelEvent.PreparePreEntry(
                    levelId = levelId,
                    processedAt = clock.instant(),
                    publicMarketData = publicMarketData,
                )
            },
        ).map { result -> result as PreEntryOpportunity }

    override fun markDispatched(levelId: UUID): Mono<Void> =
        submitPreEntryEvent(
            levelId = levelId,
            event = { publicMarketData ->
                SymbolLevelEvent.PreEntryDispatched(
                    levelId = levelId,
                    processedAt = clock.instant(),
                    publicMarketData = publicMarketData,
                )
            },
        ).then()

    override fun cancelPreparation(levelId: UUID): Mono<Void> =
        submitPreEntryEvent(
            levelId = levelId,
            event = { publicMarketData ->
                SymbolLevelEvent.CancelPreEntryPreparation(
                    levelId = levelId,
                    processedAt = clock.instant(),
                    publicMarketData = publicMarketData,
                )
            },
        ).then()

    override fun crossedBeforeProtection(levelId: UUID): Boolean =
        lock.withLock {
            levels[levelId]?.snapshot?.let { snapshot ->
                snapshot.state == LevelState.TERMINAL &&
                    snapshot.terminalReason ==
                    LevelReasonCode.CROSS_BEFORE_PROTECTED
            } ?: false
        }

    override fun markProtected(
        levelId: UUID,
        confirmedPositionQuantity: BigDecimal,
        hardStopClientOrderId: String,
        hardStopPrice: BigDecimal,
        preEntryFilledAt: Instant?,
    ): Mono<Void> =
        submitPreEntryEvent(
            levelId = levelId,
            event = { publicMarketData ->
                SymbolLevelEvent.PreEntryProtected(
                    levelId = levelId,
                    confirmedPositionQuantity = confirmedPositionQuantity,
                    hardStopClientOrderId = hardStopClientOrderId,
                    hardStopPrice = hardStopPrice,
                    preEntryFilledAt = preEntryFilledAt,
                    processedAt = clock.instant(),
                    publicMarketData = publicMarketData,
                )
            },
        ).then()

    override fun validateAddition(
        request: BreakoutAdditionRequest,
    ): Mono<BreakoutDispatchDecision> =
        submitPreEntryEvent(
            levelId = request.levelId,
            event = { publicMarketData ->
                SymbolLevelEvent.ValidateBreakoutAddition(
                    request = request,
                    processedAt = clock.instant(),
                    publicMarketData = publicMarketData,
                )
            },
        ).map { result -> result as BreakoutDispatchDecision }

    override fun recordCrossingFill(
        requestId: String,
        levelId: UUID,
        confirmedPositionQuantity: BigDecimal,
    ): Mono<BreakoutContinuationDecision> =
        submitPreEntryEvent(
            levelId = levelId,
            event = { publicMarketData ->
                SymbolLevelEvent.CrossingFilled(
                    requestId = requestId,
                    levelId = levelId,
                    confirmedPositionQuantity = confirmedPositionQuantity,
                    processedAt = clock.instant(),
                    publicMarketData = publicMarketData,
                )
            },
        ).map { result -> result as BreakoutContinuationDecision }

    override fun recordFinalFill(
        requestId: String,
        levelId: UUID,
        confirmedPositionQuantity: BigDecimal,
    ): Mono<Void> =
        submitPreEntryEvent(
            levelId = levelId,
            event = { publicMarketData ->
                SymbolLevelEvent.FinalFilled(
                    requestId = requestId,
                    levelId = levelId,
                    confirmedPositionQuantity = confirmedPositionQuantity,
                    processedAt = clock.instant(),
                    publicMarketData = publicMarketData,
                )
            },
        ).then()

    override fun terminate(
        levelId: UUID,
        reason: LevelReasonCode,
        confirmedRemainingQuantity: BigDecimal,
        hasUnresolvedOrder: Boolean,
    ): Mono<Void> =
        submitPreEntryEvent(
            levelId = levelId,
            event = { publicMarketData ->
                SymbolLevelEvent.TerminatePreEntry(
                    levelId = levelId,
                    reason = reason,
                    confirmedRemainingQuantity = confirmedRemainingQuantity,
                    hasUnresolvedOrder = hasUnresolvedOrder,
                    processedAt = clock.instant(),
                    publicMarketData = publicMarketData,
                )
            },
        ).then()

    private fun submitPreEntryEvent(
        levelId: UUID,
        event: (PublicMarketDataSnapshot?) -> SymbolLevelEvent,
    ): Mono<Any> {
        val symbolRuntime = lock.withLock {
            val symbol = levels[levelId]?.snapshot?.symbol ?: throw levelError(
                LevelReasonCode.LEVEL_NOT_FOUND,
                "Level $levelId does not exist",
            )
            checkNotNull(symbolRuntimes[symbol]) {
                "Event queue for $symbol is missing"
            }.also { it.pendingEvents += 1 }
        }
        return submit(
            runtime = symbolRuntime,
            event = event(publicMarketData(levelId)),
        )
    }

    @PreDestroy
    fun close() {
        val runtimes = lock.withLock {
            symbolRuntimes.values.toList().also {
                symbolRuntimes.clear()
                levels.clear()
            }
        }
        symbolConfigurationPermits.clear()
        preEntryOpportunitySink.tryEmitComplete()
        breakoutRequestSink.tryEmitComplete()
        runtimes.forEach(SymbolRuntime::close)
        eventScheduler.dispose()
    }

    private fun submitSymbolEvent(
        symbol: String,
        event: SymbolLevelEvent,
    ): Mono<Void> = submitSymbolEventResult(symbol, event).then()

    private fun submitSymbolEventResult(
        symbol: String,
        event: SymbolLevelEvent,
    ): Mono<Any> {
        val normalizedSymbol = symbol.trim().uppercase()
        val runtime = lock.withLock {
            symbolRuntimes[normalizedSymbol]?.also { it.pendingEvents += 1 }
        } ?: return Mono.error(
            levelError(
                LevelReasonCode.LEVEL_NOT_FOUND,
                "No levels exist for $normalizedSymbol",
            ),
        )
        return submit(runtime, event)
    }

    private fun handleSymbolEvent(
        symbol: String,
        event: SymbolLevelEvent,
    ): Any =
        lock.withLock {
            when (event) {
                is SymbolLevelEvent.AddLevel -> addLevel(event)
                is SymbolLevelEvent.DeleteLevel -> deleteLevel(event)
                is SymbolLevelEvent.UpdateOwnership -> updateOwnership(event)
                is SymbolLevelEvent.PreparePreEntry -> preparePreEntry(event)
                is SymbolLevelEvent.PreEntryDispatched ->
                    recordPreEntryDispatch(event)

                is SymbolLevelEvent.CancelPreEntryPreparation ->
                    cancelPreEntryPreparation(event)

                is SymbolLevelEvent.PreEntryProtected ->
                    recordPreEntryProtection(event)

                is SymbolLevelEvent.TerminatePreEntry ->
                    terminatePreEntry(event)

                is SymbolLevelEvent.ValidateBreakoutAddition ->
                    validateBreakoutAddition(event)

                is SymbolLevelEvent.CrossingFilled ->
                    recordCrossingFill(event)

                is SymbolLevelEvent.FinalFilled ->
                    recordFinalFill(event)

                is SymbolLevelEvent.AggregateTrade -> {
                    evidenceRecorder.record(event.event)
                    levelsForSymbol(symbol).forEach { stored ->
                        val before = stored.snapshot
                        stored.signalTracker.record(event.event)
                        when {
                            stored.snapshot.state ==
                                LevelState.PRE_ENTRY_PENDING &&
                                crossesLevel(stored, event.event.price) ->
                                transitionToTerminal(
                                    stored = stored,
                                    processedAt = event.processedAt,
                                    reason =
                                        LevelReasonCode.CROSS_BEFORE_PROTECTED,
                                )

                            stored.preEntryPreparationInProgress &&
                                crossesLevel(stored, event.event.price) ->
                                transitionToTerminal(
                                    stored = stored,
                                    processedAt = event.processedAt,
                                    reason = LevelReasonCode.LEVEL_ALREADY_CROSSED,
                                )

                            stored.snapshot.state == LevelState.WARMING_UP &&
                                crossesLevel(stored, event.event.price) ->
                                transitionToMissed(stored, event.processedAt)

                            else -> {
                                advanceWarmup(
                                    stored = stored,
                                    processedAt = event.processedAt,
                                    marketHealthy = event.marketHealthy,
                                )
                                enterApproachIfEligible(
                                    stored = stored,
                                    referencePrice = event.event.price,
                                    processedAt = event.processedAt,
                                    publicMarketData = event.publicMarketData,
                                )
                            }
                        }
                        refreshSignal(stored, event.processedAt, event.publicMarketData)
                        advanceBreakout(
                            stored = stored,
                            processedAt = event.processedAt,
                            publicMarketData = event.publicMarketData,
                            marketHealthy = event.marketHealthy,
                            crossingTrade = event.event,
                        )
                        recordStateTransition(
                            before = before,
                            after = stored.snapshot,
                            publicMarketData = event.publicMarketData,
                        )
                        publishPreEntryOpportunity(
                            stored = stored,
                            now = event.processedAt,
                            publicMarketData = event.publicMarketData,
                        )
                    }
                    Unit
                }

                is SymbolLevelEvent.BookTicker -> {
                    evidenceRecorder.record(event.event)
                    val referencePrice = event.event.bidPrice
                        .add(event.event.askPrice)
                        .divide(TWO, CALCULATION_SCALE, RoundingMode.HALF_UP)
                    levelsForSymbol(symbol).forEach { stored ->
                        val before = stored.snapshot
                        stored.signalTracker.record(event.event)
                        advanceWarmup(
                            stored = stored,
                            processedAt = event.processedAt,
                            marketHealthy = event.marketHealthy,
                        )
                        enterApproachIfEligible(
                            stored = stored,
                            referencePrice = referencePrice,
                            processedAt = event.processedAt,
                            publicMarketData = event.publicMarketData,
                        )
                        refreshSignal(stored, event.processedAt, event.publicMarketData)
                        advanceBreakout(
                            stored = stored,
                            processedAt = event.processedAt,
                            publicMarketData = event.publicMarketData,
                            marketHealthy = event.marketHealthy,
                        )
                        recordStateTransition(
                            before = before,
                            after = stored.snapshot,
                            publicMarketData = event.publicMarketData,
                        )
                        publishPreEntryOpportunity(
                            stored = stored,
                            now = event.processedAt,
                            publicMarketData = event.publicMarketData,
                        )
                    }
                    Unit
                }

                is SymbolLevelEvent.Timer -> {
                    evidenceRecorder.advance(event.processedAt)
                    levelsForSymbol(symbol).forEach { stored ->
                        val before = stored.snapshot
                        stored.signalTracker.tick(event.processedAt, stored.npuMode)
                        advanceWarmup(
                            stored = stored,
                            processedAt = event.processedAt,
                            marketHealthy = event.marketHealthy,
                        )
                        val referencePrice = stored.signalTracker.snapshot(
                            publicMarketData = event.publicMarketData,
                            privateStreamReadiness =
                                privateStreamReadinessProvider(),
                            hasUnresolvedOrder = stored.snapshot.hasUnresolvedOrder,
                            now = event.processedAt,
                        ).midPrice
                        if (referencePrice != null) {
                            enterApproachIfEligible(
                                stored = stored,
                                referencePrice = referencePrice,
                                processedAt = event.processedAt,
                                publicMarketData = event.publicMarketData,
                            )
                        }
                        refreshSignal(stored, event.processedAt, event.publicMarketData)
                        advanceBreakout(
                            stored = stored,
                            processedAt = event.processedAt,
                            publicMarketData = event.publicMarketData,
                            marketHealthy = event.marketHealthy,
                        )
                        recordStateTransition(
                            before = before,
                            after = stored.snapshot,
                            publicMarketData = event.publicMarketData,
                        )
                        publishPreEntryOpportunity(
                            stored = stored,
                            now = event.processedAt,
                            publicMarketData = event.publicMarketData,
                        )
                    }
                    Unit
                }

                is SymbolLevelEvent.Execution -> event.action()
            }
        }

    private fun addLevel(event: SymbolLevelEvent.AddLevel): LevelSnapshot {
        checkCollectionConstraintsLocked(event.plan.key)
        val tracker = LevelSignalTracker(
            symbol = event.plan.input.symbol,
            direction = event.plan.input.direction,
            levelPrice = event.plan.sizing.normalizedLevelPrice,
            tickSize = event.plan.tickSize,
            clock = clock,
        )
        val initialSignal = tracker.snapshot(
            publicMarketData = event.publicMarketData,
            privateStreamReadiness = BinanceReadiness.NOT_READY,
            hasUnresolvedOrder = false,
            now = event.processedAt,
        )
        val snapshot = LevelSnapshot(
            id = UUID.randomUUID(),
            createdAt = event.processedAt,
            symbol = event.plan.input.symbol,
            direction = event.plan.input.direction,
            requestedLevelPrice = event.plan.input.requestedLevelPrice,
            normalizedLevelPrice = event.plan.sizing.normalizedLevelPrice,
            positionNotionalUsdt = event.plan.input.positionNotionalUsdt,
            maxImpulsePct = event.plan.input.maxImpulsePct,
            sizingReferencePrice = event.plan.sizing.currentPrice,
            plannedQuantity = event.plan.sizing.plannedQuantity,
            entryAllocation = event.plan.sizing.allocation,
            leverage = event.plan.leverage,
            projectedIsolatedMargin = event.plan.projectedIsolatedMargin,
            riskBoundaryStopPrice = event.plan.riskBoundaryStopPrice,
            estimatedLiquidationPrice = event.plan.estimatedLiquidationPrice,
            state = LevelState.WARMING_UP,
            stateChangedAt = event.processedAt,
            warmupHealthySince = if (event.marketHealthy) {
                event.processedAt
            } else {
                null
            },
            terminalReason = null,
            globalState = GlobalTradingState.RUNNING,
            blockers = listOf(LevelBlocker.WARMING_UP),
            signal = initialSignal,
            attemptNumber = 1,
            attemptConsumed = false,
            preEntryDispatchedAt = null,
            confirmedPositionQuantity = BigDecimal.ZERO,
            hardStopPrice = null,
            hardStopClientOrderId = null,
            hardStopConfirmedAt = null,
            ownsActiveAttempt = false,
            ownsExposure = false,
            hasUnresolvedOrder = false,
            deleteAllowed = true,
        )
        val stored = StoredLevel(
            key = event.plan.key,
            snapshot = snapshot,
            signalTracker = tracker,
            tickSize = event.plan.tickSize,
        )
        levels[snapshot.id] = stored
        return currentSnapshot(
            stored = stored,
            publicMarketData = event.publicMarketData,
            privateStreamReadiness = BinanceReadiness.NOT_READY,
            globalState = GlobalTradingState.RUNNING,
            now = event.processedAt,
        ).also {
            stored.snapshot = it
            evidenceRecorder.recordLevelCreated(it, event.publicMarketData)
        }
    }

    private fun deleteLevel(event: SymbolLevelEvent.DeleteLevel): LevelSnapshot {
        val stored = levels[event.levelId] ?: throw levelError(
            LevelReasonCode.LEVEL_NOT_FOUND,
            "Level ${event.levelId} does not exist",
        )
        when {
            stored.snapshot.ownsExposure -> throw levelError(
                LevelReasonCode.LEVEL_HAS_EXPOSURE,
                "Level ${event.levelId} cannot be deleted while it owns exposure",
            )

            stored.snapshot.hasUnresolvedOrder -> throw levelError(
                LevelReasonCode.LEVEL_HAS_UNRESOLVED_ORDER,
                "Level ${event.levelId} cannot be deleted while it owns an unresolved order",
            )
        }
        checkNotNull(levels.remove(event.levelId)) {
            "Level ${event.levelId} disappeared from its ordered event queue"
        }
        val deleted = currentSnapshot(
            stored = stored,
            publicMarketData = event.publicMarketData,
            privateStreamReadiness = BinanceReadiness.NOT_READY,
            globalState = GlobalTradingState.RUNNING,
            now = event.processedAt,
        )
        evidenceRecorder.recordLevelDeleted(
            level = deleted,
            marketData = event.publicMarketData,
            deletedAt = event.processedAt,
        )
        if (levels.values.none { candidate -> candidate.snapshot.symbol == deleted.symbol }) {
            evidenceRecorder.discardRollingBuffer(deleted.symbol)
        }
        return deleted
    }

    private fun updateOwnership(
        event: SymbolLevelEvent.UpdateOwnership,
    ): LevelSnapshot {
        val stored = levels[event.levelId] ?: throw levelError(
            LevelReasonCode.LEVEL_NOT_FOUND,
            "Level ${event.levelId} does not exist",
        )
        val claimsOwnership =
            event.ownsActiveAttempt ||
                event.ownsExposure ||
                event.hasUnresolvedOrder
        val otherOwner = levels.values.firstOrNull { candidate ->
            candidate.snapshot.symbol == stored.snapshot.symbol &&
                candidate.snapshot.id != event.levelId &&
                candidate.claimsSymbolOwnership()
        }
        if (claimsOwnership && otherOwner != null) {
            throw levelError(
                LevelReasonCode.SYMBOL_OWNERSHIP_CONFLICT,
                "Level ${otherOwner.snapshot.id} already owns the active attempt for ${stored.snapshot.symbol}",
            )
        }
        val before = stored.snapshot
        stored.snapshot = stored.snapshot.copy(
            ownsActiveAttempt = event.ownsActiveAttempt,
            ownsExposure = event.ownsExposure,
            hasUnresolvedOrder = event.hasUnresolvedOrder,
            deleteAllowed = !event.ownsExposure && !event.hasUnresolvedOrder,
        )
        return currentSnapshot(
            stored = stored,
            publicMarketData = event.publicMarketData,
            privateStreamReadiness = BinanceReadiness.NOT_READY,
            globalState = GlobalTradingState.RUNNING,
            now = event.processedAt,
        ).also {
            stored.snapshot = it
            evidenceRecorder.recordOwnershipChange(
                before = before,
                after = it,
                marketData = event.publicMarketData,
                changedAt = event.processedAt,
            )
        }
    }

    private fun preparePreEntry(
        event: SymbolLevelEvent.PreparePreEntry,
    ): PreEntryOpportunity {
        val stored = levels[event.levelId] ?: throw levelError(
            LevelReasonCode.LEVEL_NOT_FOUND,
            "Level ${event.levelId} does not exist",
        )
        if (stored.preEntryPreparationInProgress) {
            throw levelError(
                LevelReasonCode.PRE_ENTRY_NOT_ELIGIBLE,
                "Pre-entry preparation is already in progress for ${event.levelId}",
            )
        }
        refreshSignal(
            stored = stored,
            now = event.processedAt,
            publicMarketData = event.publicMarketData,
        )
        val opportunity = buildPreEntryOpportunity(
            stored = stored,
            now = event.processedAt,
            publicMarketData = event.publicMarketData,
        ) ?: throw levelError(
            LevelReasonCode.PRE_ENTRY_NOT_ELIGIBLE,
            "Level ${event.levelId} is not eligible for pre-entry",
        )
        stored.preEntryPreparationInProgress = true
        stored.preparedOpportunity = opportunity
        return opportunity
    }

    private fun recordPreEntryDispatch(
        event: SymbolLevelEvent.PreEntryDispatched,
    ): LevelSnapshot {
        val stored = levels[event.levelId] ?: throw levelError(
            LevelReasonCode.LEVEL_NOT_FOUND,
            "Level ${event.levelId} does not exist",
        )
        if (
            stored.snapshot.state != LevelState.APPROACH ||
            stored.snapshot.attemptConsumed ||
            !stored.preEntryPreparationInProgress ||
            stored.preparedOpportunity == null
        ) {
            throw levelError(
                LevelReasonCode.LEVEL_ALREADY_CONSUMED,
                "Level ${event.levelId} cannot dispatch another pre-entry",
            )
        }
        refreshSignal(
            stored = stored,
            now = event.processedAt,
            publicMarketData = event.publicMarketData,
        )
        val preparedOpportunity = checkNotNull(stored.preparedOpportunity)
        val currentOpportunity = buildPreEntryOpportunity(
            stored = stored,
            now = event.processedAt,
            publicMarketData = event.publicMarketData,
            ignorePreparation = true,
        )
        if (
            currentOpportunity == null ||
            currentOpportunity.copy(
                preparedAt = preparedOpportunity.preparedAt,
            ) != preparedOpportunity
        ) {
            stored.preEntryPreparationInProgress = false
            stored.preparedOpportunity = null
            throw levelError(
                LevelReasonCode.PRE_ENTRY_NOT_ELIGIBLE,
                "Pre-entry conditions changed before dispatch for ${event.levelId}",
            )
        }
        val before = stored.snapshot
        stored.preEntryPreparationInProgress = false
        stored.preparedOpportunity = null
        stored.pendingBreakoutRequest = null
        stored.breakoutRequestClaimed = false
        stored.deferredBreakoutReason = null
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.PRE_ENTRY_PENDING,
            stateChangedAt = event.processedAt,
            terminalReason = null,
            attemptConsumed = true,
            preEntryDispatchedAt = event.processedAt,
            ownsActiveAttempt = true,
            hasUnresolvedOrder = true,
            deleteAllowed = false,
        )
        return currentSnapshot(
            stored = stored,
            publicMarketData = event.publicMarketData,
            privateStreamReadiness = privateStreamReadinessProvider(),
            globalState = globalTradingStateProvider(),
            now = event.processedAt,
        ).also { after ->
            stored.snapshot = after
            recordStateTransition(before, after, event.publicMarketData)
        }
    }

    private fun cancelPreEntryPreparation(
        event: SymbolLevelEvent.CancelPreEntryPreparation,
    ) {
        val stored = levels[event.levelId] ?: throw levelError(
            LevelReasonCode.LEVEL_NOT_FOUND,
            "Level ${event.levelId} does not exist",
        )
        stored.preEntryPreparationInProgress = false
        stored.preparedOpportunity = null
        refreshSignal(stored, event.processedAt, event.publicMarketData)
    }

    private fun recordPreEntryProtection(
        event: SymbolLevelEvent.PreEntryProtected,
    ): LevelSnapshot {
        require(event.confirmedPositionQuantity.signum() > 0) {
            "confirmedPositionQuantity must be positive"
        }
        require(event.hardStopClientOrderId.isNotBlank()) {
            "hardStopClientOrderId must not be blank"
        }
        require(event.hardStopPrice.signum() > 0) {
            "hardStopPrice must be positive"
        }
        val stored = levels[event.levelId] ?: throw levelError(
            LevelReasonCode.LEVEL_NOT_FOUND,
            "Level ${event.levelId} does not exist",
        )
        if (stored.snapshot.state != LevelState.PRE_ENTRY_PENDING) {
            throw levelError(
                LevelReasonCode.PRE_ENTRY_NOT_ELIGIBLE,
                "Level ${event.levelId} is no longer awaiting pre-entry protection",
            )
        }
        val filledAt = minOf(
            event.preEntryFilledAt ?: event.processedAt,
            event.processedAt,
        )
        val before = stored.snapshot
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.PRE_ENTRY,
            stateChangedAt = event.processedAt,
            terminalReason = null,
            confirmedPositionQuantity = event.confirmedPositionQuantity,
            hardStopPrice = event.hardStopPrice,
            hardStopClientOrderId = event.hardStopClientOrderId,
            hardStopConfirmedAt = event.processedAt,
            ownsActiveAttempt = true,
            ownsExposure = true,
            hasUnresolvedOrder = false,
            deleteAllowed = false,
            preEntryFilledAt = filledAt,
        )
        return currentSnapshot(
            stored = stored,
            publicMarketData = event.publicMarketData,
            privateStreamReadiness = privateStreamReadinessProvider(),
            globalState = globalTradingStateProvider(),
            now = event.processedAt,
        ).also { after ->
            stored.snapshot = after
            val observedTradePrices = stored.signalTracker.tradePrices(
                now = event.processedAt,
                duration = Duration.between(filledAt, event.processedAt),
            ).toMutableList()
            after.signal.latestTradePrice?.let { latestPrice ->
                if (observedTradePrices.lastOrNull() != latestPrice) {
                    observedTradePrices += latestPrice
                }
            }
            stored.breakoutStateMachine.startPreEntry(
                direction = after.direction,
                levelPrice = after.normalizedLevelPrice,
                frozenNpu = checkNotNull(after.signal.npu.absolute),
                filledAt = filledAt,
                observedTradePrices = observedTradePrices,
            )
            recordStateTransition(before, after, event.publicMarketData)
        }
    }

    private fun terminatePreEntry(
        event: SymbolLevelEvent.TerminatePreEntry,
    ): LevelSnapshot {
        require(event.confirmedRemainingQuantity.signum() >= 0) {
            "confirmedRemainingQuantity must not be negative"
        }
        val stored = levels[event.levelId] ?: throw levelError(
            LevelReasonCode.LEVEL_NOT_FOUND,
            "Level ${event.levelId} does not exist",
        )
        val before = stored.snapshot
        stored.preEntryPreparationInProgress = false
        stored.preparedOpportunity = null
        stored.pendingBreakoutRequest = null
        stored.breakoutRequestClaimed = false
        stored.deferredBreakoutReason = null
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.TERMINAL,
            stateChangedAt = if (before.state == LevelState.TERMINAL) {
                before.stateChangedAt
            } else {
                event.processedAt
            },
            terminalReason = event.reason,
            confirmedPositionQuantity = event.confirmedRemainingQuantity,
            ownsActiveAttempt =
                event.confirmedRemainingQuantity.signum() > 0 ||
                    event.hasUnresolvedOrder,
            ownsExposure = event.confirmedRemainingQuantity.signum() > 0,
            hasUnresolvedOrder = event.hasUnresolvedOrder,
            deleteAllowed =
                event.confirmedRemainingQuantity.signum() == 0 &&
                    !event.hasUnresolvedOrder,
        )
        return currentSnapshot(
            stored = stored,
            publicMarketData = event.publicMarketData,
            privateStreamReadiness = privateStreamReadinessProvider(),
            globalState = globalTradingStateProvider(),
            now = event.processedAt,
        ).also { after ->
            stored.snapshot = after
            recordStateTransition(before, after, event.publicMarketData)
        }
    }

    private fun validateBreakoutAddition(
        event: SymbolLevelEvent.ValidateBreakoutAddition,
    ): BreakoutDispatchDecision {
        val stored = levels[event.request.levelId] ?: throw levelError(
            LevelReasonCode.LEVEL_NOT_FOUND,
            "Level ${event.request.levelId} does not exist",
        )
        val before = stored.snapshot
        refreshSignal(stored, event.processedAt, event.publicMarketData)
        val pending = stored.pendingBreakoutRequest
        val freshRequest = buildBreakoutAdditionRequest(
            stored = stored,
            tranche = event.request.tranche,
            requestId = event.request.requestId,
        ) ?: event.request
        val expectedState = when (event.request.tranche) {
            BreakoutTranche.CROSSING -> LevelState.CROSS_ENTRY_PENDING
            BreakoutTranche.FINAL -> LevelState.CONFIRM_ENTRY_PENDING
        }
        val valid =
            stored.snapshot.state == expectedState &&
                pending?.requestId == event.request.requestId &&
                pending.tranche == event.request.tranche &&
                !stored.breakoutRequestClaimed &&
                stored.deferredBreakoutReason == null &&
                additionGatesValid(stored, event.publicMarketData) &&
                freshRequest.hardStopClientOrderId ==
                stored.snapshot.hardStopClientOrderId
        if (valid) {
            stored.pendingBreakoutRequest = freshRequest
            stored.breakoutRequestClaimed = true
            return BreakoutDispatchDecision(
                request = freshRequest,
                dispatchAllowed = true,
            )
        }
        val reason = stored.deferredBreakoutReason ?: when (event.request.tranche) {
            BreakoutTranche.CROSSING -> LevelReasonCode.PRE_ENTRY_INVALIDATED
            BreakoutTranche.FINAL -> LevelReasonCode.BREAK_CONFIRM_FAILED
        }
        transitionToExiting(stored, event.processedAt, reason)
        refreshSignal(stored, event.processedAt, event.publicMarketData)
        recordStateTransition(before, stored.snapshot, event.publicMarketData)
        return BreakoutDispatchDecision(
            request = freshRequest,
            dispatchAllowed = false,
            terminalReason = reason,
        )
    }

    private fun recordCrossingFill(
        event: SymbolLevelEvent.CrossingFilled,
    ): BreakoutContinuationDecision {
        require(event.confirmedPositionQuantity.signum() > 0) {
            "confirmedPositionQuantity must be positive"
        }
        val stored = levels[event.levelId] ?: throw levelError(
            LevelReasonCode.LEVEL_NOT_FOUND,
            "Level ${event.levelId} does not exist",
        )
        val pending = stored.pendingBreakoutRequest
        check(
            stored.snapshot.state == LevelState.CROSS_ENTRY_PENDING &&
                stored.breakoutRequestClaimed &&
                pending?.requestId == event.requestId &&
                pending.tranche == BreakoutTranche.CROSSING,
        ) {
            "Crossing request ${event.requestId} is no longer active"
        }
        val before = stored.snapshot
        stored.pendingBreakoutRequest = null
        stored.breakoutRequestClaimed = false
        stored.snapshot = stored.snapshot.copy(
            confirmedPositionQuantity = event.confirmedPositionQuantity,
            hasUnresolvedOrder = false,
        )
        val deferredReason = stored.deferredBreakoutReason
        stored.deferredBreakoutReason = null
        if (deferredReason != null) {
            transitionToExiting(stored, event.processedAt, deferredReason)
            refreshSignal(stored, event.processedAt, event.publicMarketData)
            recordStateTransition(before, stored.snapshot, event.publicMarketData)
            return BreakoutContinuationDecision(
                continueAttempt = false,
                terminalReason = deferredReason,
            )
        }
        stored.breakoutStateMachine.startConfirmation(event.processedAt)
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.BREAK_CONFIRM,
            stateChangedAt = event.processedAt,
            terminalReason = null,
            confirmationStartedAt = event.processedAt,
        )
        refreshSignal(stored, event.processedAt, event.publicMarketData)
        val confirmation = stored.breakoutStateMachine.evaluateConfirmation(
            breakoutObservation(
                stored = stored,
                processedAt = event.processedAt,
                publicMarketData = event.publicMarketData,
            ),
        )
        if (confirmation.status == BreakoutConfirmationStatus.FAILED) {
            val reason = checkNotNull(confirmation.terminalReason)
            transitionToExiting(stored, event.processedAt, reason)
            refreshSignal(stored, event.processedAt, event.publicMarketData)
            recordStateTransition(before, stored.snapshot, event.publicMarketData)
            return BreakoutContinuationDecision(
                continueAttempt = false,
                terminalReason = reason,
            )
        }
        recordStateTransition(before, stored.snapshot, event.publicMarketData)
        return BreakoutContinuationDecision(continueAttempt = true)
    }

    private fun recordFinalFill(
        event: SymbolLevelEvent.FinalFilled,
    ): LevelSnapshot {
        require(event.confirmedPositionQuantity.signum() > 0) {
            "confirmedPositionQuantity must be positive"
        }
        val stored = levels[event.levelId] ?: throw levelError(
            LevelReasonCode.LEVEL_NOT_FOUND,
            "Level ${event.levelId} does not exist",
        )
        val pending = stored.pendingBreakoutRequest
        check(
            stored.snapshot.state == LevelState.CONFIRM_ENTRY_PENDING &&
                stored.breakoutRequestClaimed &&
                pending?.requestId == event.requestId &&
                pending.tranche == BreakoutTranche.FINAL,
        ) {
            "Final request ${event.requestId} is no longer active"
        }
        val before = stored.snapshot
        stored.pendingBreakoutRequest = null
        stored.breakoutRequestClaimed = false
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.POSITION_MANAGEMENT,
            stateChangedAt = event.processedAt,
            terminalReason = null,
            confirmedPositionQuantity = event.confirmedPositionQuantity,
            ownsActiveAttempt = true,
            ownsExposure = true,
            hasUnresolvedOrder = false,
            deleteAllowed = false,
        )
        return currentSnapshot(
            stored = stored,
            publicMarketData = event.publicMarketData,
            privateStreamReadiness = privateStreamReadinessProvider(),
            globalState = globalTradingStateProvider(),
            now = event.processedAt,
        ).also { after ->
            stored.snapshot = after
            recordStateTransition(before, after, event.publicMarketData)
        }
    }

    private fun advanceBreakout(
        stored: StoredLevel,
        processedAt: Instant,
        publicMarketData: PublicMarketDataSnapshot?,
        marketHealthy: Boolean,
        crossingTrade: AggregateTradeEvent? = null,
    ) {
        val observation = breakoutObservation(
            stored = stored,
            processedAt = processedAt,
            publicMarketData = publicMarketData,
            marketHealthy = marketHealthy,
        )
        when (stored.snapshot.state) {
            LevelState.PRE_ENTRY -> {
                val invalidation = stored.breakoutStateMachine.evaluatePreBreak(
                    observation = observation,
                    includeNoCrossTimeout = false,
                )
                if (invalidation != null) {
                    startBreakoutExit(stored, processedAt, invalidation)
                    return
                }
                if (
                    crossingTrade != null &&
                    crossesLevel(stored, crossingTrade.price)
                ) {
                    if (additionGatesValid(stored, publicMarketData)) {
                        beginCrossing(stored, crossingTrade, processedAt)
                    } else {
                        startBreakoutExit(
                            stored,
                            processedAt,
                            LevelReasonCode.PRE_ENTRY_INVALIDATED,
                        )
                    }
                    return
                }
                val timeout = stored.breakoutStateMachine.evaluatePreBreak(
                    observation = observation,
                    includeNoCrossTimeout = true,
                )
                if (timeout != null) {
                    startBreakoutExit(stored, processedAt, timeout)
                }
            }

            LevelState.CROSS_ENTRY_PENDING -> {
                val invalidation = stored.breakoutStateMachine.evaluatePreBreak(
                    observation = observation,
                    includeNoCrossTimeout = false,
                )
                if (invalidation != null) {
                    stored.deferredBreakoutReason =
                        stored.deferredBreakoutReason ?: invalidation
                }
            }

            LevelState.BREAK_CONFIRM -> {
                val confirmation = stored.breakoutStateMachine
                    .evaluateConfirmation(observation)
                when (confirmation.status) {
                    BreakoutConfirmationStatus.PENDING -> Unit
                    BreakoutConfirmationStatus.FAILED -> startBreakoutExit(
                        stored = stored,
                        processedAt = processedAt,
                        reason = checkNotNull(confirmation.terminalReason),
                    )

                    BreakoutConfirmationStatus.PASSED ->
                        beginFinalAddition(stored, processedAt)
                }
            }

            else -> Unit
        }
    }

    private fun beginCrossing(
        stored: StoredLevel,
        trade: AggregateTradeEvent,
        processedAt: Instant,
    ) {
        stored.breakoutStateMachine.markCrossed()
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.CROSS_ENTRY_PENDING,
            stateChangedAt = processedAt,
            terminalReason = null,
            crossingTradeId = trade.aggregateTradeId,
            crossedAt = trade.receivedAt,
        )
        val request = checkNotNull(
            buildBreakoutAdditionRequest(
                stored = stored,
                tranche = BreakoutTranche.CROSSING,
                requestId = "cross:${trade.aggregateTradeId}",
            ),
        )
        stored.pendingBreakoutRequest = request
        breakoutRequestSink.tryEmitNext(request)
    }

    private fun beginFinalAddition(
        stored: StoredLevel,
        processedAt: Instant,
    ) {
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.CONFIRM_ENTRY_PENDING,
            stateChangedAt = processedAt,
            terminalReason = null,
            breakoutConfirmedAt = processedAt,
        )
        val request = buildBreakoutAdditionRequest(
            stored = stored,
            tranche = BreakoutTranche.FINAL,
            requestId = "final:${processedAt.toEpochMilli()}",
        )
        if (request == null) {
            startBreakoutExit(
                stored,
                processedAt,
                LevelReasonCode.BREAK_CONFIRM_FAILED,
            )
            return
        }
        stored.pendingBreakoutRequest = request
        breakoutRequestSink.tryEmitNext(request)
    }

    private fun startBreakoutExit(
        stored: StoredLevel,
        processedAt: Instant,
        reason: LevelReasonCode,
    ) {
        transitionToExiting(stored, processedAt, reason)
        if (stored.snapshot.confirmedPositionQuantity.signum() == 0) {
            transitionToTerminal(stored, processedAt, reason)
            return
        }
        breakoutRequestSink.tryEmitNext(
            BreakoutExitRequest(
                requestId = "exit:${reason.name}:${processedAt.toEpochMilli()}",
                levelId = stored.snapshot.id,
                attemptNumber = stored.snapshot.attemptNumber,
                symbol = stored.snapshot.symbol,
                direction = stored.snapshot.direction,
                confirmedPositionQuantity =
                    stored.snapshot.confirmedPositionQuantity,
                reason = reason,
            ),
        )
    }

    private fun transitionToExiting(
        stored: StoredLevel,
        processedAt: Instant,
        reason: LevelReasonCode,
    ) {
        stored.pendingBreakoutRequest = null
        stored.breakoutRequestClaimed = false
        stored.deferredBreakoutReason = null
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.EXITING,
            stateChangedAt = processedAt,
            terminalReason = reason,
        )
    }

    private fun additionGatesValid(
        stored: StoredLevel,
        publicMarketData: PublicMarketDataSnapshot?,
    ): Boolean {
        val snapshot = stored.snapshot
        return snapshot.attemptConsumed &&
            snapshot.ownsActiveAttempt &&
            snapshot.ownsExposure &&
            !snapshot.hasUnresolvedOrder &&
            snapshot.hardStopConfirmedAt != null &&
            snapshot.hardStopClientOrderId != null &&
            snapshot.hardStopPrice != null &&
            snapshot.globalState == GlobalTradingState.RUNNING &&
            snapshot.blockers.isEmpty() &&
            snapshot.signal.npu.frozen &&
            snapshot.signal.mandatoryGates.entryEligible &&
            publicMarketData?.healthy == true &&
            privateStreamReadinessProvider() == BinanceReadiness.READY
    }

    private fun buildBreakoutAdditionRequest(
        stored: StoredLevel,
        tranche: BreakoutTranche,
        requestId: String,
    ): BreakoutAdditionRequest? {
        val snapshot = stored.snapshot
        val role = when (tranche) {
            BreakoutTranche.CROSSING -> LevelEntryRole.CROSSING
            BreakoutTranche.FINAL -> LevelEntryRole.CONFIRMATION
        }
        return BreakoutAdditionRequest(
            requestId = requestId,
            levelId = snapshot.id,
            attemptNumber = snapshot.attemptNumber,
            symbol = snapshot.symbol,
            direction = snapshot.direction,
            confirmedPositionQuantity = snapshot.confirmedPositionQuantity,
            tranche = tranche,
            requestedQuantity = snapshot.entryAllocation
                .single { entry -> entry.role == role }
                .quantity,
            bestBidPrice = snapshot.signal.bidPrice ?: return null,
            bestAskPrice = snapshot.signal.askPrice ?: return null,
            frozenNpu = snapshot.signal.npu.absolute ?: return null,
            hardStopClientOrderId = snapshot.hardStopClientOrderId ?: return null,
            hardStopPrice = snapshot.hardStopPrice ?: return null,
        )
    }

    private fun breakoutObservation(
        stored: StoredLevel,
        processedAt: Instant,
        publicMarketData: PublicMarketDataSnapshot?,
        marketHealthy: Boolean = publicMarketData?.healthy == true,
    ): BreakoutObservation =
        BreakoutObservation(
            observedAt = processedAt,
            signal = stored.snapshot.signal,
            publicDataHealthy = marketHealthy,
            privateDataHealthy =
                privateStreamReadinessProvider() == BinanceReadiness.READY,
        )

    private fun advanceWarmup(
        stored: StoredLevel,
        processedAt: Instant,
        marketHealthy: Boolean,
    ) {
        if (stored.snapshot.state != LevelState.WARMING_UP) {
            return
        }
        if (!marketHealthy) {
            stored.snapshot = stored.snapshot.copy(warmupHealthySince = null)
            return
        }
        val healthySince = stored.snapshot.warmupHealthySince ?: processedAt
        stored.snapshot = stored.snapshot.copy(warmupHealthySince = healthySince)
        if (processedAt.isBefore(healthySince.plus(WARMUP_DURATION))) {
            return
        }
        stored.npuMode = NpuMode.ARMED
        stored.signalTracker.tick(processedAt, NpuMode.ARMED)
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.ARMED,
            stateChangedAt = processedAt,
            terminalReason = null,
        )
    }

    private fun enterApproachIfEligible(
        stored: StoredLevel,
        referencePrice: BigDecimal,
        processedAt: Instant,
        publicMarketData: PublicMarketDataSnapshot?,
    ) {
        if (stored.snapshot.state != LevelState.ARMED) {
            return
        }
        val npu = stored.signalTracker.snapshot(
            publicMarketData = publicMarketData,
            privateStreamReadiness = BinanceReadiness.NOT_READY,
            hasUnresolvedOrder = stored.snapshot.hasUnresolvedOrder,
            now = processedAt,
        ).npu.absolute ?: return
        val activationDistance = npu.multiply(ACTIVATION_NPU_MULTIPLIER)
        if (
            stored.snapshot.normalizedLevelPrice
                .subtract(referencePrice)
                .abs() > activationDistance
        ) {
            return
        }
        stored.npuMode = NpuMode.FROZEN
        stored.signalTracker.tick(processedAt, NpuMode.FROZEN)
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.APPROACH,
            stateChangedAt = processedAt,
        )
    }

    private fun publishPreEntryOpportunity(
        stored: StoredLevel,
        now: Instant,
        publicMarketData: PublicMarketDataSnapshot?,
    ) {
        if (stored.preEntryPreparationInProgress) {
            return
        }
        if (
            buildPreEntryOpportunity(
                stored = stored,
                now = now,
                publicMarketData = publicMarketData,
            ) != null
        ) {
            preEntryOpportunitySink.tryEmitNext(stored.snapshot.id)
        }
    }

    private fun buildPreEntryOpportunity(
        stored: StoredLevel,
        now: Instant,
        publicMarketData: PublicMarketDataSnapshot?,
        ignorePreparation: Boolean = false,
    ): PreEntryOpportunity? {
        val snapshot = currentSnapshot(
            stored = stored,
            publicMarketData = publicMarketData,
            privateStreamReadiness = privateStreamReadinessProvider(),
            globalState = globalTradingStateProvider(),
            now = now,
        )
        stored.snapshot = snapshot
        if (
            snapshot.state != LevelState.APPROACH ||
            snapshot.attemptConsumed ||
            (!ignorePreparation && stored.preEntryPreparationInProgress) ||
            snapshot.globalState != GlobalTradingState.RUNNING ||
            snapshot.blockers.isNotEmpty() ||
            !snapshot.signal.mandatoryGates.entryEligible ||
            !snapshot.signal.npu.frozen
        ) {
            return null
        }
        val frozenNpu = snapshot.signal.npu.absolute ?: return null
        val referencePrice = snapshot.signal.latestTradePrice ?: return null
        val preBreakDistance = when (snapshot.direction) {
            LevelDirection.LONG -> if (
                referencePrice < snapshot.normalizedLevelPrice
            ) {
                snapshot.normalizedLevelPrice.subtract(referencePrice)
            } else {
                null
            }

            LevelDirection.SHORT -> if (
                referencePrice > snapshot.normalizedLevelPrice
            ) {
                referencePrice.subtract(snapshot.normalizedLevelPrice)
            } else {
                null
            }
        } ?: return null
        if (preBreakDistance > frozenNpu.multiply(PRE_ENTRY_NPU_MULTIPLIER)) {
            return null
        }
        val bestBidPrice = snapshot.signal.bidPrice ?: return null
        val bestAskPrice = snapshot.signal.askPrice ?: return null
        val precedingTradePrices = stored.signalTracker.tradePrices(
            now = now,
            duration = PRE_ENTRY_MICRO_SWING_WINDOW,
        )
        if (precedingTradePrices.isEmpty()) {
            return null
        }
        val preEntryQuantity = snapshot.entryAllocation
            .single { tranche -> tranche.role == LevelEntryRole.PRE_BREAK }
            .quantity
        return PreEntryOpportunity(
            levelId = snapshot.id,
            attemptNumber = snapshot.attemptNumber,
            symbol = snapshot.symbol,
            direction = snapshot.direction,
            levelPrice = snapshot.normalizedLevelPrice,
            positionNotionalUsdt = snapshot.positionNotionalUsdt,
            plannedQuantity = snapshot.plannedQuantity,
            preEntryQuantity = preEntryQuantity,
            maxImpulsePct = snapshot.maxImpulsePct,
            frozenNpu = frozenNpu,
            precedingOneSecondTradePrices = precedingTradePrices,
            bestBidPrice = bestBidPrice,
            bestAskPrice = bestAskPrice,
            tickSize = stored.tickSize,
            preparedAt = now,
        )
    }

    private fun transitionToMissed(stored: StoredLevel, processedAt: Instant) {
        transitionToTerminal(
            stored = stored,
            processedAt = processedAt,
            reason = LevelReasonCode.MISSED_DURING_WARMUP,
        )
    }

    private fun transitionToTerminal(
        stored: StoredLevel,
        processedAt: Instant,
        reason: LevelReasonCode,
    ) {
        stored.preEntryPreparationInProgress = false
        stored.preparedOpportunity = null
        stored.pendingBreakoutRequest = null
        stored.breakoutRequestClaimed = false
        stored.deferredBreakoutReason = null
        stored.snapshot = stored.snapshot.copy(
            state = LevelState.TERMINAL,
            stateChangedAt = processedAt,
            terminalReason = reason,
        )
    }

    private fun crossesLevel(stored: StoredLevel, tradePrice: BigDecimal): Boolean =
        when (stored.snapshot.direction) {
            LevelDirection.LONG ->
                tradePrice >= stored.snapshot.normalizedLevelPrice

            LevelDirection.SHORT ->
                tradePrice <= stored.snapshot.normalizedLevelPrice
        }

    private fun refreshSignal(
        stored: StoredLevel,
        now: Instant,
        publicMarketData: PublicMarketDataSnapshot?,
    ) {
        stored.snapshot = currentSnapshot(
            stored = stored,
            publicMarketData = publicMarketData,
            privateStreamReadiness = privateStreamReadinessProvider(),
            globalState = globalTradingStateProvider(),
            now = now,
        )
    }

    private fun recordStateTransition(
        before: LevelSnapshot,
        after: LevelSnapshot,
        publicMarketData: PublicMarketDataSnapshot?,
    ) {
        if (before.state == after.state) {
            return
        }
        val decision = when (after.state) {
            LevelState.ARMED -> "WARMUP_COMPLETE"
            LevelState.APPROACH -> "ACTIVATION_BAND_ENTERED"
            LevelState.PRE_ENTRY_PENDING -> "PRE_ENTRY_DISPATCHED"
            LevelState.PRE_ENTRY -> "HARD_STOP_CONFIRMED"
            LevelState.CROSS_ENTRY_PENDING -> "CROSSING_TRADE"
            LevelState.BREAK_CONFIRM -> "CROSSING_FILL_RESOLVED"
            LevelState.CONFIRM_ENTRY_PENDING -> "BREAKOUT_CONFIRMED"
            LevelState.POSITION_MANAGEMENT -> "FINAL_TRANCHE_FILLED"
            LevelState.EXITING ->
                after.terminalReason?.name ?: "BREAKOUT_EXIT"

            LevelState.TERMINAL ->
                after.terminalReason?.name ?: "ATTEMPT_TERMINAL"

            LevelState.WARMING_UP -> "WARMUP_STARTED"
        }
        evidenceRecorder.recordStateTransition(
            before = before,
            after = after,
            marketData = publicMarketData,
            decision = decision,
        )
    }

    private fun levelsForSymbol(symbol: String): List<StoredLevel> =
        levels.values.filter { stored -> stored.snapshot.symbol == symbol }

    private fun validateInput(command: CreateLevelCommand): ValidatedInput {
        val symbol = command.symbol.trim().uppercase()
        if (symbol.isEmpty()) {
            throw levelError(
                LevelReasonCode.INVALID_SYMBOL,
                "Symbol must not be blank",
            )
        }
        requirePositive(command.levelPrice, "levelPrice")
        requirePositive(command.positionNotionalUsdt, "positionNotionalUsdt")
        requirePositive(command.maxImpulsePct, "maxImpulsePct")
        return ValidatedInput(
            symbol = symbol,
            direction = command.direction,
            requestedLevelPrice = command.levelPrice,
            positionNotionalUsdt = command.positionNotionalUsdt,
            maxImpulsePct = command.maxImpulsePct,
        )
    }

    private fun withSymbolConfigurationPermit(
        symbol: String,
        action: () -> Mono<LevelSnapshot>,
    ): Mono<LevelSnapshot> {
        val permit = symbolConfigurationPermits.computeIfAbsent(symbol) {
            Semaphore(1, true)
        }
        return Mono
            .fromCallable {
                permit.acquire()
                permit
            }
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap { acquired ->
                Mono.defer(action)
                    .doFinally { acquired.release() }
            }
    }

    private fun requirePositive(value: BigDecimal, field: String) {
        if (value.signum() <= 0) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "$field must be positive and finite",
            )
        }
    }

    private fun createPlan(
        input: ValidatedInput,
        metadata: BinanceSymbolMetadata,
    ): Mono<LevelPlan> {
        val filters = validateMetadata(metadata)
        val normalizedLevelPrice = roundToIncrement(
            input.requestedLevelPrice,
            filters.tickSize,
            RoundingMode.HALF_UP,
        )
        validateNormalizedPrice(
            normalizedLevelPrice = normalizedLevelPrice,
            symbol = metadata.symbol,
            filters = filters,
        )

        return client
            .markPrice(input.symbol)
            .onErrorMap { error ->
                mapBoundaryFailure(
                    error = error,
                    code = LevelReasonCode.INVALID_LEVEL,
                    message = "Could not load a current price for ${input.symbol}",
                )
            }
            .map { currentPrice ->
                requirePositive(currentPrice, "currentPrice")
                validatePreBreakSide(
                    direction = input.direction,
                    currentPrice = currentPrice,
                    normalizedLevelPrice = normalizedLevelPrice,
                )
                val plannedQuantity = roundToIncrement(
                    input.positionNotionalUsdt.divide(
                        currentPrice,
                        CALCULATION_SCALE,
                        RoundingMode.DOWN,
                    ),
                    filters.lotSize.stepSize,
                    RoundingMode.DOWN,
                )
                val allocation = planAllocation(
                    plannedQuantity = plannedQuantity,
                    minimumExecutionPrice = minOf(
                        currentPrice,
                        normalizedLevelPrice,
                    ),
                    lotSize = filters.lotSize,
                    minimumNotional = filters.minimumNotional,
                )
                PlannedSizing(
                    currentPrice = currentPrice,
                    normalizedLevelPrice = normalizedLevelPrice,
                    plannedQuantity = plannedQuantity,
                    allocation = allocation,
                )
            }
            .flatMap { sizing ->
                client
                    .leverageBrackets(input.symbol)
                    .map { brackets ->
                        val plannedNotional = sizing.plannedQuantity
                            .multiply(sizing.currentPrice)
                        val bracket = applicableBracket(brackets, plannedNotional)
                        val leverage = min(MAX_LEVERAGE, bracket.initialLeverage)
                        if (leverage <= 0) {
                            throw levelError(
                                LevelReasonCode.INVALID_LEVEL,
                                "No executable leverage bracket exists for ${input.symbol}",
                            )
                        }
                        val riskBoundaryStopPrice = riskBoundaryStopPrice(
                            input = input,
                            plannedQuantity = sizing.plannedQuantity,
                            tickSize = filters.tickSize,
                            normalizedLevelPrice = sizing.normalizedLevelPrice,
                        )
                        LevelPlan(
                            input = input,
                            key = LevelKey(
                                symbol = input.symbol,
                                direction = input.direction,
                                normalizedLevelPrice = sizing.normalizedLevelPrice,
                            ),
                            sizing = sizing,
                            tickSize = filters.tickSize,
                            leverage = leverage,
                            projectedIsolatedMargin = plannedNotional.divide(
                                leverage.toBigDecimal(),
                                CALCULATION_SCALE,
                                RoundingMode.UP,
                            ),
                            riskBoundaryStopPrice = riskBoundaryStopPrice,
                            estimatedLiquidationPrice = estimatedLiquidationPrice(
                                direction = input.direction,
                                entryPrice = sizing.normalizedLevelPrice,
                                quantity = sizing.plannedQuantity,
                                leverage = leverage,
                                bracket = bracket,
                            ),
                        )
                    }
                    .onErrorMap { error ->
                        mapBoundaryFailure(
                            error = error,
                            code = LevelReasonCode.INVALID_LEVEL,
                            message = "Could not plan leverage for ${input.symbol}",
                        )
                    }
            }
    }

    private fun validateMetadata(metadata: BinanceSymbolMetadata): ValidatedFilters {
        if (
            metadata.status != TRADING_STATUS ||
            metadata.contractType != PERPETUAL_CONTRACT ||
            metadata.quoteAsset != USDT_ASSET ||
            metadata.marginAsset != USDT_ASSET
        ) {
            throw levelError(
                LevelReasonCode.INVALID_SYMBOL,
                "Symbol ${metadata.symbol} is not a tradable Binance USD-M perpetual",
            )
        }
        val priceFilter = metadata.priceFilter
        val lotSize = metadata.lotSizeFilter
        val minimumNotional = metadata.minimumNotional
        if (
            priceFilter == null ||
            priceFilter.tickSize.signum() <= 0 ||
            priceFilter.minimumPrice.signum() < 0 ||
            priceFilter.maximumPrice.signum() < 0 ||
            (
                priceFilter.minimumPrice.signum() > 0 &&
                    priceFilter.maximumPrice.signum() > 0 &&
                    priceFilter.maximumPrice < priceFilter.minimumPrice
            ) ||
            lotSize == null ||
            lotSize.stepSize.signum() <= 0 ||
            lotSize.minimumQuantity.signum() <= 0 ||
            lotSize.maximumQuantity < lotSize.minimumQuantity ||
            minimumNotional == null ||
            minimumNotional.signum() <= 0
        ) {
            throw levelError(
                LevelReasonCode.INVALID_SYMBOL,
                "Symbol ${metadata.symbol} has incomplete or invalid execution filters",
            )
        }
        return ValidatedFilters(
            minimumPrice = priceFilter.minimumPrice,
            maximumPrice = priceFilter.maximumPrice,
            tickSize = priceFilter.tickSize,
            lotSize = lotSize,
            minimumNotional = minimumNotional,
        )
    }

    private fun validateNormalizedPrice(
        normalizedLevelPrice: BigDecimal,
        symbol: String,
        filters: ValidatedFilters,
    ) {
        val belowMinimum =
            filters.minimumPrice.signum() > 0 &&
                normalizedLevelPrice < filters.minimumPrice
        val aboveMaximum =
            filters.maximumPrice.signum() > 0 &&
                normalizedLevelPrice > filters.maximumPrice
        if (normalizedLevelPrice.signum() <= 0 || belowMinimum || aboveMaximum) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Normalized level price is outside Binance filters for $symbol",
            )
        }
    }

    private fun validatePreBreakSide(
        direction: LevelDirection,
        currentPrice: BigDecimal,
        normalizedLevelPrice: BigDecimal,
    ) {
        val crossed = when (direction) {
            LevelDirection.LONG -> currentPrice >= normalizedLevelPrice
            LevelDirection.SHORT -> currentPrice <= normalizedLevelPrice
        }
        if (crossed) {
            throw levelError(
                LevelReasonCode.LEVEL_ALREADY_CROSSED,
                "Current price must be strictly on the pre-break side of the level",
            )
        }
    }

    private fun planAllocation(
        plannedQuantity: BigDecimal,
        minimumExecutionPrice: BigDecimal,
        lotSize: BinanceLotSizeFilter,
        minimumNotional: BigDecimal,
    ): List<LevelEntryTranche> {
        if (
            plannedQuantity < lotSize.minimumQuantity ||
            plannedQuantity > lotSize.maximumQuantity
        ) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Planned quantity is outside Binance quantity filters",
            )
        }
        val firstQuantity = roundToIncrement(
            plannedQuantity.multiply(THIRTY_PERCENT),
            lotSize.stepSize,
            RoundingMode.DOWN,
        )
        val secondQuantity = roundToIncrement(
            plannedQuantity.multiply(THIRTY_PERCENT),
            lotSize.stepSize,
            RoundingMode.DOWN,
        )
        val finalQuantity = plannedQuantity
            .subtract(firstQuantity)
            .subtract(secondQuantity)
        val allocation = listOf(
            LevelEntryTranche(LevelEntryRole.PRE_BREAK, 30, firstQuantity),
            LevelEntryTranche(LevelEntryRole.CROSSING, 30, secondQuantity),
            LevelEntryTranche(LevelEntryRole.CONFIRMATION, 40, finalQuantity),
        )
        if (
            allocation.any { tranche ->
                tranche.quantity < lotSize.minimumQuantity ||
                    tranche.quantity.multiply(minimumExecutionPrice) < minimumNotional
            }
        ) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Every 30/30/40 entry tranche must satisfy Binance minimum quantity and notional filters",
            )
        }
        return allocation
    }

    private fun applicableBracket(
        brackets: BinanceSymbolLeverageBrackets,
        plannedNotional: BigDecimal,
    ): BinanceLeverageBracket {
        if (brackets.brackets.isEmpty() || brackets.notionalCoefficient.signum() <= 0) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Binance returned no valid leverage brackets for ${brackets.symbol}",
            )
        }
        val ordered = brackets.brackets.sortedBy(BinanceLeverageBracket::notionalFloor)
        return ordered.firstOrNull { bracket ->
            val floor = bracket.notionalFloor.multiply(brackets.notionalCoefficient)
            val cap = bracket.notionalCap.multiply(brackets.notionalCoefficient)
            plannedNotional >= floor && plannedNotional < cap
        } ?: throw levelError(
            LevelReasonCode.INVALID_LEVEL,
            "Planned notional is outside leverage brackets for ${brackets.symbol}",
        )
    }

    private fun riskBoundaryStopPrice(
        input: ValidatedInput,
        plannedQuantity: BigDecimal,
        tickSize: BigDecimal,
        normalizedLevelPrice: BigDecimal,
    ): BigDecimal {
        val maximumPriceLoss = input.positionNotionalUsdt
            .multiply(ONE_PERCENT)
            .divide(plannedQuantity, CALCULATION_SCALE, RoundingMode.DOWN)
        return when (input.direction) {
            LevelDirection.LONG -> roundToIncrement(
                normalizedLevelPrice.subtract(maximumPriceLoss),
                tickSize,
                RoundingMode.UP,
            )

            LevelDirection.SHORT -> roundToIncrement(
                normalizedLevelPrice.add(maximumPriceLoss),
                tickSize,
                RoundingMode.DOWN,
            )
        }
    }

    private fun estimatedLiquidationPrice(
        direction: LevelDirection,
        entryPrice: BigDecimal,
        quantity: BigDecimal,
        leverage: Int,
        bracket: BinanceLeverageBracket,
    ): BigDecimal {
        val maintenanceRatio = bracket.maintenanceMarginRatio
        if (maintenanceRatio.signum() < 0 || maintenanceRatio >= BigDecimal.ONE) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Binance returned an invalid maintenance margin ratio",
            )
        }
        val leverageFraction = BigDecimal.ONE.divide(
            leverage.toBigDecimal(),
            CALCULATION_SCALE,
            RoundingMode.HALF_UP,
        )
        val cumulativePerUnit = bracket.cumulativeMaintenanceAmount.divide(
            quantity,
            CALCULATION_SCALE,
            RoundingMode.HALF_UP,
        )
        val liquidationPrice = when (direction) {
            LevelDirection.LONG -> entryPrice
                .multiply(BigDecimal.ONE.subtract(leverageFraction))
                .subtract(cumulativePerUnit)
                .divide(
                    BigDecimal.ONE.subtract(maintenanceRatio),
                    CALCULATION_SCALE,
                    RoundingMode.HALF_UP,
                )

            LevelDirection.SHORT -> entryPrice
                .multiply(BigDecimal.ONE.add(leverageFraction))
                .add(cumulativePerUnit)
                .divide(
                    BigDecimal.ONE.add(maintenanceRatio),
                    CALCULATION_SCALE,
                    RoundingMode.HALF_UP,
                )
        }
        if (liquidationPrice.signum() <= 0) {
            throw levelError(
                LevelReasonCode.LIQUIDATION_TOO_CLOSE,
                "Estimated liquidation price is not safe",
            )
        }
        return liquidationPrice
    }

    private fun validateLiquidation(plan: LevelPlan) {
        val safe = when (plan.input.direction) {
            LevelDirection.LONG ->
                plan.estimatedLiquidationPrice < plan.riskBoundaryStopPrice

            LevelDirection.SHORT ->
                plan.estimatedLiquidationPrice > plan.riskBoundaryStopPrice
        }
        if (!safe) {
            throw levelError(
                LevelReasonCode.LIQUIDATION_TOO_CLOSE,
                "Estimated liquidation must remain beyond the planned loss boundary",
            )
        }
    }

    private fun configureSymbol(plan: LevelPlan): Mono<Void> =
        client
            .symbolConfiguration(plan.input.symbol)
            .flatMap { current ->
                if (
                    current.marginType == BinanceMarginType.ISOLATED &&
                    current.autoAddMargin
                ) {
                    return@flatMap Mono.error<Void>(
                        levelError(
                            LevelReasonCode.SYMBOL_CONFIGURATION_FAILED,
                            "Auto-Add Margin is enabled for ${plan.input.symbol}",
                        ),
                    )
                }
                val isolated = if (current.marginType == BinanceMarginType.ISOLATED) {
                    Mono.empty<Void>()
                } else {
                    client.changeMarginType(
                        plan.input.symbol,
                        BinanceMarginType.ISOLATED,
                    )
                }
                isolated
                    .then(
                        client.changeInitialLeverage(
                            plan.input.symbol,
                            plan.leverage,
                        ),
                    )
                    .then(client.symbolConfiguration(plan.input.symbol))
                    .flatMap { verified ->
                        val plannedNotional = plan.sizing.plannedQuantity
                            .multiply(plan.sizing.currentPrice)
                        if (
                            verified.symbol == plan.input.symbol &&
                            verified.marginType == BinanceMarginType.ISOLATED &&
                            !verified.autoAddMargin &&
                            verified.leverage == plan.leverage &&
                            verified.maximumNotional >= plannedNotional
                        ) {
                            Mono.empty<Void>()
                        } else {
                            Mono.error<Void>(
                                levelError(
                                    LevelReasonCode.SYMBOL_CONFIGURATION_FAILED,
                                    "Binance symbol configuration verification failed for ${plan.input.symbol}",
                                ),
                            )
                        }
                    }
            }
            .onErrorMap { error ->
                mapBoundaryFailure(
                    error = error,
                    code = LevelReasonCode.SYMBOL_CONFIGURATION_FAILED,
                    message = "Could not establish safe symbol configuration for ${plan.input.symbol}",
                )
            }

    private fun checkCollectionConstraints(key: LevelKey) {
        lock.withLock {
            checkCollectionConstraintsLocked(key)
        }
    }

    private fun checkCollectionConstraintsLocked(key: LevelKey) {
        if (levels.values.any { stored -> stored.key == key }) {
            throw levelError(
                LevelReasonCode.DUPLICATE_LEVEL,
                "An exact normalized level already exists",
            )
        }
        if (levels.size >= MAX_LEVELS) {
            throw levelError(
                LevelReasonCode.LEVEL_CAPACITY_REACHED,
                "At most $MAX_LEVELS levels may be stored",
            )
        }
    }

    private fun store(plan: LevelPlan): Mono<LevelSnapshot> {
        val runtime = try {
            reserveSymbolRuntime(plan.input.symbol)
        } catch (error: RuntimeException) {
            return Mono.error(
                levelError(
                    LevelReasonCode.INVALID_LEVEL,
                    "Could not start ordered market processing for ${plan.input.symbol}",
                    error,
                ),
            )
        }
        val publicMarketData = currentPublicMarketData(plan.input.symbol)
        return submit(
            runtime,
            SymbolLevelEvent.AddLevel(
                plan = plan,
                processedAt = clock.instant(),
                publicMarketData = publicMarketData,
                marketHealthy = publicMarketData?.healthy == true,
            ),
        )
            .map { result -> result as LevelSnapshot }
    }

    private fun reserveSymbolRuntime(symbol: String): SymbolRuntime {
        var created = false
        val runtime = lock.withLock {
            val selected = symbolRuntimes[symbol] ?: run {
                val marketDataSubscription = publicMarketDataService.observe(symbol)
                val queue = OrderedSymbolEventQueue<SymbolLevelEvent>(
                    symbol = symbol,
                    scheduler = eventScheduler,
                    handler = { event -> handleSymbolEvent(symbol, event) },
                )
                SymbolRuntime(
                    symbol = symbol,
                    queue = queue,
                    marketDataSubscription = marketDataSubscription,
                ).also {
                    symbolRuntimes[symbol] = it
                    created = true
                }
            }
            selected.pendingEvents += 1
            selected
        }
        if (created) {
            try {
                startRuntime(runtime)
            } catch (error: RuntimeException) {
                releasePendingEvent(runtime)
                throw error
            }
        }
        return runtime
    }

    private fun startRuntime(runtime: SymbolRuntime) {
        runtime.subscriptions += runtime.marketDataSubscription.aggregateTrades
            .onErrorComplete()
            .subscribe { event ->
                val marketData = currentPublicMarketData(runtime.symbol)
                runtime.queue.publish(
                    SymbolLevelEvent.AggregateTrade(
                        event = event,
                        processedAt = clock.instant(),
                        publicMarketData = marketData,
                        marketHealthy = marketData?.healthy == true,
                    ),
                )
            }
        runtime.subscriptions += runtime.marketDataSubscription.bookTickers
            .onErrorComplete()
            .subscribe { event ->
                val marketData = currentPublicMarketData(runtime.symbol)
                runtime.queue.publish(
                    SymbolLevelEvent.BookTicker(
                        event = event,
                        processedAt = clock.instant(),
                        publicMarketData = marketData,
                        marketHealthy = marketData?.healthy == true,
                    ),
                )
            }
        if (automaticTimers) {
            runtime.subscriptions += Flux
                .interval(EVENT_SAMPLE_INTERVAL, EVENT_SAMPLE_INTERVAL, eventScheduler)
                .subscribe {
                    val marketData = currentPublicMarketData(runtime.symbol)
                    runtime.queue.publish(
                        SymbolLevelEvent.Timer(
                            processedAt = clock.instant(),
                            publicMarketData = marketData,
                            marketHealthy = marketData?.healthy == true,
                        ),
                    )
                }
        }
    }

    private fun closeRuntimeWhenUnused(symbol: String) {
        val runtime = lock.withLock {
            val current = symbolRuntimes[symbol]
            if (
                current == null ||
                current.pendingEvents > 0 ||
                levels.values.any { stored -> stored.snapshot.symbol == symbol }
            ) {
                null
            } else {
                symbolRuntimes.remove(symbol)
            }
        }
        runtime?.close()
    }

    private fun submit(
        runtime: SymbolRuntime,
        event: SymbolLevelEvent,
    ): Mono<Any> =
        try {
            runtime.queue.submit(
                event = event,
                afterProcessed = { releasePendingEvent(runtime) },
            )
        } catch (error: RuntimeException) {
            releasePendingEvent(runtime)
            Mono.error(error)
        }

    private fun releasePendingEvent(runtime: SymbolRuntime) {
        lock.withLock {
            check(runtime.pendingEvents > 0) {
                "No pending event exists for ${runtime.symbol}"
            }
            runtime.pendingEvents -= 1
        }
        closeRuntimeWhenUnused(runtime.symbol)
    }

    private fun publicMarketData(levelId: UUID): PublicMarketDataSnapshot? {
        val symbol = lock.withLock { levels[levelId]?.snapshot?.symbol }
            ?: return null
        return currentPublicMarketData(symbol)
    }

    private fun currentPublicMarketData(symbol: String): PublicMarketDataSnapshot? =
        publicMarketDataProvider(symbol)

    private fun currentSnapshot(
        stored: StoredLevel,
        publicMarketData: PublicMarketDataSnapshot?,
        privateStreamReadiness: BinanceReadiness,
        globalState: GlobalTradingState,
        now: Instant = clock.instant(),
    ): LevelSnapshot =
        stored.snapshot.copy(
            globalState = globalState,
            blockers = blockers(stored, globalState),
            signal = stored.signalTracker.snapshot(
                publicMarketData = publicMarketData,
                privateStreamReadiness = privateStreamReadiness,
                hasUnresolvedOrder = stored.snapshot.hasUnresolvedOrder,
                now = now,
            ),
        )

    private fun blockers(
        stored: StoredLevel,
        globalState: GlobalTradingState,
    ): List<LevelBlocker> =
        buildList {
            when (stored.snapshot.state) {
                LevelState.WARMING_UP -> add(LevelBlocker.WARMING_UP)
                LevelState.TERMINAL -> add(LevelBlocker.TERMINAL)
                LevelState.ARMED,
                LevelState.APPROACH,
                LevelState.PRE_ENTRY_PENDING,
                LevelState.PRE_ENTRY,
                LevelState.CROSS_ENTRY_PENDING,
                LevelState.BREAK_CONFIRM,
                LevelState.CONFIRM_ENTRY_PENDING,
                LevelState.POSITION_MANAGEMENT,
                LevelState.EXITING,
                -> Unit
            }
            when (globalState) {
                GlobalTradingState.RUNNING -> Unit
                GlobalTradingState.ENTRY_COOLDOWN ->
                    add(LevelBlocker.ENTRY_COOLDOWN)

                GlobalTradingState.SAFE_MODE -> add(LevelBlocker.SAFE_MODE)
                GlobalTradingState.DAILY_LOCKED -> add(LevelBlocker.DAILY_LOCKED)
                GlobalTradingState.MANUAL_LOCK -> add(LevelBlocker.MANUAL_LOCK)
            }
            val anotherOwner = levels.values.any { candidate ->
                candidate.snapshot.symbol == stored.snapshot.symbol &&
                    candidate.snapshot.id != stored.snapshot.id &&
                    candidate.claimsSymbolOwnership()
            }
            if (!stored.claimsSymbolOwnership() && anotherOwner) {
                add(LevelBlocker.SYMBOL_HAS_ACTIVE_OWNER)
            }
        }

    private fun roundToIncrement(
        value: BigDecimal,
        increment: BigDecimal,
        roundingMode: RoundingMode,
    ): BigDecimal {
        val incrementScale = increment.stripTrailingZeros().scale().coerceAtLeast(0)
        return value
            .divide(increment, 0, roundingMode)
            .multiply(increment)
            .setScale(incrementScale)
    }

    private fun mapBoundaryFailure(
        error: Throwable,
        code: LevelReasonCode,
        message: String,
    ): Throwable =
        if (error is LevelException) {
            error
        } else {
            levelError(code, message, error)
        }
}

private data class StoredLevel(
    val key: LevelKey,
    var snapshot: LevelSnapshot,
    val signalTracker: LevelSignalTracker,
    val tickSize: BigDecimal,
    var npuMode: NpuMode = NpuMode.WARMING_UP,
    var preEntryPreparationInProgress: Boolean = false,
    var preparedOpportunity: PreEntryOpportunity? = null,
    val breakoutStateMachine: BreakoutStateMachine = BreakoutStateMachine(),
    var pendingBreakoutRequest: BreakoutAdditionRequest? = null,
    var breakoutRequestClaimed: Boolean = false,
    var deferredBreakoutReason: LevelReasonCode? = null,
) {
    fun claimsSymbolOwnership(): Boolean =
        snapshot.ownsActiveAttempt ||
            snapshot.ownsExposure ||
            snapshot.hasUnresolvedOrder
}

private data class SymbolRuntime(
    val symbol: String,
    val queue: OrderedSymbolEventQueue<SymbolLevelEvent>,
    val marketDataSubscription: PublicMarketDataSubscription,
    val subscriptions: MutableList<Disposable> = mutableListOf(),
    var pendingEvents: Int = 0,
) : AutoCloseable {
    override fun close() {
        subscriptions.forEach(Disposable::dispose)
        subscriptions.clear()
        queue.close()
        marketDataSubscription.close()
    }
}

private data class ValidatedInput(
    val symbol: String,
    val direction: LevelDirection,
    val requestedLevelPrice: BigDecimal,
    val positionNotionalUsdt: BigDecimal,
    val maxImpulsePct: BigDecimal,
)

private data class ValidatedFilters(
    val minimumPrice: BigDecimal,
    val maximumPrice: BigDecimal,
    val tickSize: BigDecimal,
    val lotSize: BinanceLotSizeFilter,
    val minimumNotional: BigDecimal,
)

private data class PlannedSizing(
    val currentPrice: BigDecimal,
    val normalizedLevelPrice: BigDecimal,
    val plannedQuantity: BigDecimal,
    val allocation: List<LevelEntryTranche>,
)

private data class LevelPlan(
    val input: ValidatedInput,
    val key: LevelKey,
    val sizing: PlannedSizing,
    val tickSize: BigDecimal,
    val leverage: Int,
    val projectedIsolatedMargin: BigDecimal,
    val riskBoundaryStopPrice: BigDecimal,
    val estimatedLiquidationPrice: BigDecimal,
)

private data class LevelKey(
    val symbol: String,
    val direction: LevelDirection,
    val normalizedLevelPrice: BigDecimal,
)

private sealed interface SymbolLevelEvent {
    data class AddLevel(
        val plan: LevelPlan,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
        val marketHealthy: Boolean,
    ) : SymbolLevelEvent

    data class DeleteLevel(
        val levelId: UUID,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
    ) : SymbolLevelEvent

    data class UpdateOwnership(
        val levelId: UUID,
        val ownsActiveAttempt: Boolean,
        val ownsExposure: Boolean,
        val hasUnresolvedOrder: Boolean,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
    ) : SymbolLevelEvent

    data class PreparePreEntry(
        val levelId: UUID,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
    ) : SymbolLevelEvent

    data class PreEntryDispatched(
        val levelId: UUID,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
    ) : SymbolLevelEvent

    data class CancelPreEntryPreparation(
        val levelId: UUID,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
    ) : SymbolLevelEvent

    data class PreEntryProtected(
        val levelId: UUID,
        val confirmedPositionQuantity: BigDecimal,
        val hardStopClientOrderId: String,
        val hardStopPrice: BigDecimal,
        val preEntryFilledAt: Instant?,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
    ) : SymbolLevelEvent

    data class TerminatePreEntry(
        val levelId: UUID,
        val reason: LevelReasonCode,
        val confirmedRemainingQuantity: BigDecimal,
        val hasUnresolvedOrder: Boolean,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
    ) : SymbolLevelEvent

    data class ValidateBreakoutAddition(
        val request: BreakoutAdditionRequest,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
    ) : SymbolLevelEvent

    data class CrossingFilled(
        val requestId: String,
        val levelId: UUID,
        val confirmedPositionQuantity: BigDecimal,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
    ) : SymbolLevelEvent

    data class FinalFilled(
        val requestId: String,
        val levelId: UUID,
        val confirmedPositionQuantity: BigDecimal,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
    ) : SymbolLevelEvent

    data class AggregateTrade(
        val event: AggregateTradeEvent,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
        val marketHealthy: Boolean,
    ) : SymbolLevelEvent

    data class BookTicker(
        val event: BookTickerEvent,
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
        val marketHealthy: Boolean,
    ) : SymbolLevelEvent

    data class Timer(
        val processedAt: Instant,
        val publicMarketData: PublicMarketDataSnapshot?,
        val marketHealthy: Boolean,
    ) : SymbolLevelEvent

    data class Execution(
        val eventId: String,
        val processedAt: Instant,
        val action: () -> Any,
    ) : SymbolLevelEvent
}

private fun levelError(
    code: LevelReasonCode,
    message: String,
    cause: Throwable? = null,
): LevelException = LevelException(code, message, cause)

private const val TRADING_STATUS = "TRADING"
private const val PERPETUAL_CONTRACT = "PERPETUAL"
private const val USDT_ASSET = "USDT"
private const val MAX_LEVELS = 100
private const val MAX_LEVERAGE = 20
private const val CALCULATION_SCALE = 16
private val EVENT_SAMPLE_INTERVAL: Duration = Duration.ofMillis(100)
private val WARMUP_DURATION: Duration = Duration.ofSeconds(10)
private val ACTIVATION_NPU_MULTIPLIER = BigDecimal("8")
private val PRE_ENTRY_NPU_MULTIPLIER = BigDecimal("2")
private val PRE_ENTRY_MICRO_SWING_WINDOW: Duration = Duration.ofSeconds(1)
private val TWO = BigDecimal("2")
private val THIRTY_PERCENT = BigDecimal("0.30")
private val ONE_PERCENT = BigDecimal("0.01")
