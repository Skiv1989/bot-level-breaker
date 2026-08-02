package com.scalpsecta.breakoutbot.failure

import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceClient
import com.scalpsecta.breakoutbot.binance.BinanceAssetMode
import com.scalpsecta.breakoutbot.binance.BinanceMarginType
import com.scalpsecta.breakoutbot.binance.BinancePositionRisk
import com.scalpsecta.breakoutbot.binance.BinancePositionMode
import com.scalpsecta.breakoutbot.execution.BreakoutExecutionService
import com.scalpsecta.breakoutbot.execution.ExecutionRuntimeReconciliation
import com.scalpsecta.breakoutbot.execution.ExecutionService
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.level.LevelService
import com.scalpsecta.breakoutbot.level.LevelSnapshot
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Instant

data class SignedRuntimeReconciliation(
    val observedAt: Instant,
    val positions: List<BinancePositionRisk>,
    val openBotOrderIds: Set<String>,
    val orphanedBotOrderIds: Set<String>,
    val unresolvedOrderIds: Set<String>,
    val unexplainedPositionSymbols: Set<String>,
    val symbolChecksHealthy: Boolean,
    val accountChecksHealthy: Boolean = true,
    internal val execution: ExecutionRuntimeReconciliation? = null,
) {
    fun fingerprint(): RuntimeReconciliationFingerprint =
        RuntimeReconciliationFingerprint(
            positions = positions
                .sortedBy(BinancePositionRisk::symbol)
                .map { position ->
                    ReconciledPositionFingerprint(
                        symbol = position.symbol,
                        positionAmount = position.positionAmount.stripTrailingZeros(),
                        entryPrice = position.entryPrice.stripTrailingZeros(),
                    )
                },
            openBotOrderIds = openBotOrderIds.toSortedSet(),
            orphanedBotOrderIds = orphanedBotOrderIds.toSortedSet(),
            unresolvedOrderIds = unresolvedOrderIds.toSortedSet(),
            unexplainedPositionSymbols = unexplainedPositionSymbols.toSortedSet(),
            symbolChecksHealthy = symbolChecksHealthy,
            accountChecksHealthy = accountChecksHealthy,
        )
}

data class RuntimeReconciliationFingerprint(
    val positions: List<ReconciledPositionFingerprint>,
    val openBotOrderIds: Set<String>,
    val orphanedBotOrderIds: Set<String>,
    val unresolvedOrderIds: Set<String>,
    val unexplainedPositionSymbols: Set<String>,
    val symbolChecksHealthy: Boolean,
    val accountChecksHealthy: Boolean,
)

data class ReconciledPositionFingerprint(
    val symbol: String,
    val positionAmount: BigDecimal,
    val entryPrice: BigDecimal,
)

interface SafeModeExecutionGateway {
    fun runtimeSymbols(): Set<String>

    fun hasTrackedExposure(): Boolean

    fun reconcile(): Mono<SignedRuntimeReconciliation>

    fun closeReconciledExposure(
        reason: LevelReasonCode,
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void>

    fun flattenAllAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void>

    fun flattenAllAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
        terminalReason: LevelReasonCode,
    ): Mono<Void> = flattenAllAccountExposure(reconciliation, operationId)
}

@Service
class LiveSafeModeExecutionGateway(
    private val executionService: ExecutionService,
    private val levelService: LevelService,
    private val breakoutExecutionService: BreakoutExecutionService,
    private val authenticatedBinanceClient: AuthenticatedBinanceClient,
    private val riskService: AttemptRiskService,
) : SafeModeExecutionGateway {
    override fun runtimeSymbols(): Set<String> =
        levels().mapTo(sortedSetOf(), LevelSnapshot::symbol)

    override fun hasTrackedExposure(): Boolean =
        levels().any { level ->
            level.ownsExposure || level.confirmedPositionQuantity.signum() != 0
        }

    override fun reconcile(): Mono<SignedRuntimeReconciliation> =
        executionService.reconcileRuntime().flatMap { execution ->
            val levels = levels()
            Mono.zip(symbolChecks(levels), accountChecks()).map { checks ->
                val expectedPositions = expectedPositions(levels)
                val actualPositions = execution.positions.associateBy { position ->
                    position.symbol
                }
                val unexplainedSymbols = buildSet {
                    actualPositions.forEach { (symbol, position) ->
                        if (
                            expectedPositions[symbol]
                                ?.compareTo(position.positionAmount) != 0
                        ) {
                            add(symbol)
                        }
                    }
                    expectedPositions.forEach { (symbol, expectedAmount) ->
                        if (
                            expectedAmount.signum() != 0 &&
                            actualPositions[symbol] == null
                        ) {
                            add(symbol)
                        }
                    }
                }
                SignedRuntimeReconciliation(
                    observedAt = execution.observedAt,
                    positions = execution.positions,
                    openBotOrderIds = execution.openBotOrders
                        .mapTo(sortedSetOf()) { order -> order.clientOrderId },
                    orphanedBotOrderIds = execution.orphanedBotOrderIds,
                    unresolvedOrderIds = execution.unresolvedOrderIds,
                    unexplainedPositionSymbols = unexplainedSymbols,
                    symbolChecksHealthy = checks.t1,
                    accountChecksHealthy = checks.t2,
                    execution = execution,
                )
            }
        }

    override fun closeReconciledExposure(
        reason: LevelReasonCode,
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void> {
        val positionsBySymbol = reconciliation.positions.associateBy { position ->
            position.symbol
        }
        val exitPlans = levels()
            .filter { level ->
                level.ownsExposure ||
                    level.confirmedPositionQuantity.signum() != 0
            }
            .sortedBy { level -> level.id.toString() }
            .mapNotNull { level ->
                val position = positionsBySymbol[level.symbol]
                    ?: BinancePositionRisk(
                        symbol = level.symbol,
                        positionAmount = BigDecimal.ZERO,
                        entryPrice = BigDecimal.ZERO,
                    )
                if (positionMatchesDirection(level, position.positionAmount)) {
                    level to position
                } else {
                    null
                }
            }
        val handledSymbols = exitPlans.mapTo(mutableSetOf()) { (level, _) ->
            level.symbol
        }
        val levelExits = Flux.fromIterable(exitPlans)
            .concatMap { (level, position) ->
                executionService
                    .cancelActiveTakeProfits(level.id)
                    .then(
                        levelService.claimFailureExit(
                            levelId = level.id,
                            reason = reason,
                            reconciledPositionAmount = position.positionAmount,
                        ),
                    )
                    .flatMap { request ->
                        if (request == null) {
                            Mono.empty()
                        } else {
                            breakoutExecutionService.execute(request).then()
                        }
                    }
            }
            .then()
        val unexplainedPositions = reconciliation.positions.filter { position ->
            position.symbol !in handledSymbols
        }
        return levelExits.then(
            executionService
                .closeAccountPositions(unexplainedPositions, operationId)
                .then(),
        )
    }

    override fun flattenAllAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void> = flattenAccountExposure(
        reconciliation = reconciliation,
        operationId = operationId,
        terminalReason = null,
    )

    override fun flattenAllAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
        terminalReason: LevelReasonCode,
    ): Mono<Void> = flattenAccountExposure(
        reconciliation = reconciliation,
        operationId = operationId,
        terminalReason = terminalReason,
    )

    private fun flattenAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
        terminalReason: LevelReasonCode?,
    ): Mono<Void> {
        val execution = checkNotNull(reconciliation.execution) {
            "Live account flattening requires an execution reconciliation"
        }
        return executionService
            .cancelBotOrders(
                openOrders = execution.openBotOrders,
                retainHardStops = true,
            )
            .flatMap { canceled ->
                if (!canceled) {
                    Mono.error(
                        IllegalStateException(
                            "Could not cancel every non-stop bot order",
                        ),
                    )
                } else {
                    executionService.closeAccountPositions(
                        reconciledPositions = reconciliation.positions,
                        operationId = operationId,
                    ).then(reconcile())
                }
            }
            .flatMap { confirmed ->
                if (confirmed.positions.isEmpty()) {
                    val confirmedExecution = checkNotNull(confirmed.execution) {
                        "Live account flattening requires a final execution reconciliation"
                    }
                    executionService.cancelBotOrders(
                        openOrders = confirmedExecution.openBotOrders,
                        retainHardStops = false,
                    ).flatMap { canceled ->
                        if (!canceled) {
                            Mono.error(
                                IllegalStateException(
                                    "Could not cancel protective hard stops",
                                ),
                            )
                        } else if (terminalReason == null) {
                            Mono.empty()
                        } else {
                            synchronizeConfirmedFlat(terminalReason)
                        }
                    }
                } else {
                    Mono.empty()
                }
            }
    }

    private fun synchronizeConfirmedFlat(
        terminalReason: LevelReasonCode,
    ): Mono<Void> {
        val levelsById = levels().associateBy(LevelSnapshot::id)
        val activeAttemptIds = riskService
            .currentState()
            .reservations
            .map { reservation -> reservation.levelId }
        return Flux
            .fromIterable(activeAttemptIds)
            .concatMap { levelId ->
                Mono.defer {
                    if (
                        riskService.currentState().reservations.none {
                            reservation -> reservation.levelId == levelId
                        }
                    ) {
                        return@defer Mono.empty()
                    }
                    val netResult = executionService.positionResult(levelId)
                    val terminateLevel = levelsById[levelId]?.let {
                        levelService.terminatePosition(
                            levelId = levelId,
                            reason = terminalReason,
                            confirmedRemainingQuantity = BigDecimal.ZERO,
                            hasUnresolvedOrder = false,
                            netResult = netResult,
                        )
                    } ?: Mono.empty()
                    terminateLevel.then(
                        riskService.recordConfirmedFlat(
                            levelId = levelId,
                            netPnl = netResult?.netPnl,
                        ),
                    )
                }
            }
            .then()
    }

    private fun levels(): List<LevelSnapshot> = levelService.currentState()

    private fun expectedPositions(
        levels: List<LevelSnapshot>,
    ): Map<String, BigDecimal> =
        levels
            .filter { level ->
                level.ownsExposure || level.confirmedPositionQuantity.signum() != 0
            }
            .groupBy(LevelSnapshot::symbol)
            .mapValues { (_, symbolLevels) ->
                symbolLevels.fold(BigDecimal.ZERO) { total, level ->
                    total.add(
                        when (level.direction) {
                            LevelDirection.LONG -> level.confirmedPositionQuantity
                            LevelDirection.SHORT ->
                                level.confirmedPositionQuantity.negate()
                        },
                    )
                }
            }

    private fun symbolChecks(levels: List<LevelSnapshot>): Mono<Boolean> =
        Flux
            .fromIterable(levels.distinctBy(LevelSnapshot::symbol))
            .concatMap { level ->
                authenticatedBinanceClient
                    .symbolConfiguration(level.symbol)
                    .map { configuration ->
                        configuration.marginType == BinanceMarginType.ISOLATED &&
                            !configuration.autoAddMargin &&
                            configuration.leverage == level.leverage
                    }
                    .onErrorReturn(false)
            }
            .all { healthy -> healthy }

    private fun accountChecks(): Mono<Boolean> =
        Mono.zip(
            authenticatedBinanceClient.accountSummary(),
            authenticatedBinanceClient.positionMode(),
            authenticatedBinanceClient.assetMode(),
        )
            .map { checks ->
                checks.t1.canTrade &&
                    checks.t2 == BinancePositionMode.ONE_WAY &&
                    checks.t3 == BinanceAssetMode.SINGLE_ASSET
            }
            .onErrorReturn(false)

    private fun positionMatchesDirection(
        level: LevelSnapshot,
        positionAmount: BigDecimal,
    ): Boolean =
        positionAmount.signum() == 0 ||
            when (level.direction) {
                LevelDirection.LONG -> positionAmount.signum() > 0
                LevelDirection.SHORT -> positionAmount.signum() < 0
            }
}
