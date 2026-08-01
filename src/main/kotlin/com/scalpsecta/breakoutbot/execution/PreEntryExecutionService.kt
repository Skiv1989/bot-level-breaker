package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceClient
import com.scalpsecta.breakoutbot.binance.BinanceSymbolLeverageBrackets
import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.risk.AttemptAdmissionRequest
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import com.scalpsecta.breakoutbot.risk.RiskAccountState
import com.scalpsecta.breakoutbot.risk.RiskLeverageBracket
import com.scalpsecta.breakoutbot.service.AuthenticatedBinanceReadinessService
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.publisher.Mono
import java.math.BigDecimal

@Service
class PreEntryExecutionService internal constructor(
    private val levelCoordinator: PreEntryLevelCoordinator,
    private val riskContextProvider: PreEntryRiskContextProvider,
    private val riskService: AttemptRiskService,
    private val orderExecutor: PreEntryOrderExecutor,
    automaticDispatch: Boolean,
) {
    @Autowired
    constructor(
        levelCoordinator: PreEntryLevelCoordinator,
        riskContextProvider: PreEntryRiskContextProvider,
        riskService: AttemptRiskService,
        orderExecutor: PreEntryOrderExecutor,
    ) : this(
        levelCoordinator = levelCoordinator,
        riskContextProvider = riskContextProvider,
        riskService = riskService,
        orderExecutor = orderExecutor,
        automaticDispatch = true,
    )

    private val automaticSubscription: Disposable? = if (automaticDispatch) {
        levelCoordinator
            .opportunities()
            .flatMap(
                { levelId ->
                    execute(levelId).onErrorResume { Mono.empty() }
                },
                MAXIMUM_CONCURRENT_ATTEMPTS,
            )
            .subscribe()
    } else {
        null
    }

    fun execute(levelId: java.util.UUID): Mono<PreEntryResult> =
        levelCoordinator
            .prepare(levelId)
            .flatMap(::admitAndDispatch)
            .onErrorResume { error ->
                levelCoordinator
                    .cancelPreparation(levelId)
                    .onErrorResume { Mono.empty() }
                    .then(Mono.error(error))
            }
            .cache()

    @PreDestroy
    fun close() {
        automaticSubscription?.dispose()
    }

    private fun admitAndDispatch(
        opportunity: PreEntryOpportunity,
    ): Mono<PreEntryResult> =
        riskContextProvider
            .load(opportunity)
            .flatMap { context ->
                riskService.admit(
                    request = opportunity.admissionRequest(context),
                    accountState = context.accountState,
                )
            }
            .flatMap { decision ->
                if (!decision.admitted) {
                    return@flatMap levelCoordinator
                        .cancelPreparation(opportunity.levelId)
                        .thenReturn(
                            PreEntryResult(
                                levelId = opportunity.levelId,
                                status = PreEntryResultStatus.RISK_BLOCKED,
                                requestedQuantity =
                                    opportunity.preEntryQuantity,
                            ),
                        )
                }
                levelCoordinator
                    .markDispatched(opportunity.levelId)
                    .onErrorResume { error ->
                        riskService
                            .recordConfirmedFlat(opportunity.levelId)
                            .onErrorResume { Mono.empty() }
                            .then(Mono.error(error))
                    }
                    .then(
                        Mono.defer {
                            orderExecutor.execute(
                                opportunity.entryRequest(
                                    limitPrice =
                                        decision.plan.worstCappedEntryPrice,
                                ),
                            )
                        },
                    )
                    .flatMap { resolution ->
                        handleEntryResolution(
                            opportunity = opportunity,
                            structuralStopPrice =
                                decision.plan.structuralStopPrice,
                            resolution = resolution,
                        )
                    }
            }

    private fun handleEntryResolution(
        opportunity: PreEntryOpportunity,
        structuralStopPrice: BigDecimal,
        resolution: OrderResolution,
    ): Mono<PreEntryResult> {
        val confirmedPositionAmount = resolution.confirmedPositionAmount
        val confirmedQuantity = confirmedPositionAmount.abs()
        val recordExposure = if (confirmedQuantity.signum() > 0) {
            riskService
                .recordConfirmedExposure(
                    levelId = opportunity.levelId,
                    confirmedPositionQuantity = confirmedQuantity,
                )
                .then()
        } else {
            Mono.empty()
        }
        return recordExposure.then(
            Mono.defer {
                when {
                    resolution.outcome == OrderOutcome.UNKNOWN ->
                        levelCoordinator
                            .terminate(
                                levelId = opportunity.levelId,
                                reason = LevelReasonCode.ORDER_OUTCOME_UNKNOWN,
                                confirmedRemainingQuantity = confirmedQuantity,
                                hasUnresolvedOrder = true,
                            )
                            .thenReturn(
                                terminatedResult(
                                    opportunity = opportunity,
                                    resolution = resolution,
                                    reason =
                                        LevelReasonCode.ORDER_OUTCOME_UNKNOWN,
                                    remainingQuantity = confirmedQuantity,
                                ),
                            )

                    levelCoordinator.crossedBeforeProtection(
                        opportunity.levelId,
                    ) -> terminateAndClose(
                        opportunity = opportunity,
                        resolution = resolution,
                        reason = LevelReasonCode.CROSS_BEFORE_PROTECTED,
                        confirmedPositionAmount = confirmedPositionAmount,
                    )

                    !minimumFillSatisfied(
                        filledQuantity = resolution.actualFilledQuantity,
                        requestedQuantity = opportunity.preEntryQuantity,
                    ) -> terminateAndClose(
                        opportunity = opportunity,
                        resolution = resolution,
                        reason = LevelReasonCode.INSUFFICIENT_LIQUIDITY,
                        confirmedPositionAmount = confirmedPositionAmount,
                    )

                    confirmedQuantity.signum() == 0 ->
                        failStopSetup(
                            opportunity = opportunity,
                            resolution = resolution,
                            confirmedPositionAmount =
                                confirmedPositionAmount,
                            hardStopConfirmation = null,
                        )

                    else -> confirmProtection(
                        opportunity = opportunity,
                        resolution = resolution,
                        structuralStopPrice = structuralStopPrice,
                        confirmedPositionAmount = confirmedPositionAmount,
                    )
                }
            },
        )
    }

    private fun confirmProtection(
        opportunity: PreEntryOpportunity,
        resolution: OrderResolution,
        structuralStopPrice: BigDecimal,
        confirmedPositionAmount: BigDecimal,
    ): Mono<PreEntryResult> =
        orderExecutor
            .confirmHardStop(
                OrderIntentRequest(
                    levelId = opportunity.levelId,
                    attemptNumber = opportunity.attemptNumber,
                    symbol = opportunity.symbol,
                    role = OrderRole.HARD_STOP,
                    slot = PRE_ENTRY_SLOT,
                    side = closingSide(confirmedPositionAmount),
                    type = OrderType.STOP_MARKET,
                    stopPrice = structuralStopPrice,
                    workingType = TriggerWorkingType.CONTRACT_PRICE,
                    priceProtect = false,
                    closePosition = true,
                    confirmedPositionAmount = confirmedPositionAmount,
                ),
            )
            .flatMap { confirmation ->
                when {
                    !confirmation.confirmed -> failStopSetup(
                        opportunity = opportunity,
                        resolution = resolution,
                        confirmedPositionAmount = confirmedPositionAmount,
                        hardStopConfirmation = confirmation,
                    )

                    levelCoordinator.crossedBeforeProtection(
                        opportunity.levelId,
                    ) -> terminateAndClose(
                        opportunity = opportunity,
                        resolution = resolution,
                        reason = LevelReasonCode.CROSS_BEFORE_PROTECTED,
                        confirmedPositionAmount = confirmedPositionAmount,
                        hardStopConfirmation = confirmation,
                        persistentUnresolvedOrder = true,
                    )

                    else -> levelCoordinator
                        .markProtected(
                            levelId = opportunity.levelId,
                            confirmedPositionQuantity =
                                confirmation.confirmedPositionAmount.abs(),
                            hardStopClientOrderId =
                                confirmation.intent.clientOrderId,
                            hardStopPrice = structuralStopPrice,
                        )
                        .thenReturn(
                            PreEntryResult(
                                levelId = opportunity.levelId,
                                status = PreEntryResultStatus.PROTECTED,
                                requestedQuantity =
                                    opportunity.preEntryQuantity,
                                actualFilledQuantity =
                                    resolution.actualFilledQuantity,
                                confirmedPositionQuantity =
                                    confirmation.confirmedPositionAmount.abs(),
                                hardStopConfirmation = confirmation,
                            ),
                        )
                        .onErrorResume { error ->
                            if (
                                levelCoordinator.crossedBeforeProtection(
                                    opportunity.levelId,
                                )
                            ) {
                                terminateAndClose(
                                    opportunity = opportunity,
                                    resolution = resolution,
                                    reason =
                                        LevelReasonCode.CROSS_BEFORE_PROTECTED,
                                    confirmedPositionAmount =
                                        confirmation.confirmedPositionAmount,
                                    hardStopConfirmation = confirmation,
                                    persistentUnresolvedOrder = true,
                                )
                            } else {
                                Mono.error(error)
                            }
                        }
                }
            }

    private fun failStopSetup(
        opportunity: PreEntryOpportunity,
        resolution: OrderResolution,
        confirmedPositionAmount: BigDecimal,
        hardStopConfirmation: HardStopConfirmation?,
    ): Mono<PreEntryResult> =
        riskService
            .enterSafeMode(LevelReasonCode.STOP_SETUP_FAILED.name)
            .then(
                terminateAndClose(
                    opportunity = opportunity,
                    resolution = resolution,
                    reason = LevelReasonCode.STOP_SETUP_FAILED,
                    confirmedPositionAmount =
                        hardStopConfirmation?.confirmedPositionAmount
                            ?: confirmedPositionAmount,
                    hardStopConfirmation = hardStopConfirmation,
                    persistentUnresolvedOrder =
                        hardStopConfirmation?.confirmed == false,
                ),
            )

    private fun terminateAndClose(
        opportunity: PreEntryOpportunity,
        resolution: OrderResolution,
        reason: LevelReasonCode,
        confirmedPositionAmount: BigDecimal,
        hardStopConfirmation: HardStopConfirmation? = null,
        persistentUnresolvedOrder: Boolean = false,
    ): Mono<PreEntryResult> {
        val confirmedQuantity = confirmedPositionAmount.abs()
        val terminal = levelCoordinator.terminate(
            levelId = opportunity.levelId,
            reason = reason,
            confirmedRemainingQuantity = confirmedQuantity,
            hasUnresolvedOrder = persistentUnresolvedOrder,
        )
        if (confirmedQuantity.signum() == 0) {
            return terminal
                .then(riskService.recordConfirmedFlat(opportunity.levelId))
                .thenReturn(
                    terminatedResult(
                        opportunity = opportunity,
                        resolution = resolution,
                        reason = reason,
                        hardStopConfirmation = hardStopConfirmation,
                    ),
                )
        }
        return terminal
            .then(
                orderExecutor.execute(
                    OrderIntentRequest(
                        levelId = opportunity.levelId,
                        attemptNumber = opportunity.attemptNumber,
                        symbol = opportunity.symbol,
                        role = OrderRole.CLOSE,
                        slot = PRE_ENTRY_SLOT,
                        side = closingSide(confirmedPositionAmount),
                        type = OrderType.MARKET,
                        confirmedQuantity = confirmedQuantity,
                        reduceOnly = true,
                        confirmedPositionAmount = confirmedPositionAmount,
                    ),
                ),
            )
            .flatMap { closeResolution ->
                val remainingQuantity =
                    closeResolution.confirmedPositionAmount.abs()
                val updateRisk = when {
                    remainingQuantity.signum() == 0 ->
                        riskService.recordConfirmedFlat(opportunity.levelId)

                    remainingQuantity < confirmedQuantity ->
                        riskService.recordConfirmedReducingFill(
                            levelId = opportunity.levelId,
                            confirmedRemainingQuantity = remainingQuantity,
                        )

                    else -> Mono.just(riskService.currentState())
                }
                val unresolved =
                    persistentUnresolvedOrder ||
                        closeResolution.outcome == OrderOutcome.UNKNOWN
                updateRisk
                    .then(
                        levelCoordinator.terminate(
                            levelId = opportunity.levelId,
                            reason = reason,
                            confirmedRemainingQuantity = remainingQuantity,
                            hasUnresolvedOrder = unresolved,
                        ),
                    )
                    .thenReturn(
                        terminatedResult(
                            opportunity = opportunity,
                            resolution = resolution,
                            reason = reason,
                            remainingQuantity = remainingQuantity,
                            hardStopConfirmation = hardStopConfirmation,
                        ),
                    )
            }
    }

    private fun terminatedResult(
        opportunity: PreEntryOpportunity,
        resolution: OrderResolution,
        reason: LevelReasonCode,
        remainingQuantity: BigDecimal = BigDecimal.ZERO,
        hardStopConfirmation: HardStopConfirmation? = null,
    ): PreEntryResult =
        PreEntryResult(
            levelId = opportunity.levelId,
            status = PreEntryResultStatus.TERMINATED,
            terminalReason = reason,
            requestedQuantity = opportunity.preEntryQuantity,
            actualFilledQuantity = resolution.actualFilledQuantity,
            confirmedPositionQuantity = remainingQuantity,
            hardStopConfirmation = hardStopConfirmation,
        )
}

@Component
class BinancePreEntryRiskContextProvider(
    private val client: AuthenticatedBinanceClient,
    private val readinessService: AuthenticatedBinanceReadinessService,
) : PreEntryRiskContextProvider {
    override fun load(
        opportunity: PreEntryOpportunity,
    ): Mono<PreEntryRiskContext> =
        Mono.defer {
            val readiness = readinessService.snapshot()
            if (
                readiness.clock.readiness != BinanceReadiness.READY ||
                readiness.account.readiness != BinanceReadiness.READY ||
                readiness.privateStream.readiness != BinanceReadiness.READY
            ) {
                return@defer Mono.error(
                    OrderExecutionException(
                        "Authenticated Binance readiness is unhealthy",
                    ),
                )
            }
            Mono.zip(
                client.accountSummary(),
                client.commissionRate(opportunity.symbol),
                client.leverageBrackets(opportunity.symbol),
            ).map { values ->
                val account = values.t1
                val cappedEntryPrice = when (opportunity.direction) {
                    LevelDirection.LONG ->
                        opportunity.bestAskPrice.add(opportunity.frozenNpu)

                    LevelDirection.SHORT ->
                        opportunity.bestBidPrice.subtract(opportunity.frozenNpu)
                }
                PreEntryRiskContext(
                    accountState = RiskAccountState(
                        dailyAnchorEquity =
                            readiness.temporaryDailyAnchorEquity
                                ?: account.totalMarginBalance,
                        currentTotalAccountEquity = account.totalMarginBalance,
                        availableMargin = account.availableBalance,
                    ),
                    takerFeeRate = values.t2.takerRate,
                    leverageBracket = values.t3.riskBracket(
                        plannedNotional = cappedEntryPrice
                            .multiply(opportunity.plannedQuantity),
                    ),
                )
            }
        }
}

private fun PreEntryOpportunity.admissionRequest(
    context: PreEntryRiskContext,
): AttemptAdmissionRequest =
    AttemptAdmissionRequest(
        levelId = levelId,
        symbol = symbol,
        direction = direction,
        levelPrice = levelPrice,
        positionNotionalUsdt = positionNotionalUsdt,
        plannedQuantity = plannedQuantity,
        maxImpulsePct = maxImpulsePct,
        frozenNpu = frozenNpu,
        precedingOneSecondTradePrices = precedingOneSecondTradePrices,
        bestBidPrice = bestBidPrice,
        bestAskPrice = bestAskPrice,
        tickSize = tickSize,
        takerFeeRate = context.takerFeeRate,
        leverageBracket = context.leverageBracket,
    )

private fun PreEntryOpportunity.entryRequest(
    limitPrice: BigDecimal,
): OrderIntentRequest =
    OrderIntentRequest(
        levelId = levelId,
        attemptNumber = attemptNumber,
        symbol = symbol,
        role = OrderRole.ENTRY,
        slot = PRE_ENTRY_SLOT,
        side = when (direction) {
            LevelDirection.LONG -> OrderSide.BUY
            LevelDirection.SHORT -> OrderSide.SELL
        },
        type = OrderType.LIMIT,
        timeInForce = OrderTimeInForce.IOC,
        confirmedQuantity = preEntryQuantity,
        price = limitPrice,
    )

private fun minimumFillSatisfied(
    filledQuantity: BigDecimal,
    requestedQuantity: BigDecimal,
): Boolean =
    filledQuantity.multiply(ONE_HUNDRED) >=
        requestedQuantity.multiply(MINIMUM_FILL_PERCENT)

private fun closingSide(positionAmount: BigDecimal): OrderSide =
    if (positionAmount.signum() > 0) OrderSide.SELL else OrderSide.BUY

private fun BinanceSymbolLeverageBrackets.riskBracket(
    plannedNotional: BigDecimal,
): RiskLeverageBracket {
    require(notionalCoefficient.signum() > 0) {
        "notionalCoefficient must be positive"
    }
    val bracket = brackets
        .sortedBy { candidate -> candidate.notionalFloor }
        .firstOrNull { candidate ->
            val floor = candidate.notionalFloor.multiply(notionalCoefficient)
            val cap = candidate.notionalCap.multiply(notionalCoefficient)
            plannedNotional >= floor && plannedNotional < cap
        } ?: throw OrderExecutionException(
        "No leverage bracket covers the planned notional for $symbol",
    )
    return RiskLeverageBracket(
        maximumLeverage = bracket.initialLeverage,
        maintenanceMarginRatio = bracket.maintenanceMarginRatio,
        cumulativeMaintenanceAmount =
            bracket.cumulativeMaintenanceAmount,
    )
}

private const val PRE_ENTRY_SLOT = 0
private const val MAXIMUM_CONCURRENT_ATTEMPTS = 5
private val ONE_HUNDRED = BigDecimal("100")
private val MINIMUM_FILL_PERCENT = BigDecimal("80")
