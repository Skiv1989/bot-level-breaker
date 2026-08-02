package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Duration

@Service
class BreakoutExecutionService internal constructor(
    private val levelCoordinator: BreakoutLevelCoordinator,
    private val riskService: AttemptRiskService,
    private val orderExecutor: BreakoutOrderExecutor,
    automaticDispatch: Boolean,
    private val takeProfitSetupTimeout: Duration = Duration.ofSeconds(3),
) {
    @Autowired
    constructor(
        levelCoordinator: BreakoutLevelCoordinator,
        riskService: AttemptRiskService,
        orderExecutor: BreakoutOrderExecutor,
        @Value("\${bot.execution.take-profit-setup-timeout:3s}")
        takeProfitSetupTimeout: Duration,
    ) : this(
        levelCoordinator = levelCoordinator,
        riskService = riskService,
        orderExecutor = orderExecutor,
        automaticDispatch = true,
        takeProfitSetupTimeout = takeProfitSetupTimeout,
    )

    init {
        require(
            !takeProfitSetupTimeout.isZero &&
                !takeProfitSetupTimeout.isNegative,
        ) {
            "takeProfitSetupTimeout must be positive"
        }
    }

    private val automaticSubscription: Disposable? = if (automaticDispatch) {
        levelCoordinator
            .breakoutRequests()
            .flatMap(
                { request ->
                    execute(request).onErrorResume { Mono.empty() }
                },
                MAXIMUM_CONCURRENT_ATTEMPTS,
            )
            .subscribe()
    } else {
        null
    }
    private val takeProfitFillSubscription: Disposable = orderExecutor
        .takeProfitFills()
        .concatMap { fill ->
            recordTakeProfitFill(fill).onErrorResume { Mono.empty() }
        }
        .subscribe()

    fun execute(request: BreakoutExecutionRequest): Mono<BreakoutResult> =
        when (request) {
            is BreakoutAdditionRequest -> executeAddition(request)
            is BreakoutExitRequest -> closeExposure(
                request = request,
                reason = request.reason,
            )
        }.cache()

    @PreDestroy
    fun close() {
        automaticSubscription?.dispose()
        takeProfitFillSubscription.dispose()
    }

    private fun executeAddition(
        originalRequest: BreakoutAdditionRequest,
    ): Mono<BreakoutResult> =
        levelCoordinator
            .validateAddition(originalRequest)
            .flatMap { decision ->
                val request = decision.request
                if (!decision.dispatchAllowed) {
                    return@flatMap closeExposure(
                        request = request,
                        reason = checkNotNull(decision.terminalReason),
                    )
                }
                orderExecutor
                    .execute(request.orderIntent())
                    .flatMap { resolution ->
                        handleAdditionResolution(request, resolution)
                    }
            }

    private fun handleAdditionResolution(
        request: BreakoutAdditionRequest,
        resolution: OrderResolution,
    ): Mono<BreakoutResult> {
        val confirmedQuantity = resolution.confirmedPositionAmount.abs()
        val recordExposure = if (confirmedQuantity.signum() > 0) {
            riskService
                .recordConfirmedExposure(request.levelId, confirmedQuantity)
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
                                levelId = request.levelId,
                                reason = LevelReasonCode.ORDER_OUTCOME_UNKNOWN,
                                confirmedRemainingQuantity = confirmedQuantity,
                                hasUnresolvedOrder = true,
                            )
                            .thenReturn(
                                terminatedResult(
                                    request = request,
                                    resolution = resolution,
                                    reason =
                                        LevelReasonCode.ORDER_OUTCOME_UNKNOWN,
                                    remainingQuantity = confirmedQuantity,
                                ),
                            )

                    !minimumFillSatisfied(
                        filledQuantity = resolution.actualFilledQuantity,
                        requestedQuantity = request.requestedQuantity,
                    ) -> closeExposure(
                        request = request,
                        reason = LevelReasonCode.INSUFFICIENT_LIQUIDITY,
                        confirmedPositionAmount =
                            resolution.confirmedPositionAmount,
                        additionResolution = resolution,
                    )

                    request.tranche == BreakoutTranche.CROSSING ->
                        continueAfterCrossing(
                            request = request,
                            resolution = resolution,
                            confirmedQuantity = confirmedQuantity,
                        )

                    else -> installTakeProfits(request, resolution)
                }
            },
        )
    }

    private fun installTakeProfits(
        request: BreakoutAdditionRequest,
        resolution: OrderResolution,
    ): Mono<BreakoutResult> {
        val setupStartedAt = System.nanoTime()
        val failedConfirmation = TakeProfitSetConfirmation(
            intents = emptyList(),
            confirmed = false,
            confirmedPositionAmount = resolution.confirmedPositionAmount,
            reconciliationChecks = 0,
        )
        val setup = orderExecutor
            .reconcilePosition(
                symbol = request.symbol,
                clientOrderId = resolution.intent.clientOrderId,
            )
            .timeout(takeProfitSetupTimeout)
            .flatMap { positionAmount ->
                requireExpectedPosition(request, positionAmount)
                val reconciledQuantity = positionAmount.abs()
                riskService
                    .recordConfirmedExposure(
                        levelId = request.levelId,
                        confirmedPositionQuantity = reconciledQuantity,
                    )
                    .then(
                        orderExecutor.confirmTakeProfits(
                            request.takeProfitRequests(positionAmount),
                            remainingSetupTime(setupStartedAt),
                        ),
                    )
            }
            .onErrorReturn(failedConfirmation)
        return setup.flatMap { confirmation ->
            if (!confirmation.confirmed) {
                failTakeProfitSetup(
                    request = request,
                    resolution = resolution,
                    confirmation = confirmation,
                )
            } else {
                requireExpectedPosition(
                    request,
                    confirmation.confirmedPositionAmount,
                )
                val confirmedQuantity =
                    confirmation.confirmedPositionAmount.abs()
                val activate = levelCoordinator
                    .recordFinalFill(
                        requestId = request.requestId,
                        levelId = request.levelId,
                        confirmedPositionQuantity = confirmedQuantity,
                    )
                    .then(orderExecutor.activateTakeProfits(confirmation))
                    .thenReturn(
                        BreakoutResult(
                            levelId = request.levelId,
                            status = BreakoutResultStatus.CONFIRMED,
                            tranche = request.tranche,
                            requestedQuantity = request.requestedQuantity,
                            actualFilledQuantity =
                                resolution.actualFilledQuantity,
                            confirmedPositionQuantity = confirmedQuantity,
                        ),
                    )
                activate.onErrorResume {
                    failTakeProfitSetup(
                        request = request,
                        resolution = resolution,
                        confirmation = confirmation,
                    )
                }
            }
        }
    }

    private fun remainingSetupTime(setupStartedAt: Long): Duration {
        val elapsed = Duration.ofNanos(
            System.nanoTime() - setupStartedAt,
        )
        val remaining = takeProfitSetupTimeout.minus(elapsed)
        check(!remaining.isZero && !remaining.isNegative) {
            "Take-profit setup deadline elapsed before placement"
        }
        return remaining
    }

    private fun failTakeProfitSetup(
        request: BreakoutAdditionRequest,
        resolution: OrderResolution,
        confirmation: TakeProfitSetConfirmation,
    ): Mono<BreakoutResult> {
        val confirmedPositionAmount =
            confirmation.confirmedPositionAmount.takeIf { position ->
                position.signum() != 0 &&
                    position.signum() == request.expectedPositionSign()
            } ?: resolution.confirmedPositionAmount
        return synchronizeRiskPosition(
            levelId = request.levelId,
            confirmedQuantity = confirmedPositionAmount.abs(),
        )
            .then(
                riskService.enterSafeMode(
                    LevelReasonCode.TP_SETUP_FAILED.name,
                ),
            )
            .then(orderExecutor.cancelTakeProfits(confirmation.intents))
            .flatMap { cancellationComplete ->
                closeExposure(
                    request = request,
                    reason = LevelReasonCode.TP_SETUP_FAILED,
                    confirmedPositionAmount = confirmedPositionAmount,
                    additionResolution = resolution,
                    persistentUnresolvedOrder = !cancellationComplete,
                )
            }
    }

    private fun recordTakeProfitFill(
        fill: TakeProfitFill,
    ): Mono<Void> = Mono.defer {
        val attempt = riskService
            .currentState()
            .attempts
            .firstOrNull { candidate -> candidate.levelId == fill.levelId }
            ?: return@defer Mono.empty()
        if (
            fill.confirmedRemainingQuantity >=
            attempt.confirmedPositionQuantity
        ) {
            return@defer Mono.empty()
        }
        val riskUpdate = if (fill.confirmedRemainingQuantity.signum() == 0) {
            riskService.recordConfirmedFlat(fill.levelId)
        } else {
            riskService.recordConfirmedReducingFill(
                levelId = fill.levelId,
                confirmedRemainingQuantity =
                    fill.confirmedRemainingQuantity,
            )
        }
        riskUpdate
            .then(
                levelCoordinator.recordTakeProfitFill(
                    levelId = fill.levelId,
                    confirmedRemainingQuantity =
                        fill.confirmedRemainingQuantity,
                ),
            )
    }

    private fun synchronizeRiskPosition(
        levelId: java.util.UUID,
        confirmedQuantity: BigDecimal,
    ): Mono<*> {
        val currentQuantity = riskService
            .currentState()
            .attempts
            .firstOrNull { attempt -> attempt.levelId == levelId }
            ?.confirmedPositionQuantity
            ?: return Mono.just(riskService.currentState())
        return when {
            confirmedQuantity.signum() == 0 ->
                riskService.recordConfirmedFlat(levelId)

            confirmedQuantity < currentQuantity ->
                riskService.recordConfirmedReducingFill(
                    levelId = levelId,
                    confirmedRemainingQuantity = confirmedQuantity,
                )

            confirmedQuantity > currentQuantity ->
                riskService.recordConfirmedExposure(
                    levelId = levelId,
                    confirmedPositionQuantity = confirmedQuantity,
                )

            else -> Mono.just(riskService.currentState())
        }
    }

    private fun continueAfterCrossing(
        request: BreakoutAdditionRequest,
        resolution: OrderResolution,
        confirmedQuantity: BigDecimal,
    ): Mono<BreakoutResult> =
        levelCoordinator
            .recordCrossingFill(
                requestId = request.requestId,
                levelId = request.levelId,
                confirmedPositionQuantity = confirmedQuantity,
            )
            .flatMap { decision ->
                if (!decision.continueAttempt) {
                    closeExposure(
                        request = request,
                        reason = checkNotNull(decision.terminalReason),
                        confirmedPositionAmount =
                            resolution.confirmedPositionAmount,
                        additionResolution = resolution,
                    )
                } else {
                    Mono.just(
                        BreakoutResult(
                            levelId = request.levelId,
                            status = BreakoutResultStatus.CONFIRMING,
                            tranche = request.tranche,
                            requestedQuantity = request.requestedQuantity,
                            actualFilledQuantity =
                                resolution.actualFilledQuantity,
                            confirmedPositionQuantity = confirmedQuantity,
                        ),
                    )
                }
            }

    private fun closeExposure(
        request: BreakoutExecutionRequest,
        reason: LevelReasonCode,
        confirmedPositionAmount: BigDecimal = request.signedPositionAmount(),
        additionResolution: OrderResolution? = null,
        persistentUnresolvedOrder: Boolean = false,
    ): Mono<BreakoutResult> {
        val confirmedQuantity = confirmedPositionAmount.abs()
        val terminal = levelCoordinator.terminate(
            levelId = request.levelId,
            reason = reason,
            confirmedRemainingQuantity = confirmedQuantity,
            hasUnresolvedOrder = persistentUnresolvedOrder,
        )
        if (confirmedQuantity.signum() == 0) {
            return terminal
                .then(riskService.recordConfirmedFlat(request.levelId))
                .thenReturn(
                    terminatedResult(
                        request = request,
                        resolution = additionResolution,
                        reason = reason,
                    ),
                )
        }
        return terminal
            .then(
                orderExecutor.execute(
                    OrderIntentRequest(
                        levelId = request.levelId,
                        attemptNumber = request.attemptNumber,
                        symbol = request.symbol,
                        role = OrderRole.CLOSE,
                        slot = CLOSE_SLOT,
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
                updateRiskAfterClose(
                    levelId = request.levelId,
                    previousQuantity = confirmedQuantity,
                    remainingQuantity = remainingQuantity,
                ).then(
                    levelCoordinator.terminate(
                        levelId = request.levelId,
                        reason = reason,
                        confirmedRemainingQuantity = remainingQuantity,
                        hasUnresolvedOrder =
                            persistentUnresolvedOrder ||
                                closeResolution.outcome == OrderOutcome.UNKNOWN,
                    ),
                ).thenReturn(
                    terminatedResult(
                        request = request,
                        resolution = additionResolution,
                        reason = reason,
                        remainingQuantity = remainingQuantity,
                    ),
                )
            }
    }

    private fun updateRiskAfterClose(
        levelId: java.util.UUID,
        previousQuantity: BigDecimal,
        remainingQuantity: BigDecimal,
    ): Mono<*> = when {
        remainingQuantity.signum() == 0 ->
            riskService.recordConfirmedFlat(levelId)

        remainingQuantity < previousQuantity ->
            riskService.recordConfirmedReducingFill(
                levelId = levelId,
                confirmedRemainingQuantity = remainingQuantity,
            )

        else -> Mono.just(riskService.currentState())
    }

    private fun terminatedResult(
        request: BreakoutExecutionRequest,
        resolution: OrderResolution?,
        reason: LevelReasonCode,
        remainingQuantity: BigDecimal = BigDecimal.ZERO,
    ): BreakoutResult =
        BreakoutResult(
            levelId = request.levelId,
            status = BreakoutResultStatus.TERMINATED,
            tranche = (request as? BreakoutAdditionRequest)?.tranche,
            terminalReason = reason,
            requestedQuantity =
                (request as? BreakoutAdditionRequest)?.requestedQuantity,
            actualFilledQuantity =
                resolution?.actualFilledQuantity ?: BigDecimal.ZERO,
            confirmedPositionQuantity = remainingQuantity,
        )
}

private fun BreakoutAdditionRequest.orderIntent(): OrderIntentRequest =
    OrderIntentRequest(
        levelId = levelId,
        attemptNumber = attemptNumber,
        symbol = symbol,
        role = OrderRole.ADDITION,
        slot = tranche.slot,
        side = when (direction) {
            LevelDirection.LONG -> OrderSide.BUY
            LevelDirection.SHORT -> OrderSide.SELL
        },
        type = OrderType.LIMIT,
        timeInForce = OrderTimeInForce.IOC,
        confirmedQuantity = requestedQuantity,
        price = when (direction) {
            LevelDirection.LONG -> bestAskPrice.add(frozenNpu)
            LevelDirection.SHORT -> bestBidPrice.subtract(frozenNpu)
        },
        confirmedPositionAmount = signedPositionAmount(),
    )

private fun BreakoutExecutionRequest.signedPositionAmount(): BigDecimal =
    when (direction) {
        LevelDirection.LONG -> confirmedPositionQuantity
        LevelDirection.SHORT -> confirmedPositionQuantity.negate()
    }

private fun BreakoutAdditionRequest.takeProfitRequests(
    positionAmount: BigDecimal,
): List<OrderIntentRequest> {
    check(tranche == BreakoutTranche.FINAL) {
        "Take profits can be installed only after the final tranche"
    }
    requireExpectedPosition(this, positionAmount)
    require(tickSize.signum() > 0) { "tickSize must be positive" }
    require(quantityStepSize.signum() > 0) {
        "quantityStepSize must be positive"
    }
    val positionQuantity = positionAmount.abs()
    require(isIncrementAligned(positionQuantity, quantityStepSize)) {
        "Reconciled position quantity is not executable"
    }
    val firstQuantity = roundToIncrement(
        positionQuantity.multiply(TP1_ALLOCATION),
        quantityStepSize,
        RoundingMode.DOWN,
    )
    val secondQuantity = roundToIncrement(
        positionQuantity.multiply(TP2_ALLOCATION),
        quantityStepSize,
        RoundingMode.DOWN,
    )
    val quantities = listOf(
        firstQuantity,
        secondQuantity,
        positionQuantity.subtract(firstQuantity).subtract(secondQuantity),
    )
    require(
        quantities.all { quantity ->
            quantity >= minimumQuantity &&
                quantity <= maximumQuantity &&
                isIncrementAligned(quantity, quantityStepSize)
        },
    ) {
        "Every take-profit quantity must satisfy Binance quantity filters"
    }
    require(quantities.fold(BigDecimal.ZERO, BigDecimal::add) <= positionQuantity) {
        "Take-profit quantities cannot exceed reconciled exposure"
    }
    val prices = TAKE_PROFIT_FRACTIONS.map { fraction ->
        val impulse = levelPrice
            .multiply(maxImpulsePct)
            .multiply(fraction)
            .divide(ONE_HUNDRED, CALCULATION_SCALE, RoundingMode.HALF_UP)
        val rawPrice = when (direction) {
            LevelDirection.LONG -> levelPrice.add(impulse)
            LevelDirection.SHORT -> levelPrice.subtract(impulse)
        }
        roundToIncrement(
            value = rawPrice,
            increment = tickSize,
            roundingMode = when (direction) {
                LevelDirection.LONG -> RoundingMode.DOWN
                LevelDirection.SHORT -> RoundingMode.UP
            },
        ).also { price ->
            require(price.signum() > 0) {
                "Take-profit prices must be positive"
            }
        }
    }
    return prices.indices.map { index ->
        OrderIntentRequest(
            levelId = levelId,
            attemptNumber = attemptNumber,
            symbol = symbol,
            role = OrderRole.TAKE_PROFIT,
            slot = index + 1,
            side = closingSide(positionAmount),
            type = OrderType.LIMIT,
            timeInForce = OrderTimeInForce.GTC,
            confirmedQuantity = quantities[index],
            price = prices[index],
            reduceOnly = true,
            confirmedPositionAmount = positionAmount,
        )
    }
}

private fun requireExpectedPosition(
    request: BreakoutExecutionRequest,
    positionAmount: BigDecimal,
) {
    require(positionAmount.signum() == request.expectedPositionSign()) {
        "Reconciled position does not match breakout direction"
    }
}

private fun BreakoutExecutionRequest.expectedPositionSign(): Int =
    when (direction) {
        LevelDirection.LONG -> 1
        LevelDirection.SHORT -> -1
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

private fun isIncrementAligned(
    value: BigDecimal,
    increment: BigDecimal,
): Boolean = roundToIncrement(
    value,
    increment,
    RoundingMode.DOWN,
).compareTo(value) == 0

private const val CLOSE_SLOT = 0
private const val MAXIMUM_CONCURRENT_ATTEMPTS = 5
private const val CALCULATION_SCALE = 16
private val TP1_ALLOCATION = BigDecimal("0.33")
private val TP2_ALLOCATION = BigDecimal("0.33")
private val TAKE_PROFIT_FRACTIONS = listOf(
    BigDecimal("0.35"),
    BigDecimal("0.70"),
    BigDecimal.ONE,
)
private val ONE_HUNDRED = BigDecimal("100")
