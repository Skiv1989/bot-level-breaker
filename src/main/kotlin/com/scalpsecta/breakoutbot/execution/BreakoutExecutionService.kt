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
    private val normalExitWait: Duration = Duration.ofMillis(500),
) {
    @Autowired
    constructor(
        levelCoordinator: BreakoutLevelCoordinator,
        riskService: AttemptRiskService,
        orderExecutor: BreakoutOrderExecutor,
        @Value("\${bot.execution.take-profit-setup-timeout:3s}")
        takeProfitSetupTimeout: Duration,
        @Value("\${bot.execution.normal-exit-wait:500ms}")
        normalExitWait: Duration,
    ) : this(
        levelCoordinator = levelCoordinator,
        riskService = riskService,
        orderExecutor = orderExecutor,
        automaticDispatch = true,
        takeProfitSetupTimeout = takeProfitSetupTimeout,
        normalExitWait = normalExitWait,
    )

    init {
        require(
            !takeProfitSetupTimeout.isZero &&
                !takeProfitSetupTimeout.isNegative,
        ) {
            "takeProfitSetupTimeout must be positive"
        }
        require(
            !normalExitWait.isZero &&
                !normalExitWait.isNegative &&
                normalExitWait <= MAXIMUM_NORMAL_EXIT_WAIT,
        ) {
            "normalExitWait must be positive and at most 500 ms"
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
    private val positionReductionSubscription: Disposable = orderExecutor
        .positionReductions()
        .concatMap { reduction ->
            recordPositionReduction(reduction).onErrorResume { Mono.empty() }
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
        positionReductionSubscription.dispose()
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
                            .terminatePosition(
                                levelId = request.levelId,
                                reason = LevelReasonCode.ORDER_OUTCOME_UNKNOWN,
                                confirmedRemainingQuantity = confirmedQuantity,
                                hasUnresolvedOrder = true,
                                netResult = null,
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

    private fun recordPositionReduction(
        reduction: PositionReduction,
    ): Mono<Void> = Mono.defer {
        val attempt = riskService
            .currentState()
            .attempts
            .firstOrNull { candidate -> candidate.levelId == reduction.levelId }
            ?: return@defer Mono.empty()
        if (
            reduction.confirmedRemainingQuantity >=
            attempt.confirmedPositionQuantity
        ) {
            return@defer Mono.empty()
        }
        val riskUpdate = if (reduction.confirmedRemainingQuantity.signum() == 0) {
            riskService.recordConfirmedFlat(
                levelId = reduction.levelId,
                netPnl = reduction.netResult?.netPnl,
            )
        } else {
            riskService.recordConfirmedReducingFill(
                levelId = reduction.levelId,
                confirmedRemainingQuantity =
                    reduction.confirmedRemainingQuantity,
            )
        }
        val cancelOtherProtection = if (
            reduction.confirmedRemainingQuantity.signum() == 0
        ) {
            when (reduction.role) {
                OrderRole.HARD_STOP ->
                    orderExecutor.cancelActiveTakeProfits(reduction.levelId)

                OrderRole.TAKE_PROFIT ->
                    orderExecutor.cancelActiveHardStop(reduction.levelId)

                else -> Mono.just(true)
            }.then()
        } else {
            Mono.empty()
        }
        riskUpdate
            .then(
                levelCoordinator.recordPositionReduction(
                    levelId = reduction.levelId,
                    confirmedRemainingQuantity =
                        reduction.confirmedRemainingQuantity,
                    terminalReason = reduction.terminalReason,
                    netResult = reduction.netResult,
                ),
            )
            .then(cancelOtherProtection)
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
    ): Mono<BreakoutResult> =
        if (
            request is BreakoutExitRequest &&
            reason in SOFT_EXIT_REASONS &&
            request.softCloseIntent(confirmedPositionAmount) != null
        ) {
            closeExposureNormally(
                request = request,
                reason = reason,
                confirmedPositionAmount = confirmedPositionAmount,
            )
        } else {
            closeExposureWithMarket(
                request = request,
                reason = reason,
                confirmedPositionAmount = confirmedPositionAmount,
                additionResolution = additionResolution,
                persistentUnresolvedOrder = persistentUnresolvedOrder,
            )
        }

    private fun closeExposureNormally(
        request: BreakoutExitRequest,
        reason: LevelReasonCode,
        confirmedPositionAmount: BigDecimal,
    ): Mono<BreakoutResult> {
        val confirmedQuantity = confirmedPositionAmount.abs()
        if (confirmedQuantity.signum() == 0) {
            return updateRiskAfterClose(
                levelId = request.levelId,
                remainingQuantity = BigDecimal.ZERO,
            ).then(
                finalizeClose(
                    request = request,
                    reason = reason,
                    remainingQuantity = BigDecimal.ZERO,
                    hasUnresolvedOrder = false,
                ),
            )
        }
        val iocRequest = checkNotNull(
            request.softCloseIntent(confirmedPositionAmount),
        )
        return Mono.zip(
            orderExecutor.cancelActiveTakeProfits(request.levelId),
            orderExecutor.executeNormalExit(
                request = iocRequest,
                wait = normalExitWait,
            ),
        )
            .map { tuple ->
                val cancellationComplete = tuple.t1
                val iocResolution = tuple.t2
                requireExpectedPositionOrFlat(
                    request,
                    iocResolution.confirmedPositionAmount,
                )
                NormalExitReconciliation(
                    remainingPositionAmount =
                        iocResolution.confirmedPositionAmount,
                    hasUnresolvedOrder =
                        !cancellationComplete ||
                            iocResolution.hasUnresolvedOrder,
                )
            }
            .flatMap { reconciliation ->
                val remainingQuantity =
                    reconciliation.remainingPositionAmount.abs()
                updateRiskAfterClose(
                    levelId = request.levelId,
                    remainingQuantity = remainingQuantity,
                ).then(
                    if (remainingQuantity.signum() == 0) {
                        finalizeClose(
                            request = request,
                            reason = reason,
                            remainingQuantity = remainingQuantity,
                            hasUnresolvedOrder =
                                reconciliation.hasUnresolvedOrder,
                        )
                    } else {
                        closeResidualWithMarket(
                            request = request,
                            reason = reason,
                            confirmedPositionAmount =
                                reconciliation.remainingPositionAmount,
                            persistentUnresolvedOrder =
                                reconciliation.hasUnresolvedOrder,
                        )
                    },
                )
            }
    }

    private fun closeExposureWithMarket(
        request: BreakoutExecutionRequest,
        reason: LevelReasonCode,
        confirmedPositionAmount: BigDecimal,
        additionResolution: OrderResolution?,
        persistentUnresolvedOrder: Boolean,
    ): Mono<BreakoutResult> {
        val confirmedQuantity = confirmedPositionAmount.abs()
        if (confirmedQuantity.signum() == 0) {
            return updateRiskAfterClose(
                levelId = request.levelId,
                remainingQuantity = BigDecimal.ZERO,
            ).then(
                finalizeClose(
                    request = request,
                    reason = reason,
                    remainingQuantity = BigDecimal.ZERO,
                    hasUnresolvedOrder = persistentUnresolvedOrder,
                    additionResolution = additionResolution,
                ),
            )
        }
        return orderExecutor
            .execute(
                request.marketCloseIntent(
                    confirmedPositionAmount = confirmedPositionAmount,
                    slot = CLOSE_MARKET_SLOT,
                ),
            )
            .flatMap { closeResolution ->
                val remainingQuantity =
                    closeResolution.confirmedPositionAmount.abs()
                updateRiskAfterClose(
                    levelId = request.levelId,
                    remainingQuantity = remainingQuantity,
                ).then(
                    finalizeClose(
                        request = request,
                        reason = reason,
                        remainingQuantity = remainingQuantity,
                        hasUnresolvedOrder =
                            persistentUnresolvedOrder ||
                                closeResolution.outcome == OrderOutcome.UNKNOWN,
                        additionResolution = additionResolution,
                    ),
                )
            }
    }

    private fun closeResidualWithMarket(
        request: BreakoutExitRequest,
        reason: LevelReasonCode,
        confirmedPositionAmount: BigDecimal,
        persistentUnresolvedOrder: Boolean,
    ): Mono<BreakoutResult> =
        orderExecutor
            .execute(
                request.marketCloseIntent(
                    confirmedPositionAmount = confirmedPositionAmount,
                    slot = RESIDUAL_CLOSE_SLOT,
                ),
            )
            .flatMap { closeResolution ->
                val remainingQuantity =
                    closeResolution.confirmedPositionAmount.abs()
                updateRiskAfterClose(
                    levelId = request.levelId,
                    remainingQuantity = remainingQuantity,
                ).then(
                    finalizeClose(
                        request = request,
                        reason = reason,
                        remainingQuantity = remainingQuantity,
                        hasUnresolvedOrder =
                            persistentUnresolvedOrder ||
                                closeResolution.outcome == OrderOutcome.UNKNOWN,
                    ),
                )
            }

    private fun finalizeClose(
        request: BreakoutExecutionRequest,
        reason: LevelReasonCode,
        remainingQuantity: BigDecimal,
        hasUnresolvedOrder: Boolean,
        additionResolution: OrderResolution? = null,
    ): Mono<BreakoutResult> {
        val cancelHardStop = if (remainingQuantity.signum() == 0) {
            orderExecutor.cancelActiveHardStop(request.levelId).then()
        } else {
            Mono.empty()
        }
        return levelCoordinator
            .terminatePosition(
                levelId = request.levelId,
                reason = reason,
                confirmedRemainingQuantity = remainingQuantity,
                hasUnresolvedOrder = hasUnresolvedOrder,
                netResult = orderExecutor.positionResult(request.levelId),
            )
            .then(cancelHardStop)
            .thenReturn(
                terminatedResult(
                    request = request,
                    resolution = additionResolution,
                    reason = reason,
                    remainingQuantity = remainingQuantity,
                ),
            )
    }

    private fun updateRiskAfterClose(
        levelId: java.util.UUID,
        remainingQuantity: BigDecimal,
    ): Mono<*> {
        val currentQuantity = riskService
            .currentState()
            .attempts
            .firstOrNull { attempt -> attempt.levelId == levelId }
            ?.confirmedPositionQuantity
            ?: return Mono.just(riskService.currentState())
        return when {
            remainingQuantity >= currentQuantity ->
                Mono.just(riskService.currentState())

            remainingQuantity.signum() == 0 ->
                riskService.recordConfirmedFlat(
                    levelId = levelId,
                    netPnl = orderExecutor.positionResult(levelId)?.netPnl,
                )

            else -> riskService.recordConfirmedReducingFill(
                levelId = levelId,
                confirmedRemainingQuantity = remainingQuantity,
            )
        }
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

private fun BreakoutExitRequest.softCloseIntent(
    confirmedPositionAmount: BigDecimal,
): OrderIntentRequest? {
    val npu = frozenNpu ?: return null
    val increment = tickSize ?: return null
    val rawPrice = when (direction) {
        LevelDirection.LONG -> (bestBidPrice ?: return null).subtract(npu)
        LevelDirection.SHORT -> (bestAskPrice ?: return null).add(npu)
    }
    if (rawPrice.signum() <= 0 || increment.signum() <= 0) {
        return null
    }
    val cappedPrice = roundToIncrement(
        value = rawPrice,
        increment = increment,
        roundingMode = when (direction) {
            LevelDirection.LONG -> RoundingMode.UP
            LevelDirection.SHORT -> RoundingMode.DOWN
        },
    )
    return OrderIntentRequest(
        levelId = levelId,
        attemptNumber = attemptNumber,
        symbol = symbol,
        role = OrderRole.CLOSE,
        slot = SOFT_CLOSE_IOC_SLOT,
        side = closingSide(confirmedPositionAmount),
        type = OrderType.LIMIT,
        timeInForce = OrderTimeInForce.IOC,
        confirmedQuantity = confirmedPositionAmount.abs(),
        price = cappedPrice,
        reduceOnly = true,
        confirmedPositionAmount = confirmedPositionAmount,
    )
}

private fun BreakoutExecutionRequest.marketCloseIntent(
    confirmedPositionAmount: BigDecimal,
    slot: Int,
): OrderIntentRequest =
    OrderIntentRequest(
        levelId = levelId,
        attemptNumber = attemptNumber,
        symbol = symbol,
        role = OrderRole.CLOSE,
        slot = slot,
        side = closingSide(confirmedPositionAmount),
        type = OrderType.MARKET,
        confirmedQuantity = confirmedPositionAmount.abs(),
        reduceOnly = true,
        confirmedPositionAmount = confirmedPositionAmount,
    )

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

private fun requireExpectedPositionOrFlat(
    request: BreakoutExecutionRequest,
    positionAmount: BigDecimal,
) {
    require(
        positionAmount.signum() == 0 ||
            positionAmount.signum() == request.expectedPositionSign(),
    ) {
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

private data class NormalExitReconciliation(
    val remainingPositionAmount: BigDecimal,
    val hasUnresolvedOrder: Boolean,
)

private const val CLOSE_MARKET_SLOT = 0
private const val SOFT_CLOSE_IOC_SLOT = 0
private const val RESIDUAL_CLOSE_SLOT = 1
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
private val MAXIMUM_NORMAL_EXIT_WAIT: Duration = Duration.ofMillis(500)
private val SOFT_EXIT_REASONS = setOf(
    LevelReasonCode.EXIT_SCORE,
    LevelReasonCode.SNAPBACK,
    LevelReasonCode.MAX_HOLD_TIME,
    LevelReasonCode.MANUAL_CLOSE,
)
