package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.publisher.Mono
import java.math.BigDecimal

@Service
class BreakoutExecutionService internal constructor(
    private val levelCoordinator: BreakoutLevelCoordinator,
    private val riskService: AttemptRiskService,
    private val orderExecutor: PreEntryOrderExecutor,
    automaticDispatch: Boolean,
) {
    @Autowired
    constructor(
        levelCoordinator: BreakoutLevelCoordinator,
        riskService: AttemptRiskService,
        orderExecutor: PreEntryOrderExecutor,
    ) : this(
        levelCoordinator = levelCoordinator,
        riskService = riskService,
        orderExecutor = orderExecutor,
        automaticDispatch = true,
    )

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

                    else -> levelCoordinator
                        .recordFinalFill(
                            requestId = request.requestId,
                            levelId = request.levelId,
                            confirmedPositionQuantity = confirmedQuantity,
                        )
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
                }
            },
        )
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
    ): Mono<BreakoutResult> {
        val confirmedQuantity = confirmedPositionAmount.abs()
        val terminal = levelCoordinator.terminate(
            levelId = request.levelId,
            reason = reason,
            confirmedRemainingQuantity = confirmedQuantity,
            hasUnresolvedOrder = false,
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

private const val CLOSE_SLOT = 0
private const val MAXIMUM_CONCURRENT_ATTEMPTS = 5
