package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Duration
import java.util.UUID

sealed interface BreakoutExecutionRequest {
    val levelId: UUID
    val attemptNumber: Long
    val symbol: String
    val direction: LevelDirection
    val confirmedPositionQuantity: BigDecimal
}

data class BreakoutAdditionRequest(
    val requestId: String,
    override val levelId: UUID,
    override val attemptNumber: Long,
    override val symbol: String,
    override val direction: LevelDirection,
    override val confirmedPositionQuantity: BigDecimal,
    val tranche: BreakoutTranche,
    val requestedQuantity: BigDecimal,
    val bestBidPrice: BigDecimal,
    val bestAskPrice: BigDecimal,
    val frozenNpu: BigDecimal,
    val hardStopClientOrderId: String,
    val hardStopPrice: BigDecimal,
    val levelPrice: BigDecimal,
    val maxImpulsePct: BigDecimal,
    val tickSize: BigDecimal,
    val quantityStepSize: BigDecimal,
    val minimumQuantity: BigDecimal,
    val maximumQuantity: BigDecimal,
) : BreakoutExecutionRequest

data class BreakoutExitRequest(
    val requestId: String,
    override val levelId: UUID,
    override val attemptNumber: Long,
    override val symbol: String,
    override val direction: LevelDirection,
    override val confirmedPositionQuantity: BigDecimal,
    val reason: LevelReasonCode,
) : BreakoutExecutionRequest

enum class BreakoutTranche(
    val slot: Int,
) {
    CROSSING(1),
    FINAL(2),
}

data class BreakoutDispatchDecision(
    val request: BreakoutAdditionRequest,
    val dispatchAllowed: Boolean,
    val terminalReason: LevelReasonCode? = null,
)

data class BreakoutContinuationDecision(
    val continueAttempt: Boolean,
    val terminalReason: LevelReasonCode? = null,
)

enum class BreakoutResultStatus {
    CONFIRMING,
    CONFIRMED,
    TERMINATED,
}

data class BreakoutResult(
    val levelId: UUID,
    val status: BreakoutResultStatus,
    val tranche: BreakoutTranche? = null,
    val terminalReason: LevelReasonCode? = null,
    val requestedQuantity: BigDecimal? = null,
    val actualFilledQuantity: BigDecimal = BigDecimal.ZERO,
    val confirmedPositionQuantity: BigDecimal = BigDecimal.ZERO,
)

data class TakeProfitSetConfirmation(
    val intents: List<OrderIntent>,
    val confirmed: Boolean,
    val confirmedPositionAmount: BigDecimal,
    val reconciliationChecks: Int,
)

data class TakeProfitFill(
    val levelId: UUID,
    val symbol: String,
    val clientOrderId: String,
    val confirmedRemainingQuantity: BigDecimal,
    val allTakeProfitsFilled: Boolean,
)

interface BreakoutOrderExecutor {
    fun execute(request: OrderIntentRequest): Mono<OrderResolution>

    fun reconcilePosition(
        symbol: String,
        clientOrderId: String,
    ): Mono<BigDecimal>

    fun confirmTakeProfits(
        requests: List<OrderIntentRequest>,
        timeout: Duration,
    ): Mono<TakeProfitSetConfirmation>

    fun activateTakeProfits(
        confirmation: TakeProfitSetConfirmation,
    ): Mono<Void>

    fun cancelTakeProfits(intents: List<OrderIntent>): Mono<Boolean>

    fun takeProfitFills(): Flux<TakeProfitFill>
}

interface BreakoutLevelCoordinator {
    fun breakoutRequests(): Flux<BreakoutExecutionRequest>

    fun validateAddition(
        request: BreakoutAdditionRequest,
    ): Mono<BreakoutDispatchDecision>

    fun recordCrossingFill(
        requestId: String,
        levelId: UUID,
        confirmedPositionQuantity: BigDecimal,
    ): Mono<BreakoutContinuationDecision>

    fun recordFinalFill(
        requestId: String,
        levelId: UUID,
        confirmedPositionQuantity: BigDecimal,
    ): Mono<Void>

    fun recordTakeProfitFill(
        levelId: UUID,
        confirmedRemainingQuantity: BigDecimal,
    ): Mono<Void>

    fun terminate(
        levelId: UUID,
        reason: LevelReasonCode,
        confirmedRemainingQuantity: BigDecimal,
        hasUnresolvedOrder: Boolean,
    ): Mono<Void>
}
