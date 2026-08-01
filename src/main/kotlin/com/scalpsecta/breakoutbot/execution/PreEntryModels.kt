package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.risk.RiskAccountState
import com.scalpsecta.breakoutbot.risk.RiskLeverageBracket
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class PreEntryOpportunity(
    val levelId: UUID,
    val attemptNumber: Long,
    val symbol: String,
    val direction: LevelDirection,
    val levelPrice: BigDecimal,
    val positionNotionalUsdt: BigDecimal,
    val plannedQuantity: BigDecimal,
    val preEntryQuantity: BigDecimal,
    val maxImpulsePct: BigDecimal,
    val frozenNpu: BigDecimal,
    val precedingOneSecondTradePrices: List<BigDecimal>,
    val bestBidPrice: BigDecimal,
    val bestAskPrice: BigDecimal,
    val tickSize: BigDecimal,
    val preparedAt: Instant,
)

data class PreEntryRiskContext(
    val accountState: RiskAccountState,
    val takerFeeRate: BigDecimal,
    val leverageBracket: RiskLeverageBracket,
)

enum class PreEntryResultStatus {
    RISK_BLOCKED,
    PROTECTED,
    TERMINATED,
}

data class PreEntryResult(
    val levelId: UUID,
    val status: PreEntryResultStatus,
    val terminalReason: LevelReasonCode? = null,
    val requestedQuantity: BigDecimal? = null,
    val actualFilledQuantity: BigDecimal = BigDecimal.ZERO,
    val confirmedPositionQuantity: BigDecimal = BigDecimal.ZERO,
    val hardStopConfirmation: HardStopConfirmation? = null,
)

interface PreEntryLevelCoordinator {
    fun opportunities(): Flux<UUID>

    fun prepare(levelId: UUID): Mono<PreEntryOpportunity>

    fun markDispatched(levelId: UUID): Mono<Void>

    fun cancelPreparation(levelId: UUID): Mono<Void>

    fun crossedBeforeProtection(levelId: UUID): Boolean

    fun markProtected(
        levelId: UUID,
        confirmedPositionQuantity: BigDecimal,
        hardStopClientOrderId: String,
        hardStopPrice: BigDecimal,
    ): Mono<Void>

    fun terminate(
        levelId: UUID,
        reason: LevelReasonCode,
        confirmedRemainingQuantity: BigDecimal,
        hasUnresolvedOrder: Boolean,
    ): Mono<Void>
}

fun interface PreEntryRiskContextProvider {
    fun load(opportunity: PreEntryOpportunity): Mono<PreEntryRiskContext>
}
