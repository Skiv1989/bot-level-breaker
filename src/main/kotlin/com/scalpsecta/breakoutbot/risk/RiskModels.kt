package com.scalpsecta.breakoutbot.risk

import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class RiskAccountState(
    val dailyAnchorEquity: BigDecimal,
    val currentTotalAccountEquity: BigDecimal,
    val availableMargin: BigDecimal,
    val depositsSinceAnchor: BigDecimal = BigDecimal.ZERO,
    val withdrawalsSinceAnchor: BigDecimal = BigDecimal.ZERO,
)

data class AttemptAdmissionRequest(
    val levelId: UUID,
    val symbol: String,
    val direction: LevelDirection,
    val levelPrice: BigDecimal,
    val positionNotionalUsdt: BigDecimal,
    val plannedQuantity: BigDecimal,
    val maxImpulsePct: BigDecimal,
    val frozenNpu: BigDecimal,
    val precedingOneSecondTradePrices: List<BigDecimal>,
    val bestBidPrice: BigDecimal,
    val bestAskPrice: BigDecimal,
    val tickSize: BigDecimal,
    val takerFeeRate: BigDecimal,
    val leverageBracket: RiskLeverageBracket,
)

data class RiskLeverageBracket(
    val maximumLeverage: Int,
    val maintenanceMarginRatio: BigDecimal,
    val cumulativeMaintenanceAmount: BigDecimal,
)

data class AttemptAdmissionDecision(
    val admitted: Boolean,
    val blockers: List<RiskBlockerCode>,
    val plan: AttemptRiskPlan,
    val state: GlobalRiskSnapshot,
)

enum class RiskBlockerCode {
    BLOCKED_SAFE_MODE,
    STOP_RISK_TOO_HIGH,
    PLANNED_NET_R_TOO_LOW,
    BLOCKED_MARGIN_BUFFER,
    LIQUIDATION_TOO_CLOSE,
    BLOCKED_POSITION_CAP,
    BLOCKED_SYMBOL_ATTEMPT,
    BLOCKED_DAILY_RISK,
}

data class AttemptRiskPlan(
    val levelRiskBudget: BigDecimal,
    val structuralStopPrice: BigDecimal,
    val worstCappedEntryPrice: BigDecimal,
    val reservedExitPrice: BigDecimal,
    val takeProfits: List<PlannedTakeProfit>,
    val estimatedEntryFee: BigDecimal,
    val estimatedLossExitFee: BigDecimal,
    val estimatedWorstNetLoss: BigDecimal,
    val estimatedNetReward: BigDecimal,
    val plannedNetR: BigDecimal,
    val selectedLeverage: Int,
    val projectedIsolatedMargin: BigDecimal,
    val estimatedLiquidationPrice: BigDecimal,
)

data class PlannedTakeProfit(
    val allocationPercent: Int,
    val impulseFraction: BigDecimal,
    val price: BigDecimal,
    val quantity: BigDecimal,
    val estimatedExitFee: BigDecimal,
)

data class GlobalRiskSnapshot(
    val observedAt: Instant,
    val globalTradingState: GlobalTradingState,
    val stateReason: String?,
    val dailyAnchorEquity: BigDecimal?,
    val currentTotalAccountEquity: BigDecimal?,
    val dailyLossLimit: BigDecimal?,
    val tradingDrawdown: BigDecimal?,
    val reservedRiskForOpenPositions: BigDecimal,
    val reservedRiskForPendingAttempts: BigDecimal,
    val totalReservedRisk: BigDecimal,
    val remainingDailyCapacity: BigDecimal?,
    val openSymbolCount: Int,
    val activeAttemptSymbolCount: Int,
    val attempts: List<RiskAttemptSnapshot>,
    val reservations: List<RiskReservationSnapshot>,
)

data class RiskAttemptSnapshot(
    val sequence: Long,
    val levelId: UUID,
    val symbol: String,
    val status: RiskAttemptStatus,
    val admittedAt: Instant,
    val completedAt: Instant?,
    val confirmedPositionQuantity: BigDecimal,
    val plan: AttemptRiskPlan,
)

enum class RiskAttemptStatus {
    PENDING_ENTRY,
    OPEN_POSITION,
    FLAT_CONFIRMED,
}

data class RiskReservationSnapshot(
    val sequence: Long,
    val levelId: UUID,
    val symbol: String,
    val status: RiskReservationStatus,
    val levelRiskBudget: BigDecimal,
    val reservedRisk: BigDecimal,
    val plannedQuantity: BigDecimal,
)

enum class RiskReservationStatus {
    PENDING_ATTEMPT,
    OPEN_POSITION,
}
