package com.scalpsecta.breakoutbot.level

import com.scalpsecta.breakoutbot.signal.LevelSignalSnapshot
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class CreateLevelCommand(
    val symbol: String,
    val direction: LevelDirection,
    val levelPrice: BigDecimal,
    val positionNotionalUsdt: BigDecimal,
    val maxImpulsePct: BigDecimal,
)

enum class LevelDirection {
    LONG,
    SHORT,
}

data class LevelSnapshot(
    val id: UUID,
    val createdAt: Instant,
    val symbol: String,
    val direction: LevelDirection,
    val requestedLevelPrice: BigDecimal,
    val normalizedLevelPrice: BigDecimal,
    val positionNotionalUsdt: BigDecimal,
    val maxImpulsePct: BigDecimal,
    val sizingReferencePrice: BigDecimal,
    val plannedQuantity: BigDecimal,
    val entryAllocation: List<LevelEntryTranche>,
    val leverage: Int,
    val projectedIsolatedMargin: BigDecimal,
    val riskBoundaryStopPrice: BigDecimal,
    val estimatedLiquidationPrice: BigDecimal,
    val state: LevelState,
    val stateChangedAt: Instant,
    val warmupHealthySince: Instant?,
    val terminalReason: LevelReasonCode?,
    val globalState: GlobalTradingState,
    val blockers: List<LevelBlocker>,
    val signal: LevelSignalSnapshot,
    val attemptNumber: Long,
    val attemptConsumed: Boolean,
    val preEntryDispatchedAt: Instant?,
    val confirmedPositionQuantity: BigDecimal,
    val hardStopPrice: BigDecimal?,
    val hardStopClientOrderId: String?,
    val hardStopConfirmedAt: Instant?,
    val ownsActiveAttempt: Boolean,
    val ownsExposure: Boolean,
    val hasUnresolvedOrder: Boolean,
    val deleteAllowed: Boolean,
    val preEntryFilledAt: Instant? = null,
    val crossingTradeId: Long? = null,
    val crossedAt: Instant? = null,
    val confirmationStartedAt: Instant? = null,
    val breakoutConfirmedAt: Instant? = null,
)

data class LevelEntryTranche(
    val role: LevelEntryRole,
    val allocationPercent: Int,
    val quantity: BigDecimal,
)

enum class LevelEntryRole {
    PRE_BREAK,
    CROSSING,
    CONFIRMATION,
}

enum class LevelState {
    WARMING_UP,
    ARMED,
    APPROACH,
    PRE_ENTRY_PENDING,
    PRE_ENTRY,
    CROSS_ENTRY_PENDING,
    BREAK_CONFIRM,
    CONFIRM_ENTRY_PENDING,
    POSITION_MANAGEMENT,
    EXITING,
    TERMINAL,
}

enum class LevelBlocker {
    WARMING_UP,
    TERMINAL,
    ENTRY_COOLDOWN,
    SAFE_MODE,
    DAILY_LOCKED,
    MANUAL_LOCK,
    SYMBOL_HAS_ACTIVE_OWNER,
}

enum class GlobalTradingState {
    RUNNING,
    ENTRY_COOLDOWN,
    SAFE_MODE,
    DAILY_LOCKED,
    MANUAL_LOCK,
}

enum class LevelReasonCode {
    INVALID_SYMBOL,
    INVALID_LEVEL,
    DUPLICATE_LEVEL,
    LEVEL_CAPACITY_REACHED,
    LEVEL_ALREADY_CROSSED,
    SYMBOL_CONFIGURATION_FAILED,
    LIQUIDATION_TOO_CLOSE,
    LEVEL_NOT_FOUND,
    LEVEL_HAS_EXPOSURE,
    LEVEL_HAS_UNRESOLVED_ORDER,
    MISSED_DURING_WARMUP,
    SYMBOL_OWNERSHIP_CONFLICT,
    LEVEL_ALREADY_CONSUMED,
    PRE_ENTRY_NOT_ELIGIBLE,
    INSUFFICIENT_LIQUIDITY,
    STOP_SETUP_FAILED,
    CROSS_BEFORE_PROTECTED,
    PRE_ENTRY_INVALIDATED,
    PRE_ENTRY_TIMEOUT,
    BREAK_CONFIRM_FAILED,
    MARKET_DATA_FAILURE,
    PRIVATE_STREAM_FAILURE,
    ORDER_OUTCOME_UNKNOWN,
    TP_SETUP_FAILED,
    TAKE_PROFITS_COMPLETE,
}

class LevelException(
    val code: LevelReasonCode,
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)
