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
    val blockers: List<LevelBlocker>,
    val signal: LevelSignalSnapshot,
    val ownsExposure: Boolean,
    val hasUnresolvedOrder: Boolean,
    val deleteAllowed: Boolean,
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
}

enum class LevelBlocker {
    WARMING_UP,
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
}

class LevelException(
    val code: LevelReasonCode,
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)
