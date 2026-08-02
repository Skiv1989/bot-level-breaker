package com.scalpsecta.breakoutbot.control

import com.scalpsecta.breakoutbot.level.GlobalTradingState
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class OperatorControlsSnapshot(
    val observedAt: Instant,
    val commands: List<OperatorCommandSnapshot>,
)

data class OperatorCommandSnapshot(
    val commandId: UUID,
    val type: OperatorCommandType,
    val symbol: String?,
    val status: OperatorCommandStatus,
    val code: OperatorCommandCode,
    val message: String,
    val requestedAt: Instant,
    val completedAt: Instant?,
    val blockers: List<OperatorCommandBlocker>,
    val residualExposure: List<ResidualExposureSnapshot>,
    val openBotOrderIds: Set<String>,
    val globalTradingState: GlobalTradingState,
)

enum class OperatorCommandType {
    MANUAL_CLOSE,
    KILL_SWITCH,
    MANUAL_UNLOCK,
}

enum class OperatorCommandStatus {
    IN_PROGRESS,
    SUCCEEDED,
    BLOCKED,
    FAILED,
}

enum class OperatorCommandCode {
    COMMAND_IN_PROGRESS,
    MANUAL_CLOSE_COMPLETED,
    MANUAL_CLOSE_ALREADY_COMPLETED,
    MANUAL_CLOSE_INCOMPLETE,
    POSITION_NOT_ACTIVE,
    KILL_SWITCH_COMPLETED,
    KILL_SWITCH_INCOMPLETE,
    MANUAL_UNLOCK_COMPLETED,
    MANUAL_UNLOCK_REJECTED,
    COMMAND_FAILED,
    COMMAND_ID_CONFLICT,
}

enum class OperatorCommandBlocker {
    POSITION_NOT_TRACKED,
    RESIDUAL_EXPOSURE,
    BOT_ORDERS_REMAIN,
    UNRESOLVED_ORDER,
    UNEXPLAINED_EXPOSURE,
    ORPHANED_BOT_ORDER,
    SYMBOL_CHECK_FAILED,
    ACCOUNT_CHECK_FAILED,
    PUBLIC_DATA_UNHEALTHY,
    PRIVATE_STREAM_UNHEALTHY,
    ACCOUNT_UNHEALTHY,
    CLOCK_UNHEALTHY,
    RECOVERY_HEALTH_WINDOW_INCOMPLETE,
    RECONCILIATION_MISMATCH,
    NOT_MANUAL_LOCKED,
    GLOBAL_LOCK_REMAINS,
}

data class ResidualExposureSnapshot(
    val symbol: String,
    val positionAmount: BigDecimal,
    val entryPrice: BigDecimal,
)

class OperatorCommandException(
    val code: OperatorCommandCode,
    message: String,
) : RuntimeException(message)
