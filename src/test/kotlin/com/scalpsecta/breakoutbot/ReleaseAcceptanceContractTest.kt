package com.scalpsecta.breakoutbot

import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.risk.RiskBlockerCode
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ReleaseAcceptanceContractTest {
    @Test
    fun `stable reason namespaces contain every PRD reason code`() {
        val availableCodes = (
            LevelReasonCode.entries.map { reason -> reason.name } +
                RiskBlockerCode.entries.map { blocker -> blocker.name }
            ).toSet()

        assertThat(availableCodes).containsAll(PRD_STABLE_REASON_CODES)
    }
}

private val PRD_STABLE_REASON_CODES = setOf(
    "INVALID_SYMBOL",
    "INVALID_LEVEL",
    "DUPLICATE_LEVEL",
    "LEVEL_CAPACITY_REACHED",
    "LEVEL_ALREADY_CROSSED",
    "SYMBOL_CONFIGURATION_FAILED",
    "LIQUIDATION_TOO_CLOSE",
    "MISSED_DURING_WARMUP",
    "BLOCKED_DAILY_RISK",
    "BLOCKED_MARGIN_BUFFER",
    "BLOCKED_POSITION_CAP",
    "STOP_RISK_TOO_HIGH",
    "PLANNED_NET_R_TOO_LOW",
    "PRE_ENTRY_INVALIDATED",
    "PRE_ENTRY_TIMEOUT",
    "CROSS_BEFORE_PROTECTED",
    "BREAK_CONFIRM_FAILED",
    "INSUFFICIENT_LIQUIDITY",
    "STOP_SETUP_FAILED",
    "TP_SETUP_FAILED",
    "EXIT_SCORE",
    "SNAPBACK",
    "MAX_HOLD_TIME",
    "MARKET_DATA_FAILURE",
    "PRIVATE_STREAM_FAILURE",
    "ORDER_OUTCOME_UNKNOWN",
    "DAILY_LOSS_LIMIT",
    "MANUAL_CLOSE",
    "KILL_SWITCH",
    "HARD_STOP_FILLED",
    "TAKE_PROFITS_COMPLETE",
)
