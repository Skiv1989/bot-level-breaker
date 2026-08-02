package com.scalpsecta.breakoutbot.domain

import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceSnapshot
import com.scalpsecta.breakoutbot.control.OperatorControlsSnapshot
import com.scalpsecta.breakoutbot.evidence.EvidenceSnapshot
import com.scalpsecta.breakoutbot.execution.ExecutionSnapshot
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.level.LevelSnapshot
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import com.scalpsecta.breakoutbot.risk.GlobalRiskSnapshot
import java.time.Instant

data class RuntimeSnapshot(
    val startedAt: Instant,
    val levelCount: Int,
    val levels: List<LevelSnapshot>,
    val recoveredAttemptCount: Int,
    val globalTradingState: GlobalTradingState,
    val publicMarketData: List<PublicMarketDataSnapshot>,
    val authenticatedBinance: AuthenticatedBinanceSnapshot,
    val risk: GlobalRiskSnapshot,
    val evidence: EvidenceSnapshot,
    val execution: ExecutionSnapshot,
    val controls: OperatorControlsSnapshot,
    val health: RuntimeHealth,
)

data class RuntimeHealth(
    val publicDataReadiness: BinanceReadiness,
    val privateStreamReadiness: BinanceReadiness,
    val clockReadiness: BinanceReadiness,
    val accountReadiness: BinanceReadiness,
    val tradingReadiness: TradingReadiness,
) {
    companion object {
        fun notReady(): RuntimeHealth =
            RuntimeHealth(
                publicDataReadiness = BinanceReadiness.NOT_READY,
                privateStreamReadiness = BinanceReadiness.NOT_READY,
                clockReadiness = BinanceReadiness.NOT_READY,
                accountReadiness = BinanceReadiness.NOT_READY,
                tradingReadiness = TradingReadiness.BLOCKED,
            )
    }
}

enum class BinanceReadiness {
    READY,
    NOT_READY,
}

enum class TradingReadiness {
    READY,
    BLOCKED,
}
