package com.scalpsecta.breakoutbot.domain

import java.time.Instant

data class RuntimeSnapshot(
    val startedAt: Instant,
    val levelCount: Int,
    val recoveredAttemptCount: Int,
    val health: RuntimeHealth,
)

data class RuntimeHealth(
    val publicDataReadiness: BinanceReadiness,
    val privateStreamReadiness: BinanceReadiness,
    val tradingReadiness: TradingReadiness,
) {
    companion object {
        fun notReady(): RuntimeHealth =
            RuntimeHealth(
                publicDataReadiness = BinanceReadiness.NOT_READY,
                privateStreamReadiness = BinanceReadiness.NOT_READY,
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
