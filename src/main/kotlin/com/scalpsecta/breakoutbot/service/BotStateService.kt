package com.scalpsecta.breakoutbot.service

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.domain.RuntimeHealth
import com.scalpsecta.breakoutbot.domain.RuntimeSnapshot
import com.scalpsecta.breakoutbot.domain.TradingReadiness
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataService
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class BotStateService(
    private val publicMarketDataService: PublicMarketDataService,
) {
    private val startedAt = Instant.now()

    fun currentState(): RuntimeSnapshot {
        val publicMarketData = publicMarketDataService.snapshots()
        return RuntimeSnapshot(
            startedAt = startedAt,
            levelCount = 0,
            recoveredAttemptCount = 0,
            publicMarketData = publicMarketData,
            health = RuntimeHealth(
                publicDataReadiness =
                    publicMarketDataService.readiness(publicMarketData),
                privateStreamReadiness = BinanceReadiness.NOT_READY,
                tradingReadiness = TradingReadiness.BLOCKED,
            ),
        )
    }
}
