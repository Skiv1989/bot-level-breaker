package com.scalpsecta.breakoutbot.service

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.domain.RuntimeHealth
import com.scalpsecta.breakoutbot.domain.RuntimeSnapshot
import com.scalpsecta.breakoutbot.domain.TradingReadiness
import com.scalpsecta.breakoutbot.level.LevelService
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataService
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class BotStateService(
    private val publicMarketDataService: PublicMarketDataService,
    private val authenticatedBinanceReadinessService:
        AuthenticatedBinanceReadinessService,
    private val levelService: LevelService,
) {
    private val startedAt = Instant.now()

    fun currentState(): RuntimeSnapshot {
        val publicMarketData = publicMarketDataService.snapshots()
        val publicDataReadiness =
            publicMarketDataService.readiness(publicMarketData)
        val authenticatedBinance =
            authenticatedBinanceReadinessService.snapshot()
        val privateStreamReadiness =
            authenticatedBinance.privateStream.readiness
        val clockReadiness = authenticatedBinance.clock.readiness
        val accountReadiness = authenticatedBinance.account.readiness
        val levels = levelService.currentState(
            privateStreamReadiness = privateStreamReadiness,
            publicMarketData = publicMarketData,
        )
        return RuntimeSnapshot(
            startedAt = startedAt,
            levelCount = levels.size,
            levels = levels,
            recoveredAttemptCount = 0,
            publicMarketData = publicMarketData,
            authenticatedBinance = authenticatedBinance,
            health = RuntimeHealth(
                publicDataReadiness = publicDataReadiness,
                privateStreamReadiness = privateStreamReadiness,
                clockReadiness = clockReadiness,
                accountReadiness = accountReadiness,
                tradingReadiness = if (
                    publicDataReadiness == BinanceReadiness.READY &&
                    privateStreamReadiness == BinanceReadiness.READY &&
                    clockReadiness == BinanceReadiness.READY &&
                    accountReadiness == BinanceReadiness.READY
                ) {
                    TradingReadiness.READY
                } else {
                    TradingReadiness.BLOCKED
                },
            ),
        )
    }
}
