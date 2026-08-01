package com.scalpsecta.breakoutbot.service

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.domain.ApplicationRun
import com.scalpsecta.breakoutbot.domain.RuntimeHealth
import com.scalpsecta.breakoutbot.domain.RuntimeSnapshot
import com.scalpsecta.breakoutbot.domain.TradingReadiness
import com.scalpsecta.breakoutbot.evidence.EvidenceService
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.level.LevelService
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataService
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import org.springframework.stereotype.Service

@Service
class BotStateService(
    private val publicMarketDataService: PublicMarketDataService,
    private val authenticatedBinanceReadinessService:
        AuthenticatedBinanceReadinessService,
    private val levelService: LevelService,
    private val attemptRiskService: AttemptRiskService,
    private val evidenceService: EvidenceService,
    private val applicationRun: ApplicationRun,
) {
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
        val globalTradingState = GlobalTradingState.RUNNING
        val levels = levelService.currentState(
            privateStreamReadiness = privateStreamReadiness,
            publicMarketData = publicMarketData,
            globalState = globalTradingState,
        )
        return RuntimeSnapshot(
            startedAt = applicationRun.startedAt,
            levelCount = levels.size,
            levels = levels,
            recoveredAttemptCount = 0,
            globalTradingState = globalTradingState,
            publicMarketData = publicMarketData,
            authenticatedBinance = authenticatedBinance,
            risk = attemptRiskService.currentState(),
            evidence = evidenceService.currentSnapshot(),
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
