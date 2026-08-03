package com.scalpsecta.breakoutbot.api

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.domain.TradingReadiness
import com.scalpsecta.breakoutbot.service.BotStateService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/health")
class OperatorHealthController(
    private val botStateService: BotStateService,
) {
    @GetMapping("/liveness")
    fun liveness(): ProcessLivenessSnapshot = ProcessLivenessSnapshot()

    @GetMapping("/readiness")
    fun readiness(): RuntimeReadinessSnapshot {
        val health = botStateService.currentState().health
        return RuntimeReadinessSnapshot(
            publicDataReadiness = health.publicDataReadiness,
            privateStreamReadiness = health.privateStreamReadiness,
            clockReadiness = health.clockReadiness,
            accountReadiness = health.accountReadiness,
            tradingReadiness = health.tradingReadiness,
        )
    }
}

data class ProcessLivenessSnapshot(
    val process: ProcessLiveness = ProcessLiveness.LIVE,
    val http: ProcessLiveness = ProcessLiveness.LIVE,
)

data class RuntimeReadinessSnapshot(
    val publicDataReadiness: BinanceReadiness,
    val privateStreamReadiness: BinanceReadiness,
    val clockReadiness: BinanceReadiness,
    val accountReadiness: BinanceReadiness,
    val tradingReadiness: TradingReadiness,
)

enum class ProcessLiveness {
    LIVE,
}
