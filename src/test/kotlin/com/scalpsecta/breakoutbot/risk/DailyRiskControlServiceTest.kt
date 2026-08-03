package com.scalpsecta.breakoutbot.risk

import com.scalpsecta.breakoutbot.binance.BinancePositionRisk
import com.scalpsecta.breakoutbot.failure.SafeModeExecutionGateway
import com.scalpsecta.breakoutbot.failure.SignedRuntimeReconciliation
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicReference

class DailyRiskControlServiceTest {
    private val clock = DailyRiskClock(
        Instant.parse("2026-08-01T12:00:00Z"),
    )
    private val observation = AtomicReference(account("1000"))
    private val gateway = RecordingDailyRiskExecutionGateway(clock.instant())
    private val riskScheduler = Schedulers.newSingle("daily-risk-test-queue")
    private val monitorScheduler = Schedulers.newSingle("daily-risk-test-monitor")
    private val riskService = AttemptRiskService(
        clock = clock,
        scheduler = riskScheduler,
    )
    private val service = DailyRiskControlService(
        clock = clock,
        scheduler = monitorScheduler,
        automaticMonitoring = false,
        riskService = riskService,
        executionGateway = gateway,
        accountObservation = { Mono.just(observation.get()) },
        transferEvents = Flux.empty(),
        monitorInterval = Duration.ofSeconds(1),
    )

    @AfterEach
    fun closeServices() {
        service.close()
        riskService.close()
    }

    @Test
    fun `exact daily breach flattens every reconciled account position`() {
        service.evaluateNow().block()
        observation.set(account("950"))

        val state = service.evaluateNow().block()!!

        assertThat(state.globalTradingState)
            .isEqualTo(GlobalTradingState.DAILY_LOCKED)
        assertThat(gateway.flattenCount).isOne()
        assertThat(gateway.flattenedPositions.map(BinancePositionRisk::symbol))
            .containsExactly("BTCUSDT", "UNTRACKEDUSDT")
        assertThat(gateway.operationIds.single())
            .startsWith("daily-lock:")
        assertThat(gateway.terminalReasons)
            .containsExactly(LevelReasonCode.DAILY_LOSS_LIMIT)
    }
}

private class RecordingDailyRiskExecutionGateway(
    observedAt: Instant,
) : SafeModeExecutionGateway {
    private val reconciliation = SignedRuntimeReconciliation(
        observedAt = observedAt,
        positions = listOf(
            BinancePositionRisk(
                symbol = "BTCUSDT",
                positionAmount = BigDecimal("0.10"),
                entryPrice = BigDecimal("100"),
            ),
            BinancePositionRisk(
                symbol = "UNTRACKEDUSDT",
                positionAmount = BigDecimal("3"),
                entryPrice = BigDecimal("10"),
            ),
        ),
        openBotOrderIds = setOf("entry", "take-profit", "hard-stop"),
        orphanedBotOrderIds = emptySet(),
        unresolvedOrderIds = emptySet(),
        unexplainedPositionSymbols = setOf("UNTRACKEDUSDT"),
        symbolChecksHealthy = true,
    )
    var flattenCount = 0
    var flattenedPositions = emptyList<BinancePositionRisk>()
    val operationIds = mutableListOf<String>()
    val terminalReasons = mutableListOf<LevelReasonCode>()

    override fun runtimeSymbols(): Set<String> = setOf("BTCUSDT")

    override fun hasTrackedExposure(): Boolean = true

    override fun reconcile(): Mono<SignedRuntimeReconciliation> =
        Mono.just(reconciliation)

    override fun closeReconciledExposure(
        reason: LevelReasonCode,
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void> = Mono.empty()

    override fun flattenAllAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void> = Mono.fromRunnable<Void> {
        flattenCount += 1
        flattenedPositions = reconciliation.positions
        operationIds += operationId
    }.then()

    override fun flattenAllAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
        terminalReason: LevelReasonCode,
    ): Mono<Void> {
        terminalReasons += terminalReason
        return flattenAllAccountExposure(reconciliation, operationId)
    }
}

private fun account(equity: String): DailyRiskAccountObservation =
    DailyRiskAccountObservation(
        totalAccountEquity = BigDecimal(equity),
        availableMargin = BigDecimal("1000"),
    )

private class DailyRiskClock(
    initialInstant: Instant,
) : Clock() {
    private val currentInstant = AtomicReference(initialInstant)

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = currentInstant.get()
}
