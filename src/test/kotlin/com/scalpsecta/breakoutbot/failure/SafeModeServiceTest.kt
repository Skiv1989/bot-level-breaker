package com.scalpsecta.breakoutbot.failure

import com.scalpsecta.breakoutbot.binance.BinancePositionRisk
import com.scalpsecta.breakoutbot.evidence.NoOpEvidenceRecorder
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicReference

class SafeModeServiceTest {
    private val resources = mutableListOf<AutoCloseable>()

    @AfterEach
    fun closeResources() {
        resources.reversed().forEach(AutoCloseable::close)
    }

    @Test
    fun `public outage closes once only after the strict three second boundary`() {
        val harness = harness()
        harness.service.evaluateNow().block(TIMEOUT)

        harness.health.updateAndGet { health ->
            health.copy(publicDataHealthy = false)
        }
        assertThat(harness.service.evaluateNow().block(TIMEOUT)!!
            .entriesAndAdditionsBlocked).isTrue()

        harness.clock.advance(Duration.ofSeconds(3))
        harness.service.evaluateNow().block(TIMEOUT)
        assertThat(harness.gateway.closeReasons).isEmpty()

        harness.clock.advance(Duration.ofMillis(1))
        val failed = harness.service.evaluateNow().block(TIMEOUT)!!

        assertThat(harness.gateway.closeReasons)
            .containsExactly(LevelReasonCode.MARKET_DATA_FAILURE)
        assertThat(failed.globalTradingState)
            .isEqualTo(GlobalTradingState.SAFE_MODE)
        assertThat(failed.safeModeEventCount).isOne()

        harness.service.evaluateNow().block(TIMEOUT)
        assertThat(harness.gateway.closeReasons).hasSize(1)
    }

    @Test
    fun `private loss reconciles immediately and restoration before five seconds avoids exit`() {
        val harness = harness()
        harness.service.evaluateNow().block(TIMEOUT)

        harness.health.updateAndGet { health ->
            health.copy(privateStreamHealthy = false)
        }
        val unhealthy = harness.service.evaluateNow().block(TIMEOUT)!!

        assertThat(unhealthy.entriesAndAdditionsBlocked).isTrue()
        assertThat(harness.gateway.reconciliationCount).isOne()

        harness.clock.advance(Duration.ofMillis(4_999))
        harness.health.updateAndGet { health ->
            health.copy(privateStreamHealthy = true)
        }
        val restored = harness.service.evaluateNow().block(TIMEOUT)!!

        assertThat(harness.gateway.closeReasons).isEmpty()
        assertThat(restored.globalTradingState)
            .isEqualTo(GlobalTradingState.RUNNING)
        assertThat(restored.privateStreamUnhealthySince).isNull()
    }

    @Test
    fun `private outage closes reconciled exposure only after five seconds`() {
        val harness = harness()
        harness.service.evaluateNow().block(TIMEOUT)
        harness.health.updateAndGet { health ->
            health.copy(privateStreamHealthy = false)
        }
        harness.service.evaluateNow().block(TIMEOUT)

        harness.clock.advance(Duration.ofSeconds(5))
        harness.service.evaluateNow().block(TIMEOUT)
        assertThat(harness.gateway.closeReasons).isEmpty()

        harness.clock.advance(Duration.ofMillis(1))
        val failed = harness.service.evaluateNow().block(TIMEOUT)!!

        assertThat(harness.gateway.closeReasons)
            .containsExactly(LevelReasonCode.PRIVATE_STREAM_FAILURE)
        assertThat(harness.gateway.reconciliationCount).isEqualTo(2)
        assertThat(failed.globalTradingState)
            .isEqualTo(GlobalTradingState.SAFE_MODE)
    }

    @Test
    fun `reconciliation mismatch resets recovery evidence`() {
        val harness = harness()
        harness.riskService.enterSafeMode("TEST_FAILURE").block(TIMEOUT)

        harness.service.evaluateNow().block(TIMEOUT)
        harness.clock.advance(Duration.ofSeconds(1))
        assertThat(harness.service.evaluateNow().block(TIMEOUT)!!
            .matchingReconciliationCount).isEqualTo(2)

        harness.gateway.reconciliation.updateAndGet { reconciliation ->
            reconciliation.copy(
                positions = listOf(position(entryPrice = "101")),
            )
        }
        harness.clock.advance(Duration.ofSeconds(1))
        assertThat(harness.service.evaluateNow().block(TIMEOUT)!!
            .matchingReconciliationCount).isOne()

        harness.clock.advance(Duration.ofSeconds(28))
        val stillSafe = harness.service.evaluateNow().block(TIMEOUT)!!
        assertThat(stillSafe.matchingReconciliationCount).isEqualTo(2)
        assertThat(stillSafe.globalTradingState)
            .isEqualTo(GlobalTradingState.SAFE_MODE)

        harness.clock.advance(Duration.ofSeconds(1))
        val recovered = harness.service.evaluateNow().block(TIMEOUT)!!
        assertThat(recovered.globalTradingState)
            .isEqualTo(GlobalTradingState.RUNNING)
    }

    @Test
    fun `unresolved order blocks automatic recovery until three clean matches`() {
        val harness = harness()
        harness.gateway.reconciliation.updateAndGet { reconciliation ->
            reconciliation.copy(unresolvedOrderIds = setOf("unknown-order"))
        }
        harness.riskService.enterSafeMode("ORDER_OUTCOME_UNKNOWN").block(TIMEOUT)

        harness.service.evaluateNow().block(TIMEOUT)
        harness.clock.advance(Duration.ofSeconds(1))
        harness.service.evaluateNow().block(TIMEOUT)
        harness.clock.advance(Duration.ofSeconds(29))
        val blocked = harness.service.evaluateNow().block(TIMEOUT)!!

        assertThat(blocked.matchingReconciliationCount).isEqualTo(3)
        assertThat(blocked.globalTradingState)
            .isEqualTo(GlobalTradingState.SAFE_MODE)

        harness.gateway.reconciliation.updateAndGet { reconciliation ->
            reconciliation.copy(unresolvedOrderIds = emptySet())
        }
        repeat(3) {
            harness.clock.advance(Duration.ofSeconds(1))
            harness.service.evaluateNow().block(TIMEOUT)
        }

        assertThat(harness.service.currentState().globalTradingState)
            .isEqualTo(GlobalTradingState.RUNNING)
    }

    @Test
    fun `third safe mode event inside fifteen minutes flattens and locks`() {
        val harness = harness()

        repeat(2) { index ->
            harness.riskService.enterSafeMode("FAILURE_$index").block(TIMEOUT)
            harness.riskService.recoverFromSafeMode().block(TIMEOUT)
            harness.clock.advance(Duration.ofMinutes(1))
        }
        val escalated = harness.riskService
            .enterSafeMode("FAILURE_3")
            .block(TIMEOUT)!!

        assertThat(escalated.safeModeEventCount).isEqualTo(3)
        assertThat(escalated.globalTradingState)
            .isEqualTo(GlobalTradingState.MANUAL_LOCK)

        val monitored = harness.service.evaluateNow().block(TIMEOUT)!!
        assertThat(harness.gateway.flattenCount).isOne()
        assertThat(monitored.globalTradingState)
            .isEqualTo(GlobalTradingState.MANUAL_LOCK)

        harness.service.evaluateNow().block(TIMEOUT)
        assertThat(harness.gateway.flattenCount).isOne()
    }

    private fun harness(): SafeModeHarness {
        val clock = MutableFailureClock(STARTED_AT)
        val health = AtomicReference(healthyRuntime())
        val gateway = FakeSafeModeExecutionGateway(clock)
        val riskScheduler = Schedulers.newSingle("safe-mode-test-risk")
        val monitorScheduler = Schedulers.newSingle("safe-mode-test-monitor")
        val riskService = AttemptRiskService(
            clock = clock,
            scheduler = riskScheduler,
            evidenceRecorder = NoOpEvidenceRecorder,
        )
        val service = SafeModeService(
            clock = clock,
            scheduler = monitorScheduler,
            automaticMonitoring = false,
            riskService = riskService,
            executionGateway = gateway,
            runtimeHealth = health::get,
            monitorInterval = Duration.ofMillis(50),
            reconciliationInterval = Duration.ofSeconds(1),
            publicOutageTimeout = Duration.ofSeconds(3),
            privateOutageTimeout = Duration.ofSeconds(5),
            recoveryHealthDuration = Duration.ofSeconds(30),
        )
        resources += AutoCloseable {
            service.close()
            riskService.close()
            riskScheduler.dispose()
        }
        return SafeModeHarness(
            clock = clock,
            health = health,
            gateway = gateway,
            riskService = riskService,
            service = service,
        )
    }
}

private data class SafeModeHarness(
    val clock: MutableFailureClock,
    val health: AtomicReference<FailureRuntimeHealth>,
    val gateway: FakeSafeModeExecutionGateway,
    val riskService: AttemptRiskService,
    val service: SafeModeService,
)

private class FakeSafeModeExecutionGateway(
    private val clock: Clock,
) : SafeModeExecutionGateway {
    val reconciliation = AtomicReference(
        SignedRuntimeReconciliation(
            observedAt = clock.instant(),
            positions = listOf(position()),
            openBotOrderIds = setOf("bot-stop"),
            orphanedBotOrderIds = emptySet(),
            unresolvedOrderIds = emptySet(),
            unexplainedPositionSymbols = emptySet(),
            symbolChecksHealthy = true,
        ),
    )
    val closeReasons = mutableListOf<LevelReasonCode>()
    var reconciliationCount = 0
    var flattenCount = 0
    var trackedExposure = true

    override fun runtimeSymbols(): Set<String> = setOf("BTCUSDT")

    override fun hasTrackedExposure(): Boolean = trackedExposure

    override fun reconcile(): Mono<SignedRuntimeReconciliation> =
        Mono.fromSupplier {
            reconciliationCount += 1
            reconciliation.get().copy(observedAt = clock.instant())
        }

    override fun closeReconciledExposure(
        reason: LevelReasonCode,
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void> = Mono.fromRunnable<Void> {
        closeReasons += reason
    }.then()

    override fun flattenAllAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void> = Mono.fromRunnable<Void> {
        flattenCount += 1
    }.then()
}

private class MutableFailureClock(
    initialInstant: Instant,
) : Clock() {
    private val currentInstant = AtomicReference(initialInstant)

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = currentInstant.get()

    fun advance(duration: Duration) {
        currentInstant.updateAndGet { instant -> instant.plus(duration) }
    }
}

private fun healthyRuntime(): FailureRuntimeHealth =
    FailureRuntimeHealth(
        publicDataHealthy = true,
        privateStreamHealthy = true,
        accountHealthy = true,
        clockHealthy = true,
    )

private fun position(entryPrice: String = "100"): BinancePositionRisk =
    BinancePositionRisk(
        symbol = "BTCUSDT",
        positionAmount = BigDecimal("0.30"),
        entryPrice = BigDecimal(entryPrice),
    )

private val STARTED_AT: Instant = Instant.parse("2026-08-02T07:00:00Z")
private val TIMEOUT: Duration = Duration.ofSeconds(2)
