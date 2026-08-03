package com.scalpsecta.breakoutbot.control

import com.scalpsecta.breakoutbot.binance.BinancePositionRisk
import com.scalpsecta.breakoutbot.evidence.CommandEvidence
import com.scalpsecta.breakoutbot.evidence.EvidenceRecorder
import com.scalpsecta.breakoutbot.evidence.NoOpEvidenceRecorder
import com.scalpsecta.breakoutbot.failure.SignedRuntimeReconciliation
import com.scalpsecta.breakoutbot.level.GlobalTradingState
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
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

class OperatorControlServiceTest {
    private val scheduler = Schedulers.newSingle("operator-control-risk-test")
    private val riskService = AttemptRiskService(
        clock = CLOCK,
        scheduler = scheduler,
        evidenceRecorder = NoOpEvidenceRecorder,
    )
    private val gateway = FakeOperatorControlExecutionGateway()
    private val evidenceRecorder = RecordingCommandEvidenceRecorder()
    private val service = OperatorControlService(
        clock = CLOCK,
        riskService = riskService,
        executionGateway = gateway,
        evidenceRecorder = evidenceRecorder,
    )

    @AfterEach
    fun closeResources() {
        riskService.close()
        scheduler.dispose()
    }

    @Test
    fun `duplicate kill command flattens once and remains manually locked`() {
        gateway.reconciliation.set(exposedReconciliation())
        val commandId = UUID.randomUUID()

        val first = service.kill(commandId).block(TIMEOUT)!!
        val duplicate = service.kill(commandId).block(TIMEOUT)!!

        assertThat(first).isEqualTo(duplicate)
        assertThat(first.status).isEqualTo(OperatorCommandStatus.SUCCEEDED)
        assertThat(first.code)
            .isEqualTo(OperatorCommandCode.KILL_SWITCH_COMPLETED)
        assertThat(first.globalTradingState)
            .isEqualTo(GlobalTradingState.MANUAL_LOCK)
        assertThat(gateway.flattenCount).isOne()
        assertThat(service.currentState().commands).containsExactly(first)
        assertThat(evidenceRecorder.commands).containsExactly(
            NOW to CommandEvidence(
                commandId = commandId,
                type = "KILL_SWITCH",
                symbol = null,
            ),
        )
    }

    @Test
    fun `unlock rejects residual exposure and reports it in command snapshot`() {
        riskService.enterManualLock("TEST_LOCK").block(TIMEOUT)
        gateway.reconciliation.set(exposedReconciliation())

        val result = service.unlock(UUID.randomUUID()).block(TIMEOUT)!!

        assertThat(result.status).isEqualTo(OperatorCommandStatus.BLOCKED)
        assertThat(result.code)
            .isEqualTo(OperatorCommandCode.MANUAL_UNLOCK_REJECTED)
        assertThat(result.blockers)
            .contains(OperatorCommandBlocker.RESIDUAL_EXPOSURE)
        assertThat(result.residualExposure.single().positionAmount)
            .isEqualByComparingTo("0.30")
        assertThat(riskService.currentState().globalTradingState)
            .isEqualTo(GlobalTradingState.MANUAL_LOCK)
    }

    @Test
    fun `unlock succeeds after three matching healthy flat reconciliations`() {
        riskService.enterManualLock("TEST_LOCK").block(TIMEOUT)
        gateway.reconciliation.set(flatReconciliation())

        val result = service.unlock(UUID.randomUUID()).block(TIMEOUT)!!

        assertThat(result.status).isEqualTo(OperatorCommandStatus.SUCCEEDED)
        assertThat(result.code)
            .isEqualTo(OperatorCommandCode.MANUAL_UNLOCK_COMPLETED)
        assertThat(result.blockers).isEmpty()
        assertThat(gateway.reconciliationCount).isEqualTo(3)
        assertThat(riskService.currentState().globalTradingState)
            .isEqualTo(GlobalTradingState.RUNNING)
    }
}

private class RecordingCommandEvidenceRecorder :
    EvidenceRecorder by NoOpEvidenceRecorder {
    val commands = mutableListOf<Pair<Instant, CommandEvidence>>()

    override fun recordCommand(timestamp: Instant, command: CommandEvidence) {
        commands += timestamp to command
    }
}

private class FakeOperatorControlExecutionGateway :
    OperatorControlExecutionGateway {
    val reconciliation = AtomicReference(flatReconciliation())
    var reconciliationCount = 0
    var flattenCount = 0
    var health = OperatorRuntimeHealth(
        publicDataHealthy = true,
        privateStreamHealthy = true,
        accountHealthy = true,
        clockHealthy = true,
        recoveryHealthDurationSatisfied = true,
    )

    override fun reconcile(): Mono<SignedRuntimeReconciliation> =
        Mono.fromSupplier {
            reconciliationCount += 1
            reconciliation.get()
        }

    override fun closePosition(
        symbol: String,
        commandId: UUID,
        reconciliation: SignedRuntimeReconciliation,
    ): Mono<ManualCloseExecution> = Mono.error(
        UnsupportedOperationException("Not used by these tests"),
    )

    override fun flattenAllAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void> = Mono.fromRunnable<Void> {
        flattenCount += 1
        this.reconciliation.set(flatReconciliation())
    }.then()

    override fun runtimeHealth(): OperatorRuntimeHealth = health
}

private fun exposedReconciliation(): SignedRuntimeReconciliation =
    SignedRuntimeReconciliation(
        observedAt = NOW,
        positions = listOf(
            BinancePositionRisk(
                symbol = "BTCUSDT",
                positionAmount = BigDecimal("0.30"),
                entryPrice = BigDecimal("100"),
            ),
        ),
        openBotOrderIds = setOf("bot-hard-stop"),
        orphanedBotOrderIds = emptySet(),
        unresolvedOrderIds = emptySet(),
        unexplainedPositionSymbols = emptySet(),
        symbolChecksHealthy = true,
    )

private fun flatReconciliation(): SignedRuntimeReconciliation =
    SignedRuntimeReconciliation(
        observedAt = NOW,
        positions = emptyList(),
        openBotOrderIds = emptySet(),
        orphanedBotOrderIds = emptySet(),
        unresolvedOrderIds = emptySet(),
        unexplainedPositionSymbols = emptySet(),
        symbolChecksHealthy = true,
    )

private val NOW: Instant = Instant.parse("2026-08-02T14:00:00Z")
private val CLOCK: Clock = Clock.fixed(NOW, ZoneOffset.UTC)
private val TIMEOUT: Duration = Duration.ofSeconds(2)
