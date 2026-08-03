package com.scalpsecta.breakoutbot.replay

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.evidence.AttemptEvidenceEvent
import com.scalpsecta.breakoutbot.evidence.AttemptEvidenceEventType
import com.scalpsecta.breakoutbot.evidence.AttemptStateChange
import com.scalpsecta.breakoutbot.evidence.CommandEvidence
import com.scalpsecta.breakoutbot.evidence.OrderEvidence
import com.scalpsecta.breakoutbot.evidence.ReconciliationEvidence
import com.scalpsecta.breakoutbot.evidence.TimerEvidence
import com.scalpsecta.breakoutbot.level.LevelState
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.AggressorSide
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.UUID
import java.util.zip.GZIPOutputStream

class RecordedAttemptReplayTest {
    @TempDir
    lateinit var tempDirectory: Path

    private val objectMapper = jacksonObjectMapper()
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    @Test
    fun `compressed recorder artifact replays inputs in order on original time`() {
        val artifact = writeArtifact(recordedEvents())
        val replayer = RecordedAttemptReplayer(objectMapper)

        val run = replayer.requireMatchesRecording(artifact) { clock ->
            DeterministicBoundary(clock)
        }

        assertThat(run.inputs.map(ReplayInputRecord::eventType)).containsExactly(
            AttemptEvidenceEventType.AGGREGATE_TRADE,
            AttemptEvidenceEventType.TIMER,
            AttemptEvidenceEventType.PRIVATE_ORDER,
            AttemptEvidenceEventType.RECONCILIATION,
            AttemptEvidenceEventType.COMMAND,
        )
        assertThat(run.inputs.map(ReplayInputRecord::replayedAt)).containsExactly(
            EVENT_AT,
            EVENT_AT.plusSeconds(1),
            EVENT_AT.plusSeconds(2),
            EVENT_AT.plusSeconds(3),
            EVENT_AT.plusSeconds(4),
        )
        assertThat(run.inputs.first().exchangeTimestamp)
            .isEqualTo(EVENT_AT.minusMillis(2))
        assertThat(run.inputs.first().receivedAt).isEqualTo(EVENT_AT)
        assertThat(run.recordedOutputs.stateTransitions.single().decision)
            .isEqualTo("PRE_ENTRY_INVALIDATED")
        assertThat(run.recordedOutputs.orderIntents.single().clientOrderId)
            .isEqualTo("recorded-order")
        assertThat(run.actualOutputs.decisions).containsExactly(
            "TRADE_5933014",
            "PUBLIC_OUTAGE",
            "PRIVATE_FILLED",
            "FILLED",
            "MANUAL_CLOSE",
        )
        assertThat(run.actualOutputs.reasonCodes).containsExactly("MARKET_DATA_FAILURE")
    }

    @Test
    fun `reader rejects a replay timeline that would move virtual time backwards`() {
        val events = recordedEvents().toMutableList()
        events[1] = events[1].copy(timestamp = EVENT_AT.minusMillis(1))

        org.assertj.core.api.Assertions.assertThatThrownBy {
            writeArtifact(events)
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("must not move backwards")
    }

    private fun recordedEvents(): List<AttemptEvidenceEvent> {
        val trade = AggregateTradeEvent(
            symbol = SYMBOL,
            aggregateTradeId = 5_933_014L,
            eventTime = EVENT_AT.minusMillis(2),
            tradeTime = EVENT_AT.minusMillis(3),
            price = BigDecimal("68123.45"),
            quantity = BigDecimal("0.003"),
            buyerIsMaker = false,
            aggressorSide = AggressorSide.BUY,
            receivedAt = EVENT_AT,
        )
        val privateOrder = BinanceUserDataEvent.OrderUpdate(
            eventTime = EVENT_AT.plusSeconds(2).minusMillis(2),
            transactionTime = EVENT_AT.plusSeconds(2).minusMillis(1),
            receivedAt = EVENT_AT.plusSeconds(2),
            symbol = SYMBOL,
            clientOrderId = "recorded-order",
            side = "BUY",
            orderType = "LIMIT",
            timeInForce = "IOC",
            originalQuantity = BigDecimal("0.003"),
            originalPrice = BigDecimal("68123.45"),
            averagePrice = BigDecimal("68123.45"),
            stopPrice = BigDecimal.ZERO,
            executionType = "TRADE",
            orderStatus = "FILLED",
            orderId = 42L,
            lastFilledQuantity = BigDecimal("0.003"),
            accumulatedFilledQuantity = BigDecimal("0.003"),
            lastFilledPrice = BigDecimal("68123.45"),
            commissionAsset = "USDT",
            commission = BigDecimal("0.01"),
            tradeId = 43L,
            realizedProfit = BigDecimal.ZERO,
            positionSide = "BOTH",
            reduceOnly = false,
        )
        return listOf(
            event(
                sequence = 1,
                timestamp = EVENT_AT,
                eventType = AttemptEvidenceEventType.AGGREGATE_TRADE,
                exchangeTimestamp = trade.eventTime,
                receivedAt = trade.receivedAt,
                aggregateTrade = trade,
            ),
            event(
                sequence = 2,
                timestamp = EVENT_AT.plusSeconds(1),
                eventType = AttemptEvidenceEventType.TIMER,
                timer = TimerEvidence(
                    publicMarketDataHealthy = false,
                    privateStreamHealthy = true,
                ),
            ),
            event(
                sequence = 3,
                timestamp = EVENT_AT.plusSeconds(2),
                eventType = AttemptEvidenceEventType.PRIVATE_ORDER,
                exchangeTimestamp = privateOrder.eventTime,
                receivedAt = privateOrder.receivedAt,
                privateEvent = privateOrder,
            ),
            event(
                sequence = 4,
                timestamp = EVENT_AT.plusSeconds(3),
                eventType = AttemptEvidenceEventType.RECONCILIATION,
                reconciliation = ReconciliationEvidence(
                    clientOrderId = "recorded-order",
                    attemptNumber = 1,
                    result = "FILLED",
                    exchangeOrderId = 42L,
                    requestedQuantity = BigDecimal("0.003"),
                    filledQuantity = BigDecimal("0.003"),
                ),
            ),
            event(
                sequence = 5,
                timestamp = EVENT_AT.plusSeconds(4),
                eventType = AttemptEvidenceEventType.COMMAND,
                command = CommandEvidence(
                    commandId = COMMAND_ID,
                    type = "MANUAL_CLOSE",
                    symbol = SYMBOL,
                ),
            ),
            event(
                sequence = 6,
                timestamp = EVENT_AT.plusSeconds(5),
                eventType = AttemptEvidenceEventType.STATE_TRANSITION,
                stateChange = AttemptStateChange(
                    stateBefore = LevelState.APPROACH,
                    stateAfter = LevelState.TERMINAL,
                    decision = "PRE_ENTRY_INVALIDATED",
                    blockerReasons = listOf("MARKET_DATA_FAILURE"),
                ),
            ),
            event(
                sequence = 7,
                timestamp = EVENT_AT.plusSeconds(6),
                eventType = AttemptEvidenceEventType.ORDER_INTENT,
                orderIntent = OrderEvidence(
                    clientOrderId = "recorded-order",
                    role = "ENTRY",
                    requestedQuantity = BigDecimal("0.003"),
                ),
            ),
        )
    }

    private fun event(
        sequence: Long,
        timestamp: Instant,
        eventType: AttemptEvidenceEventType,
        exchangeTimestamp: Instant? = null,
        receivedAt: Instant? = null,
        aggregateTrade: AggregateTradeEvent? = null,
        privateEvent: BinanceUserDataEvent? = null,
        stateChange: AttemptStateChange? = null,
        orderIntent: OrderEvidence? = null,
        reconciliation: ReconciliationEvidence? = null,
        timer: TimerEvidence? = null,
        command: CommandEvidence? = null,
    ): AttemptEvidenceEvent = AttemptEvidenceEvent(
        eventId = UUID.nameUUIDFromBytes("event-$sequence".toByteArray()),
        sequence = sequence,
        timestamp = timestamp,
        applicationStartedAt = EVENT_AT.minusSeconds(60),
        symbol = SYMBOL,
        levelId = LEVEL_ID,
        eventType = eventType,
        exchangeTimestamp = exchangeTimestamp,
        receivedAt = receivedAt,
        aggregateTrade = aggregateTrade,
        privateEvent = privateEvent,
        stateChange = stateChange,
        orderIntent = orderIntent,
        reconciliation = reconciliation,
        timer = timer,
        command = command,
    )

    private fun writeArtifact(
        events: List<AttemptEvidenceEvent>,
    ): RecordedAttemptArtifact {
        val path = tempDirectory.resolve("attempt.jsonl.gz")
        GZIPOutputStream(Files.newOutputStream(path)).bufferedWriter().use { writer ->
            events.forEach { event ->
                writer.write(objectMapper.writeValueAsString(event))
                writer.newLine()
            }
        }
        return RecordedAttemptReplayer(objectMapper).read(path)
    }
}

private class DeterministicBoundary(
    private val clock: ReplayVirtualClock,
) : ReplayDecisionBoundary {
    private val decisions = mutableListOf<String>()
    private val reasons = mutableListOf<String>()
    private val audits = mutableListOf<ReplayAuditResult>()
    private val transitions = mutableListOf<AttemptStateChange>()
    private val orders = mutableListOf<OrderEvidence>()

    override fun onAggregateTrade(event: AggregateTradeEvent) {
        record("TRADE_${event.aggregateTradeId}")
    }

    override fun onBookTicker(
        @Suppress("UNUSED_PARAMETER") event:
            com.scalpsecta.breakoutbot.marketdata.BookTickerEvent,
    ) = Unit

    override fun onPrivateEvent(event: BinanceUserDataEvent) {
        record("PRIVATE_${(event as BinanceUserDataEvent.OrderUpdate).orderStatus}")
    }

    override fun onTimer(symbol: String, timer: TimerEvidence) {
        if (!timer.publicMarketDataHealthy) {
            reasons += "MARKET_DATA_FAILURE"
            record("PUBLIC_OUTAGE", reasons.takeLast(1))
        }
    }

    override fun onReconciliation(
        symbol: String,
        reconciliation: ReconciliationEvidence,
    ) {
        orders += OrderEvidence(
            clientOrderId = "recorded-order",
            role = "ENTRY",
            requestedQuantity = BigDecimal("0.003"),
        )
        record(reconciliation.result)
    }

    override fun onCommand(command: CommandEvidence) {
        transitions += AttemptStateChange(
            stateBefore = LevelState.APPROACH,
            stateAfter = LevelState.TERMINAL,
            decision = "PRE_ENTRY_INVALIDATED",
            blockerReasons = listOf("MARKET_DATA_FAILURE"),
        )
        record(command.type)
    }

    override fun trace(): ReplayDecisionTrace = ReplayDecisionTrace(
        stateTransitions = transitions.toList(),
        decisions = decisions.toList(),
        orderIntents = orders.toList(),
        reasonCodes = reasons.toList(),
        auditResults = audits.toList(),
    )

    private fun record(decision: String, reasonCodes: List<String> = emptyList()) {
        decisions += decision
        audits += ReplayAuditResult(
            timestamp = clock.instant(),
            symbol = SYMBOL,
            eventType = "DECISION",
            decision = decision,
            reasonCodes = reasonCodes,
        )
    }
}

private val EVENT_AT: Instant = Instant.parse("2026-08-03T09:00:00Z")
private val LEVEL_ID: UUID = UUID.fromString("11111111-1111-1111-1111-111111111111")
private val COMMAND_ID: UUID = UUID.fromString("22222222-2222-2222-2222-222222222222")
private const val SYMBOL = "BTCUSDT"
