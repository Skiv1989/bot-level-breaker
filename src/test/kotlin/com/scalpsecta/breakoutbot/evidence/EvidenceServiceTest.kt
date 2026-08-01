package com.scalpsecta.breakoutbot.evidence

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.domain.ApplicationRun
import com.scalpsecta.breakoutbot.level.LevelState
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.AggressorSide
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPInputStream

class EvidenceServiceTest {
    @TempDir
    lateinit var tempDirectory: Path

    @Test
    fun `audit appends in order and separates application runs`() {
        val levelId = UUID.randomUUID()
        val firstStart = Instant.parse("2026-08-01T09:00:00Z")
        service(firstStart).useEvidence { evidence ->
            evidence.recordAudit(audit(levelId, firstStart, "CREATED"))
            evidence.recordAudit(audit(levelId, firstStart.plusMillis(1), "ARMED"))
            evidence.recordAudit(audit(levelId, firstStart.plusMillis(2), "APPROACH"))
        }

        val secondStart = firstStart.plusSeconds(60)
        service(secondStart).useEvidence { evidence ->
            evidence.recordAudit(audit(levelId, secondStart, "RESTARTED"))
        }

        val records = auditRecords()
        assertThat(records.map { record -> record["decision"].asText() })
            .containsExactly("CREATED", "ARMED", "APPROACH", "RESTARTED")
        assertThat(records.map { record -> record["sequence"].asLong() })
            .containsExactly(1L, 2L, 3L, 1L)
        assertThat(records.take(3).map { record -> record["applicationStartedAt"].asText() })
            .containsOnly(firstStart.toString())
        assertThat(records.last()["applicationStartedAt"].asText())
            .isEqualTo(secondStart.toString())
        assertThat(records.map { record -> record["eventId"].asText() }.toSet())
            .hasSize(4)
    }

    @Test
    fun `rolling boundary and post-attempt boundary produce readable compressed evidence`() {
        val startedAt = Instant.parse("2026-08-01T10:00:00Z")
        val levelId = UUID.randomUUID()
        val clock = EvidenceMutableClock(startedAt)
        val evidence = service(startedAt, clock)

        evidence.record(trade(1, startedAt))
        evidence.record(trade(2, startedAt.plusSeconds(1)))
        evidence.record(trade(3, startedAt.plusSeconds(11)))
        evidence.startAttempt(levelId, SYMBOL, startedAt.plusSeconds(11))
        evidence.record(bookTicker(startedAt.plusSeconds(12)))
        evidence.record(orderUpdate(startedAt.plusSeconds(12).plusMillis(1)))
        evidence.recordOrderIntent(
            levelId = levelId,
            symbol = SYMBOL,
            timestamp = startedAt.plusSeconds(12).plusMillis(2),
            order = OrderEvidence(
                clientOrderId = "safe-client-order-id",
                role = "PRE_BREAK",
                requestedQuantity = BigDecimal("0.010"),
            ),
        )
        evidence.recordReconciliation(
            levelId = levelId,
            symbol = SYMBOL,
            timestamp = startedAt.plusSeconds(12).plusMillis(3),
            reconciliation = ReconciliationEvidence(
                clientOrderId = "safe-client-order-id",
                attemptNumber = 1,
                result = "FILLED",
                filledQuantity = BigDecimal("0.010"),
            ),
        )
        evidence.completeAttempt(levelId, SYMBOL, startedAt.plusSeconds(12))
        evidence.record(trade(4, startedAt.plusSeconds(22)))
        evidence.record(trade(5, startedAt.plusSeconds(22).plusMillis(1)))
        assertThat(evidence.flush()).isTrue()
        evidence.close()

        val records = attemptRecords(levelId)
        assertThat(
            records
                .filter { record -> record["eventType"].asText() == "AGGREGATE_TRADE" }
                .map { record -> record["aggregateTrade"]["aggregateTradeId"].asLong() },
        ).containsExactly(2L, 3L, 4L)
        assertThat(records.map { record -> record["eventType"].asText() })
            .contains(
                "BOOK_TICKER",
                "PRIVATE_ORDER",
                "ORDER_INTENT",
                "RECONCILIATION",
            )
        assertThat(records.map { record -> record["sequence"].asLong() })
            .isSorted()
        assertThat(records.map { record -> record["levelId"].asText() })
            .containsOnly(levelId.toString())
    }

    @Test
    fun `snapshot retains only configured recent audit and trade summaries`() {
        val startedAt = Instant.parse("2026-08-01T11:00:00Z")
        val properties = properties(recentAuditLimit = 2, recentTradeLimit = 2)
        val evidence = service(startedAt, properties = properties)
        val levelId = UUID.randomUUID()

        repeat(3) { index ->
            val timestamp = startedAt.plusMillis(index.toLong())
            evidence.recordAudit(audit(levelId, timestamp, "AUDIT_$index"))
            evidence.record(trade(index.toLong(), timestamp))
        }

        val snapshot = evidence.currentSnapshot()
        assertThat(snapshot.persistentFilesAuthoritative).isTrue()
        assertThat(snapshot.recentAudit.map(RecentAuditSummary::decision))
            .containsExactly("AUDIT_1", "AUDIT_2")
        assertThat(snapshot.recentTrades.map(RecentTradeSummary::aggregateTradeId))
            .containsExactly(1L, 2L)
        evidence.close()
    }

    @Test
    fun `graceful close flushes pending audit and finishes compressed stream`() {
        val startedAt = Instant.parse("2026-08-01T12:00:00Z")
        val evidence = service(startedAt)
        val levelId = UUID.randomUUID()

        evidence.recordAudit(audit(levelId, startedAt, "PENDING_AUDIT"))
        evidence.record(trade(1, startedAt))
        evidence.startAttempt(levelId, SYMBOL, startedAt)
        evidence.record(bookTicker(startedAt.plusMillis(1)))
        evidence.close()

        assertThat(auditRecords().map { record -> record["decision"].asText() })
            .containsExactly("PENDING_AUDIT")
        assertThat(attemptRecords(levelId).map { record -> record["eventType"].asText() })
            .containsExactly("AGGREGATE_TRADE", "BOOK_TICKER")
    }

    private fun service(
        startedAt: Instant,
        clock: Clock = Clock.fixed(startedAt, ZoneOffset.UTC),
        properties: EvidenceProperties = properties(),
    ): EvidenceService =
        EvidenceService(
            objectMapper = jacksonObjectMapper().findAndRegisterModules(),
            properties = properties,
            applicationRun = ApplicationRun(Clock.fixed(startedAt, ZoneOffset.UTC)),
            clock = clock,
        )

    private fun properties(
        recentAuditLimit: Int = 20,
        recentTradeLimit: Int = 20,
    ): EvidenceProperties =
        EvidenceProperties(
            directory = tempDirectory.toString(),
            recentAuditLimit = recentAuditLimit,
            recentTradeLimit = recentTradeLimit,
            rawEventLimit = 100,
            shutdownFlushTimeout = Duration.ofSeconds(2),
        )

    private fun audit(
        levelId: UUID,
        timestamp: Instant,
        decision: String,
    ): AuditRecordDraft =
        AuditRecordDraft(
            timestamp = timestamp,
            symbol = SYMBOL,
            levelId = levelId,
            stateBefore = LevelState.WARMING_UP,
            stateAfter = LevelState.ARMED,
            eventType = AuditEventType.STATE_TRANSITION,
            decision = decision,
        )

    private fun trade(id: Long, timestamp: Instant): AggregateTradeEvent =
        AggregateTradeEvent(
            symbol = SYMBOL,
            aggregateTradeId = id,
            eventTime = timestamp,
            tradeTime = timestamp,
            price = BigDecimal("100.0").add(id.toBigDecimal()),
            quantity = BigDecimal("0.010"),
            buyerIsMaker = false,
            aggressorSide = AggressorSide.BUY,
            receivedAt = timestamp,
        )

    private fun bookTicker(timestamp: Instant): BookTickerEvent =
        BookTickerEvent(
            symbol = SYMBOL,
            updateId = timestamp.toEpochMilli(),
            eventTime = timestamp,
            transactionTime = timestamp,
            bidPrice = BigDecimal("100.0"),
            bidQuantity = BigDecimal.ONE,
            askPrice = BigDecimal("100.1"),
            askQuantity = BigDecimal.ONE,
            receivedAt = timestamp,
        )

    private fun orderUpdate(timestamp: Instant): BinanceUserDataEvent.OrderUpdate =
        BinanceUserDataEvent.OrderUpdate(
            eventTime = timestamp,
            transactionTime = timestamp,
            receivedAt = timestamp,
            symbol = SYMBOL,
            clientOrderId = "safe-client-order-id",
            side = "BUY",
            orderType = "LIMIT",
            timeInForce = "IOC",
            originalQuantity = BigDecimal("0.010"),
            originalPrice = BigDecimal("100.0"),
            averagePrice = BigDecimal("100.0"),
            stopPrice = BigDecimal.ZERO,
            executionType = "TRADE",
            orderStatus = "FILLED",
            orderId = 42L,
            lastFilledQuantity = BigDecimal("0.010"),
            accumulatedFilledQuantity = BigDecimal("0.010"),
            lastFilledPrice = BigDecimal("100.0"),
            commissionAsset = "USDT",
            commission = BigDecimal("0.001"),
            tradeId = 43L,
            realizedProfit = BigDecimal.ZERO,
            positionSide = "BOTH",
            reduceOnly = false,
        )

    private fun auditRecords(): List<JsonNode> =
        Files.readAllLines(tempDirectory.resolve("audit.jsonl"))
            .filter(String::isNotBlank)
            .map(objectMapper()::readTree)

    private fun attemptRecords(levelId: UUID): List<JsonNode> {
        val path = Files.list(tempDirectory).use { files ->
            files
                .filter { file ->
                    file.fileName.toString().contains(levelId.toString())
                }
                .findFirst()
                .orElseThrow()
        }
        return GZIPInputStream(Files.newInputStream(path))
            .bufferedReader()
            .useLines { lines ->
                lines.filter(String::isNotBlank).map(objectMapper()::readTree).toList()
            }
    }

    private fun objectMapper() = jacksonObjectMapper().findAndRegisterModules()

    private fun EvidenceService.useEvidence(block: (EvidenceService) -> Unit) {
        try {
            block(this)
        } finally {
            close()
        }
    }
}

private class EvidenceMutableClock(
    initialInstant: Instant,
) : Clock() {
    private val currentInstant = AtomicReference(initialInstant)

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = currentInstant.get()
}

private const val SYMBOL = "BTCUSDT"
