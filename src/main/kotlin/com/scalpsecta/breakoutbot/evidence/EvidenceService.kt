package com.scalpsecta.breakoutbot.evidence

import com.fasterxml.jackson.databind.ObjectMapper
import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.domain.ApplicationRun
import com.scalpsecta.breakoutbot.level.LevelSnapshot
import com.scalpsecta.breakoutbot.level.LevelState
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import jakarta.annotation.PreDestroy
import org.springframework.stereotype.Service
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.ArrayDeque
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Service
class EvidenceService(
    objectMapper: ObjectMapper,
    private val properties: EvidenceProperties,
    private val applicationRun: ApplicationRun,
    private val clock: Clock,
) : EvidenceRecorder {
    private val lock = ReentrantLock()
    private val writer = EvidenceFileWriter(Path.of(properties.directory), objectMapper)
    private val auditSequence = AtomicLong()
    private val attemptEventSequence = AtomicLong()
    private val rollingEvents = mutableMapOf<String, ArrayDeque<AttemptEvidenceEvent>>()
    private val activeAttempts = linkedMapOf<UUID, ActiveAttemptRecording>()
    private val recentAudit = ArrayDeque<RecentAuditSummary>()
    private val recentTrades = ArrayDeque<RecentTradeSummary>()

    override fun recordAudit(draft: AuditRecordDraft) {
        lock.withLock {
            val record = AuditRecord(
                eventId = UUID.randomUUID(),
                sequence = auditSequence.incrementAndGet(),
                timestamp = draft.timestamp,
                applicationStartedAt = applicationRun.startedAt,
                symbol = draft.symbol,
                levelId = draft.levelId,
                stateBefore = draft.stateBefore,
                stateAfter = draft.stateAfter,
                eventType = draft.eventType,
                decision = draft.decision,
                blockerReasons = draft.blockerReasons,
                recoveryDetail = draft.recoveryDetail,
                exception = draft.exception,
                evidence = draft.evidence,
            )
            recentAudit.addLast(
                RecentAuditSummary(
                    eventId = record.eventId,
                    timestamp = record.timestamp,
                    symbol = record.symbol,
                    levelId = record.levelId,
                    eventType = record.eventType,
                    stateBefore = record.stateBefore,
                    stateAfter = record.stateAfter,
                    decision = record.decision,
                    blockerReasons = record.blockerReasons,
                ),
            )
            trimToLimit(recentAudit, properties.recentAuditLimit)
            writer.appendAudit(record)
        }
    }

    override fun recordLevelCreated(
        level: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
    ) {
        recordAudit(
            AuditRecordDraft(
                timestamp = level.createdAt,
                symbol = level.symbol,
                levelId = level.id,
                stateBefore = null,
                stateAfter = level.state,
                eventType = AuditEventType.LEVEL_CREATED,
                decision = "LEVEL_ACCEPTED",
                blockerReasons = level.blockers.map(Enum<*>::name),
                evidence = level.decisionEvidence(marketData),
            ),
        )
    }

    override fun recordLevelDeleted(
        level: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        deletedAt: Instant,
    ) {
        recordAudit(
            AuditRecordDraft(
                timestamp = deletedAt,
                symbol = level.symbol,
                levelId = level.id,
                stateBefore = level.state,
                stateAfter = null,
                eventType = AuditEventType.LEVEL_DELETED,
                decision = "LEVEL_DELETED",
                blockerReasons = level.blockers.map(Enum<*>::name),
                evidence = level.decisionEvidence(marketData),
            ),
        )
        completeAttempt(level.id, level.symbol, deletedAt)
    }

    override fun recordStateTransition(
        before: LevelSnapshot,
        after: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        decision: String,
    ) {
        if (before.state == after.state) {
            return
        }
        if (after.state == LevelState.APPROACH) {
            startAttempt(after.id, after.symbol, after.stateChangedAt)
        }
        val blockers = (
            after.blockers.map(Enum<*>::name) +
                after.signal.mandatoryGates.blockerReasons.map(Enum<*>::name)
            ).distinct()
        recordAudit(
            AuditRecordDraft(
                timestamp = after.stateChangedAt,
                symbol = after.symbol,
                levelId = after.id,
                stateBefore = before.state,
                stateAfter = after.state,
                eventType = AuditEventType.STATE_TRANSITION,
                decision = decision,
                blockerReasons = blockers,
                evidence = after.decisionEvidence(marketData),
            ),
        )
        appendToAttempt(
            levelId = after.id,
            event = attemptEvent(
                timestamp = after.stateChangedAt,
                symbol = after.symbol,
                eventType = AttemptEvidenceEventType.STATE_TRANSITION,
                stateChange = AttemptStateChange(
                    stateBefore = before.state,
                    stateAfter = after.state,
                    decision = decision,
                    blockerReasons = blockers,
                ),
            ),
        )
        if (after.state == LevelState.TERMINAL) {
            completeAttempt(after.id, after.symbol, after.stateChangedAt)
        }
    }

    override fun recordOwnershipChange(
        before: LevelSnapshot,
        after: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        changedAt: Instant,
    ) {
        val decision = "OWNERSHIP_UPDATED"
        recordAudit(
            AuditRecordDraft(
                timestamp = changedAt,
                symbol = after.symbol,
                levelId = after.id,
                stateBefore = before.state,
                stateAfter = after.state,
                eventType = AuditEventType.OWNERSHIP_CHANGED,
                decision = decision,
                blockerReasons = after.blockers.map(Enum<*>::name),
                evidence = after.decisionEvidence(marketData),
            ),
        )
        appendToAttempt(
            levelId = after.id,
            event = attemptEvent(
                timestamp = changedAt,
                symbol = after.symbol,
                eventType = AttemptEvidenceEventType.STATE_TRANSITION,
                stateChange = AttemptStateChange(
                    stateBefore = before.state,
                    stateAfter = after.state,
                    decision = decision,
                    blockerReasons = after.blockers.map(Enum<*>::name),
                ),
            ),
        )
        if (before.claimsAttempt() && !after.claimsAttempt()) {
            completeAttempt(after.id, after.symbol, changedAt)
        }
    }

    override fun record(event: AggregateTradeEvent) {
        val evidenceEvent = attemptEvent(
            timestamp = event.receivedAt,
            symbol = event.symbol,
            eventType = AttemptEvidenceEventType.AGGREGATE_TRADE,
            exchangeTimestamp = event.eventTime,
            receivedAt = event.receivedAt,
            aggregateTrade = event,
        )
        val targets = recordBufferedEvent(evidenceEvent)
        targets.forEach { levelId ->
            writer.appendAttempt(levelId, evidenceEvent.copy(levelId = levelId))
        }
        lock.withLock {
            recentTrades.addLast(
                RecentTradeSummary(
                    eventId = evidenceEvent.eventId,
                    timestamp = evidenceEvent.timestamp,
                    symbol = event.symbol,
                    aggregateTradeId = event.aggregateTradeId,
                    price = event.price,
                    quantity = event.quantity,
                ),
            )
            trimToLimit(recentTrades, properties.recentTradeLimit)
        }
    }

    override fun record(event: BookTickerEvent) {
        val evidenceEvent = attemptEvent(
            timestamp = event.receivedAt,
            symbol = event.symbol,
            eventType = AttemptEvidenceEventType.BOOK_TICKER,
            exchangeTimestamp = event.eventTime ?: event.transactionTime,
            receivedAt = event.receivedAt,
            bookTicker = event,
        )
        val targets = recordBufferedEvent(evidenceEvent)
        targets.forEach { levelId ->
            writer.appendAttempt(levelId, evidenceEvent.copy(levelId = levelId))
        }
    }

    override fun record(event: BinanceUserDataEvent) {
        val eventType = when (event) {
            is BinanceUserDataEvent.AccountUpdate ->
                AttemptEvidenceEventType.PRIVATE_ACCOUNT

            is BinanceUserDataEvent.OrderUpdate ->
                AttemptEvidenceEventType.PRIVATE_ORDER

            is BinanceUserDataEvent.ListenKeyExpired ->
                AttemptEvidenceEventType.PRIVATE_LISTEN_KEY_EXPIRED
        }
        val targetSymbols = when (event) {
            is BinanceUserDataEvent.AccountUpdate -> emptySet()

            is BinanceUserDataEvent.OrderUpdate -> setOf(event.symbol)
            is BinanceUserDataEvent.ListenKeyExpired -> emptySet()
        }
        val baseEvent = attemptEvent(
            timestamp = event.receivedAt,
            symbol = targetSymbols.singleOrNull().orEmpty(),
            eventType = eventType,
            exchangeTimestamp = event.eventTime,
            receivedAt = event.receivedAt,
            privateEvent = event,
        )
        val targets = lock.withLock {
            closeExpiredLocked(event.receivedAt)
            activeAttempts.values
                .filter { attempt ->
                    targetSymbols.isEmpty() || attempt.symbol in targetSymbols
                }
                .map { attempt -> attempt.levelId to attempt.symbol }
        }
        targets.forEach { (levelId, symbol) ->
            writer.appendAttempt(
                levelId,
                baseEvent.copy(levelId = levelId, symbol = symbol),
            )
        }
    }

    override fun recordOrderIntent(
        levelId: UUID,
        symbol: String,
        timestamp: Instant,
        order: OrderEvidence,
    ) {
        appendToAttempt(
            levelId,
            attemptEvent(
                timestamp = timestamp,
                symbol = symbol,
                eventType = AttemptEvidenceEventType.ORDER_INTENT,
                orderIntent = order,
            ),
        )
        recordAudit(
            AuditRecordDraft(
                timestamp = timestamp,
                symbol = symbol,
                levelId = levelId,
                stateBefore = LevelState.APPROACH,
                stateAfter = LevelState.APPROACH,
                eventType = AuditEventType.ORDER_INTENT,
                decision = "ORDER_INTENT_RECORDED",
                evidence = DecisionEvidence(
                    order = order,
                    quantity = QuantityEvidence(
                        requestedQuantity = order.requestedQuantity,
                        filledQuantity = order.filledQuantity,
                    ),
                ),
            ),
        )
    }

    override fun recordReconciliation(
        levelId: UUID,
        symbol: String,
        timestamp: Instant,
        reconciliation: ReconciliationEvidence,
    ) {
        appendToAttempt(
            levelId,
            attemptEvent(
                timestamp = timestamp,
                symbol = symbol,
                eventType = AttemptEvidenceEventType.RECONCILIATION,
                reconciliation = reconciliation,
            ),
        )
        recordAudit(
            AuditRecordDraft(
                timestamp = timestamp,
                symbol = symbol,
                levelId = levelId,
                stateBefore = LevelState.APPROACH,
                stateAfter = LevelState.APPROACH,
                eventType = AuditEventType.RECONCILIATION,
                decision = reconciliation.result,
                recoveryDetail = reconciliation.safeDetail,
                evidence = DecisionEvidence(
                    order = OrderEvidence(
                        clientOrderId = reconciliation.clientOrderId,
                        exchangeOrderId = reconciliation.exchangeOrderId,
                        requestedQuantity = reconciliation.requestedQuantity,
                        filledQuantity = reconciliation.filledQuantity,
                        status = reconciliation.result,
                    ),
                ),
            ),
        )
    }

    override fun recordTimer(
        symbol: String,
        timestamp: Instant,
        publicMarketDataHealthy: Boolean,
        privateStreamHealthy: Boolean,
    ) {
        val evidenceEvent = attemptEvent(
            timestamp = timestamp,
            symbol = symbol,
            eventType = AttemptEvidenceEventType.TIMER,
            timer = TimerEvidence(
                publicMarketDataHealthy = publicMarketDataHealthy,
                privateStreamHealthy = privateStreamHealthy,
            ),
        )
        val targets = recordBufferedEvent(evidenceEvent)
        targets.forEach { levelId ->
            writer.appendAttempt(levelId, evidenceEvent.copy(levelId = levelId))
        }
    }

    override fun recordCommand(
        timestamp: Instant,
        command: CommandEvidence,
    ) {
        val normalizedSymbol = command.symbol?.normalizedSymbol()
        val baseEvent = attemptEvent(
            timestamp = timestamp,
            symbol = normalizedSymbol.orEmpty(),
            eventType = AttemptEvidenceEventType.COMMAND,
            command = command.copy(symbol = normalizedSymbol),
        )
        val targets = lock.withLock {
            closeExpiredLocked(timestamp)
            activeAttempts.values
                .filter { attempt ->
                    normalizedSymbol == null || attempt.symbol == normalizedSymbol
                }
                .map { attempt -> attempt.levelId to attempt.symbol }
        }
        targets.forEach { (levelId, symbol) ->
            writer.appendAttempt(
                levelId,
                baseEvent.copy(levelId = levelId, symbol = symbol),
            )
        }
    }

    override fun completeAttempt(
        levelId: UUID,
        symbol: String,
        completedAt: Instant,
    ) {
        lock.withLock {
            activeAttempts[levelId]?.let { attempt ->
                check(attempt.symbol == symbol) {
                    "Attempt $levelId belongs to ${attempt.symbol}, not $symbol"
                }
                if (attempt.completedAt == null) {
                    attempt.completedAt = completedAt
                    attempt.recordsUntil = completedAt.plus(POST_ATTEMPT_RETENTION)
                }
            }
        }
    }

    override fun discardRollingBuffer(symbol: String) {
        lock.withLock {
            rollingEvents.remove(symbol.normalizedSymbol())
        }
    }

    fun currentSnapshot(): EvidenceSnapshot {
        advance(clock.instant())
        return lock.withLock {
            EvidenceSnapshot(
                applicationStartedAt = applicationRun.startedAt,
                persistentDirectory = writer.directory.toString(),
                auditFile = writer.auditPath.fileName.toString(),
                persistentFilesAuthoritative = true,
                writerHealthy = writer.healthy(),
                lastWriteError = writer.lastError(),
                activeAttemptRecordings = activeAttempts.values.map { attempt ->
                    AttemptRecordingSnapshot(
                        levelId = attempt.levelId,
                        symbol = attempt.symbol,
                        startedAt = attempt.startedAt,
                        completedAt = attempt.completedAt,
                        recordsUntil = attempt.recordsUntil,
                        file = attempt.file,
                    )
                },
                recentAudit = recentAudit.toList(),
                recentTrades = recentTrades.toList(),
            )
        }
    }

    override fun advance(now: Instant) {
        lock.withLock { closeExpiredLocked(now) }
    }

    fun flush(timeout: Duration = properties.shutdownFlushTimeout): Boolean =
        writer.flush(timeout)

    @PreDestroy
    fun close() {
        lock.withLock {
            activeAttempts.clear()
            rollingEvents.clear()
        }
        writer.close(properties.shutdownFlushTimeout)
    }

    internal fun startAttempt(
        levelId: UUID,
        symbol: String,
        startedAt: Instant,
    ) {
        val normalizedSymbol = symbol.normalizedSymbol()
        val initialEvents = lock.withLock {
            if (levelId in activeAttempts) {
                return
            }
            pruneRollingEventsLocked(normalizedSymbol, startedAt)
            val file = writer.attemptFileName(levelId, normalizedSymbol, startedAt)
            activeAttempts[levelId] = ActiveAttemptRecording(
                levelId = levelId,
                symbol = normalizedSymbol,
                startedAt = startedAt,
                file = file,
            )
            rollingEvents[normalizedSymbol]
                .orEmpty()
                .map { event -> event.copy(levelId = levelId) }
        }
        writer.startAttempt(levelId, normalizedSymbol, startedAt, initialEvents)
    }

    private fun recordBufferedEvent(event: AttemptEvidenceEvent): List<UUID> =
        lock.withLock {
            val symbol = event.symbol.normalizedSymbol()
            closeExpiredLocked(event.timestamp)
            val buffer = rollingEvents.getOrPut(symbol, ::ArrayDeque)
            buffer.addLast(event.copy(symbol = symbol))
            pruneRollingEventsLocked(symbol, event.timestamp)
            activeAttempts.values
                .filter { attempt -> attempt.symbol == symbol }
                .map(ActiveAttemptRecording::levelId)
        }

    private fun pruneRollingEventsLocked(symbol: String, now: Instant) {
        val buffer = rollingEvents[symbol] ?: return
        val cutoff = now.minus(RAW_EVENT_RETENTION)
        while (buffer.isNotEmpty() && buffer.first.timestamp.isBefore(cutoff)) {
            buffer.removeFirst()
        }
        while (buffer.size > properties.rawEventLimit) {
            buffer.removeFirst()
        }
    }

    private fun closeExpiredLocked(now: Instant) {
        val expired = activeAttempts.values
            .filter { attempt ->
                attempt.recordsUntil?.let { end -> now.isAfter(end) } == true
            }
            .map(ActiveAttemptRecording::levelId)
        expired.forEach { levelId ->
            activeAttempts.remove(levelId)
            writer.finishAttempt(levelId)
        }
    }

    private fun appendToAttempt(levelId: UUID, event: AttemptEvidenceEvent) {
        val active = lock.withLock {
            closeExpiredLocked(event.timestamp)
            activeAttempts[levelId] != null
        }
        if (active) {
            writer.appendAttempt(levelId, event.copy(levelId = levelId))
        }
    }

    private fun attemptEvent(
        timestamp: Instant,
        symbol: String,
        eventType: AttemptEvidenceEventType,
        exchangeTimestamp: Instant? = null,
        receivedAt: Instant? = null,
        aggregateTrade: AggregateTradeEvent? = null,
        bookTicker: BookTickerEvent? = null,
        privateEvent: BinanceUserDataEvent? = null,
        stateChange: AttemptStateChange? = null,
        orderIntent: OrderEvidence? = null,
        reconciliation: ReconciliationEvidence? = null,
        timer: TimerEvidence? = null,
        command: CommandEvidence? = null,
    ): AttemptEvidenceEvent =
        AttemptEvidenceEvent(
            eventId = UUID.randomUUID(),
            sequence = attemptEventSequence.incrementAndGet(),
            timestamp = timestamp,
            applicationStartedAt = applicationRun.startedAt,
            symbol = symbol.normalizedSymbol(),
            levelId = null,
            eventType = eventType,
            exchangeTimestamp = exchangeTimestamp,
            receivedAt = receivedAt,
            aggregateTrade = aggregateTrade,
            bookTicker = bookTicker,
            privateEvent = privateEvent,
            stateChange = stateChange,
            orderIntent = orderIntent,
            reconciliation = reconciliation,
            timer = timer,
            command = command,
        )

    private fun <T> trimToLimit(values: ArrayDeque<T>, limit: Int) {
        while (values.size > limit) {
            values.removeFirst()
        }
    }
}

private data class ActiveAttemptRecording(
    val levelId: UUID,
    val symbol: String,
    val startedAt: Instant,
    val file: String,
    var completedAt: Instant? = null,
    var recordsUntil: Instant? = null,
)

private fun LevelSnapshot.claimsAttempt(): Boolean =
    ownsActiveAttempt || ownsExposure || hasUnresolvedOrder

private fun String.normalizedSymbol(): String = trim().uppercase()

private val RAW_EVENT_RETENTION: Duration = Duration.ofSeconds(10)
private val POST_ATTEMPT_RETENTION: Duration = Duration.ofSeconds(10)
