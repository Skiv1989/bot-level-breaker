package com.scalpsecta.breakoutbot.replay

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.evidence.AttemptEvidenceEvent
import com.scalpsecta.breakoutbot.evidence.AttemptEvidenceEventType
import com.scalpsecta.breakoutbot.evidence.AttemptStateChange
import com.scalpsecta.breakoutbot.evidence.CommandEvidence
import com.scalpsecta.breakoutbot.evidence.DecisionEvidence
import com.scalpsecta.breakoutbot.evidence.ExceptionEvidence
import com.scalpsecta.breakoutbot.evidence.OrderEvidence
import com.scalpsecta.breakoutbot.evidence.ReconciliationEvidence
import com.scalpsecta.breakoutbot.evidence.TimerEvidence
import com.scalpsecta.breakoutbot.level.LevelState
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.UUID
import java.util.zip.GZIPInputStream

data class RecordedAttemptArtifact(
    val source: Path,
    val events: List<AttemptEvidenceEvent>,
)

data class ReplayInputRecord(
    val sequence: Long,
    val eventType: AttemptEvidenceEventType,
    val replayedAt: Instant,
    val exchangeTimestamp: Instant?,
    val receivedAt: Instant?,
)

data class ReplayAuditResult(
    val timestamp: Instant,
    val symbol: String,
    val eventType: String,
    val decision: String,
    val reasonCodes: List<String> = emptyList(),
    val stateBefore: LevelState? = null,
    val stateAfter: LevelState? = null,
    val recoveryDetail: String? = null,
    val exception: ExceptionEvidence? = null,
    val evidence: DecisionEvidence? = null,
)

data class ReplayDecisionTrace(
    val stateTransitions: List<AttemptStateChange> = emptyList(),
    val decisions: List<String> = emptyList(),
    val orderIntents: List<OrderEvidence> = emptyList(),
    val reasonCodes: List<String> = emptyList(),
    val auditResults: List<ReplayAuditResult> = emptyList(),
)

data class ReplayRun(
    val inputs: List<ReplayInputRecord>,
    val recordedOutputs: ReplayDecisionTrace,
    val actualOutputs: ReplayDecisionTrace,
)

interface ReplayDecisionBoundary : AutoCloseable {
    fun beginReplay() = Unit

    fun onAggregateTrade(event: AggregateTradeEvent)

    fun onBookTicker(event: BookTickerEvent)

    fun onPrivateEvent(event: BinanceUserDataEvent)

    fun onTimer(symbol: String, timer: TimerEvidence)

    fun onReconciliation(
        symbol: String,
        reconciliation: ReconciliationEvidence,
    )

    fun onCommand(command: CommandEvidence)

    fun trace(): ReplayDecisionTrace

    override fun close() = Unit
}

fun interface ReplayDecisionBoundaryFactory {
    fun create(clock: ReplayVirtualClock): ReplayDecisionBoundary
}

class RecordedAttemptReplayer(
    private val objectMapper: ObjectMapper,
) {
    fun read(path: Path): RecordedAttemptArtifact {
        require(Files.isRegularFile(path)) {
            "Recorded attempt does not exist: $path"
        }
        require(path.fileName.toString().endsWith(".jsonl.gz")) {
            "Recorded attempt must be a .jsonl.gz artifact: $path"
        }
        val events = GZIPInputStream(Files.newInputStream(path))
            .bufferedReader()
            .useLines { lines ->
                lines
                    .filter(String::isNotBlank)
                    .mapIndexed { index, line -> parseEvent(line, index + 1) }
                    .toList()
            }
        require(events.isNotEmpty()) {
            "Recorded attempt is empty: $path"
        }
        val applicationStartedAt = events.first().applicationStartedAt
        events.forEach { event ->
            require(event.applicationStartedAt == applicationStartedAt) {
                "Recorded attempt mixes application runs"
            }
            require(!event.timestamp.isBefore(applicationStartedAt)) {
                "Recorded event predates application start: ${event.timestamp}"
            }
        }
        events.zipWithNext().forEach { (before, after) ->
            require(after.sequence > before.sequence) {
                "Recorded attempt sequence must increase strictly: " +
                    "${before.sequence}, ${after.sequence}"
            }
            require(!after.timestamp.isBefore(before.timestamp)) {
                "Recorded attempt timestamps must not move backwards: " +
                    "${before.timestamp}, ${after.timestamp}"
            }
        }
        return RecordedAttemptArtifact(path.toAbsolutePath().normalize(), events)
    }

    fun replay(
        artifact: RecordedAttemptArtifact,
        boundaryFactory: ReplayDecisionBoundaryFactory,
    ): ReplayRun {
        val clock = ReplayVirtualClock(
            artifact.events.first().applicationStartedAt,
        )
        val boundary = try {
            boundaryFactory.create(clock)
        } catch (error: Exception) {
            clock.close()
            throw error
        }
        return try {
            boundary.beginReplay()
            val inputs = buildList {
                artifact.events.forEach { event ->
                    clock.advanceTo(event.timestamp)
                    if (dispatch(event, boundary)) {
                        add(
                            ReplayInputRecord(
                                sequence = event.sequence,
                                eventType = event.eventType,
                                replayedAt = clock.instant(),
                                exchangeTimestamp = event.exchangeTimestamp,
                                receivedAt = event.receivedAt,
                            ),
                        )
                    }
                }
            }
            ReplayRun(
                inputs = inputs,
                recordedOutputs = recordedOutputs(artifact.events),
                actualOutputs = boundary.trace(),
            )
        } finally {
            try {
                boundary.close()
            } finally {
                clock.close()
            }
        }
    }

    fun replay(
        path: Path,
        boundaryFactory: ReplayDecisionBoundaryFactory,
    ): ReplayRun = replay(read(path), boundaryFactory)

    fun requireDeterministic(
        artifact: RecordedAttemptArtifact,
        boundaryFactory: ReplayDecisionBoundaryFactory,
    ): ReplayRun {
        val first = replay(artifact, boundaryFactory)
        val second = replay(artifact, boundaryFactory)
        check(first == second) {
            "Recorded attempt replay produced different inputs or decisions"
        }
        return first
    }

    fun requireDeterministic(
        path: Path,
        boundaryFactory: ReplayDecisionBoundaryFactory,
    ): ReplayRun = requireDeterministic(read(path), boundaryFactory)

    fun requireMatchesRecording(
        artifact: RecordedAttemptArtifact,
        boundaryFactory: ReplayDecisionBoundaryFactory,
    ): ReplayRun {
        val run = requireDeterministic(artifact, boundaryFactory)
        check(
            run.actualOutputs.stateTransitions ==
                run.recordedOutputs.stateTransitions,
        ) {
            "Replay state transitions differ from the recorded attempt"
        }
        check(run.actualOutputs.orderIntents == run.recordedOutputs.orderIntents) {
            "Replay order intents differ from the recorded attempt"
        }
        check(run.actualOutputs.reasonCodes == run.recordedOutputs.reasonCodes) {
            "Replay reason codes differ from the recorded attempt"
        }
        return run
    }

    fun requireMatchesRecording(
        path: Path,
        boundaryFactory: ReplayDecisionBoundaryFactory,
    ): ReplayRun = requireMatchesRecording(read(path), boundaryFactory)

    private fun dispatch(
        event: AttemptEvidenceEvent,
        boundary: ReplayDecisionBoundary,
    ): Boolean = when (event.eventType) {
        AttemptEvidenceEventType.AGGREGATE_TRADE -> {
            boundary.onAggregateTrade(requireNotNull(event.aggregateTrade))
            true
        }

        AttemptEvidenceEventType.BOOK_TICKER -> {
            boundary.onBookTicker(requireNotNull(event.bookTicker))
            true
        }

        AttemptEvidenceEventType.PRIVATE_ORDER,
        AttemptEvidenceEventType.PRIVATE_ACCOUNT,
        AttemptEvidenceEventType.PRIVATE_LISTEN_KEY_EXPIRED,
        -> {
            boundary.onPrivateEvent(requireNotNull(event.privateEvent))
            true
        }

        AttemptEvidenceEventType.TIMER -> {
            boundary.onTimer(event.symbol, requireNotNull(event.timer))
            true
        }

        AttemptEvidenceEventType.RECONCILIATION -> {
            boundary.onReconciliation(
                event.symbol,
                requireNotNull(event.reconciliation),
            )
            true
        }

        AttemptEvidenceEventType.COMMAND -> {
            boundary.onCommand(requireNotNull(event.command))
            true
        }

        AttemptEvidenceEventType.STATE_TRANSITION,
        AttemptEvidenceEventType.ORDER_INTENT,
        -> false
    }

    private fun recordedOutputs(
        events: List<AttemptEvidenceEvent>,
    ): ReplayDecisionTrace {
        val transitions = events.mapNotNull(AttemptEvidenceEvent::stateChange)
        return ReplayDecisionTrace(
            stateTransitions = transitions,
            decisions = transitions.map(AttemptStateChange::decision),
            orderIntents = events.mapNotNull(AttemptEvidenceEvent::orderIntent),
            reasonCodes = transitions.flatMap(AttemptStateChange::blockerReasons),
        )
    }

    private fun parseEvent(line: String, lineNumber: Int): AttemptEvidenceEvent {
        val root = try {
            objectMapper.readTree(line)
        } catch (error: Exception) {
            throw IllegalArgumentException(
                "Recorded attempt line $lineNumber is not valid JSON",
                error,
            )
        }
        val eventType = root.requiredText("eventType").let(
            AttemptEvidenceEventType::valueOf,
        )
        return AttemptEvidenceEvent(
            eventId = UUID.fromString(root.requiredText("eventId")),
            sequence = root.requiredLong("sequence"),
            timestamp = root.requiredInstant("timestamp"),
            applicationStartedAt = root.requiredInstant("applicationStartedAt"),
            symbol = root.requiredText("symbol"),
            levelId = root.optionalText("levelId")?.let(UUID::fromString),
            eventType = eventType,
            exchangeTimestamp = root.optionalInstant("exchangeTimestamp"),
            receivedAt = root.optionalInstant("receivedAt"),
            aggregateTrade = root.value("aggregateTrade"),
            bookTicker = root.value("bookTicker"),
            privateEvent = root.privateEvent(eventType),
            stateChange = root.value("stateChange"),
            orderIntent = root.value("orderIntent"),
            reconciliation = root.value("reconciliation"),
            timer = root.value("timer"),
            command = root.value("command"),
        )
    }

    private inline fun <reified T> JsonNode.value(name: String): T? =
        get(name)
            ?.takeUnless(JsonNode::isNull)
            ?.let { value -> objectMapper.treeToValue(value, T::class.java) }

    private fun JsonNode.privateEvent(
        eventType: AttemptEvidenceEventType,
    ): BinanceUserDataEvent? {
        val value = get("privateEvent")?.takeUnless(JsonNode::isNull) ?: return null
        val type = when (eventType) {
            AttemptEvidenceEventType.PRIVATE_ACCOUNT ->
                BinanceUserDataEvent.AccountUpdate::class.java

            AttemptEvidenceEventType.PRIVATE_ORDER ->
                BinanceUserDataEvent.OrderUpdate::class.java

            AttemptEvidenceEventType.PRIVATE_LISTEN_KEY_EXPIRED ->
                BinanceUserDataEvent.ListenKeyExpired::class.java

            else -> return null
        }
        return objectMapper.treeToValue(value, type)
    }
}

private fun JsonNode.requiredText(name: String): String =
    requireNotNull(get(name)?.takeUnless(JsonNode::isNull)?.asText()) {
        "Recorded attempt omitted required field $name"
    }

private fun JsonNode.optionalText(name: String): String? =
    get(name)?.takeUnless(JsonNode::isNull)?.asText()

private fun JsonNode.requiredLong(name: String): Long =
    requireNotNull(get(name)?.takeUnless(JsonNode::isNull)?.asLong()) {
        "Recorded attempt omitted required field $name"
    }

private fun JsonNode.requiredInstant(name: String): Instant =
    Instant.parse(requiredText(name))

private fun JsonNode.optionalInstant(name: String): Instant? =
    optionalText(name)?.let(Instant::parse)
