package com.scalpsecta.breakoutbot.replay

import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.evidence.AttemptStateChange
import com.scalpsecta.breakoutbot.evidence.AuditRecordDraft
import com.scalpsecta.breakoutbot.evidence.CommandEvidence
import com.scalpsecta.breakoutbot.evidence.DecisionEvidence
import com.scalpsecta.breakoutbot.evidence.EvidenceRecorder
import com.scalpsecta.breakoutbot.evidence.ExceptionEvidence
import com.scalpsecta.breakoutbot.evidence.NoOpEvidenceRecorder
import com.scalpsecta.breakoutbot.evidence.OrderEvidence
import com.scalpsecta.breakoutbot.evidence.ReconciliationEvidence
import com.scalpsecta.breakoutbot.evidence.TimerEvidence
import com.scalpsecta.breakoutbot.level.LevelService
import com.scalpsecta.breakoutbot.level.LevelSnapshot
import com.scalpsecta.breakoutbot.level.LevelState
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Test-only adapter into the same ordered level and timer methods used by the
 * live runtime. Execution reconciliation and operator commands stay explicit
 * callbacks because their production services require scenario-specific state.
 */
class ProductionReplayDecisionBoundary(
    private val levelService: LevelService,
    private val exchange: ScriptedFakeExchange,
    private val traceRecorder: ReplayTraceEvidenceRecorder,
    private val onRecordedReconciliation:
        (String, ReconciliationEvidence) -> Unit,
    private val onRecordedCommand: (CommandEvidence) -> Unit,
) : ReplayDecisionBoundary {
    private val publicHealth = mutableMapOf<String, Boolean>()

    override fun beginReplay() {
        traceRecorder.reset()
    }

    override fun onAggregateTrade(event: AggregateTradeEvent) {
        levelService.process(
            event = event,
            marketHealthy = publicHealth.getOrDefault(event.symbol, true),
        ).block(PROCESSING_TIMEOUT)
    }

    override fun onBookTicker(event: BookTickerEvent) {
        levelService.process(
            event = event,
            marketHealthy = publicHealth.getOrDefault(event.symbol, true),
        ).block(PROCESSING_TIMEOUT)
    }

    override fun onPrivateEvent(event: BinanceUserDataEvent) {
        exchange.emit(event)
    }

    override fun onTimer(symbol: String, timer: TimerEvidence) {
        publicHealth[symbol] = timer.publicMarketDataHealthy
        exchange.setPublicStreamConnected(
            symbol,
            timer.publicMarketDataHealthy,
        )
        exchange.setPrivateStreamConnected(timer.privateStreamHealthy)
        levelService.processTimer(
            symbol = symbol,
            marketHealthy = timer.publicMarketDataHealthy,
        ).block(PROCESSING_TIMEOUT)
    }

    override fun onReconciliation(
        symbol: String,
        reconciliation: ReconciliationEvidence,
    ) {
        onRecordedReconciliation(symbol, reconciliation)
    }

    override fun onCommand(command: CommandEvidence) {
        onRecordedCommand(command)
    }

    override fun trace(): ReplayDecisionTrace = traceRecorder.trace()
}

class ReplayTraceEvidenceRecorder :
    EvidenceRecorder by NoOpEvidenceRecorder {
    private val lock = ReentrantLock()
    private val transitions = mutableListOf<AttemptStateChange>()
    private val decisions = mutableListOf<String>()
    private val orderIntents = mutableListOf<OrderEvidence>()
    private val reasons = mutableListOf<String>()
    private val audits = mutableListOf<ReplayAuditResult>()

    override fun recordAudit(draft: AuditRecordDraft) {
        lock.withLock {
            recordDecision(
                timestamp = draft.timestamp,
                symbol = draft.symbol,
                eventType = draft.eventType.name,
                decision = draft.decision,
                reasonCodes = draft.blockerReasons,
                stateBefore = draft.stateBefore,
                stateAfter = draft.stateAfter,
                recoveryDetail = draft.recoveryDetail,
                exception = draft.exception,
                evidence = draft.evidence,
            )
        }
    }

    override fun recordStateTransition(
        before: LevelSnapshot,
        after: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        decision: String,
    ) {
        val reasonCodes = (
            after.blockers.map(Enum<*>::name) +
                after.signal.mandatoryGates.blockerReasons.map(Enum<*>::name)
            ).distinct()
        lock.withLock {
            transitions += AttemptStateChange(
                stateBefore = before.state,
                stateAfter = after.state,
                decision = decision,
                blockerReasons = reasonCodes,
            )
            recordDecision(
                timestamp = after.stateChangedAt,
                symbol = after.symbol,
                eventType = "STATE_TRANSITION",
                decision = decision,
                reasonCodes = reasonCodes,
                stateBefore = before.state,
                stateAfter = after.state,
            )
        }
    }

    override fun recordOrderIntent(
        levelId: UUID,
        symbol: String,
        timestamp: Instant,
        order: OrderEvidence,
    ) {
        lock.withLock {
            orderIntents += order
            recordDecision(
                timestamp = timestamp,
                symbol = symbol,
                eventType = "ORDER_INTENT",
                decision = "ORDER_INTENT_RECORDED",
                reasonCodes = emptyList(),
            )
        }
    }

    fun trace(): ReplayDecisionTrace = lock.withLock {
        ReplayDecisionTrace(
            stateTransitions = transitions.toList(),
            decisions = decisions.toList(),
            orderIntents = orderIntents.toList(),
            reasonCodes = reasons.toList(),
            auditResults = audits.toList(),
        )
    }

    fun reset() {
        lock.withLock {
            transitions.clear()
            decisions.clear()
            orderIntents.clear()
            reasons.clear()
            audits.clear()
        }
    }

    private fun recordDecision(
        timestamp: Instant,
        symbol: String,
        eventType: String,
        decision: String,
        reasonCodes: List<String>,
        stateBefore: LevelState? = null,
        stateAfter: LevelState? = null,
        recoveryDetail: String? = null,
        exception: ExceptionEvidence? = null,
        evidence: DecisionEvidence? = null,
    ) {
        decisions += decision
        reasons += reasonCodes
        audits += ReplayAuditResult(
            timestamp = timestamp,
            symbol = symbol,
            eventType = eventType,
            decision = decision,
            reasonCodes = reasonCodes,
            stateBefore = stateBefore,
            stateAfter = stateAfter,
            recoveryDetail = recoveryDetail,
            exception = exception,
            evidence = evidence,
        )
    }
}

private val PROCESSING_TIMEOUT: Duration = Duration.ofSeconds(5)
