package com.scalpsecta.breakoutbot.evidence

import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.level.LevelSnapshot
import com.scalpsecta.breakoutbot.level.LevelState
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import com.scalpsecta.breakoutbot.risk.AttemptRiskPlan
import com.scalpsecta.breakoutbot.signal.LevelSignalSnapshot
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class AuditRecord(
    val eventId: UUID,
    val sequence: Long,
    val timestamp: Instant,
    val applicationStartedAt: Instant,
    val symbol: String,
    val levelId: UUID,
    val stateBefore: LevelState?,
    val stateAfter: LevelState?,
    val eventType: AuditEventType,
    val decision: String,
    val blockerReasons: List<String>,
    val recoveryDetail: String?,
    val exception: ExceptionEvidence?,
    val evidence: DecisionEvidence?,
)

data class AuditRecordDraft(
    val timestamp: Instant,
    val symbol: String,
    val levelId: UUID,
    val stateBefore: LevelState?,
    val stateAfter: LevelState?,
    val eventType: AuditEventType,
    val decision: String,
    val blockerReasons: List<String> = emptyList(),
    val recoveryDetail: String? = null,
    val exception: ExceptionEvidence? = null,
    val evidence: DecisionEvidence? = null,
)

enum class AuditEventType {
    LEVEL_CREATED,
    LEVEL_DELETED,
    STATE_TRANSITION,
    DECISION,
    OWNERSHIP_CHANGED,
    ORDER_INTENT,
    RECONCILIATION,
    RISK_UPDATED,
    RECOVERY,
    EXCEPTION,
}

data class DecisionEvidence(
    val marketMetrics: LevelSignalSnapshot? = null,
    val marketData: PublicMarketDataSnapshot? = null,
    val prices: PriceEvidence? = null,
    val quantity: QuantityEvidence? = null,
    val order: OrderEvidence? = null,
    val risk: RiskEvidence? = null,
    val pnl: PnlEvidence? = null,
    val exit: ExitEvidence? = null,
)

data class PriceEvidence(
    val levelPrice: BigDecimal? = null,
    val latestTradePrice: BigDecimal? = null,
    val bidPrice: BigDecimal? = null,
    val askPrice: BigDecimal? = null,
    val spread: BigDecimal? = null,
    val distanceToLevel: BigDecimal? = null,
    val npu: BigDecimal? = null,
    val stopPrice: BigDecimal? = null,
    val takeProfitPrices: List<BigDecimal> = emptyList(),
)

data class QuantityEvidence(
    val plannedQuantity: BigDecimal? = null,
    val requestedQuantity: BigDecimal? = null,
    val filledQuantity: BigDecimal? = null,
    val remainingQuantity: BigDecimal? = null,
)

data class OrderEvidence(
    val intentId: String? = null,
    val clientOrderId: String? = null,
    val exchangeOrderId: Long? = null,
    val role: String? = null,
    val side: String? = null,
    val type: String? = null,
    val timeInForce: String? = null,
    val requestedPrice: BigDecimal? = null,
    val requestedQuantity: BigDecimal? = null,
    val filledQuantity: BigDecimal? = null,
    val averageFilledPrice: BigDecimal? = null,
    val stopPrice: BigDecimal? = null,
    val takeProfitPrices: List<BigDecimal> = emptyList(),
    val status: String? = null,
    val reduceOnly: Boolean? = null,
)

data class RiskEvidence(
    val plan: AttemptRiskPlan? = null,
    val reservedRisk: BigDecimal? = null,
    val releasedRisk: BigDecimal? = null,
    val remainingReservedRisk: BigDecimal? = null,
)

data class PnlEvidence(
    val grossPnl: BigDecimal? = null,
    val fees: BigDecimal? = null,
    val funding: BigDecimal? = null,
    val slippage: BigDecimal? = null,
    val netPnl: BigDecimal? = null,
)

data class ExitEvidence(
    val exitScore: BigDecimal? = null,
    val activePointReasons: List<String> = emptyList(),
)

data class ExceptionEvidence(
    val type: String,
    val safeDetail: String,
)

data class AttemptEvidenceEvent(
    val eventId: UUID,
    val sequence: Long,
    val timestamp: Instant,
    val applicationStartedAt: Instant,
    val symbol: String,
    val levelId: UUID?,
    val eventType: AttemptEvidenceEventType,
    val exchangeTimestamp: Instant?,
    val receivedAt: Instant?,
    val aggregateTrade: AggregateTradeEvent? = null,
    val bookTicker: BookTickerEvent? = null,
    val privateEvent: BinanceUserDataEvent? = null,
    val stateChange: AttemptStateChange? = null,
    val orderIntent: OrderEvidence? = null,
    val reconciliation: ReconciliationEvidence? = null,
)

enum class AttemptEvidenceEventType {
    AGGREGATE_TRADE,
    BOOK_TICKER,
    STATE_TRANSITION,
    ORDER_INTENT,
    PRIVATE_ORDER,
    PRIVATE_ACCOUNT,
    PRIVATE_LISTEN_KEY_EXPIRED,
    RECONCILIATION,
}

data class AttemptStateChange(
    val stateBefore: LevelState?,
    val stateAfter: LevelState?,
    val decision: String,
    val blockerReasons: List<String>,
)

data class ReconciliationEvidence(
    val clientOrderId: String?,
    val attemptNumber: Int,
    val result: String,
    val exchangeOrderId: Long? = null,
    val requestedQuantity: BigDecimal? = null,
    val filledQuantity: BigDecimal? = null,
    val safeDetail: String? = null,
)

data class EvidenceSnapshot(
    val applicationStartedAt: Instant,
    val persistentDirectory: String,
    val auditFile: String,
    val persistentFilesAuthoritative: Boolean,
    val writerHealthy: Boolean,
    val lastWriteError: String?,
    val activeAttemptRecordings: List<AttemptRecordingSnapshot>,
    val recentAudit: List<RecentAuditSummary>,
    val recentTrades: List<RecentTradeSummary>,
)

data class AttemptRecordingSnapshot(
    val levelId: UUID,
    val symbol: String,
    val startedAt: Instant,
    val completedAt: Instant?,
    val recordsUntil: Instant?,
    val file: String,
)

data class RecentAuditSummary(
    val eventId: UUID,
    val timestamp: Instant,
    val symbol: String,
    val levelId: UUID,
    val eventType: AuditEventType,
    val stateBefore: LevelState?,
    val stateAfter: LevelState?,
    val decision: String,
    val blockerReasons: List<String>,
)

data class RecentTradeSummary(
    val eventId: UUID,
    val timestamp: Instant,
    val symbol: String,
    val aggregateTradeId: Long,
    val price: BigDecimal,
    val quantity: BigDecimal,
)

internal fun LevelSnapshot.decisionEvidence(
    marketData: PublicMarketDataSnapshot?,
): DecisionEvidence =
    DecisionEvidence(
        marketMetrics = signal,
        marketData = marketData,
        prices = PriceEvidence(
            levelPrice = normalizedLevelPrice,
            latestTradePrice = signal.latestTradePrice,
            bidPrice = signal.bidPrice,
            askPrice = signal.askPrice,
            spread = signal.spread,
            distanceToLevel = signal.distanceToLevel,
            npu = signal.npu.absolute,
            stopPrice = riskBoundaryStopPrice,
        ),
        quantity = QuantityEvidence(plannedQuantity = plannedQuantity),
    )
