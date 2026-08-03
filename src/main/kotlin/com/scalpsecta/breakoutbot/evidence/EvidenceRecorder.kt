package com.scalpsecta.breakoutbot.evidence

import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.level.LevelSnapshot
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import java.time.Instant
import java.util.UUID

interface EvidenceRecorder {
    fun recordAudit(draft: AuditRecordDraft)

    fun recordLevelCreated(
        level: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
    )

    fun recordLevelDeleted(
        level: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        deletedAt: Instant,
    )

    fun recordStateTransition(
        before: LevelSnapshot,
        after: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        decision: String,
    )

    fun recordOwnershipChange(
        before: LevelSnapshot,
        after: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        changedAt: Instant,
    )

    fun record(event: AggregateTradeEvent)

    fun record(event: BookTickerEvent)

    fun record(event: BinanceUserDataEvent)

    fun recordOrderIntent(
        levelId: UUID,
        symbol: String,
        timestamp: Instant,
        order: OrderEvidence,
    )

    fun recordReconciliation(
        levelId: UUID,
        symbol: String,
        timestamp: Instant,
        reconciliation: ReconciliationEvidence,
    )

    fun recordTimer(
        symbol: String,
        timestamp: Instant,
        publicMarketDataHealthy: Boolean,
        privateStreamHealthy: Boolean,
    ) = Unit

    fun recordCommand(
        timestamp: Instant,
        command: CommandEvidence,
    ) = Unit

    fun completeAttempt(levelId: UUID, symbol: String, completedAt: Instant)

    fun discardRollingBuffer(symbol: String)

    fun advance(now: Instant)
}

object NoOpEvidenceRecorder : EvidenceRecorder {
    override fun recordAudit(draft: AuditRecordDraft) = Unit

    override fun recordLevelCreated(
        level: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
    ) = Unit

    override fun recordLevelDeleted(
        level: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        deletedAt: Instant,
    ) = Unit

    override fun recordStateTransition(
        before: LevelSnapshot,
        after: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        decision: String,
    ) = Unit

    override fun recordOwnershipChange(
        before: LevelSnapshot,
        after: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        changedAt: Instant,
    ) = Unit

    override fun record(event: AggregateTradeEvent) = Unit

    override fun record(event: BookTickerEvent) = Unit

    override fun record(event: BinanceUserDataEvent) = Unit

    override fun recordOrderIntent(
        levelId: UUID,
        symbol: String,
        timestamp: Instant,
        order: OrderEvidence,
    ) = Unit

    override fun recordReconciliation(
        levelId: UUID,
        symbol: String,
        timestamp: Instant,
        reconciliation: ReconciliationEvidence,
    ) = Unit

    override fun recordTimer(
        symbol: String,
        timestamp: Instant,
        publicMarketDataHealthy: Boolean,
        privateStreamHealthy: Boolean,
    ) = Unit

    override fun recordCommand(
        timestamp: Instant,
        command: CommandEvidence,
    ) = Unit

    override fun completeAttempt(
        levelId: UUID,
        symbol: String,
        completedAt: Instant,
    ) = Unit

    override fun discardRollingBuffer(symbol: String) = Unit

    override fun advance(now: Instant) = Unit
}
