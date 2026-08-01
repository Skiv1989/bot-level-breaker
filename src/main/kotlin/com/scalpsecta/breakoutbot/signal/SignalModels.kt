package com.scalpsecta.breakoutbot.signal

import java.math.BigDecimal
import java.time.Instant

data class LevelSignalSnapshot(
    val observedAt: Instant,
    val latestTradePrice: BigDecimal?,
    val bidPrice: BigDecimal?,
    val askPrice: BigDecimal?,
    val midPrice: BigDecimal?,
    val spread: BigDecimal?,
    val distanceToLevel: BigDecimal?,
    val npu: NpuSnapshot,
    val windows: MetricWindowsSnapshot,
    val acceleration: AccelerationSnapshot,
    val ramp: RampSnapshot,
    val burst: BurstSnapshot,
    val pressureScore: PressureScoreSnapshot,
    val mandatoryGates: MandatoryGatesSnapshot,
)

data class NpuSnapshot(
    val absolute: BigDecimal?,
    val percentage: BigDecimal?,
    val frozen: Boolean,
    val lastRecomputedAt: Instant?,
    val retainedSampleCount: Int,
)

data class MetricWindowsSnapshot(
    val fast: WindowMetricsSnapshot,
    val mid: WindowMetricsSnapshot,
    val slow: WindowMetricsSnapshot,
)

data class WindowMetricsSnapshot(
    val durationMillis: Long,
    val tradeCount: Int,
    val aggressiveVolume: BigDecimal,
    val tradesPerSecond: BigDecimal,
    val volumeRate: BigDecimal,
    val averageTradeSize: BigDecimal,
    val buyShare: BigDecimal,
    val sellShare: BigDecimal,
    val deltaRate: BigDecimal,
    val averageBuySize: BigDecimal,
    val averageSellSize: BigDecimal,
    val signedPriceProgress: BigDecimal,
    val adversePullback: BigDecimal,
    val flowEfficiency: BigDecimal,
)

data class AccelerationSnapshot(
    val tradesPerSecondRatio: BigDecimal,
    val volumeRateRatio: BigDecimal,
    val averageTradeSizeRatio: BigDecimal,
)

data class RampSnapshot(
    val bins: List<RampBinSnapshot>,
    val nonNegativeChangeCount: Int,
    val finalToInitialActivityRatio: BigDecimal,
    val latestToStrongestActivityRatio: BigDecimal,
    val score: BigDecimal,
)

data class RampBinSnapshot(
    val index: Int,
    val tradeCount: Int,
    val tradesPerSecond: BigDecimal,
    val aggressiveQuoteVolumeRate: BigDecimal,
    val normalizedTradesPerSecond: BigDecimal,
    val normalizedQuoteVolumeRate: BigDecimal,
    val activity: BigDecimal,
)

data class BurstSnapshot(
    val status: BurstStatus,
    val aggregateTradeId: Long?,
    val largestFastTradeShare: BigDecimal,
    val followingActivityRatio: BigDecimal?,
    val postBurstSignedProgress: BigDecimal?,
    val detectedAt: Instant?,
    val expiresAt: Instant?,
)

enum class BurstStatus {
    NONE,
    UNRESOLVED,
    ACTIVE,
    CLEARED,
}

data class PressureScoreSnapshot(
    val value: BigDecimal,
    val diagnosticOnly: Boolean,
    val components: List<PressureScoreComponentSnapshot>,
)

data class PressureScoreComponentSnapshot(
    val component: PressureScoreComponent,
    val rawValue: BigDecimal,
    val normalizationTarget: BigDecimal?,
    val normalizedScore: BigDecimal,
    val weight: BigDecimal,
    val contribution: BigDecimal,
)

enum class PressureScoreComponent {
    ACCEL_TRADES_PER_SECOND,
    ACCEL_VOLUME,
    SIZE_RATIO,
    DIRECTIONAL_SHARE,
    RAMP,
    FLOW_EFFICIENCY,
}

data class MandatoryGatesSnapshot(
    val entryEligible: Boolean,
    val blockerReasons: List<SignalBlockerReason>,
    val gates: List<SignalGateResult>,
    val actualSpread: BigDecimal?,
    val npuSpreadLimit: BigDecimal?,
    val percentageSpreadLimit: BigDecimal?,
    val effectiveSpreadLimit: BigDecimal?,
    val requiredMarketEventAgeMillis: Long?,
)

data class SignalGateResult(
    val gate: SignalGate,
    val passed: Boolean,
    val blockerReasons: List<SignalBlockerReason>,
)

enum class SignalGate {
    ACCELERATION,
    DIRECTIONAL_FLOW,
    RAMP,
    ONE_SHOT_BURST,
    PRICE_RESPONSE,
    LATENCY,
    SPREAD,
    DATA_INTEGRITY,
}

enum class SignalBlockerReason {
    ACCEL_TRADES_PER_SECOND_BELOW_MINIMUM,
    ACCEL_VOLUME_BELOW_MINIMUM,
    ACCEL_STRONG_IMPULSE_MISSING,
    DIRECTIONAL_SHARE_BELOW_MINIMUM,
    FAST_DELTA_NOT_DIRECTIONAL,
    MID_DELTA_NOT_DIRECTIONAL,
    RAMP_ACTIVITY_MISSING,
    RAMP_NON_NEGATIVE_CHANGES_BELOW_MINIMUM,
    RAMP_FINAL_ACTIVITY_BELOW_MINIMUM,
    RAMP_LATEST_ACTIVITY_BELOW_MINIMUM,
    ONE_SHOT_BURST_UNRESOLVED,
    ONE_SHOT_BURST_ACTIVE,
    NPU_UNAVAILABLE,
    MID_PROGRESS_BELOW_MINIMUM,
    MID_PULLBACK_ABOVE_MAXIMUM,
    REQUIRED_MARKET_EVENT_MISSING,
    REQUIRED_MARKET_EVENT_TOO_OLD,
    SPREAD_ABOVE_NPU,
    SPREAD_ABOVE_PERCENTAGE_LIMIT,
    PUBLIC_STREAM_DISCONNECTED,
    AGGREGATE_TRADE_GAP_UNRESOLVED,
    BID_ASK_MISSING,
    BID_ASK_INVALID,
    BID_ASK_STALE,
    PRIVATE_ORDER_OUTCOME_UNRESOLVED,
    PRIVATE_STREAM_UNHEALTHY,
}

enum class NpuMode {
    WARMING_UP,
    ARMED,
    FROZEN,
}
