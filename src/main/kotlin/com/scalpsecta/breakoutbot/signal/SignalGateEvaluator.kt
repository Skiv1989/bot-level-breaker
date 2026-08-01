package com.scalpsecta.breakoutbot.signal

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeGapStatus
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import com.scalpsecta.breakoutbot.marketdata.PublicStreamConnectionState
import java.math.BigDecimal
import java.math.MathContext

internal object SignalGateEvaluator {
    fun evaluate(
        direction: LevelDirection,
        windows: MetricWindowsSnapshot,
        acceleration: AccelerationSnapshot,
        ramp: RampSnapshot,
        burst: BurstSnapshot,
        npu: BigDecimal?,
        publicMarketData: PublicMarketDataSnapshot?,
        privateStreamReadiness: BinanceReadiness,
        hasUnresolvedOrder: Boolean,
    ): MandatoryGatesSnapshot {
        val accelerationGate = accelerationGate(acceleration)
        val directionalFlowGate = directionalFlowGate(
            direction = direction,
            fast = windows.fast,
            mid = windows.mid,
        )
        val rampGate = rampGate(ramp)
        val burstGate = burstGate(burst)
        val priceResponseGate = priceResponseGate(
            mid = windows.mid,
            npu = npu,
        )
        val latencyGate = latencyGate(publicMarketData)
        val spreadEvaluation = spreadGate(
            publicMarketData = publicMarketData,
            npu = npu,
        )
        val dataIntegrityGate = dataIntegrityGate(
            publicMarketData = publicMarketData,
            privateStreamReadiness = privateStreamReadiness,
            hasUnresolvedOrder = hasUnresolvedOrder,
        )
        val gates = listOf(
            accelerationGate,
            directionalFlowGate,
            rampGate,
            burstGate,
            priceResponseGate,
            latencyGate.result,
            spreadEvaluation.result,
            dataIntegrityGate,
        )

        return MandatoryGatesSnapshot(
            entryEligible = gates.all(SignalGateResult::passed),
            blockerReasons = gates
                .flatMap(SignalGateResult::blockerReasons)
                .distinct(),
            gates = gates,
            actualSpread = spreadEvaluation.actualSpread,
            npuSpreadLimit = npu,
            percentageSpreadLimit = spreadEvaluation.percentageLimit,
            effectiveSpreadLimit = spreadEvaluation.effectiveLimit,
            requiredMarketEventAgeMillis = latencyGate.eventAgeMillis,
        )
    }

    internal fun accelerationGate(
        acceleration: AccelerationSnapshot,
    ): SignalGateResult {
        val reasons = buildList {
            if (acceleration.tradesPerSecondRatio < ACCEL_MINIMUM) {
                add(SignalBlockerReason.ACCEL_TRADES_PER_SECOND_BELOW_MINIMUM)
            }
            if (acceleration.volumeRateRatio < ACCEL_MINIMUM) {
                add(SignalBlockerReason.ACCEL_VOLUME_BELOW_MINIMUM)
            }
            if (
                acceleration.tradesPerSecondRatio < ACCEL_STRONG_MINIMUM &&
                acceleration.volumeRateRatio < ACCEL_STRONG_MINIMUM
            ) {
                add(SignalBlockerReason.ACCEL_STRONG_IMPULSE_MISSING)
            }
        }
        return gate(SignalGate.ACCELERATION, reasons)
    }

    internal fun directionalFlowGate(
        direction: LevelDirection,
        fast: WindowMetricsSnapshot,
        mid: WindowMetricsSnapshot,
    ): SignalGateResult {
        val directionalShare = when (direction) {
            LevelDirection.LONG -> fast.buyShare
            LevelDirection.SHORT -> fast.sellShare
        }
        val fastDeltaDirectional = when (direction) {
            LevelDirection.LONG -> fast.deltaRate.signum() > 0
            LevelDirection.SHORT -> fast.deltaRate.signum() < 0
        }
        val midDeltaDirectional = when (direction) {
            LevelDirection.LONG -> mid.deltaRate.signum() > 0
            LevelDirection.SHORT -> mid.deltaRate.signum() < 0
        }
        val reasons = buildList {
            if (directionalShare < DIRECTIONAL_SHARE_MINIMUM) {
                add(SignalBlockerReason.DIRECTIONAL_SHARE_BELOW_MINIMUM)
            }
            if (!fastDeltaDirectional) {
                add(SignalBlockerReason.FAST_DELTA_NOT_DIRECTIONAL)
            }
            if (!midDeltaDirectional) {
                add(SignalBlockerReason.MID_DELTA_NOT_DIRECTIONAL)
            }
        }
        return gate(SignalGate.DIRECTIONAL_FLOW, reasons)
    }

    internal fun rampGate(ramp: RampSnapshot): SignalGateResult {
        val strongestActivity = ramp.bins.maxOfOrNull(RampBinSnapshot::activity)
            ?: BigDecimal.ZERO
        val reasons = buildList {
            if (strongestActivity.signum() == 0) {
                add(SignalBlockerReason.RAMP_ACTIVITY_MISSING)
            }
            if (ramp.nonNegativeChangeCount < RAMP_NON_NEGATIVE_MINIMUM) {
                add(
                    SignalBlockerReason
                        .RAMP_NON_NEGATIVE_CHANGES_BELOW_MINIMUM,
                )
            }
            if (ramp.finalToInitialActivityRatio < RAMP_FINAL_RATIO_MINIMUM) {
                add(SignalBlockerReason.RAMP_FINAL_ACTIVITY_BELOW_MINIMUM)
            }
            if (ramp.latestToStrongestActivityRatio < RAMP_LATEST_RATIO_MINIMUM) {
                add(SignalBlockerReason.RAMP_LATEST_ACTIVITY_BELOW_MINIMUM)
            }
        }
        return gate(SignalGate.RAMP, reasons)
    }

    internal fun burstGate(burst: BurstSnapshot): SignalGateResult {
        val reasons = when (burst.status) {
            BurstStatus.UNRESOLVED ->
                listOf(SignalBlockerReason.ONE_SHOT_BURST_UNRESOLVED)

            BurstStatus.ACTIVE ->
                listOf(SignalBlockerReason.ONE_SHOT_BURST_ACTIVE)

            BurstStatus.NONE,
            BurstStatus.CLEARED,
            -> emptyList()
        }
        return gate(SignalGate.ONE_SHOT_BURST, reasons)
    }

    internal fun priceResponseGate(
        mid: WindowMetricsSnapshot,
        npu: BigDecimal?,
    ): SignalGateResult {
        if (npu == null) {
            return gate(
                SignalGate.PRICE_RESPONSE,
                listOf(SignalBlockerReason.NPU_UNAVAILABLE),
            )
        }
        val minimumProgress = npu.multiply(HALF)
        val maximumPullback = npu.multiply(TWO)
        val reasons = buildList {
            if (mid.signedPriceProgress < minimumProgress) {
                add(SignalBlockerReason.MID_PROGRESS_BELOW_MINIMUM)
            }
            if (mid.adversePullback > maximumPullback) {
                add(SignalBlockerReason.MID_PULLBACK_ABOVE_MAXIMUM)
            }
        }
        return gate(SignalGate.PRICE_RESPONSE, reasons)
    }

    internal fun latencyGate(
        publicMarketData: PublicMarketDataSnapshot?,
    ): LatencyGateEvaluation {
        val bookAge = publicMarketData?.bookTickerAge
        if (bookAge == null) {
            return LatencyGateEvaluation(
                result = gate(
                    SignalGate.LATENCY,
                    listOf(SignalBlockerReason.REQUIRED_MARKET_EVENT_MISSING),
                ),
                eventAgeMillis = null,
            )
        }
        val age = maxOf(
            bookAge.receiveAgeMillis,
            bookAge.exchangeAgeMillis ?: 0L,
        )
        val reasons = if (age > MAX_EVENT_AGE_MILLIS) {
            listOf(SignalBlockerReason.REQUIRED_MARKET_EVENT_TOO_OLD)
        } else {
            emptyList()
        }
        return LatencyGateEvaluation(
            result = gate(SignalGate.LATENCY, reasons),
            eventAgeMillis = age,
        )
    }

    internal fun spreadGate(
        publicMarketData: PublicMarketDataSnapshot?,
        npu: BigDecimal?,
    ): SpreadGateEvaluation {
        val bid = publicMarketData?.latestBidPrice
        val ask = publicMarketData?.latestAskPrice
        val actualSpread = if (bid != null && ask != null) {
            ask.subtract(bid)
        } else {
            null
        }
        val midPrice = if (bid != null && ask != null) {
            bid.add(ask).divide(TWO, MATH_CONTEXT)
        } else {
            null
        }
        val percentageLimit = midPrice?.multiply(SPREAD_PERCENT_LIMIT)
        val effectiveLimit = when {
            npu == null -> percentageLimit
            percentageLimit == null -> npu
            else -> minOf(npu, percentageLimit)
        }
        val reasons = buildList {
            if (actualSpread == null) {
                add(SignalBlockerReason.BID_ASK_MISSING)
            }
            if (npu == null) {
                add(SignalBlockerReason.NPU_UNAVAILABLE)
            }
            if (actualSpread != null && npu != null && actualSpread > npu) {
                add(SignalBlockerReason.SPREAD_ABOVE_NPU)
            }
            if (
                actualSpread != null &&
                percentageLimit != null &&
                actualSpread > percentageLimit
            ) {
                add(SignalBlockerReason.SPREAD_ABOVE_PERCENTAGE_LIMIT)
            }
        }
        return SpreadGateEvaluation(
            result = gate(SignalGate.SPREAD, reasons),
            actualSpread = actualSpread,
            percentageLimit = percentageLimit,
            effectiveLimit = effectiveLimit,
        )
    }

    internal fun dataIntegrityGate(
        publicMarketData: PublicMarketDataSnapshot?,
        privateStreamReadiness: BinanceReadiness,
        hasUnresolvedOrder: Boolean,
    ): SignalGateResult {
        val bid = publicMarketData?.latestBidPrice
        val ask = publicMarketData?.latestAskPrice
        val reasons = buildList {
            if (
                publicMarketData?.connectionState !=
                PublicStreamConnectionState.CONNECTED
            ) {
                add(SignalBlockerReason.PUBLIC_STREAM_DISCONNECTED)
            }
            if (
                publicMarketData?.gapStatus ==
                AggregateTradeGapStatus.GAP_DETECTED
            ) {
                add(SignalBlockerReason.AGGREGATE_TRADE_GAP_UNRESOLVED)
            }
            if (bid == null || ask == null) {
                add(SignalBlockerReason.BID_ASK_MISSING)
            } else if (
                bid.signum() <= 0 ||
                ask.signum() <= 0 ||
                ask < bid
            ) {
                add(SignalBlockerReason.BID_ASK_INVALID)
            }
            if (publicMarketData?.bidAskHeartbeatHealthy != true) {
                add(SignalBlockerReason.BID_ASK_STALE)
            }
            if (hasUnresolvedOrder) {
                add(SignalBlockerReason.PRIVATE_ORDER_OUTCOME_UNRESOLVED)
            }
            if (privateStreamReadiness != BinanceReadiness.READY) {
                add(SignalBlockerReason.PRIVATE_STREAM_UNHEALTHY)
            }
        }
        return gate(SignalGate.DATA_INTEGRITY, reasons)
    }

    private fun gate(
        gate: SignalGate,
        blockerReasons: List<SignalBlockerReason>,
    ): SignalGateResult =
        SignalGateResult(
            gate = gate,
            passed = blockerReasons.isEmpty(),
            blockerReasons = blockerReasons,
        )
}

internal data class LatencyGateEvaluation(
    val result: SignalGateResult,
    val eventAgeMillis: Long?,
)

internal data class SpreadGateEvaluation(
    val result: SignalGateResult,
    val actualSpread: BigDecimal?,
    val percentageLimit: BigDecimal?,
    val effectiveLimit: BigDecimal?,
)

private val MATH_CONTEXT = MathContext.DECIMAL128
private val ACCEL_MINIMUM = BigDecimal("1.5")
private val ACCEL_STRONG_MINIMUM = BigDecimal("2.0")
private val DIRECTIONAL_SHARE_MINIMUM = BigDecimal("0.62")
private const val RAMP_NON_NEGATIVE_MINIMUM = 5
private val RAMP_FINAL_RATIO_MINIMUM = BigDecimal("1.5")
private val RAMP_LATEST_RATIO_MINIMUM = BigDecimal("0.70")
private const val MAX_EVENT_AGE_MILLIS = 250L
private val HALF = BigDecimal("0.5")
private val TWO = BigDecimal("2")
private val SPREAD_PERCENT_LIMIT = BigDecimal("0.001")
