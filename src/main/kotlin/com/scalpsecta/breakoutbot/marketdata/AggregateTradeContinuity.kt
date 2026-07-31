package com.scalpsecta.breakoutbot.marketdata

import java.util.TreeMap

internal class AggregateTradeContinuity {
    private var highestObservedId: Long? = null
    private val missingRanges = TreeMap<Long, Long>()

    fun observe(aggregateTradeId: Long): AggregateTradeObservation {
        val highestId = highestObservedId
        if (highestId == null) {
            highestObservedId = aggregateTradeId
            return AggregateTradeObservation.ACCEPTED
        }

        if (aggregateTradeId > highestId) {
            if (
                aggregateTradeId > highestId + 1 &&
                highestId != Long.MAX_VALUE
            ) {
                missingRanges[highestId + 1] = aggregateTradeId - 1
            }
            highestObservedId = aggregateTradeId
            return AggregateTradeObservation.ACCEPTED
        }

        val containingRange = missingRanges.floorEntry(aggregateTradeId)
        if (
            containingRange == null ||
            aggregateTradeId > containingRange.value
        ) {
            return AggregateTradeObservation.DUPLICATE
        }

        removeObservedIdFromMissingRange(
            observedId = aggregateTradeId,
            rangeStartId = containingRange.key,
            rangeEndIdInclusive = containingRange.value,
        )
        return AggregateTradeObservation.ACCEPTED
    }

    private fun removeObservedIdFromMissingRange(
        observedId: Long,
        rangeStartId: Long,
        rangeEndIdInclusive: Long,
    ) {
        missingRanges.remove(rangeStartId)
        if (rangeStartId < observedId) {
            missingRanges[rangeStartId] = observedId - 1
        }
        if (observedId < rangeEndIdInclusive) {
            missingRanges[observedId + 1] = rangeEndIdInclusive
        }
    }

    fun gapStatus(): AggregateTradeGapStatus =
        if (missingRanges.isEmpty()) {
            AggregateTradeGapStatus.CONTINUOUS
        } else {
            AggregateTradeGapStatus.GAP_DETECTED
        }
}

internal enum class AggregateTradeObservation {
    ACCEPTED,
    DUPLICATE,
}
