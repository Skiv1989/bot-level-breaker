package com.scalpsecta.breakoutbot.marketdata

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class AggregateTradeContinuityTest {
    @Test
    fun `observed ID in the middle of a gap leaves both unresolved ranges`() {
        val continuity = AggregateTradeContinuity()
        continuity.observe(100L)
        continuity.observe(105L)

        assertThat(continuity.observe(103L))
            .isEqualTo(AggregateTradeObservation.ACCEPTED)
        assertThat(continuity.gapStatus())
            .isEqualTo(AggregateTradeGapStatus.GAP_DETECTED)
        assertThat(continuity.observe(103L))
            .isEqualTo(AggregateTradeObservation.DUPLICATE)

        continuity.observe(101L)
        continuity.observe(102L)
        continuity.observe(104L)

        assertThat(continuity.gapStatus())
            .isEqualTo(AggregateTradeGapStatus.CONTINUOUS)
    }
}
