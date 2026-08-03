package com.scalpsecta.breakoutbot.replay

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit

class ReplayVirtualClockTest {
    @Test
    fun `scheduled production deadlines observe their exact virtual instants`() {
        ReplayVirtualClock(STARTED_AT).use { clock ->
            val observedDeadlines = mutableListOf<Instant>()
            clock.scheduler().schedule(
                { observedDeadlines += clock.instant() },
                2,
                TimeUnit.SECONDS,
            )
            clock.scheduler().schedule(
                { observedDeadlines += clock.instant() },
                10,
                TimeUnit.MINUTES,
            )

            clock.advanceBy(Duration.ofMinutes(10))

            assertThat(observedDeadlines).containsExactly(
                STARTED_AT.plusSeconds(2),
                STARTED_AT.plus(Duration.ofMinutes(10)),
            )
            assertThat(clock.instant())
                .isEqualTo(STARTED_AT.plus(Duration.ofMinutes(10)))
        }
    }
}

private val STARTED_AT: Instant = Instant.parse("2026-08-03T09:00:00Z")
