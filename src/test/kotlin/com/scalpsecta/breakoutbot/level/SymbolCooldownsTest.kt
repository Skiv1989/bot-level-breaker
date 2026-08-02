package com.scalpsecta.breakoutbot.level

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class SymbolCooldownsTest {
    @Test
    fun `confirmed flat blocks only its symbol for exactly thirty seconds`() {
        val cooldowns = SymbolCooldowns()
        val confirmedFlatAt = Instant.parse("2026-08-02T00:00:00Z")
        val cooldownUntil = cooldowns.start("BTCUSDT", confirmedFlatAt)

        assertThat(cooldownUntil)
            .isEqualTo(Instant.parse("2026-08-02T00:00:30Z"))
        assertThat(cooldowns.activeUntil("BTCUSDT", confirmedFlatAt.plusSeconds(29)))
            .isEqualTo(cooldownUntil)
        assertThat(cooldowns.activeUntil("ETHUSDT", confirmedFlatAt.plusSeconds(29)))
            .isNull()
        assertThat(cooldowns.activeUntil("BTCUSDT", cooldownUntil)).isNull()
    }
}
