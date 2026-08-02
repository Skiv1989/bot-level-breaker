package com.scalpsecta.breakoutbot.level

import java.time.Duration
import java.time.Instant

internal class SymbolCooldowns(
    private val duration: Duration = Duration.ofSeconds(30),
) {
    private val cooldowns = mutableMapOf<String, Instant>()

    init {
        require(!duration.isZero && !duration.isNegative) {
            "Symbol cooldown duration must be positive"
        }
    }

    fun start(symbol: String, confirmedFlatAt: Instant): Instant =
        confirmedFlatAt.plus(duration).also { cooldownUntil ->
            cooldowns[symbol] = cooldownUntil
        }

    fun activeUntil(symbol: String, now: Instant): Instant? {
        val cooldownUntil = cooldowns[symbol] ?: return null
        if (!now.isBefore(cooldownUntil)) {
            cooldowns.remove(symbol)
            return null
        }
        return cooldownUntil
    }

    fun clear() {
        cooldowns.clear()
    }
}
