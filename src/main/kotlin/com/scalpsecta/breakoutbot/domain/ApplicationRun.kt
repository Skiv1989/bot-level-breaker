package com.scalpsecta.breakoutbot.domain

import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Instant

@Component
class ApplicationRun(clock: Clock) {
    val startedAt: Instant = clock.instant()
}
