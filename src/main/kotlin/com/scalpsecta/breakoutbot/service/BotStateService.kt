package com.scalpsecta.breakoutbot.service

import com.scalpsecta.breakoutbot.domain.RuntimeHealth
import com.scalpsecta.breakoutbot.domain.RuntimeSnapshot
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class BotStateService {
    private val startedAt = Instant.now()

    fun currentState(): RuntimeSnapshot =
        RuntimeSnapshot(
            startedAt = startedAt,
            levelCount = 0,
            recoveredAttemptCount = 0,
            health = RuntimeHealth.notReady(),
        )
}
