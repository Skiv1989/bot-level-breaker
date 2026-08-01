package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.level.LevelService
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.util.UUID

interface SymbolExecutionCoordinator {
    fun submit(
        symbol: String,
        eventId: String,
        action: () -> Any,
    ): Mono<Any>

    fun recordOwnership(
        levelId: UUID,
        ownsActiveAttempt: Boolean,
        ownsExposure: Boolean,
        hasUnresolvedOrder: Boolean,
    ): Mono<Void>
}

@Component
class LevelSymbolExecutionCoordinator(
    private val levelService: LevelService,
) : SymbolExecutionCoordinator {
    override fun submit(
        symbol: String,
        eventId: String,
        action: () -> Any,
    ): Mono<Any> =
        levelService.processExecutionEvent(symbol, eventId, action)

    override fun recordOwnership(
        levelId: UUID,
        ownsActiveAttempt: Boolean,
        ownsExposure: Boolean,
        hasUnresolvedOrder: Boolean,
    ): Mono<Void> =
        levelService.recordOwnership(
            levelId = levelId,
            ownsActiveAttempt = ownsActiveAttempt,
            ownsExposure = ownsExposure,
            hasUnresolvedOrder = hasUnresolvedOrder,
        ).then()
}
