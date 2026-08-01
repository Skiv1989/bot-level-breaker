package com.scalpsecta.breakoutbot.api

import com.scalpsecta.breakoutbot.level.CreateLevelCommand
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelSnapshot
import com.scalpsecta.breakoutbot.level.LevelService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.util.UUID

@RestController
@RequestMapping("/api/levels")
class LevelController(
    private val levelService: LevelService,
) {
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun create(
        @RequestBody request: CreateLevelRequest,
    ): Mono<LevelSnapshot> =
        levelService.create(
            CreateLevelCommand(
                symbol = request.symbol,
                direction = request.direction,
                levelPrice = request.levelPrice,
                positionNotionalUsdt = request.positionNotionalUsdt,
                maxImpulsePct = request.maxImpulsePct,
            ),
        )

    @DeleteMapping("/{levelId}")
    fun delete(@PathVariable levelId: UUID): LevelSnapshot =
        levelService.delete(levelId)
}

data class CreateLevelRequest(
    val symbol: String,
    val direction: LevelDirection,
    val levelPrice: BigDecimal,
    val positionNotionalUsdt: BigDecimal,
    val maxImpulsePct: BigDecimal,
)

