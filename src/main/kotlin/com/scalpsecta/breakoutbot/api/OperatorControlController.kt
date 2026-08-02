package com.scalpsecta.breakoutbot.api

import com.scalpsecta.breakoutbot.control.OperatorCommandSnapshot
import com.scalpsecta.breakoutbot.control.OperatorControlService
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.util.UUID

@RestController
@RequestMapping("/api")
class OperatorControlController(
    private val operatorControlService: OperatorControlService,
) {
    @PostMapping("/positions/{symbol}/close")
    fun closePosition(
        @PathVariable symbol: String,
        @RequestBody request: OperatorCommandRequest,
    ): Mono<OperatorCommandSnapshot> =
        operatorControlService.closePosition(symbol, request.commandId)

    @PostMapping("/controls/kill")
    fun kill(
        @RequestBody request: OperatorCommandRequest,
    ): Mono<OperatorCommandSnapshot> =
        operatorControlService.kill(request.commandId)

    @PostMapping("/controls/unlock")
    fun unlock(
        @RequestBody request: OperatorCommandRequest,
    ): Mono<OperatorCommandSnapshot> =
        operatorControlService.unlock(request.commandId)
}

data class OperatorCommandRequest(
    val commandId: UUID,
)
