package com.scalpsecta.breakoutbot.api

import com.scalpsecta.breakoutbot.domain.RuntimeSnapshot
import com.scalpsecta.breakoutbot.service.BotStateService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api")
class OperatorSnapshotController(
    private val botStateService: BotStateService,
) {
    @GetMapping("/snapshot")
    fun snapshot(): RuntimeSnapshot = botStateService.currentState()
}
