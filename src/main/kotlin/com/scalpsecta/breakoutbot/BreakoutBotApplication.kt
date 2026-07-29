package com.scalpsecta.breakoutbot

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication(
    excludeName = [
        "com.scalpsecta.starter.autoconfigure.AsterConfiguration",
        "com.scalpsecta.starter.autoconfigure.BinanceConfiguration",
        "com.scalpsecta.starter.autoconfigure.BitgetConfiguration",
        "com.scalpsecta.starter.autoconfigure.BybitConfiguration",
        "com.scalpsecta.starter.autoconfigure.GateConfiguration",
        "com.scalpsecta.starter.autoconfigure.HyperliquidConfiguration",
        "com.scalpsecta.starter.autoconfigure.KucoinConfiguration",
        "com.scalpsecta.starter.autoconfigure.OkxConfiguration",
    ],
)
class BreakoutBotApplication

fun main(args: Array<String>) {
    runApplication<BreakoutBotApplication>(*args)
}

