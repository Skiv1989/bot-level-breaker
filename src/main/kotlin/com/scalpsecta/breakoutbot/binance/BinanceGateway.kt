package com.scalpsecta.breakoutbot.binance

import reactor.core.publisher.Mono

fun interface BinanceGateway {
    fun execute(operation: BinanceOperation): Mono<Void>
}

enum class BinanceOperation {
    DISCOVER_POSITIONS,
    DISCOVER_OPEN_ORDERS,
    CANCEL_ORDER,
    PLACE_ORDER,
    CLOSE_EXPOSURE,
    CHANGE_ACCOUNT_MODE,
}

