package com.scalpsecta.breakoutbot.binance

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.publisher.Mono

@Configuration(proxyBeanMethods = false)
class BinanceBoundaryConfiguration {
    @Bean
    @ConditionalOnMissingBean(BinanceGateway::class)
    fun binanceGateway(): BinanceGateway = UnavailableBinanceGateway()
}

class UnavailableBinanceGateway : BinanceGateway {
    override fun execute(operation: BinanceOperation): Mono<Void> =
        Mono.error(
            IllegalStateException(
                "Binance operation $operation is unavailable until a live adapter is configured",
            ),
        )
}

