package com.scalpsecta.breakoutbot.marketdata

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration(proxyBeanMethods = false)
class PublicMarketDataConfiguration {
    @Bean
    @ConditionalOnMissingBean(Clock::class)
    fun clock(): Clock = Clock.systemUTC()

    @Bean
    fun detailedAggTradeBinanceWebSocketPool(
        clock: Clock,
    ): DetailedAggTradeBinanceWebSocketPool =
        DetailedAggTradeBinanceWebSocketPool(clock)

    @Bean
    fun bookTickerBinanceWebSocketPool(
        clock: Clock,
    ): BookTickerBinanceWebSocketPool =
        BookTickerBinanceWebSocketPool(clock)

    @Bean
    fun publicMarketDataStreamProvider(
        aggregateTradePool: DetailedAggTradeBinanceWebSocketPool,
        bookTickerPool: BookTickerBinanceWebSocketPool,
    ): PublicMarketDataStreamProvider =
        BinancePublicMarketDataStreamProvider(
            aggregateTradePool,
            bookTickerPool,
        )
}
