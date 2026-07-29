package com.scalpsecta.breakoutbot

import com.scalpsecta.breakoutbot.binance.BinanceGateway
import com.scalpsecta.breakoutbot.binance.UnavailableBinanceGateway
import com.scalpsecta.starter.service.binance.rest.BinanceMarketDataServiceFutures
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder

class ProductionBootstrapIntegrationTest {
    @Test
    fun `production bootstrap starts fail closed without starter exchange clients`() {
        val context = SpringApplicationBuilder(BreakoutBotApplication::class.java)
            .web(WebApplicationType.REACTIVE)
            .properties(
                "server.port=0",
                "spring.main.banner-mode=off",
            )
            .run()

        try {
            assertThat(context.getBean(BinanceGateway::class.java))
                .isInstanceOf(UnavailableBinanceGateway::class.java)
            assertThat(
                context.getBeansOfType(BinanceMarketDataServiceFutures::class.java),
            ).isEmpty()
        } finally {
            context.close()
        }
    }
}
