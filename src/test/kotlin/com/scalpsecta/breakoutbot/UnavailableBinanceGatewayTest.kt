package com.scalpsecta.breakoutbot

import com.scalpsecta.breakoutbot.binance.BinanceOperation
import com.scalpsecta.breakoutbot.binance.UnavailableBinanceGateway
import org.junit.jupiter.api.Test
import reactor.test.StepVerifier

class UnavailableBinanceGatewayTest {
    @Test
    fun `production bootstrap gateway fails closed for every operation`() {
        BinanceOperation.entries.forEach { operation ->
            StepVerifier.create(UnavailableBinanceGateway().execute(operation))
                .expectErrorMatches { error ->
                    error is IllegalStateException &&
                        error.message?.contains(operation.name) == true
                }
                .verify()
        }
    }
}
