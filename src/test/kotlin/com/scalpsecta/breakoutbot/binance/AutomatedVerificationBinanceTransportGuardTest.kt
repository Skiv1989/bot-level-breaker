package com.scalpsecta.breakoutbot.binance

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFunction
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

class AutomatedVerificationBinanceTransportGuardTest {
    @TempDir
    lateinit var tempDirectory: Path

    @Test
    fun `read requests and listen-key maintenance stay available to tests`() {
        val marker = tempDirectory.resolve("attempt.txt")
        val downstreamCalls = AtomicInteger()
        val client = guardedClient(marker, downstreamCalls)

        client.get()
            .uri("/fapi/v3/account?signature=must-not-be-recorded")
            .retrieve()
            .toBodilessEntity()
            .block()
        client.post()
            .uri("/fapi/v1/listenKey")
            .retrieve()
            .toBodilessEntity()
            .block()
        client.put()
            .uri("/fapi/v1/listenKey")
            .retrieve()
            .toBodilessEntity()
            .block()

        assertThat(downstreamCalls).hasValue(3)
        assertThat(marker).doesNotExist()
    }

    @Test
    fun `trading mutation is stopped before transport and leaves safe evidence`() {
        val marker = tempDirectory.resolve("attempt.txt")
        val downstreamCalls = AtomicInteger()
        val client = guardedClient(marker, downstreamCalls)

        assertThatThrownBy {
            client.post()
                .uri(
                    "/fapi/v1/order?signature=secret-signature" +
                        "&newClientOrderId=sensitive-order-id",
                )
                .retrieve()
                .toBodilessEntity()
                .block()
        }
            .hasMessageContaining(
                "Automated verification blocked a live Binance trading request",
            )
            .hasMessageContaining("POST /fapi/v1/order")
            .hasMessageNotContaining("secret-signature")
            .hasMessageNotContaining("sensitive-order-id")

        assertThat(downstreamCalls).hasValue(0)
        assertThat(Files.readString(marker))
            .isEqualTo("POST /fapi/v1/order${System.lineSeparator()}")
    }

    @Test
    fun `non-Binance transport is outside the live trading guard`() {
        val marker = tempDirectory.resolve("attempt.txt")
        val downstreamCalls = AtomicInteger()
        val exchange = successfulExchange(downstreamCalls)
        val guard = AutomatedVerificationBinanceTransportGuard(
            enabled = true,
            attemptMarker = marker,
        )
        val client = WebClient.builder()
            .baseUrl("https://example.invalid")
            .filter(guard)
            .exchangeFunction(exchange)
            .build()

        client.post()
            .uri("/fapi/v1/order")
            .retrieve()
            .toBodilessEntity()
            .block()

        assertThat(downstreamCalls).hasValue(1)
        assertThat(marker).doesNotExist()
    }

    private fun guardedClient(
        marker: Path,
        downstreamCalls: AtomicInteger,
    ): WebClient {
        val guard = AutomatedVerificationBinanceTransportGuard(
            enabled = true,
            attemptMarker = marker,
        )
        return WebClient.builder()
            .baseUrl("https://fapi.binance.com")
            .filter(guard)
            .exchangeFunction(successfulExchange(downstreamCalls))
            .build()
    }

    private fun successfulExchange(
        calls: AtomicInteger,
    ): ExchangeFunction = ExchangeFunction {
        calls.incrementAndGet()
        Mono.just(ClientResponse.create(HttpStatus.OK).body("{}").build())
    }
}
