package com.scalpsecta.breakoutbot.binance

import org.springframework.http.HttpMethod
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFilterFunction
import org.springframework.web.reactive.function.client.ExchangeFunction
import reactor.core.publisher.Mono
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

internal class AutomatedVerificationBinanceTransportGuard(
    private val enabled: Boolean,
    private val attemptMarker: Path?,
) : ExchangeFilterFunction {
    init {
        require(!enabled || attemptMarker != null) {
            "Automated verification requires a live-trading attempt marker"
        }
    }

    override fun filter(
        request: ClientRequest,
        next: ExchangeFunction,
    ): Mono<ClientResponse> {
        if (!enabled || !request.isLiveTradingMutation()) {
            return next.exchange(request)
        }

        val safeRequest = "${request.method()} ${request.url().path}"
        return try {
            recordAttempt(safeRequest)
            Mono.error(
                IllegalStateException(
                    "Automated verification blocked a live Binance trading " +
                        "request: $safeRequest",
                ),
            )
        } catch (error: Exception) {
            Mono.error(
                IllegalStateException(
                    "Automated verification blocked a live Binance trading " +
                        "request and could not record its safe path",
                    error,
                ),
            )
        }
    }

    @Synchronized
    private fun recordAttempt(safeRequest: String) {
        val marker = checkNotNull(attemptMarker)
        marker.parent?.let { parent -> Files.createDirectories(parent) }
        Files.writeString(
            marker,
            safeRequest + System.lineSeparator(),
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND,
        )
    }

    companion object {
        fun forGradleTestWorker(): AutomatedVerificationBinanceTransportGuard {
            val testWorker = System.getProperty(GRADLE_TEST_WORKER_PROPERTY)
            val automatedVerification =
                System.getProperty(AUTOMATED_VERIFICATION_PROPERTY) == "true"
            val marker = System.getProperty(ATTEMPT_MARKER_PROPERTY)
                ?.takeIf(String::isNotBlank)
                ?.let(Path::of)
            return AutomatedVerificationBinanceTransportGuard(
                enabled = automatedVerification && !testWorker.isNullOrBlank(),
                attemptMarker = marker,
            )
        }
    }
}

private fun ClientRequest.isLiveTradingMutation(): Boolean =
    url().scheme.equals("https", ignoreCase = true) &&
        url().host.equals(LIVE_BINANCE_REST_HOST, ignoreCase = true) &&
        method() != HttpMethod.GET &&
        url().path != LISTEN_KEY_PATH

private const val LIVE_BINANCE_REST_HOST = "fapi.binance.com"
private const val LISTEN_KEY_PATH = "/fapi/v1/listenKey"
private const val GRADLE_TEST_WORKER_PROPERTY = "org.gradle.test.worker"
private const val AUTOMATED_VERIFICATION_PROPERTY =
    "breakoutbot.test.automated-verification"
private const val ATTEMPT_MARKER_PROPERTY =
    "breakoutbot.test.live-trading-attempt-marker"
