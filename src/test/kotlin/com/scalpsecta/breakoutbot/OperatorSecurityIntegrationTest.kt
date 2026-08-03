package com.scalpsecta.breakoutbot

import com.fasterxml.jackson.databind.ObjectMapper
import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceClient
import com.scalpsecta.breakoutbot.binance.BinanceAccountSummary
import com.scalpsecta.breakoutbot.binance.BinanceAssetMode
import com.scalpsecta.breakoutbot.binance.BinanceClockMeasurement
import com.scalpsecta.breakoutbot.binance.BinanceCommissionRate
import com.scalpsecta.breakoutbot.binance.BinanceExchangeInfo
import com.scalpsecta.breakoutbot.binance.BinanceLeverageBracket
import com.scalpsecta.breakoutbot.binance.BinanceLotSizeFilter
import com.scalpsecta.breakoutbot.binance.BinanceMarginType
import com.scalpsecta.breakoutbot.binance.BinancePositionMode
import com.scalpsecta.breakoutbot.binance.BinancePriceFilter
import com.scalpsecta.breakoutbot.binance.BinanceSymbolConfiguration
import com.scalpsecta.breakoutbot.binance.BinanceSymbolLeverageBrackets
import com.scalpsecta.breakoutbot.binance.BinanceSymbolMetadata
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataStreamProvider
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.boot.web.context.WebServerApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.net.URI
import java.net.URLDecoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.security.SecureRandom
import java.time.Instant
import java.util.Base64
import java.util.UUID
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

@ExtendWith(OutputCaptureExtension::class)
class OperatorSecurityIntegrationTest {
    @TempDir
    lateinit var tempDirectory: Path

    @Test
    fun `unauthenticated HTTPS snapshot access is rejected`() {
        withHttpsApplication { application ->
            val response = application.getSnapshot()

            assertThat(response.statusCode()).isEqualTo(401)
            assertThat(response.headers().firstValue("www-authenticate").orElse(""))
                .startsWith("Basic")
            assertThat(response.body()).doesNotContainCredentials()
        }
    }

    @Test
    fun `operator page and static assets use native Basic authentication`() {
        withHttpsApplication { application ->
            val challenge = application.get(path = "/")

            assertThat(challenge.statusCode()).isEqualTo(401)
            assertThat(challenge.headers().firstValue("www-authenticate").orElse(""))
                .startsWith("Basic")

            val page = application.get(
                path = "/",
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
            )
            assertThat(page.statusCode()).isEqualTo(200)
            assertThat(page.headers().firstValue("content-type").orElse(""))
                .startsWith("text/html")
            assertThat(page.body())
                .contains(
                    "System health",
                    "Risk and equity",
                    "Add level",
                    "Positions",
                    "Recent activity",
                    "src=\"/app.js\"",
                    "href=\"/styles.css\"",
                )
                .doesNotContainCredentials()

            val script = application.get(
                path = "/app.js",
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
            )
            assertThat(script.statusCode()).isEqualTo(200)
            assertThat(script.headers().firstValue("content-type").orElse(""))
                .contains("javascript")
            assertThat(script.body())
                .contains(
                    "POLL_INTERVAL_MILLIS = 1000",
                    "\"/api/snapshot\"",
                    "CSRF_HEADER_NAME = \"X-XSRF-TOKEN\"",
                    "credentials: \"same-origin\"",
                    "state.pendingActions.has(key)",
                )
                .doesNotContain("WebSocket", "EventSource", "console.")
                .doesNotContainCredentials()

            val stylesheet = application.get(
                path = "/styles.css",
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
            )
            assertThat(stylesheet.statusCode()).isEqualTo(200)
            assertThat(stylesheet.headers().firstValue("content-type").orElse(""))
                .startsWith("text/css")
            assertThat(stylesheet.body()).doesNotContainCredentials()
        }
    }

    @Test
    fun `invalid Basic credentials are rejected without leaking secrets`(
        output: CapturedOutput,
    ) {
        withHttpsApplication { application ->
            val response = application.getSnapshot(
                username = "invalid-operator",
                password = INVALID_PASSWORD,
            )

            assertThat(response.statusCode()).isEqualTo(401)
            assertThat(response.body())
                .doesNotContainCredentials()
                .doesNotContain(INVALID_PASSWORD)
        }

        assertThat(output.all)
            .doesNotContain(OPERATOR_USERNAME)
            .doesNotContain(OPERATOR_PASSWORD)
            .doesNotContain(KEYSTORE_PASSWORD)
            .doesNotContain(INVALID_PASSWORD)
    }

    @Test
    fun `valid Basic credentials read the consolidated snapshot over HTTPS`() {
        withHttpsApplication { application ->
            val response = application.getSnapshot(
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
            )

            assertThat(response.statusCode()).isEqualTo(200)
            assertThat(response.headers().firstValue("content-type").orElse(""))
                .startsWith("application/json")
            assertThat(response.headers().firstValue("strict-transport-security").orElse(""))
                .isNotBlank()

            val snapshot = application.objectMapper.readTree(response.body())
            assertThat(Instant.parse(snapshot["startedAt"].asText())).isBeforeOrEqualTo(Instant.now())
            assertThat(snapshot["health"]["publicDataReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(snapshot["publicMarketData"]).isEmpty()
            assertThat(snapshot["levels"]).isEmpty()
            assertThat(snapshot["health"]["privateStreamReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(snapshot["health"]["clockReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(snapshot["health"]["accountReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(snapshot["health"]["tradingReadiness"].asText())
                .isEqualTo("BLOCKED")
            assertThat(
                snapshot["authenticatedBinance"]["clock"]["readiness"].asText(),
            ).isEqualTo("NOT_READY")
            assertThat(
                snapshot["authenticatedBinance"]["account"]["readiness"].asText(),
            ).isEqualTo("NOT_READY")
            assertThat(
                snapshot["authenticatedBinance"]["privateStream"]["readiness"].asText(),
            ).isEqualTo("NOT_READY")
            assertThat(snapshot["authenticatedBinance"]["currentEquity"].isNull)
                .isTrue()
            assertThat(
                snapshot["authenticatedBinance"]["temporaryDailyAnchorEquity"].isNull,
            ).isTrue()
            assertThat(snapshot["risk"]["dailyAnchorEquity"].isNull).isTrue()
            assertThat(snapshot["risk"]["totalReservedRisk"].decimalValue())
                .isEqualByComparingTo(BigDecimal.ZERO)
            assertThat(snapshot["risk"]["attempts"]).isEmpty()
            assertThat(snapshot["risk"]["reservations"]).isEmpty()
            assertThat(snapshot["evidence"]["persistentFilesAuthoritative"].asBoolean())
                .isTrue()
            assertThat(snapshot["evidence"]["recentAudit"]).isEmpty()
            assertThat(snapshot["evidence"]["recentTrades"]).isEmpty()
            assertThat(response.body()).doesNotContainCredentials()
        }
    }

    @Test
    fun `health keeps HTTPS liveness separate from Binance and trading readiness`() {
        withHttpsApplication { application ->
            val unauthenticated = application.get(path = "/api/health/liveness")
            assertThat(unauthenticated.statusCode()).isEqualTo(401)

            val liveness = application.get(
                path = "/api/health/liveness",
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
            )
            assertThat(liveness.statusCode()).isEqualTo(200)
            val liveSnapshot = application.objectMapper.readTree(liveness.body())
            assertThat(liveSnapshot["process"].asText()).isEqualTo("LIVE")
            assertThat(liveSnapshot["http"].asText()).isEqualTo("LIVE")
            assertThat(liveSnapshot.has("tradingReadiness")).isFalse()

            val readiness = application.get(
                path = "/api/health/readiness",
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
            )
            assertThat(readiness.statusCode()).isEqualTo(200)
            val readinessSnapshot =
                application.objectMapper.readTree(readiness.body())
            assertThat(readinessSnapshot["publicDataReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(readinessSnapshot["privateStreamReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(readinessSnapshot["clockReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(readinessSnapshot["accountReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(readinessSnapshot["tradingReadiness"].asText())
                .isEqualTo("BLOCKED")
        }
    }

    @Test
    fun `authenticated browser receives a secure CSRF token and CORS remains disabled`() {
        withHttpsApplication { application ->
            val snapshotResponse = application.getSnapshot(
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
                origin = "https://untrusted.example",
            )

            assertThat(snapshotResponse.statusCode()).isEqualTo(200)
            assertThat(snapshotResponse.headers().allValues("access-control-allow-origin"))
                .isEmpty()

            val csrfCookie = snapshotResponse
                .headers()
                .allValues("set-cookie")
                .single { cookie -> cookie.startsWith("$CSRF_COOKIE_NAME=") }
            assertThat(csrfCookie)
                .containsIgnoringCase("Secure")
                .contains("SameSite=Strict")
                .doesNotContain("HttpOnly")

            val cookiePair = csrfCookie.substringBefore(';')
            val csrfToken = URLDecoder.decode(
                cookiePair.substringAfter('='),
                StandardCharsets.UTF_8,
            )

            val missingTokenResponse = application.post(
                path = "/api/levels",
                body = VALID_LEVEL_REQUEST,
            )
            assertThat(missingTokenResponse.statusCode()).isEqualTo(403)
            assertThat(
                application.objectMapper.readTree(missingTokenResponse.body())["code"]
                    .asText(),
            ).isEqualTo("SECURITY_POLICY_VIOLATION")

            val commandId = UUID.randomUUID()
            val crossOriginCommand = application.post(
                path = "/api/controls/unlock",
                cookie = cookiePair,
                csrfToken = csrfToken,
                body = """{"commandId":"$commandId"}""",
                origin = "https://untrusted.example",
            )
            assertThat(crossOriginCommand.statusCode()).isEqualTo(403)
            assertThat(
                application.objectMapper.readTree(crossOriginCommand.body())["code"]
                    .asText(),
            ).isEqualTo("SAME_ORIGIN_REQUIRED")

            val validCommand = application.post(
                path = "/api/controls/unlock",
                cookie = cookiePair,
                csrfToken = csrfToken,
                body = """{"commandId":"$commandId"}""",
            )
            assertThat(validCommand.statusCode()).isEqualTo(200)
            assertThat(
                application.objectMapper.readTree(validCommand.body())["code"]
                    .asText(),
            ).isEqualTo("MANUAL_UNLOCK_REJECTED")

            val validTokenResponse = application.post(
                path = "/api/levels",
                cookie = cookiePair,
                csrfToken = csrfToken,
                body = VALID_LEVEL_REQUEST,
            )
            assertThat(validTokenResponse.statusCode()).isEqualTo(201)
            val createdLevel =
                application.objectMapper.readTree(validTokenResponse.body())
            assertThat(createdLevel["state"].asText()).isEqualTo("WARMING_UP")
            assertThat(validTokenResponse.body()).doesNotContainCredentials()

            val populatedSnapshot = application.getSnapshot(
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
            )
            assertThat(populatedSnapshot.statusCode()).isEqualTo(200)
            val populated = application.objectMapper.readTree(
                populatedSnapshot.body(),
            )
            assertThat(populated["levelCount"].asInt()).isOne()
            assertThat(populated["levels"]).hasSize(1)
            assertThat(populated["controls"]["commands"]).hasSize(1)
            assertThat(
                populated["controls"]["commands"][0]["code"].asText(),
            ).isEqualTo("MANUAL_UNLOCK_REJECTED")

            val deleteResponse = application.delete(
                path = "/api/levels/${createdLevel["id"].asText()}",
                cookie = cookiePair,
                csrfToken = csrfToken,
            )
            assertThat(deleteResponse.statusCode()).isEqualTo(200)

            val emptySnapshot = application.getSnapshot(
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
            )
            assertThat(
                application.objectMapper.readTree(emptySnapshot.body())["levels"],
            ).isEmpty()

            val nonFiniteResponse = application.post(
                path = "/api/levels",
                cookie = cookiePair,
                csrfToken = csrfToken,
                body = NON_FINITE_LEVEL_REQUEST,
            )
            assertThat(nonFiniteResponse.statusCode()).isEqualTo(400)
            assertThat(
                application.objectMapper.readTree(nonFiniteResponse.body())["code"]
                    .asText(),
            ).isEqualTo("INVALID_LEVEL")
        }
    }

    private fun withHttpsApplication(
        block: (HttpsApplication) -> Unit,
    ) {
        val keyStorePath = createTestKeyStore()
        val context = SpringApplicationBuilder(
            BreakoutBotApplication::class.java,
            SecurityLevelTestConfiguration::class.java,
        )
            .web(WebApplicationType.REACTIVE)
            .properties(
                "spring.main.banner-mode=off",
            )
            .run(
                "--server.port=0",
                "--server.ssl.enabled=true",
                "--server.ssl.key-store=${keyStorePath.toUri()}",
                "--server.ssl.key-store-password=$KEYSTORE_PASSWORD",
                "--server.ssl.key-store-type=PKCS12",
                "--bot.security.username=$OPERATOR_USERNAME",
                "--bot.security.password=$OPERATOR_PASSWORD",
                "--bot.binance.startup-enabled=false",
            )

        try {
            val webContext = context as WebServerApplicationContext
            val application = HttpsApplication(
                context = context,
                client = createHttpsClient(keyStorePath),
                baseUri = URI.create("https://localhost:${webContext.webServer.port}"),
            )
            block(application)
        } finally {
            context.close()
        }
    }

    private fun createTestKeyStore(): Path {
        val keyStorePath = tempDirectory.resolve(
            "operator-${UUID.randomUUID()}.p12",
        )
        val executableName = if (System.getProperty("os.name").startsWith("Windows")) {
            "keytool.exe"
        } else {
            "keytool"
        }
        val keytool = Path.of(
            System.getProperty("java.home"),
            "bin",
            executableName,
        )
        val process = ProcessBuilder(
            keytool.toString(),
            "-genkeypair",
            "-alias",
            KEY_ALIAS,
            "-keyalg",
            "RSA",
            "-keysize",
            "2048",
            "-storetype",
            "PKCS12",
            "-keystore",
            keyStorePath.toString(),
            "-storepass",
            KEYSTORE_PASSWORD,
            "-keypass",
            KEYSTORE_PASSWORD,
            "-dname",
            "CN=localhost",
            "-validity",
            "1",
            "-ext",
            "SAN=dns:localhost,ip:127.0.0.1",
            "-noprompt",
        )
            .redirectErrorStream(true)
            .start()
        val output = process.inputStream.bufferedReader().use { reader ->
            reader.readText()
        }

        assertThat(process.waitFor())
            .describedAs("keytool output: %s", output)
            .isZero()
        assertThat(keyStorePath).isRegularFile()
        return keyStorePath
    }

    private fun createHttpsClient(keyStorePath: Path): HttpClient {
        val serverKeyStore = KeyStore.getInstance("PKCS12")
        Files.newInputStream(keyStorePath).use { input ->
            serverKeyStore.load(input, KEYSTORE_PASSWORD.toCharArray())
        }

        val trustStore = KeyStore.getInstance(KeyStore.getDefaultType())
        trustStore.load(null)
        trustStore.setCertificateEntry(
            KEY_ALIAS,
            serverKeyStore.getCertificate(KEY_ALIAS),
        )

        val trustManagerFactory = TrustManagerFactory.getInstance(
            TrustManagerFactory.getDefaultAlgorithm(),
        )
        trustManagerFactory.init(trustStore)

        val sslContext = SSLContext.getInstance("TLS")
        sslContext.init(null, trustManagerFactory.trustManagers, SecureRandom())

        return HttpClient
            .newBuilder()
            .sslContext(sslContext)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build()
    }

    private fun HttpsApplication.getSnapshot(
        username: String? = null,
        password: String? = null,
        origin: String? = null,
    ): HttpResponse<String> =
        get(
            path = "/api/snapshot",
            username = username,
            password = password,
            origin = origin,
        )

    private fun HttpsApplication.get(
        path: String,
        username: String? = null,
        password: String? = null,
        origin: String? = null,
    ): HttpResponse<String> {
        val request = HttpRequest
            .newBuilder(baseUri.resolve(path))
            .GET()
        if (username != null && password != null) {
            request.header("Authorization", basicAuthorization(username, password))
        }
        if (origin != null) {
            request.header("Origin", origin)
        }

        return client.send(
            request.build(),
            HttpResponse.BodyHandlers.ofString(),
        )
    }

    private fun HttpsApplication.post(
        path: String,
        cookie: String? = null,
        csrfToken: String? = null,
        body: String = "",
        origin: String = "${baseUri.scheme}://${baseUri.authority}",
    ): HttpResponse<String> {
        val request = HttpRequest
            .newBuilder(baseUri.resolve(path))
            .header(
                "Authorization",
                basicAuthorization(OPERATOR_USERNAME, OPERATOR_PASSWORD),
            )
            .header("Origin", origin)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
        if (cookie != null) {
            request.header("Cookie", cookie)
        }
        if (csrfToken != null) {
            request.header(CSRF_HEADER_NAME, csrfToken)
        }

        return client.send(
            request.build(),
            HttpResponse.BodyHandlers.ofString(),
        )
    }

    private fun HttpsApplication.delete(
        path: String,
        cookie: String,
        csrfToken: String,
    ): HttpResponse<String> {
        val request = HttpRequest
            .newBuilder(baseUri.resolve(path))
            .header(
                "Authorization",
                basicAuthorization(OPERATOR_USERNAME, OPERATOR_PASSWORD),
            )
            .header("Origin", "${baseUri.scheme}://${baseUri.authority}")
            .header("Cookie", cookie)
            .header(CSRF_HEADER_NAME, csrfToken)
            .DELETE()
            .build()
        return client.send(request, HttpResponse.BodyHandlers.ofString())
    }

    private fun basicAuthorization(
        username: String,
        password: String,
    ): String {
        val credentials = "$username:$password".toByteArray(StandardCharsets.UTF_8)
        return "Basic ${Base64.getEncoder().encodeToString(credentials)}"
    }

    private fun org.assertj.core.api.AbstractStringAssert<*>.doesNotContainCredentials() =
        doesNotContain(
            OPERATOR_USERNAME,
            OPERATOR_PASSWORD,
            KEYSTORE_PASSWORD,
        )

    private data class HttpsApplication(
        val context: ConfigurableApplicationContext,
        val client: HttpClient,
        val baseUri: URI,
    ) {
        val objectMapper: ObjectMapper = context.getBean(ObjectMapper::class.java)
    }

    companion object {
        private const val OPERATOR_USERNAME = "security-test-operator"
        private const val OPERATOR_PASSWORD = "security-test-basic-password"
        private const val INVALID_PASSWORD = "invalid-password-do-not-log"
        private const val KEYSTORE_PASSWORD = "security-test-keystore-password"
        private const val KEY_ALIAS = "operator"
        private const val CSRF_COOKIE_NAME = "XSRF-TOKEN"
        private const val CSRF_HEADER_NAME = "X-XSRF-TOKEN"
        private val VALID_LEVEL_REQUEST =
            """
            {
              "symbol":"btcusdt",
              "direction":"LONG",
              "levelPrice":101.2,
              "positionNotionalUsdt":1000,
              "maxImpulsePct":2.5
            }
            """.trimIndent()
        private val NON_FINITE_LEVEL_REQUEST =
            VALID_LEVEL_REQUEST.replace("101.2", "\"NaN\"")
    }
}

@TestConfiguration(proxyBeanMethods = false)
class SecurityLevelTestConfiguration {
    @Bean
    @Primary
    fun securityTestBinanceClient(): AuthenticatedBinanceClient =
        SecurityTestBinanceClient()

    @Bean
    @Primary
    fun securityTestPublicMarketDataStreamProvider():
        PublicMarketDataStreamProvider = SecurityTestMarketDataStreamProvider
}

private object SecurityTestMarketDataStreamProvider :
    PublicMarketDataStreamProvider {
    override fun aggregateTrades(symbol: String): Flux<AggregateTradeEvent> =
        Flux.never()

    override fun bookTickers(symbol: String): Flux<BookTickerEvent> = Flux.never()
}

private class SecurityTestBinanceClient : AuthenticatedBinanceClient {
    private var marginType = BinanceMarginType.CROSSED
    private var leverage = 5

    override fun synchronizeClock(): Mono<BinanceClockMeasurement> = unsupported()

    override fun accountSummary(): Mono<BinanceAccountSummary> = unsupported()

    override fun positionMode(): Mono<BinancePositionMode> = unsupported()

    override fun assetMode(): Mono<BinanceAssetMode> = unsupported()

    override fun exchangeInfo(): Mono<BinanceExchangeInfo> =
        Mono.just(
            BinanceExchangeInfo(
                serverTime = Instant.parse("2026-07-31T12:00:00Z"),
                symbols = listOf(
                    BinanceSymbolMetadata(
                        symbol = "BTCUSDT",
                        status = "TRADING",
                        contractType = "PERPETUAL",
                        baseAsset = "BTC",
                        quoteAsset = "USDT",
                        marginAsset = "USDT",
                        pricePrecision = 1,
                        quantityPrecision = 3,
                        priceFilter = BinancePriceFilter(
                            minimumPrice = BigDecimal("0.1"),
                            maximumPrice = BigDecimal("1000000"),
                            tickSize = BigDecimal("0.1"),
                        ),
                        lotSizeFilter = BinanceLotSizeFilter(
                            minimumQuantity = BigDecimal("0.001"),
                            maximumQuantity = BigDecimal("1000"),
                            stepSize = BigDecimal("0.001"),
                        ),
                        marketLotSizeFilter = null,
                        minimumNotional = BigDecimal("5"),
                    ),
                ),
            ),
        )

    override fun leverageBrackets(
        symbol: String,
    ): Mono<BinanceSymbolLeverageBrackets> =
        Mono.just(
            BinanceSymbolLeverageBrackets(
                symbol = symbol,
                notionalCoefficient = BigDecimal.ONE,
                brackets = listOf(
                    BinanceLeverageBracket(
                        bracket = 1,
                        initialLeverage = 50,
                        notionalFloor = BigDecimal.ZERO,
                        notionalCap = BigDecimal("50000"),
                        maintenanceMarginRatio = BigDecimal("0.004"),
                        cumulativeMaintenanceAmount = BigDecimal.ZERO,
                    ),
                ),
            ),
        )

    override fun commissionRate(symbol: String): Mono<BinanceCommissionRate> =
        unsupported()

    override fun markPrice(symbol: String): Mono<BigDecimal> =
        Mono.just(BigDecimal("100"))

    override fun symbolConfiguration(
        symbol: String,
    ): Mono<BinanceSymbolConfiguration> =
        Mono.just(
            BinanceSymbolConfiguration(
                symbol = symbol,
                marginType = marginType,
                autoAddMargin = false,
                leverage = leverage,
                maximumNotional = BigDecimal("50000"),
            ),
        )

    override fun changeMarginType(
        symbol: String,
        marginType: BinanceMarginType,
    ): Mono<Void> {
        this.marginType = marginType
        return Mono.empty()
    }

    override fun changeInitialLeverage(
        symbol: String,
        leverage: Int,
    ): Mono<Void> {
        this.leverage = leverage
        return Mono.empty()
    }

    override fun startUserDataStream(): Mono<String> = unsupported()

    override fun keepAliveUserDataStream(listenKey: String): Mono<Void> =
        unsupported()

    private fun <T> unsupported(): Mono<T> =
        Mono.error(UnsupportedOperationException("Not used by security tests"))
}
