package com.scalpsecta.breakoutbot.binance

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFunction
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriComponentsBuilder
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CopyOnWriteArrayList

class LiveAuthenticatedBinanceClientTest {
    private val now = Instant.parse("2026-07-31T11:12:13.456Z")
    private val credentials = BinanceApiCredentials(
        apiKey = "test-api-key",
        secret = "test-signing-secret",
    )

    @Test
    fun `credentials are obtained only from the two required environment variables`() {
        val requestedVariables = mutableListOf<String>()
        val provider = EnvironmentBinanceCredentialsProvider { name ->
            requestedVariables += name
            when (name) {
                "BINANCE_API_KEY" -> "environment-api-key"
                "BINANCE_API_SECRET" -> "environment-signing-secret"
                else -> null
            }
        }

        val loaded = provider.credentials()

        assertThat(requestedVariables)
            .containsExactly("BINANCE_API_KEY", "BINANCE_API_SECRET")
        assertThat(loaded.toString()).doesNotContain(
            "environment-api-key",
            "environment-signing-secret",
        )
    }

    @Test
    fun `client measures exchange clock and signs live futures requests`() {
        val exchange = RecordingExchange(::responseFor)
        val client = client(exchange)

        val clock = client.synchronizeClock().block()!!
        val commission = client.commissionRate("btcusdt").block()!!

        assertThat(clock.serverTime).isEqualTo(now)
        assertThat(clock.checkedAt).isEqualTo(now)
        assertThat(clock.serverOffsetMillis).isZero()
        assertThat(clock.roundTripMillis).isZero()
        assertThat(commission.symbol).isEqualTo("BTCUSDT")
        assertThat(commission.makerRate)
            .isEqualByComparingTo(BigDecimal("0.000200"))
        assertThat(commission.takerRate)
            .isEqualByComparingTo(BigDecimal("0.000500"))

        val timeRequest = exchange.requests[0]
        assertThat(timeRequest.method()).isEqualTo(HttpMethod.GET)
        assertThat(timeRequest.url().host).isEqualTo("fapi.binance.com")
        assertThat(timeRequest.url().path).isEqualTo("/fapi/v1/time")

        val signedRequest = exchange.requests[1]
        assertThat(signedRequest.method()).isEqualTo(HttpMethod.GET)
        assertThat(signedRequest.url().path)
            .isEqualTo("/fapi/v1/commissionRate")
        assertThat(signedRequest.headers().getFirst("X-MBX-APIKEY"))
            .isEqualTo("test-api-key")
        val query = UriComponentsBuilder
            .fromUri(signedRequest.url())
            .build()
            .queryParams
        assertThat(query.getFirst("symbol")).isEqualTo("BTCUSDT")
        assertThat(query.getFirst("recvWindow")).isEqualTo("5000")
        assertThat(query.getFirst("timestamp")).isEqualTo("1785496333456")
        assertThat(query.getFirst("signature"))
            .isEqualTo(
                "e6eab7ac917fff392604d135a3951fc4" +
                    "aaf9fbd0fc19b5984f423fb279e34dc9",
            )
    }

    @Test
    fun `client maps account modes metadata brackets and listen key operations`() {
        val exchange = RecordingExchange(::responseFor)
        val client = client(exchange)

        val account = client.accountSummary().block()!!
        val positionMode = client.positionMode().block()!!
        val assetMode = client.assetMode().block()!!
        val exchangeInfo = client.exchangeInfo().block()!!
        val leverage = client.leverageBrackets("btcusdt").block()!!
        val listenKey = client.startUserDataStream().block()!!
        client.keepAliveUserDataStream(listenKey).block()

        assertThat(account.canTrade).isTrue()
        assertThat(account.feeTier).isEqualTo(1)
        assertThat(account.totalMarginBalance)
            .isEqualByComparingTo(BigDecimal("1003.50000000"))
        assertThat(positionMode).isEqualTo(BinancePositionMode.ONE_WAY)
        assertThat(assetMode).isEqualTo(BinanceAssetMode.SINGLE_ASSET)

        val symbol = exchangeInfo.symbols.single()
        assertThat(symbol.symbol).isEqualTo("BTCUSDT")
        assertThat(symbol.priceFilter?.tickSize)
            .isEqualByComparingTo(BigDecimal("0.10"))
        assertThat(symbol.lotSizeFilter?.stepSize)
            .isEqualByComparingTo(BigDecimal("0.001"))
        assertThat(symbol.marketLotSizeFilter?.minimumQuantity)
            .isEqualByComparingTo(BigDecimal("0.001"))
        assertThat(symbol.minimumNotional)
            .isEqualByComparingTo(BigDecimal("5"))

        assertThat(leverage.symbol).isEqualTo("BTCUSDT")
        assertThat(leverage.notionalCoefficient)
            .isEqualByComparingTo(BigDecimal("1.5"))
        assertThat(leverage.brackets.single().initialLeverage).isEqualTo(20)
        assertThat(leverage.brackets.single().notionalCap)
            .isEqualByComparingTo(BigDecimal("50000"))
        assertThat(listenKey).isEqualTo("recorded-listen-key")

        val streamRequests = exchange.requests.filter { request ->
            request.url().path == "/fapi/v1/listenKey"
        }
        assertThat(streamRequests.map(ClientRequest::method))
            .containsExactly(HttpMethod.POST, HttpMethod.PUT)
        assertThat(streamRequests)
            .allSatisfy { request ->
                assertThat(request.headers().getFirst("X-MBX-APIKEY"))
                    .isEqualTo("test-api-key")
            }
    }

    @Test
    fun `client loads mark price and sets then verifies symbol configuration`() {
        val exchange = RecordingExchange(::responseFor)
        val client = client(exchange)

        val markPrice = client.markPrice("btcusdt").block()!!
        val configuration = client.symbolConfiguration("btcusdt").block()!!
        client.changeMarginType("btcusdt", BinanceMarginType.ISOLATED).block()
        client.changeInitialLeverage("btcusdt", 20).block()

        assertThat(markPrice).isEqualByComparingTo(BigDecimal("65432.10"))
        assertThat(configuration.symbol).isEqualTo("BTCUSDT")
        assertThat(configuration.marginType).isEqualTo(BinanceMarginType.ISOLATED)
        assertThat(configuration.autoAddMargin).isFalse()
        assertThat(configuration.leverage).isEqualTo(20)
        assertThat(configuration.maximumNotional)
            .isEqualByComparingTo(BigDecimal("50000"))

        val markRequest = exchange.requests.single { request ->
            request.url().path == "/fapi/v1/premiumIndex"
        }
        assertThat(markRequest.method()).isEqualTo(HttpMethod.GET)
        assertThat(markRequest.url().query).isEqualTo("symbol=BTCUSDT")

        val signedMutations = exchange.requests.filter { request ->
            request.url().path in setOf(
                "/fapi/v1/marginType",
                "/fapi/v1/leverage",
            )
        }
        assertThat(signedMutations).hasSize(2)
        assertThat(signedMutations).allSatisfy { request ->
            assertThat(request.method()).isEqualTo(HttpMethod.POST)
            assertThat(request.headers().getFirst("X-MBX-APIKEY"))
                .isEqualTo("test-api-key")
            assertThat(request.url().query).contains(
                "symbol=BTCUSDT",
                "recvWindow=5000",
                "timestamp=1785496333456",
                "signature=",
            )
        }
    }

    @Test
    fun `Binance errors do not expose credentials signatures or response bodies`() {
        val exchange = RecordingExchange {
            response(
                status = HttpStatus.UNAUTHORIZED,
                body = "test-api-key test-signing-secret echoed-by-server",
            )
        }
        val client = client(exchange)

        StepVerifier.create(client.commissionRate("BTCUSDT"))
            .expectErrorSatisfies { error ->
                assertThat(error).isInstanceOf(BinanceClientException::class.java)
                assertThat(error.message)
                    .contains("HTTP 401", "/fapi/v1/commissionRate")
                    .doesNotContain(
                        "test-api-key",
                        "test-signing-secret",
                        "echoed-by-server",
                        "signature=",
                    )
            }
            .verify()
    }

    private fun client(exchange: ExchangeFunction): AuthenticatedBinanceClient =
        LiveAuthenticatedBinanceClient(
            webClient = WebClient
                .builder()
                .baseUrl("https://fapi.binance.com")
                .exchangeFunction(exchange)
                .build(),
            objectMapper = ObjectMapper(),
            credentialsProvider = BinanceCredentialsProvider { credentials },
            clock = Clock.fixed(now, ZoneOffset.UTC),
        )

    private fun responseFor(request: ClientRequest): ClientResponse =
        when (request.url().path) {
            "/fapi/v1/time" -> response(body = """{"serverTime":1785496333456}""")
            "/fapi/v3/account" -> response(
                body =
                    """
                    {
                      "feeTier": 1,
                      "canTrade": true,
                      "totalWalletBalance": "1000.00000000",
                      "totalUnrealizedProfit": "3.50000000",
                      "totalMarginBalance": "1003.50000000",
                      "availableBalance": "800.00000000",
                      "updateTime": 1785496333000,
                      "positions": [{"symbol":"BTCUSDT","positionAmt":"7"}]
                    }
                    """.trimIndent(),
            )
            "/fapi/v1/positionSide/dual" ->
                response(body = """{"dualSidePosition":false}""")
            "/fapi/v1/multiAssetsMargin" ->
                response(body = """{"multiAssetsMargin":false}""")
            "/fapi/v1/exchangeInfo" -> response(
                body =
                    """
                    {
                      "serverTime": 1785496333456,
                      "symbols": [{
                        "symbol": "BTCUSDT",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "marginAsset": "USDT",
                        "pricePrecision": 2,
                        "quantityPrecision": 3,
                        "filters": [
                          {"filterType":"PRICE_FILTER","minPrice":"0.10","maxPrice":"1000000","tickSize":"0.10"},
                          {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"},
                          {"filterType":"MARKET_LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"},
                          {"filterType":"MIN_NOTIONAL","notional":"5"}
                        ]
                      }]
                    }
                    """.trimIndent(),
            )
            "/fapi/v1/leverageBracket" -> response(
                body =
                    """
                    [{
                      "symbol": "BTCUSDT",
                      "notionalCoef": "1.5",
                      "brackets": [{
                        "bracket": 1,
                        "initialLeverage": 20,
                        "notionalFloor": "0",
                        "notionalCap": "50000",
                        "maintMarginRatio": "0.004",
                        "cum": "0"
                      }]
                    }]
                    """.trimIndent(),
            )
            "/fapi/v1/commissionRate" -> response(
                body =
                    """
                    {
                      "symbol":"BTCUSDT",
                      "makerCommissionRate":"0.000200",
                      "takerCommissionRate":"0.000500"
                    }
                    """.trimIndent(),
            )
            "/fapi/v1/premiumIndex" -> response(
                body =
                    """
                    {
                      "symbol":"BTCUSDT",
                      "markPrice":"65432.10"
                    }
                    """.trimIndent(),
            )
            "/fapi/v1/symbolConfig" -> response(
                body =
                    """
                    [{
                      "symbol":"BTCUSDT",
                      "marginType":"ISOLATED",
                      "isAutoAddMargin":false,
                      "leverage":20,
                      "maxNotionalValue":"50000"
                    }]
                    """.trimIndent(),
            )
            "/fapi/v1/marginType" -> response(
                body = """{"code":200,"msg":"success"}""",
            )
            "/fapi/v1/leverage" -> response(
                body =
                    """
                    {
                      "symbol":"BTCUSDT",
                      "leverage":20,
                      "maxNotionalValue":"50000"
                    }
                    """.trimIndent(),
            )
            "/fapi/v1/listenKey" -> response(
                body = if (request.method() == HttpMethod.POST) {
                    """{"listenKey":"recorded-listen-key"}"""
                } else {
                    "{}"
                },
            )
            else -> error("Unexpected request: ${request.method()} ${request.url()}")
        }

    private fun response(
        status: HttpStatus = HttpStatus.OK,
        body: String,
    ): ClientResponse =
        ClientResponse
            .create(status)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body(body)
            .build()
}

private class RecordingExchange(
    private val response: (ClientRequest) -> ClientResponse,
) : ExchangeFunction {
    val requests = CopyOnWriteArrayList<ClientRequest>()

    override fun exchange(request: ClientRequest): Mono<ClientResponse> {
        requests += request
        return Mono.just(response(request))
    }
}
