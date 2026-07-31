package com.scalpsecta.breakoutbot.binance

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

interface AuthenticatedBinanceClient {
    fun synchronizeClock(): Mono<BinanceClockMeasurement>

    fun accountSummary(): Mono<BinanceAccountSummary>

    fun positionMode(): Mono<BinancePositionMode>

    fun assetMode(): Mono<BinanceAssetMode>

    fun exchangeInfo(): Mono<BinanceExchangeInfo>

    fun leverageBrackets(symbol: String): Mono<BinanceSymbolLeverageBrackets>

    fun commissionRate(symbol: String): Mono<BinanceCommissionRate>

    fun startUserDataStream(): Mono<String>

    fun keepAliveUserDataStream(listenKey: String): Mono<Void>
}

fun interface BinanceCredentialsProvider {
    fun credentials(): BinanceApiCredentials
}

class BinanceApiCredentials(
    internal val apiKey: String,
    internal val secret: String,
) {
    init {
        require(apiKey.isNotBlank()) {
            "BINANCE_API_KEY must not be blank"
        }
        require(secret.isNotBlank()) {
            "BINANCE_API_SECRET must not be blank"
        }
    }

    override fun toString(): String = "BinanceApiCredentials([REDACTED])"
}

class EnvironmentBinanceCredentialsProvider(
    private val environmentVariable: (String) -> String? = System::getenv,
) : BinanceCredentialsProvider {
    override fun credentials(): BinanceApiCredentials =
        BinanceApiCredentials(
            apiKey = requiredEnvironmentVariable(BINANCE_API_KEY_VARIABLE),
            secret = requiredEnvironmentVariable(BINANCE_API_SECRET_VARIABLE),
        )

    private fun requiredEnvironmentVariable(name: String): String =
        environmentVariable(name)?.takeIf(String::isNotBlank)
            ?: throw BinanceConfigurationException(
                "$name environment variable is required",
            )
}

class BinanceConfigurationException(message: String) : IllegalStateException(message)

class BinanceClientException(message: String) : IllegalStateException(message)

class BinanceRequestSigner(
    secret: String,
) {
    private val key = SecretKeySpec(
        secret.toByteArray(StandardCharsets.UTF_8),
        HMAC_SHA_256,
    )

    fun sign(payload: String): String {
        val mac = Mac.getInstance(HMAC_SHA_256)
        mac.init(key)
        return mac
            .doFinal(payload.toByteArray(StandardCharsets.UTF_8))
            .joinToString(separator = "") { byte ->
                "%02x".format(byte.toInt() and 0xff)
            }
    }

    override fun toString(): String = "BinanceRequestSigner([REDACTED])"
}

class LiveAuthenticatedBinanceClient(
    private val webClient: WebClient,
    private val objectMapper: ObjectMapper,
    private val credentialsProvider: BinanceCredentialsProvider,
    private val clock: Clock,
) : AuthenticatedBinanceClient {
    private val serverOffsetMillis = AtomicLong()

    override fun synchronizeClock(): Mono<BinanceClockMeasurement> =
        Mono.defer {
            credentialsProvider.credentials()
            val requestStartedAt = clock.millis()
            requestJson(HttpMethod.GET, SERVER_TIME_PATH)
                .map { payload ->
                    val requestCompletedAt = clock.millis()
                    val roundTripMillis =
                        (requestCompletedAt - requestStartedAt).coerceAtLeast(0)
                    val midpoint = requestStartedAt + roundTripMillis / 2
                    val serverTimeMillis = payload.requiredLong("serverTime")
                    val offsetMillis = serverTimeMillis - midpoint
                    serverOffsetMillis.set(offsetMillis)
                    BinanceClockMeasurement(
                        serverTime = Instant.ofEpochMilli(serverTimeMillis),
                        checkedAt = Instant.ofEpochMilli(requestCompletedAt),
                        serverOffsetMillis = offsetMillis,
                        roundTripMillis = roundTripMillis,
                    )
                }
        }

    override fun accountSummary(): Mono<BinanceAccountSummary> =
        signedGet(ACCOUNT_PATH)
            .map { payload ->
                BinanceAccountSummary(
                    canTrade = payload.requiredBoolean("canTrade"),
                    feeTier = payload.requiredInt("feeTier"),
                    totalWalletBalance = payload.requiredDecimal("totalWalletBalance"),
                    totalUnrealizedProfit =
                        payload.requiredDecimal("totalUnrealizedProfit"),
                    totalMarginBalance = payload.requiredDecimal("totalMarginBalance"),
                    availableBalance = payload.requiredDecimal("availableBalance"),
                    updatedAt = Instant.ofEpochMilli(payload.requiredLong("updateTime")),
                )
            }

    override fun positionMode(): Mono<BinancePositionMode> =
        signedGet(POSITION_MODE_PATH)
            .map { payload ->
                if (payload.requiredBoolean("dualSidePosition")) {
                    BinancePositionMode.HEDGE
                } else {
                    BinancePositionMode.ONE_WAY
                }
            }

    override fun assetMode(): Mono<BinanceAssetMode> =
        signedGet(ASSET_MODE_PATH)
            .map { payload ->
                if (payload.requiredBoolean("multiAssetsMargin")) {
                    BinanceAssetMode.MULTI_ASSET
                } else {
                    BinanceAssetMode.SINGLE_ASSET
                }
            }

    override fun exchangeInfo(): Mono<BinanceExchangeInfo> =
        requestJson(HttpMethod.GET, EXCHANGE_INFO_PATH)
            .map(::parseExchangeInfo)

    override fun leverageBrackets(
        symbol: String,
    ): Mono<BinanceSymbolLeverageBrackets> {
        val normalizedSymbol = normalizedSymbol(symbol)
        return signedGet(
            path = LEVERAGE_BRACKET_PATH,
            parameters = linkedMapOf("symbol" to normalizedSymbol),
        ).map { payload ->
            val symbolPayload = if (payload.isArray) {
                payload.firstOrNull()
                    ?: throw BinanceClientException(
                        "Binance returned no leverage brackets for $normalizedSymbol",
                    )
            } else {
                payload
            }
            parseLeverageBrackets(symbolPayload)
        }
    }

    override fun commissionRate(symbol: String): Mono<BinanceCommissionRate> {
        val normalizedSymbol = normalizedSymbol(symbol)
        return signedGet(
            path = COMMISSION_RATE_PATH,
            parameters = linkedMapOf("symbol" to normalizedSymbol),
        ).map { payload ->
            BinanceCommissionRate(
                symbol = payload.requiredText("symbol"),
                makerRate = payload.requiredDecimal("makerCommissionRate"),
                takerRate = payload.requiredDecimal("takerCommissionRate"),
            )
        }
    }

    override fun startUserDataStream(): Mono<String> =
        apiKeyRequest(HttpMethod.POST, LISTEN_KEY_PATH)
            .map { payload -> payload.requiredText("listenKey") }

    override fun keepAliveUserDataStream(listenKey: String): Mono<Void> {
        require(listenKey.isNotBlank()) {
            "listenKey must not be blank"
        }
        return apiKeyRequest(
            method = HttpMethod.PUT,
            path = LISTEN_KEY_PATH,
            parameters = linkedMapOf("listenKey" to listenKey),
        ).then()
    }

    private fun signedGet(
        path: String,
        parameters: LinkedHashMap<String, String> = linkedMapOf(),
    ): Mono<JsonNode> =
        Mono.defer {
            val credentials = credentialsProvider.credentials()
            val signedParameters = LinkedHashMap(parameters)
            signedParameters["recvWindow"] = RECEIVE_WINDOW_MILLIS.toString()
            signedParameters["timestamp"] =
                (clock.millis() + serverOffsetMillis.get()).toString()
            val query = encodeQuery(signedParameters)
            val signature = BinanceRequestSigner(credentials.secret).sign(query)
            requestJson(
                method = HttpMethod.GET,
                path = "$path?$query&signature=$signature",
                apiKey = credentials.apiKey,
                sanitizedPath = path,
            )
        }

    private fun apiKeyRequest(
        method: HttpMethod,
        path: String,
        parameters: LinkedHashMap<String, String> = linkedMapOf(),
    ): Mono<JsonNode> =
        Mono.defer {
            val credentials = credentialsProvider.credentials()
            val query = encodeQuery(parameters)
            val pathWithQuery = if (query.isEmpty()) path else "$path?$query"
            requestJson(
                method = method,
                path = pathWithQuery,
                apiKey = credentials.apiKey,
                sanitizedPath = path,
            )
        }

    private fun requestJson(
        method: HttpMethod,
        path: String,
        apiKey: String? = null,
        sanitizedPath: String = path,
    ): Mono<JsonNode> {
        var request = webClient
            .method(method)
            .uri(path)
            .accept(org.springframework.http.MediaType.APPLICATION_JSON)
        if (apiKey != null) {
            request = request.header(BINANCE_API_KEY_HEADER, apiKey)
        }
        return request
            .exchangeToMono { response ->
                if (response.statusCode().is2xxSuccessful) {
                    response.bodyToMono(String::class.java)
                } else {
                    response.releaseBody().then(
                        Mono.error(
                            BinanceClientException(
                                "Binance request failed with HTTP " +
                                    "${response.statusCode().value()} for " +
                                    "$method $sanitizedPath",
                            ),
                        ),
                    )
                }
            }
            .defaultIfEmpty("{}")
            .map { body -> objectMapper.readTree(body) }
    }

    private fun parseExchangeInfo(payload: JsonNode): BinanceExchangeInfo =
        BinanceExchangeInfo(
            serverTime = Instant.ofEpochMilli(payload.requiredLong("serverTime")),
            symbols = payload.required("symbols").map(::parseSymbolMetadata),
        )

    private fun parseSymbolMetadata(payload: JsonNode): BinanceSymbolMetadata {
        val filters = payload.required("filters").associateBy { filter ->
            filter.requiredText("filterType")
        }
        return BinanceSymbolMetadata(
            symbol = payload.requiredText("symbol"),
            status = payload.requiredText("status"),
            contractType = payload.requiredText("contractType"),
            baseAsset = payload.requiredText("baseAsset"),
            quoteAsset = payload.requiredText("quoteAsset"),
            marginAsset = payload.requiredText("marginAsset"),
            pricePrecision = payload.requiredInt("pricePrecision"),
            quantityPrecision = payload.requiredInt("quantityPrecision"),
            priceFilter = filters[PRICE_FILTER]?.let { filter ->
                BinancePriceFilter(
                    minimumPrice = filter.requiredDecimal("minPrice"),
                    maximumPrice = filter.requiredDecimal("maxPrice"),
                    tickSize = filter.requiredDecimal("tickSize"),
                )
            },
            lotSizeFilter = filters[LOT_SIZE]?.toLotSizeFilter(),
            marketLotSizeFilter = filters[MARKET_LOT_SIZE]?.toLotSizeFilter(),
            minimumNotional = filters[MIN_NOTIONAL]?.requiredDecimal("notional"),
        )
    }

    private fun parseLeverageBrackets(
        payload: JsonNode,
    ): BinanceSymbolLeverageBrackets =
        BinanceSymbolLeverageBrackets(
            symbol = payload.requiredText("symbol"),
            notionalCoefficient = payload.optionalDecimal("notionalCoef")
                ?: BigDecimal.ONE,
            brackets = payload.required("brackets").map { bracket ->
                BinanceLeverageBracket(
                    bracket = bracket.requiredInt("bracket"),
                    initialLeverage = bracket.requiredInt("initialLeverage"),
                    notionalFloor = bracket.requiredDecimal("notionalFloor"),
                    notionalCap = bracket.requiredDecimal("notionalCap"),
                    maintenanceMarginRatio =
                        bracket.requiredDecimal("maintMarginRatio"),
                    cumulativeMaintenanceAmount = bracket.requiredDecimal("cum"),
                )
            },
        )

    private fun normalizedSymbol(symbol: String): String =
        symbol.trim().uppercase().also { normalized ->
            require(normalized.isNotEmpty()) {
                "symbol must not be blank"
            }
        }
}

private fun encodeQuery(parameters: Map<String, String>): String =
    parameters.entries.joinToString(separator = "&") { (name, value) ->
        "${urlEncode(name)}=${urlEncode(value)}"
    }

private fun urlEncode(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8)

private fun JsonNode.toLotSizeFilter(): BinanceLotSizeFilter =
    BinanceLotSizeFilter(
        minimumQuantity = requiredDecimal("minQty"),
        maximumQuantity = requiredDecimal("maxQty"),
        stepSize = requiredDecimal("stepSize"),
    )

private fun JsonNode.required(name: String): JsonNode =
    get(name) ?: throw BinanceClientException(
        "Binance response omitted required field $name",
    )

private fun JsonNode.requiredText(name: String): String =
    required(name).asText()

private fun JsonNode.requiredLong(name: String): Long =
    required(name).asLong()

private fun JsonNode.requiredInt(name: String): Int =
    required(name).asInt()

private fun JsonNode.requiredBoolean(name: String): Boolean =
    required(name).asBoolean()

private fun JsonNode.requiredDecimal(name: String): BigDecimal =
    required(name).asText().toBigDecimal()

private fun JsonNode.optionalDecimal(name: String): BigDecimal? =
    get(name)?.takeUnless(JsonNode::isNull)?.asText()?.toBigDecimal()

private const val LIVE_REST_BASE_URL = "https://fapi.binance.com"
private const val SERVER_TIME_PATH = "/fapi/v1/time"
private const val ACCOUNT_PATH = "/fapi/v3/account"
private const val POSITION_MODE_PATH = "/fapi/v1/positionSide/dual"
private const val ASSET_MODE_PATH = "/fapi/v1/multiAssetsMargin"
private const val EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"
private const val LEVERAGE_BRACKET_PATH = "/fapi/v1/leverageBracket"
private const val COMMISSION_RATE_PATH = "/fapi/v1/commissionRate"
private const val LISTEN_KEY_PATH = "/fapi/v1/listenKey"
private const val BINANCE_API_KEY_HEADER = "X-MBX-APIKEY"
private const val BINANCE_API_KEY_VARIABLE = "BINANCE_API_KEY"
private const val BINANCE_API_SECRET_VARIABLE = "BINANCE_API_SECRET"
private const val RECEIVE_WINDOW_MILLIS = 5_000L
private const val HMAC_SHA_256 = "HmacSHA256"
private const val PRICE_FILTER = "PRICE_FILTER"
private const val LOT_SIZE = "LOT_SIZE"
private const val MARKET_LOT_SIZE = "MARKET_LOT_SIZE"
private const val MIN_NOTIONAL = "MIN_NOTIONAL"

internal fun liveBinanceWebClient(builder: WebClient.Builder): WebClient =
    builder
        .baseUrl(LIVE_REST_BASE_URL)
        .defaultHeader(HttpHeaders.USER_AGENT, "breakout-bot/0.1")
        .build()
