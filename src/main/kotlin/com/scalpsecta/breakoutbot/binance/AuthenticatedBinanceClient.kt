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
import java.util.Optional
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

    fun markPrice(symbol: String): Mono<BigDecimal>

    fun symbolConfiguration(symbol: String): Mono<BinanceSymbolConfiguration>

    fun changeMarginType(
        symbol: String,
        marginType: BinanceMarginType,
    ): Mono<Void>

    fun changeInitialLeverage(symbol: String, leverage: Int): Mono<Void>

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
) : AuthenticatedBinanceClient, BinanceExecutionClient {
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

    override fun markPrice(symbol: String): Mono<BigDecimal> {
        val normalizedSymbol = normalizedSymbol(symbol)
        return requestJson(
            method = HttpMethod.GET,
            path = "$MARK_PRICE_PATH?symbol=${urlEncode(normalizedSymbol)}",
            sanitizedPath = MARK_PRICE_PATH,
        ).map { payload -> payload.requiredDecimal("markPrice") }
    }

    override fun symbolConfiguration(
        symbol: String,
    ): Mono<BinanceSymbolConfiguration> {
        val normalizedSymbol = normalizedSymbol(symbol)
        return signedGet(
            path = SYMBOL_CONFIGURATION_PATH,
            parameters = linkedMapOf("symbol" to normalizedSymbol),
        ).map { payload ->
            val configuration = if (payload.isArray) {
                payload.firstOrNull { candidate ->
                    candidate.requiredText("symbol") == normalizedSymbol
                } ?: throw BinanceClientException(
                    "Binance returned no symbol configuration for $normalizedSymbol",
                )
            } else {
                payload
            }
            BinanceSymbolConfiguration(
                symbol = configuration.requiredText("symbol"),
                marginType = parseMarginType(
                    configuration.requiredText("marginType"),
                ),
                autoAddMargin = configuration.requiredBoolean("isAutoAddMargin"),
                leverage = configuration.requiredInt("leverage"),
                maximumNotional =
                    configuration.requiredDecimal("maxNotionalValue"),
            )
        }
    }

    override fun changeMarginType(
        symbol: String,
        marginType: BinanceMarginType,
    ): Mono<Void> {
        val normalizedSymbol = normalizedSymbol(symbol)
        return signedRequest(
            method = HttpMethod.POST,
            path = MARGIN_TYPE_PATH,
            parameters = linkedMapOf(
                "symbol" to normalizedSymbol,
                "marginType" to marginType.name,
            ),
        ).then()
    }

    override fun changeInitialLeverage(
        symbol: String,
        leverage: Int,
    ): Mono<Void> {
        require(leverage in 1..MAX_BINANCE_LEVERAGE) {
            "leverage must be between 1 and $MAX_BINANCE_LEVERAGE"
        }
        val normalizedSymbol = normalizedSymbol(symbol)
        return signedRequest(
            method = HttpMethod.POST,
            path = LEVERAGE_PATH,
            parameters = linkedMapOf(
                "symbol" to normalizedSymbol,
                "leverage" to leverage.toString(),
            ),
        ).then()
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

    override fun placeOrder(
        request: BinanceOrderRequest,
    ): Mono<BinanceOrderAcknowledgement> {
        validateOrderRequest(request)
        val parameters = linkedMapOf(
            "symbol" to normalizedSymbol(request.symbol),
            "side" to request.side,
            "type" to request.type,
        )
        request.timeInForce?.let { parameters["timeInForce"] = it }
        request.quantity?.let {
            parameters["quantity"] = it.toPlainString()
        }
        request.price?.let { parameters["price"] = it.toPlainString() }
        request.stopPrice?.let {
            parameters["stopPrice"] = it.toPlainString()
        }
        request.workingType?.let {
            parameters["workingType"] = it
        }
        request.priceProtect?.let {
            parameters["priceProtect"] = it.toString()
        }
        parameters["newClientOrderId"] = request.clientOrderId
        if (request.reduceOnly) {
            parameters["reduceOnly"] = true.toString()
        }
        if (request.closePosition) {
            parameters["closePosition"] = true.toString()
        }
        parameters["newOrderRespType"] = "ACK"
        return signedRequest(
            method = HttpMethod.POST,
            path = ORDER_PATH,
            parameters = parameters,
        ).map { payload ->
            BinanceOrderAcknowledgement(
                symbol = payload.requiredText("symbol"),
                clientOrderId = payload.requiredText("clientOrderId"),
                orderId = payload.requiredLong("orderId"),
                status = payload.requiredText("status"),
            )
        }
    }

    override fun cancelOrder(
        symbol: String,
        clientOrderId: String,
    ): Mono<Void> {
        val normalizedSymbol = normalizedSymbol(symbol)
        require(clientOrderId.matches(BINANCE_CLIENT_ORDER_ID)) {
            "clientOrderId must be Binance-safe"
        }
        return signedRequest(
            method = HttpMethod.DELETE,
            path = ORDER_PATH,
            parameters = linkedMapOf(
                "symbol" to normalizedSymbol,
                "origClientOrderId" to clientOrderId,
            ),
        ).then()
    }

    override fun reconcileOrder(
        symbol: String,
        clientOrderId: String,
    ): Mono<BinanceOrderReconciliation> {
        val normalizedSymbol = normalizedSymbol(symbol)
        require(clientOrderId.matches(BINANCE_CLIENT_ORDER_ID)) {
            "clientOrderId must be Binance-safe"
        }
        val order = signedGet(
            path = ORDER_PATH,
            parameters = linkedMapOf(
                "symbol" to normalizedSymbol,
                "origClientOrderId" to clientOrderId,
            ),
        )
            .map(::parseOrderStatus)
            .map(Optional<BinanceOrderStatus>::of)
            .onErrorReturn(Optional.empty())
        val position = signedGet(
            path = POSITION_RISK_PATH,
            parameters = linkedMapOf("symbol" to normalizedSymbol),
        ).map { payload -> parsePositionRisk(payload, normalizedSymbol) }
        val openOrders = signedGet(
            path = OPEN_ORDERS_PATH,
            parameters = linkedMapOf("symbol" to normalizedSymbol),
        ).map { payload ->
            if (payload.isArray) {
                payload.map { orderPayload ->
                    orderPayload.requiredText("clientOrderId")
                }.toSet()
            } else {
                emptySet()
            }
        }
        return Mono.zip(order, position, openOrders)
            .map { result ->
                BinanceOrderReconciliation(
                    order = result.t1.orElse(null),
                    position = result.t2,
                    openClientOrderIds = result.t3,
                )
            }
    }

    override fun reconcileAccount(): Mono<BinanceAccountReconciliation> {
        val positions = signedGet(POSITION_RISK_PATH)
            .map { payload ->
                if (payload.isArray) {
                    payload.mapNotNull(::parsePositionRisk)
                } else {
                    emptyList()
                }
            }
        val openOrders = signedGet(OPEN_ORDERS_PATH)
            .map { payload ->
                if (payload.isArray) {
                    payload.map(::parseOrderStatus)
                } else {
                    emptyList()
                }
            }
        return Mono.zip(positions, openOrders)
            .map { result ->
                BinanceAccountReconciliation(
                    positions = result.t1,
                    openOrders = result.t2,
                )
            }
    }

    private fun signedGet(
        path: String,
        parameters: LinkedHashMap<String, String> = linkedMapOf(),
    ): Mono<JsonNode> = signedRequest(HttpMethod.GET, path, parameters)

    private fun signedRequest(
        method: HttpMethod,
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
                method = method,
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
            minimumNotional =
                filters[MIN_NOTIONAL]?.requiredDecimal("notional")
                    ?: filters[NOTIONAL]?.requiredDecimal("minNotional"),
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

    private fun parseOrderStatus(payload: JsonNode): BinanceOrderStatus =
        BinanceOrderStatus(
            symbol = payload.requiredText("symbol"),
            clientOrderId = payload.requiredText("clientOrderId"),
            orderId = payload.requiredLong("orderId"),
            status = payload.requiredText("status"),
            originalQuantity = payload.requiredDecimal("origQty"),
            executedQuantity = payload.requiredDecimal("executedQty"),
            averagePrice = payload.requiredDecimal("avgPrice"),
            reduceOnly = payload.requiredBoolean("reduceOnly"),
            closePosition = payload.requiredBoolean("closePosition"),
            updatedAt = Instant.ofEpochMilli(payload.requiredLong("updateTime")),
            type = payload.optionalText("type"),
            side = payload.optionalText("side"),
            timeInForce = payload.optionalText("timeInForce"),
            price = payload.optionalDecimal("price"),
            stopPrice = payload.optionalDecimal("stopPrice"),
            workingType = payload.optionalText("workingType"),
            priceProtect = payload.optionalBoolean("priceProtect"),
        )

    private fun parsePositionRisk(
        payload: JsonNode,
        symbol: String,
    ): BinancePositionRisk {
        val position = payload
            .takeIf(JsonNode::isArray)
            ?.firstOrNull { candidate ->
                candidate.requiredText("symbol") == symbol &&
                    candidate.requiredText("positionSide") == "BOTH"
            }
        return if (position == null) {
            BinancePositionRisk(
                symbol = symbol,
                positionAmount = BigDecimal.ZERO,
                entryPrice = BigDecimal.ZERO,
            )
        } else {
            BinancePositionRisk(
                symbol = symbol,
                positionAmount = position.requiredDecimal("positionAmt"),
                entryPrice = position.requiredDecimal("entryPrice"),
                notional = position.optionalDecimal("notional"),
                unrealizedProfit = position.optionalDecimal("unRealizedProfit"),
            )
        }
    }

    private fun parsePositionRisk(payload: JsonNode): BinancePositionRisk? {
        if (payload.requiredText("positionSide") != "BOTH") {
            return null
        }
        return BinancePositionRisk(
            symbol = payload.requiredText("symbol"),
            positionAmount = payload.requiredDecimal("positionAmt"),
            entryPrice = payload.requiredDecimal("entryPrice"),
            notional = payload.optionalDecimal("notional"),
            unrealizedProfit = payload.optionalDecimal("unRealizedProfit"),
        )
    }

    private fun validateOrderRequest(request: BinanceOrderRequest) {
        require(request.clientOrderId.matches(BINANCE_CLIENT_ORDER_ID)) {
            "clientOrderId must be Binance-safe"
        }
        require(request.quantity?.signum()?.let { it > 0 } != false) {
            "quantity must be positive"
        }
        require(request.price?.signum()?.let { it > 0 } != false) {
            "price must be positive"
        }
        require(request.stopPrice?.signum()?.let { it > 0 } != false) {
            "stopPrice must be positive"
        }
        require(!request.closePosition || request.quantity == null) {
            "close-position orders must not specify quantity"
        }
        require(!request.closePosition || !request.reduceOnly) {
            "close-position orders must not also be reduce-only"
        }
        require(request.workingType == null || request.stopPrice != null) {
            "workingType requires stopPrice"
        }
        require(request.priceProtect == null || request.stopPrice != null) {
            "priceProtect requires stopPrice"
        }
    }

    private fun normalizedSymbol(symbol: String): String =
        symbol.trim().uppercase().also { normalized ->
            require(normalized.isNotEmpty()) {
                "symbol must not be blank"
            }
        }

    private fun parseMarginType(value: String): BinanceMarginType =
        when (value.uppercase()) {
            "ISOLATED" -> BinanceMarginType.ISOLATED
            "CROSSED", "CROSS" -> BinanceMarginType.CROSSED
            else -> throw BinanceClientException(
                "Binance returned an unsupported margin type",
            )
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

private fun JsonNode.optionalText(name: String): String? =
    get(name)?.takeUnless(JsonNode::isNull)?.asText()

private fun JsonNode.optionalBoolean(name: String): Boolean? =
    get(name)?.takeUnless(JsonNode::isNull)?.asBoolean()

private const val LIVE_REST_BASE_URL = "https://fapi.binance.com"
private const val SERVER_TIME_PATH = "/fapi/v1/time"
private const val ACCOUNT_PATH = "/fapi/v3/account"
private const val POSITION_MODE_PATH = "/fapi/v1/positionSide/dual"
private const val ASSET_MODE_PATH = "/fapi/v1/multiAssetsMargin"
private const val EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"
private const val LEVERAGE_BRACKET_PATH = "/fapi/v1/leverageBracket"
private const val COMMISSION_RATE_PATH = "/fapi/v1/commissionRate"
private const val MARK_PRICE_PATH = "/fapi/v1/premiumIndex"
private const val SYMBOL_CONFIGURATION_PATH = "/fapi/v1/symbolConfig"
private const val MARGIN_TYPE_PATH = "/fapi/v1/marginType"
private const val LEVERAGE_PATH = "/fapi/v1/leverage"
private const val LISTEN_KEY_PATH = "/fapi/v1/listenKey"
private const val ORDER_PATH = "/fapi/v1/order"
private const val OPEN_ORDERS_PATH = "/fapi/v1/openOrders"
private const val POSITION_RISK_PATH = "/fapi/v3/positionRisk"
private const val BINANCE_API_KEY_HEADER = "X-MBX-APIKEY"
private const val BINANCE_API_KEY_VARIABLE = "BINANCE_API_KEY"
private const val BINANCE_API_SECRET_VARIABLE = "BINANCE_API_SECRET"
private const val RECEIVE_WINDOW_MILLIS = 5_000L
private const val HMAC_SHA_256 = "HmacSHA256"
private const val PRICE_FILTER = "PRICE_FILTER"
private const val LOT_SIZE = "LOT_SIZE"
private const val MARKET_LOT_SIZE = "MARKET_LOT_SIZE"
private const val MIN_NOTIONAL = "MIN_NOTIONAL"
private const val NOTIONAL = "NOTIONAL"
private const val MAX_BINANCE_LEVERAGE = 125
private val BINANCE_CLIENT_ORDER_ID = Regex("^[.A-Za-z0-9_:/-]{1,36}$")

internal fun liveBinanceWebClient(builder: WebClient.Builder): WebClient =
    builder
        .baseUrl(LIVE_REST_BASE_URL)
        .defaultHeader(HttpHeaders.USER_AGENT, "breakout-bot/0.1")
        .build()
