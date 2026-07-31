package com.scalpsecta.breakoutbot.marketdata

import com.fasterxml.jackson.core.JsonParser
import com.scalpsecta.liner.dto.TradingType
import com.scalpsecta.starter.autoconfigure.BinanceConfiguration
import com.scalpsecta.starter.service.binance.websocket.BinanceWebSocketReactive
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

class DetailedAggTradeBinanceWebSocket(
    baseUrl: String = BinanceConfiguration.WS_BASE_URL_FUTURES_MARKET,
    private val clock: Clock = Clock.systemUTC(),
) : BinanceWebSocketReactive<AggregateTradeEvent>(
    baseUrl,
    TradingType.FUTURES,
    Long.MAX_VALUE,
) {
    override fun mapToClass(
        parser: JsonParser,
        symbol: String?,
    ): AggregateTradeEvent? {
        val receivedAt = clock.instant()
        var parsedSymbol: String? = null
        var aggregateTradeId: Long? = null
        var eventTime: Instant? = null
        var tradeTime: Instant? = null
        var price: BigDecimal? = null
        var quantity: BigDecimal? = null
        var buyerIsMaker: Boolean? = null
        var symbolType: Int? = null

        while (parser.nextToken() != null) {
            val fieldName = parser.currentName() ?: continue
            parser.nextToken()
            when (fieldName) {
                "s" -> parsedSymbol = parser.valueAsString.uppercase()
                "a" -> aggregateTradeId = parser.longValue
                "E" -> eventTime = Instant.ofEpochMilli(parser.longValue)
                "T" -> tradeTime = Instant.ofEpochMilli(parser.longValue)
                "p" -> price = parser.valueAsString.toBigDecimal()
                "q" -> quantity = parser.valueAsString.toBigDecimal()
                "m" -> buyerIsMaker = parser.booleanValue
                "st" -> symbolType = parser.intValue
                else -> parser.skipChildren()
            }
        }

        if (symbolType != null && symbolType != USD_M_SYMBOL_TYPE) {
            return null
        }

        return if (
            parsedSymbol != null &&
            aggregateTradeId != null &&
            eventTime != null &&
            tradeTime != null &&
            price != null &&
            quantity != null &&
            buyerIsMaker != null
        ) {
            AggregateTradeEvent(
                symbol = parsedSymbol,
                aggregateTradeId = aggregateTradeId,
                eventTime = eventTime,
                tradeTime = tradeTime,
                price = price,
                quantity = quantity,
                buyerIsMaker = buyerIsMaker,
                aggressorSide = if (buyerIsMaker) {
                    AggressorSide.SELL
                } else {
                    AggressorSide.BUY
                },
                receivedAt = receivedAt,
            )
        } else {
            null
        }
    }

    override fun generateSubscribeMessage(
        streamName: List<String>,
    ): Array<String> =
        arrayOf(binanceSubscribeMessage(streamName))

    private companion object {
        private const val USD_M_SYMBOL_TYPE = 1
    }
}

internal fun binanceSubscribeMessage(streamNames: List<String>): String {
    val params = streamNames.joinToString(",") { streamName ->
        "\"$streamName\""
    }
    return """{"method":"SUBSCRIBE","params":[$params],"id":"${subscriptionId.incrementAndGet()}"}"""
}

private val subscriptionId = AtomicLong()
