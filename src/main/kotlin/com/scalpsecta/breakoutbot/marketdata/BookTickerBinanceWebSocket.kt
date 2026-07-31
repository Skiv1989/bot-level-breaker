package com.scalpsecta.breakoutbot.marketdata

import com.fasterxml.jackson.core.JsonParser
import com.scalpsecta.liner.dto.TradingType
import com.scalpsecta.starter.autoconfigure.BinanceConfiguration
import com.scalpsecta.starter.service.binance.websocket.BinanceWebSocketReactive
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant

class BookTickerBinanceWebSocket(
    baseUrl: String = BinanceConfiguration.WS_BASE_URL_FUTURES_PUBLIC,
    private val clock: Clock = Clock.systemUTC(),
) : BinanceWebSocketReactive<BookTickerEvent>(
    baseUrl,
    TradingType.FUTURES,
    BOOK_TICKER_RECONNECT_STALE_MILLIS,
) {
    override fun mapToClass(
        parser: JsonParser,
        symbol: String?,
    ): BookTickerEvent? {
        val receivedAt = clock.instant()
        var parsedSymbol: String? = null
        var updateId: Long? = null
        var eventTime: Instant? = null
        var transactionTime: Instant? = null
        var bidPrice: BigDecimal? = null
        var bidQuantity: BigDecimal? = null
        var askPrice: BigDecimal? = null
        var askQuantity: BigDecimal? = null
        var symbolType: Int? = null

        while (parser.nextToken() != null) {
            val fieldName = parser.currentName() ?: continue
            parser.nextToken()
            when (fieldName) {
                "s" -> parsedSymbol = parser.valueAsString.uppercase()
                "u" -> updateId = parser.longValue
                "E" -> eventTime = Instant.ofEpochMilli(parser.longValue)
                "T" -> transactionTime = Instant.ofEpochMilli(parser.longValue)
                "b" -> bidPrice = parser.valueAsString.toBigDecimal()
                "B" -> bidQuantity = parser.valueAsString.toBigDecimal()
                "a" -> askPrice = parser.valueAsString.toBigDecimal()
                "A" -> askQuantity = parser.valueAsString.toBigDecimal()
                "st" -> symbolType = parser.intValue
                else -> parser.skipChildren()
            }
        }

        if (symbolType != null && symbolType != USD_M_SYMBOL_TYPE) {
            return null
        }

        return if (
            parsedSymbol != null &&
            updateId != null &&
            bidPrice != null &&
            bidQuantity != null &&
            askPrice != null &&
            askQuantity != null
        ) {
            BookTickerEvent(
                symbol = parsedSymbol,
                updateId = updateId,
                eventTime = eventTime,
                transactionTime = transactionTime,
                bidPrice = bidPrice,
                bidQuantity = bidQuantity,
                askPrice = askPrice,
                askQuantity = askQuantity,
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
        private const val BOOK_TICKER_RECONNECT_STALE_MILLIS = 3_000L
        private const val USD_M_SYMBOL_TYPE = 1
    }
}
