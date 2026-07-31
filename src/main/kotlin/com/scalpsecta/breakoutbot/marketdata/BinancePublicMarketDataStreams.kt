package com.scalpsecta.breakoutbot.marketdata

import com.scalpsecta.liner.dto.ExchangeName
import com.scalpsecta.liner.dto.TradingType
import com.scalpsecta.starter.autoconfigure.BinanceConfiguration
import com.scalpsecta.starter.service.LinerReactiveWebSocket
import com.scalpsecta.starter.service.LinerReactiveWebSocketPool
import reactor.core.publisher.Flux
import java.time.Clock

interface PublicMarketDataStreamProvider {
    fun aggregateTrades(symbol: String): Flux<AggregateTradeEvent>

    fun bookTickers(symbol: String): Flux<BookTickerEvent>
}

class BinancePublicMarketDataStreamProvider(
    private val aggregateTradePool: DetailedAggTradeBinanceWebSocketPool,
    private val bookTickerPool: BookTickerBinanceWebSocketPool,
) : PublicMarketDataStreamProvider {
    override fun aggregateTrades(symbol: String): Flux<AggregateTradeEvent> =
        aggregateTradePool.openStream(symbol)

    override fun bookTickers(symbol: String): Flux<BookTickerEvent> =
        bookTickerPool.openStream(symbol)
}

class DetailedAggTradeBinanceWebSocketPool(
    private val clock: Clock = Clock.systemUTC(),
    private val baseUrl: String = BinanceConfiguration.WS_BASE_URL_FUTURES_MARKET,
) : LinerReactiveWebSocketPool<AggregateTradeEvent>(
    ExchangeName.BINANCE,
    TradingType.FUTURES,
    MAX_STREAMS_PER_WEBSOCKET,
) {
    override fun getStreamType(): String = AGGREGATE_TRADE_STREAM

    override fun createWebSocket(
        tradingType: TradingType,
    ): LinerReactiveWebSocket<AggregateTradeEvent> =
        DetailedAggTradeBinanceWebSocket(baseUrl, clock)
}

class BookTickerBinanceWebSocketPool(
    private val clock: Clock = Clock.systemUTC(),
    private val baseUrl: String = BinanceConfiguration.WS_BASE_URL_FUTURES_PUBLIC,
) : LinerReactiveWebSocketPool<BookTickerEvent>(
    ExchangeName.BINANCE,
    TradingType.FUTURES,
    MAX_STREAMS_PER_WEBSOCKET,
) {
    override fun getStreamType(): String = BOOK_TICKER_STREAM

    override fun createWebSocket(
        tradingType: TradingType,
    ): LinerReactiveWebSocket<BookTickerEvent> =
        BookTickerBinanceWebSocket(baseUrl, clock)
}

private const val AGGREGATE_TRADE_STREAM = "aggTrade"
private const val BOOK_TICKER_STREAM = "bookTicker"
private const val MAX_STREAMS_PER_WEBSOCKET = 100
