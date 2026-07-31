package com.scalpsecta.breakoutbot.marketdata

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.scalpsecta.starter.service.binance.websocket.BinanceWebSocketReactive
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class BinancePublicMarketDataParserTest {
    private val receivedAt = Instant.parse("2026-07-29T10:15:30.123Z")
    private val clock = Clock.fixed(receivedAt, ZoneOffset.UTC)

    @Test
    fun `recorded aggregate trade maps buyerIsMaker false to aggressive buy`() {
        val trade = parseAggregateTrade("agg-trade-aggressive-buy.json")

        assertThat(trade).isNotNull
        assertThat(trade!!.symbol).isEqualTo("BTCUSDT")
        assertThat(trade.aggregateTradeId).isEqualTo(5_933_014L)
        assertThat(trade.eventTime)
            .isEqualTo(Instant.ofEpochMilli(1_729_785_088_750L))
        assertThat(trade.tradeTime)
            .isEqualTo(Instant.ofEpochMilli(1_729_785_088_748L))
        assertThat(trade.price)
            .isEqualByComparingTo(BigDecimal("68123.45000000"))
        assertThat(trade.quantity)
            .isEqualByComparingTo(BigDecimal("0.00300000"))
        assertThat(trade.buyerIsMaker).isFalse()
        assertThat(trade.aggressorSide).isEqualTo(AggressorSide.BUY)
        assertThat(trade.receivedAt).isEqualTo(receivedAt)
    }

    @Test
    fun `recorded aggregate trade maps buyerIsMaker true to aggressive sell`() {
        val trade = parseAggregateTrade("agg-trade-aggressive-sell.json")

        assertThat(trade).isNotNull
        assertThat(trade!!.aggregateTradeId).isEqualTo(5_933_015L)
        assertThat(trade.buyerIsMaker).isTrue()
        assertThat(trade.aggressorSide).isEqualTo(AggressorSide.SELL)
        assertThat(trade.receivedAt).isEqualTo(receivedAt)
    }

    @Test
    fun `recorded book ticker preserves prices quantities and exchange times`() {
        val parser = fixtureParser("book-ticker.json")
        val ticker = BookTickerBinanceWebSocket(clock = clock)
            .mapToClass(parser, null)

        assertThat(ticker).isNotNull
        assertThat(ticker!!.symbol).isEqualTo("BNBUSDT")
        assertThat(ticker.updateId).isEqualTo(400_900_217L)
        assertThat(ticker.eventTime)
            .isEqualTo(Instant.ofEpochMilli(1_568_014_460_893L))
        assertThat(ticker.transactionTime)
            .isEqualTo(Instant.ofEpochMilli(1_568_014_460_891L))
        assertThat(ticker.bidPrice)
            .isEqualByComparingTo(BigDecimal("25.35190000"))
        assertThat(ticker.bidQuantity)
            .isEqualByComparingTo(BigDecimal("31.21000000"))
        assertThat(ticker.askPrice)
            .isEqualByComparingTo(BigDecimal("25.36520000"))
        assertThat(ticker.askQuantity)
            .isEqualByComparingTo(BigDecimal("40.66000000"))
        assertThat(ticker.receivedAt).isEqualTo(receivedAt)
    }

    @Test
    fun `bot adapters extend starter reactive Binance infrastructure`() {
        assertThat(DetailedAggTradeBinanceWebSocket(clock = clock))
            .isInstanceOf(BinanceWebSocketReactive::class.java)
        assertThat(BookTickerBinanceWebSocket(clock = clock))
            .isInstanceOf(BinanceWebSocketReactive::class.java)
    }

    @Test
    fun `subscription request includes the identifier required by Binance`() {
        val request = ObjectMapper().readTree(
            DetailedAggTradeBinanceWebSocket(clock = clock)
                .generateSubscribeMessage(listOf("btcusdt@aggTrade"))
                .single(),
        )

        assertThat(request["method"].asText()).isEqualTo("SUBSCRIBE")
        assertThat(request["params"].single().asText())
            .isEqualTo("btcusdt@aggTrade")
        assertThat(request["id"].isTextual).isTrue()
        assertThat(request["id"].asText()).isNotBlank()
    }

    private fun parseAggregateTrade(fixtureName: String): AggregateTradeEvent? =
        DetailedAggTradeBinanceWebSocket(clock = clock)
            .mapToClass(fixtureParser(fixtureName), null)

    private fun fixtureParser(fixtureName: String): JsonParser {
        val resource = requireNotNull(
            javaClass.getResource("/binance/$fixtureName"),
        )
        return ObjectMapper()
            .factory
            .createParser(resource)
            .also(JsonParser::nextToken)
    }
}
