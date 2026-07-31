package com.scalpsecta.breakoutbot.binance

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class BinanceUserDataEventParserTest {
    private val receivedAt = Instant.parse("2026-07-31T11:12:13.456Z")
    private val parser = BinanceUserDataEventParser(
        objectMapper = ObjectMapper(),
        clock = Clock.fixed(receivedAt, ZoneOffset.UTC),
    )

    @Test
    fun `recorded account update preserves balances positions and timestamps`() {
        val event = parser.parse(fixture("account-update.json"))

        assertThat(event).isInstanceOf(BinanceUserDataEvent.AccountUpdate::class.java)
        val account = event as BinanceUserDataEvent.AccountUpdate
        assertThat(account.eventTime)
            .isEqualTo(Instant.ofEpochMilli(1_564_745_798_939L))
        assertThat(account.transactionTime)
            .isEqualTo(Instant.ofEpochMilli(1_564_745_798_938L))
        assertThat(account.receivedAt).isEqualTo(receivedAt)
        assertThat(account.reason).isEqualTo("ORDER")

        val balance = account.balances.single()
        assertThat(balance.asset).isEqualTo("USDT")
        assertThat(balance.walletBalance)
            .isEqualByComparingTo(BigDecimal("122624.12345678"))
        assertThat(balance.crossWalletBalance)
            .isEqualByComparingTo(BigDecimal("100000.12345678"))
        assertThat(balance.balanceChange)
            .isEqualByComparingTo(BigDecimal("-1.25000000"))

        val position = account.positions.single()
        assertThat(position.symbol).isEqualTo("BTCUSDT")
        assertThat(position.positionAmount)
            .isEqualByComparingTo(BigDecimal("0.00300000"))
        assertThat(position.entryPrice)
            .isEqualByComparingTo(BigDecimal("68123.45000000"))
        assertThat(position.breakEvenPrice)
            .isEqualByComparingTo(BigDecimal("68136.12345678"))
        assertThat(position.accumulatedRealizedProfit)
            .isEqualByComparingTo(BigDecimal("12.34567890"))
        assertThat(position.unrealizedProfit)
            .isEqualByComparingTo(BigDecimal("3.21098765"))
        assertThat(position.marginType).isEqualTo("isolated")
        assertThat(position.isolatedWallet)
            .isEqualByComparingTo(BigDecimal("50.00000000"))
        assertThat(position.positionSide).isEqualTo("BOTH")
    }

    @Test
    fun `recorded order update preserves identifiers quantities prices and timestamps`() {
        val event = parser.parse(fixture("order-trade-update.json"))

        assertThat(event).isInstanceOf(BinanceUserDataEvent.OrderUpdate::class.java)
        val order = event as BinanceUserDataEvent.OrderUpdate
        assertThat(order.eventTime)
            .isEqualTo(Instant.ofEpochMilli(1_568_879_465_651L))
        assertThat(order.transactionTime)
            .isEqualTo(Instant.ofEpochMilli(1_568_879_465_650L))
        assertThat(order.receivedAt).isEqualTo(receivedAt)
        assertThat(order.symbol).isEqualTo("BTCUSDT")
        assertThat(order.clientOrderId).isEqualTo("lvl-42-entry-1")
        assertThat(order.orderId).isEqualTo(8_886_774L)
        assertThat(order.tradeId).isEqualTo(109_100_866L)
        assertThat(order.side).isEqualTo("BUY")
        assertThat(order.orderType).isEqualTo("LIMIT")
        assertThat(order.timeInForce).isEqualTo("IOC")
        assertThat(order.executionType).isEqualTo("TRADE")
        assertThat(order.orderStatus).isEqualTo("PARTIALLY_FILLED")
        assertThat(order.originalQuantity)
            .isEqualByComparingTo(BigDecimal("0.00300000"))
        assertThat(order.originalPrice)
            .isEqualByComparingTo(BigDecimal("68123.45000000"))
        assertThat(order.averagePrice)
            .isEqualByComparingTo(BigDecimal("68124.12000000"))
        assertThat(order.lastFilledQuantity)
            .isEqualByComparingTo(BigDecimal("0.00200000"))
        assertThat(order.accumulatedFilledQuantity)
            .isEqualByComparingTo(BigDecimal("0.00200000"))
        assertThat(order.lastFilledPrice)
            .isEqualByComparingTo(BigDecimal("68124.12000000"))
        assertThat(order.commissionAsset).isEqualTo("USDT")
        assertThat(order.commission)
            .isEqualByComparingTo(BigDecimal("0.05449930"))
        assertThat(order.realizedProfit)
            .isEqualByComparingTo(BigDecimal("1.23456789"))
        assertThat(order.positionSide).isEqualTo("BOTH")
        assertThat(order.reduceOnly).isFalse()
    }

    private fun fixture(name: String): String =
        requireNotNull(javaClass.getResource("/binance/$name")).readText()
}
