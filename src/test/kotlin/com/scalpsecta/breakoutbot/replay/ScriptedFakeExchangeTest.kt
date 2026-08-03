package com.scalpsecta.breakoutbot.replay

import com.scalpsecta.breakoutbot.binance.BinanceAccountReconciliation
import com.scalpsecta.breakoutbot.binance.BinanceOrderAcknowledgement
import com.scalpsecta.breakoutbot.binance.BinanceOrderReconciliation
import com.scalpsecta.breakoutbot.binance.BinanceOrderRequest
import com.scalpsecta.breakoutbot.binance.BinanceOrderStatus
import com.scalpsecta.breakoutbot.binance.BinancePositionRisk
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import reactor.test.StepVerifier
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant

class ScriptedFakeExchangeTest {
    @Test
    fun `scripts fills partial fills stop and TP state without transport`() {
        ScriptedFakeExchange().use { exchange ->
            val request = orderRequest()
            exchange.scriptPlacement(
                ScriptedExchangeResult.Success(
                    BinanceOrderAcknowledgement(
                        symbol = SYMBOL,
                        clientOrderId = request.clientOrderId,
                        orderId = 41L,
                        status = "NEW",
                    ),
                ),
            )
            exchange.scriptOrderReconciliation(
                ScriptedExchangeResult.Success(
                    reconciliation(
                        request = request,
                        status = "PARTIALLY_FILLED",
                        filledQuantity = "0.008",
                    ),
                ),
            )
            exchange.scriptOrderReconciliation(
                ScriptedExchangeResult.Success(
                    reconciliation(
                        request = request.copy(
                            type = "STOP_MARKET",
                            stopPrice = BigDecimal("99.0"),
                            closePosition = true,
                        ),
                        status = "NEW",
                        filledQuantity = "0",
                    ),
                ),
            )
            exchange.scriptAccountReconciliation(
                ScriptedExchangeResult.Success(
                    BinanceAccountReconciliation(
                        positions = emptyList(),
                        openOrders = emptyList(),
                    ),
                ),
            )

            assertThat(exchange.placeOrder(request).block()!!.orderId).isEqualTo(41L)
            assertThat(
                exchange.reconcileOrder(SYMBOL, request.clientOrderId)
                    .block()!!.order!!.executedQuantity,
            ).isEqualByComparingTo(BigDecimal("0.008"))
            assertThat(
                exchange.reconcileOrder(SYMBOL, request.clientOrderId)
                    .block()!!.order!!.stopPrice,
            ).isEqualByComparingTo(BigDecimal("99.0"))
            assertThat(exchange.reconcileAccount().block()!!.positions).isEmpty()
            assertThat(exchange.placements).containsExactly(request)
            exchange.assertExhausted()
        }
    }

    @Test
    fun `scripts timeout rejection unknown reconciliation and stream outages`() {
        ScriptedFakeExchange().use { exchange ->
            exchange.scriptPlacement(ScriptedExchangeResult.Timeout)
            StepVerifier.create(exchange.placeOrder(orderRequest()))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(1))
                .thenCancel()
                .verify()

            exchange.scriptPlacement(
                ScriptedExchangeResult.Rejection("POST_ONLY_REJECTED"),
            )
            assertThatThrownBy { exchange.placeOrder(orderRequest()).block() }
                .isInstanceOf(FakeExchangeRejectionException::class.java)

            exchange.scriptOrderReconciliation(
                ScriptedExchangeResult.Success(
                    BinanceOrderReconciliation(
                        order = null,
                        position = null,
                        openClientOrderIds = setOf("unknown-order"),
                        safeDetail = "UNKNOWN_OUTCOME",
                    ),
                ),
            )
            assertThat(
                exchange.reconcileOrder(SYMBOL, "unknown-order")
                    .block()!!.safeDetail,
            ).isEqualTo("UNKNOWN_OUTCOME")

            exchange.setPublicStreamConnected(SYMBOL, false)
            exchange.setPrivateStreamConnected(false)
            assertThat(exchange.streamAvailability().publicSymbols[SYMBOL]).isFalse()
            assertThat(exchange.streamAvailability().privateStreamConnected).isFalse()
            exchange.setPublicStreamConnected(SYMBOL, true)
            exchange.setPrivateStreamConnected(true)
            assertThat(exchange.streamAvailability().publicSymbols[SYMBOL]).isTrue()
            assertThat(exchange.streamAvailability().privateStreamConnected).isTrue()
            exchange.assertExhausted()
        }
    }

    @Test
    fun `unexpected operation fails closed instead of selecting live transport`() {
        ScriptedFakeExchange().use { exchange ->
            assertThatThrownBy { exchange.placeOrder(orderRequest()).block() }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessage(
                    "Unexpected fake exchange order placement; no script remains",
                )
        }
    }

    private fun orderRequest(): BinanceOrderRequest = BinanceOrderRequest(
        symbol = SYMBOL,
        clientOrderId = "replay-order",
        side = "BUY",
        type = "LIMIT",
        timeInForce = "IOC",
        quantity = BigDecimal("0.010"),
        price = BigDecimal("100.0"),
    )

    private fun reconciliation(
        request: BinanceOrderRequest,
        status: String,
        filledQuantity: String,
    ): BinanceOrderReconciliation = BinanceOrderReconciliation(
        order = BinanceOrderStatus(
            symbol = request.symbol,
            clientOrderId = request.clientOrderId,
            orderId = 41L,
            status = status,
            originalQuantity = request.quantity ?: BigDecimal.ZERO,
            executedQuantity = BigDecimal(filledQuantity),
            averagePrice = request.price ?: BigDecimal.ZERO,
            reduceOnly = request.reduceOnly,
            closePosition = request.closePosition,
            updatedAt = Instant.parse("2026-08-03T09:00:00Z"),
            type = request.type,
            side = request.side,
            timeInForce = request.timeInForce,
            price = request.price,
            stopPrice = request.stopPrice,
        ),
        position = BinancePositionRisk(
            symbol = SYMBOL,
            positionAmount = BigDecimal("0.008"),
            entryPrice = BigDecimal("100.0"),
        ),
        openClientOrderIds = setOf(request.clientOrderId),
    )
}

private const val SYMBOL = "BTCUSDT"
