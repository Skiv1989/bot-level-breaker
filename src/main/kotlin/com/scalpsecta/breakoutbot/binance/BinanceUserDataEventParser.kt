package com.scalpsecta.breakoutbot.binance

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant

class BinanceUserDataEventParser(
    private val objectMapper: ObjectMapper,
    private val clock: Clock,
) {
    fun parse(payload: String): BinanceUserDataEvent? {
        val root = objectMapper.readTree(payload)
        val receivedAt = clock.instant()
        return when (root.requiredText("e")) {
            ACCOUNT_UPDATE_EVENT -> parseAccountUpdate(root, receivedAt)
            ORDER_UPDATE_EVENT -> parseOrderUpdate(root, receivedAt)
            LISTEN_KEY_EXPIRED_EVENT ->
                BinanceUserDataEvent.ListenKeyExpired(
                    eventTime = root.requiredInstant("E"),
                    receivedAt = receivedAt,
                )
            else -> null
        }
    }

    private fun parseAccountUpdate(
        root: JsonNode,
        receivedAt: Instant,
    ): BinanceUserDataEvent.AccountUpdate {
        val account = root.required("a")
        return BinanceUserDataEvent.AccountUpdate(
            eventTime = root.requiredInstant("E"),
            transactionTime = root.requiredInstant("T"),
            receivedAt = receivedAt,
            reason = account.requiredText("m"),
            balances = account.optionalArray("B").map { balance ->
                BinanceBalanceUpdate(
                    asset = balance.requiredText("a"),
                    walletBalance = balance.requiredDecimal("wb"),
                    crossWalletBalance = balance.requiredDecimal("cw"),
                    balanceChange = balance.requiredDecimal("bc"),
                )
            },
            positions = account.optionalArray("P").map { position ->
                BinancePositionUpdate(
                    symbol = position.requiredText("s"),
                    positionAmount = position.requiredDecimal("pa"),
                    entryPrice = position.requiredDecimal("ep"),
                    breakEvenPrice = position.optionalDecimal("bep"),
                    accumulatedRealizedProfit = position.requiredDecimal("cr"),
                    unrealizedProfit = position.requiredDecimal("up"),
                    marginType = position.requiredText("mt"),
                    isolatedWallet = position.requiredDecimal("iw"),
                    positionSide = position.requiredText("ps"),
                )
            },
        )
    }

    private fun parseOrderUpdate(
        root: JsonNode,
        receivedAt: Instant,
    ): BinanceUserDataEvent.OrderUpdate {
        val order = root.required("o")
        return BinanceUserDataEvent.OrderUpdate(
            eventTime = root.requiredInstant("E"),
            transactionTime = root.requiredInstant("T"),
            receivedAt = receivedAt,
            symbol = order.requiredText("s"),
            clientOrderId = order.requiredText("c"),
            side = order.requiredText("S"),
            orderType = order.requiredText("o"),
            timeInForce = order.requiredText("f"),
            originalQuantity = order.requiredDecimal("q"),
            originalPrice = order.requiredDecimal("p"),
            averagePrice = order.requiredDecimal("ap"),
            stopPrice = order.requiredDecimal("sp"),
            executionType = order.requiredText("x"),
            orderStatus = order.requiredText("X"),
            orderId = order.requiredLong("i"),
            lastFilledQuantity = order.requiredDecimal("l"),
            accumulatedFilledQuantity = order.requiredDecimal("z"),
            lastFilledPrice = order.requiredDecimal("L"),
            commissionAsset = order.optionalText("N"),
            commission = order.optionalDecimal("n"),
            tradeId = order.requiredLong("t"),
            realizedProfit = order.requiredDecimal("rp"),
            positionSide = order.requiredText("ps"),
            reduceOnly = order.requiredBoolean("R"),
        )
    }
}

private fun JsonNode.required(name: String): JsonNode =
    get(name) ?: throw BinanceClientException(
        "Binance user-data event omitted required field $name",
    )

private fun JsonNode.requiredText(name: String): String =
    required(name).asText()

private fun JsonNode.optionalText(name: String): String? =
    get(name)
        ?.takeUnless(JsonNode::isNull)
        ?.asText()
        ?.takeIf(String::isNotEmpty)

private fun JsonNode.optionalArray(name: String): List<JsonNode> =
    get(name)?.takeIf(JsonNode::isArray)?.toList().orEmpty()

private fun JsonNode.requiredLong(name: String): Long =
    required(name).asLong()

private fun JsonNode.requiredBoolean(name: String): Boolean =
    required(name).asBoolean()

private fun JsonNode.requiredDecimal(name: String): BigDecimal =
    required(name).asText().toBigDecimal()

private fun JsonNode.optionalDecimal(name: String): BigDecimal? =
    get(name)
        ?.takeUnless(JsonNode::isNull)
        ?.asText()
        ?.takeIf(String::isNotEmpty)
        ?.toBigDecimal()

private fun JsonNode.requiredInstant(name: String): Instant =
    Instant.ofEpochMilli(requiredLong(name))

private const val ACCOUNT_UPDATE_EVENT = "ACCOUNT_UPDATE"
private const val ORDER_UPDATE_EVENT = "ORDER_TRADE_UPDATE"
private const val LISTEN_KEY_EXPIRED_EVENT = "listenKeyExpired"
