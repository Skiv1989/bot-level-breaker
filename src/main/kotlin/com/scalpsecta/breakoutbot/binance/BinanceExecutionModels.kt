package com.scalpsecta.breakoutbot.binance

import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Instant

interface BinanceExecutionClient {
    fun placeOrder(request: BinanceOrderRequest): Mono<BinanceOrderAcknowledgement>

    fun cancelOrder(
        symbol: String,
        clientOrderId: String,
    ): Mono<Void>

    fun reconcileOrder(
        symbol: String,
        clientOrderId: String,
    ): Mono<BinanceOrderReconciliation>

    fun reconcileAccount(): Mono<BinanceAccountReconciliation> =
        Mono.error(
            IllegalStateException(
                "Binance account reconciliation is unavailable until a live execution adapter is configured",
            ),
        )
}

data class BinanceOrderRequest(
    val symbol: String,
    val clientOrderId: String,
    val side: String,
    val type: String,
    val timeInForce: String? = null,
    val quantity: BigDecimal? = null,
    val price: BigDecimal? = null,
    val stopPrice: BigDecimal? = null,
    val workingType: String? = null,
    val priceProtect: Boolean? = null,
    val reduceOnly: Boolean = false,
    val closePosition: Boolean = false,
)

data class BinanceOrderAcknowledgement(
    val symbol: String,
    val clientOrderId: String,
    val orderId: Long,
    val status: String,
)

data class BinanceOrderStatus(
    val symbol: String,
    val clientOrderId: String,
    val orderId: Long,
    val status: String,
    val originalQuantity: BigDecimal,
    val executedQuantity: BigDecimal,
    val averagePrice: BigDecimal,
    val reduceOnly: Boolean,
    val closePosition: Boolean,
    val updatedAt: Instant,
    val type: String? = null,
    val side: String? = null,
    val timeInForce: String? = null,
    val price: BigDecimal? = null,
    val stopPrice: BigDecimal? = null,
    val workingType: String? = null,
    val priceProtect: Boolean? = null,
)

data class BinancePositionRisk(
    val symbol: String,
    val positionAmount: BigDecimal,
    val entryPrice: BigDecimal,
)

data class BinanceOrderReconciliation(
    val order: BinanceOrderStatus?,
    val position: BinancePositionRisk?,
    val openClientOrderIds: Set<String>,
    val safeDetail: String? = null,
)

data class BinanceAccountReconciliation(
    val positions: List<BinancePositionRisk>,
    val openOrders: List<BinanceOrderStatus>,
    val safeDetail: String? = null,
)

class UnavailableBinanceExecutionClient : BinanceExecutionClient {
    override fun placeOrder(
        request: BinanceOrderRequest,
    ): Mono<BinanceOrderAcknowledgement> =
        Mono.error(
            IllegalStateException(
                "Binance order placement is unavailable until a live execution adapter is configured",
            ),
        )

    override fun cancelOrder(
        symbol: String,
        clientOrderId: String,
    ): Mono<Void> =
        Mono.error(
            IllegalStateException(
                "Binance order cancellation is unavailable until a live execution adapter is configured",
            ),
        )

    override fun reconcileOrder(
        symbol: String,
        clientOrderId: String,
    ): Mono<BinanceOrderReconciliation> =
        Mono.error(
            IllegalStateException(
                "Binance order reconciliation is unavailable until a live execution adapter is configured",
            ),
        )
}
