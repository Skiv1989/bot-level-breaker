package com.scalpsecta.breakoutbot.binance

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import java.math.BigDecimal
import java.time.Instant

data class BinanceClockMeasurement(
    val serverTime: Instant,
    val checkedAt: Instant,
    val serverOffsetMillis: Long,
    val roundTripMillis: Long,
)

data class BinanceAccountSummary(
    val canTrade: Boolean,
    val feeTier: Int,
    val totalWalletBalance: BigDecimal,
    val totalUnrealizedProfit: BigDecimal,
    val totalMarginBalance: BigDecimal,
    val availableBalance: BigDecimal,
    val updatedAt: Instant,
)

enum class BinancePositionMode {
    ONE_WAY,
    HEDGE,
}

enum class BinanceAssetMode {
    SINGLE_ASSET,
    MULTI_ASSET,
}

data class BinanceExchangeInfo(
    val serverTime: Instant,
    val symbols: List<BinanceSymbolMetadata>,
)

data class BinanceSymbolMetadata(
    val symbol: String,
    val status: String,
    val contractType: String,
    val baseAsset: String,
    val quoteAsset: String,
    val marginAsset: String,
    val pricePrecision: Int,
    val quantityPrecision: Int,
    val priceFilter: BinancePriceFilter?,
    val lotSizeFilter: BinanceLotSizeFilter?,
    val marketLotSizeFilter: BinanceLotSizeFilter?,
    val minimumNotional: BigDecimal?,
)

data class BinancePriceFilter(
    val minimumPrice: BigDecimal,
    val maximumPrice: BigDecimal,
    val tickSize: BigDecimal,
)

data class BinanceLotSizeFilter(
    val minimumQuantity: BigDecimal,
    val maximumQuantity: BigDecimal,
    val stepSize: BigDecimal,
)

data class BinanceSymbolLeverageBrackets(
    val symbol: String,
    val notionalCoefficient: BigDecimal,
    val brackets: List<BinanceLeverageBracket>,
)

data class BinanceLeverageBracket(
    val bracket: Int,
    val initialLeverage: Int,
    val notionalFloor: BigDecimal,
    val notionalCap: BigDecimal,
    val maintenanceMarginRatio: BigDecimal,
    val cumulativeMaintenanceAmount: BigDecimal,
)

data class BinanceCommissionRate(
    val symbol: String,
    val makerRate: BigDecimal,
    val takerRate: BigDecimal,
)

data class BinanceSymbolConfiguration(
    val symbol: String,
    val marginType: BinanceMarginType,
    val autoAddMargin: Boolean,
    val leverage: Int,
    val maximumNotional: BigDecimal,
)

enum class BinanceMarginType {
    ISOLATED,
    CROSSED,
}

sealed interface BinanceUserDataEvent {
    val eventTime: Instant
    val transactionTime: Instant?
    val receivedAt: Instant

    data class AccountUpdate(
        override val eventTime: Instant,
        override val transactionTime: Instant,
        override val receivedAt: Instant,
        val reason: String,
        val balances: List<BinanceBalanceUpdate>,
        val positions: List<BinancePositionUpdate>,
    ) : BinanceUserDataEvent

    data class OrderUpdate(
        override val eventTime: Instant,
        override val transactionTime: Instant,
        override val receivedAt: Instant,
        val symbol: String,
        val clientOrderId: String,
        val side: String,
        val orderType: String,
        val timeInForce: String,
        val originalQuantity: BigDecimal,
        val originalPrice: BigDecimal,
        val averagePrice: BigDecimal,
        val stopPrice: BigDecimal,
        val executionType: String,
        val orderStatus: String,
        val orderId: Long,
        val lastFilledQuantity: BigDecimal,
        val accumulatedFilledQuantity: BigDecimal,
        val lastFilledPrice: BigDecimal,
        val commissionAsset: String?,
        val commission: BigDecimal?,
        val tradeId: Long,
        val realizedProfit: BigDecimal,
        val positionSide: String,
        val reduceOnly: Boolean,
    ) : BinanceUserDataEvent

    data class ListenKeyExpired(
        override val eventTime: Instant,
        override val receivedAt: Instant,
    ) : BinanceUserDataEvent {
        override val transactionTime: Instant? = null
    }
}

data class BinanceBalanceUpdate(
    val asset: String,
    val walletBalance: BigDecimal,
    val crossWalletBalance: BigDecimal,
    val balanceChange: BigDecimal,
)

data class BinancePositionUpdate(
    val symbol: String,
    val positionAmount: BigDecimal,
    val entryPrice: BigDecimal,
    val breakEvenPrice: BigDecimal?,
    val accumulatedRealizedProfit: BigDecimal,
    val unrealizedProfit: BigDecimal,
    val marginType: String,
    val isolatedWallet: BigDecimal,
    val positionSide: String,
)

data class AuthenticatedBinanceSnapshot(
    val clock: BinanceClockSnapshot,
    val account: BinanceAccountReadinessSnapshot,
    val privateStream: BinancePrivateStreamSnapshot,
    val currentEquity: BigDecimal?,
    val temporaryDailyAnchorEquity: BigDecimal?,
)

data class BinanceClockSnapshot(
    val readiness: BinanceReadiness,
    val checkedAt: Instant?,
    val serverOffsetMillis: Long?,
    val roundTripMillis: Long?,
)

data class BinanceAccountReadinessSnapshot(
    val readiness: BinanceReadiness,
    val checkedAt: Instant?,
    val canTrade: Boolean?,
    val positionMode: BinancePositionMode?,
    val assetMode: BinanceAssetMode?,
    val loadedSymbolCount: Int,
)

data class BinancePrivateStreamSnapshot(
    val readiness: BinanceReadiness,
    val connectionState: BinancePrivateStreamConnectionState,
    val connectedAt: Instant?,
    val lastEventAt: Instant?,
)

enum class BinancePrivateStreamConnectionState {
    NOT_STARTED,
    CONNECTING,
    CONNECTED,
    DISCONNECTED,
}

internal fun authenticatedBinanceNotReady(): AuthenticatedBinanceSnapshot =
    AuthenticatedBinanceSnapshot(
        clock = BinanceClockSnapshot(
            readiness = BinanceReadiness.NOT_READY,
            checkedAt = null,
            serverOffsetMillis = null,
            roundTripMillis = null,
        ),
        account = BinanceAccountReadinessSnapshot(
            readiness = BinanceReadiness.NOT_READY,
            checkedAt = null,
            canTrade = null,
            positionMode = null,
            assetMode = null,
            loadedSymbolCount = 0,
        ),
        privateStream = BinancePrivateStreamSnapshot(
            readiness = BinanceReadiness.NOT_READY,
            connectionState = BinancePrivateStreamConnectionState.NOT_STARTED,
            connectedAt = null,
            lastEventAt = null,
        ),
        currentEquity = null,
        temporaryDailyAnchorEquity = null,
    )
