package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.binance.BinanceOrderStatus
import com.scalpsecta.breakoutbot.binance.BinancePositionRisk
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class OrderIntentRequest(
    val levelId: UUID,
    val attemptNumber: Long,
    val symbol: String,
    val role: OrderRole,
    val slot: Int,
    val side: OrderSide,
    val type: OrderType,
    val timeInForce: OrderTimeInForce? = null,
    val confirmedQuantity: BigDecimal? = null,
    val price: BigDecimal? = null,
    val stopPrice: BigDecimal? = null,
    val workingType: TriggerWorkingType? = null,
    val priceProtect: Boolean? = null,
    val reduceOnly: Boolean = false,
    val closePosition: Boolean = false,
    val confirmedPositionAmount: BigDecimal? = null,
)

data class OrderIntent(
    val intentSequence: Long,
    val clientOrderId: String,
    val applicationStartedAt: Instant,
    val levelId: UUID,
    val attemptNumber: Long,
    val symbol: String,
    val role: OrderRole,
    val slot: Int,
    val side: OrderSide,
    val type: OrderType,
    val timeInForce: OrderTimeInForce?,
    val confirmedQuantity: BigDecimal?,
    val price: BigDecimal?,
    val stopPrice: BigDecimal?,
    val workingType: TriggerWorkingType?,
    val priceProtect: Boolean?,
    val reduceOnly: Boolean,
    val closePosition: Boolean,
    val confirmedPositionAmount: BigDecimal?,
)

enum class OrderRole(
    internal val identityCode: Char,
    val closesExposure: Boolean,
) {
    ENTRY('e', false),
    ADDITION('a', false),
    HARD_STOP('s', true),
    TAKE_PROFIT('t', true),
    CLOSE('c', true),
    UNKNOWN_OUTCOME_CLOSE('u', true),
    SAFE_MODE_CLOSE('m', true),
}

enum class OrderSide {
    BUY,
    SELL,
}

enum class OrderType {
    LIMIT,
    MARKET,
    STOP,
    STOP_MARKET,
    TAKE_PROFIT,
    TAKE_PROFIT_MARKET,
}

enum class OrderTimeInForce {
    GTC,
    IOC,
    FOK,
}

enum class TriggerWorkingType {
    CONTRACT_PRICE,
    MARK_PRICE,
}

enum class OrderOutcome {
    ACTIVE,
    FILLED,
    PARTIALLY_FILLED,
    REJECTED,
    CANCELED,
    UNKNOWN,
}

enum class OrderResolutionSource {
    PRIVATE_STREAM,
    REST_RECONCILIATION,
    BOUNDED_UNKNOWN,
}

data class OrderResolution(
    val intent: OrderIntent,
    val outcome: OrderOutcome,
    val source: OrderResolutionSource,
    val exchangeOrderId: Long?,
    val actualFilledQuantity: BigDecimal,
    val averageFilledPrice: BigDecimal?,
    val confirmedPositionAmount: BigDecimal,
    val reconciliationChecks: Int,
    val reason: ExecutionReasonCode? = null,
)

enum class ExecutionReasonCode {
    ORDER_OUTCOME_UNKNOWN,
    STOP_SETUP_FAILED,
    TP_SETUP_FAILED,
}

data class HardStopConfirmation(
    val intent: OrderIntent,
    val confirmed: Boolean,
    val exchangeOrderId: Long?,
    val observedStopPrice: BigDecimal?,
    val observedWorkingType: TriggerWorkingType?,
    val observedPriceProtect: Boolean?,
    val reconciliationChecks: Int,
    val confirmedPositionAmount: BigDecimal,
)

data class ExecutionSnapshot(
    val observedAt: Instant,
    val entriesAndAdditionsBlocked: Boolean,
    val positions: List<ExecutionPositionSnapshot>,
    val balances: List<ExecutionBalanceSnapshot>,
    val orders: List<OrderExecutionSnapshot>,
)

data class ExecutionRuntimeReconciliation(
    val observedAt: Instant,
    val positions: List<BinancePositionRisk>,
    val openBotOrders: List<BinanceOrderStatus>,
    val orphanedBotOrderIds: Set<String>,
    val unresolvedOrderIds: Set<String>,
)

data class ExecutionPositionSnapshot(
    val symbol: String,
    val positionAmount: BigDecimal,
    val entryPrice: BigDecimal,
    val updatedAt: Instant,
    val actualNotional: BigDecimal = positionAmount.abs().multiply(entryPrice),
    val unrealizedPnl: BigDecimal? = null,
)

data class ExecutionBalanceSnapshot(
    val asset: String,
    val walletBalance: BigDecimal,
    val updatedAt: Instant,
)

data class OrderExecutionSnapshot(
    val intentSequence: Long,
    val clientOrderId: String,
    val levelId: UUID,
    val attemptNumber: Long,
    val symbol: String,
    val role: OrderRole,
    val slot: Int,
    val requestedQuantity: BigDecimal?,
    val requestedPrice: BigDecimal?,
    val stopPrice: BigDecimal?,
    val workingType: TriggerWorkingType?,
    val priceProtect: Boolean?,
    val actualFilledQuantity: BigDecimal,
    val outcome: OrderOutcome?,
    val source: OrderResolutionSource?,
    val exchangeOrderId: Long?,
    val updatedAt: Instant,
    val reason: ExecutionReasonCode?,
)

class OrderExecutionException(message: String) : IllegalArgumentException(message)

interface PreEntryOrderExecutor {
    fun execute(request: OrderIntentRequest): Mono<OrderResolution>

    fun confirmHardStop(
        request: OrderIntentRequest,
    ): Mono<HardStopConfirmation>
}
