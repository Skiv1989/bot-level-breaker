package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.domain.ApplicationRun
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32

@Component
class ClientOrderIdFactory internal constructor(
    private val applicationStartedAt: Instant,
) {
    @Autowired
    constructor(applicationRun: ApplicationRun) : this(applicationRun.startedAt)

    private val nextSequence = AtomicLong()

    fun create(request: OrderIntentRequest): OrderIntent {
        validate(request)
        val sequence = nextSequence.incrementAndGet()
        val normalizedSymbol = request.symbol.trim().uppercase()
        return OrderIntent(
            intentSequence = sequence,
            clientOrderId = clientOrderId(request, sequence),
            applicationStartedAt = applicationStartedAt,
            levelId = request.levelId,
            attemptNumber = request.attemptNumber,
            symbol = normalizedSymbol,
            role = request.role,
            slot = request.slot,
            side = request.side,
            type = request.type,
            timeInForce = request.timeInForce,
            confirmedQuantity = request.confirmedQuantity,
            price = request.price,
            stopPrice = request.stopPrice,
            reduceOnly = request.reduceOnly,
            closePosition = request.closePosition,
            confirmedPositionAmount = request.confirmedPositionAmount,
        )
    }

    private fun clientOrderId(
        request: OrderIntentRequest,
        sequence: Long,
    ): String {
        val value = buildString {
            append('b')
            append(applicationStartedAt.toEpochMilli().toString(BASE_36))
            append('-')
            append(levelIdentity(request.levelId))
            append('-')
            append(request.attemptNumber.toString(BASE_36))
            append(request.role.identityCode)
            append(request.slot.toString(BASE_36))
            append('-')
            append(sequence.toString(BASE_36))
        }
        check(value.length <= MAX_CLIENT_ORDER_ID_LENGTH) {
            "Generated clientOrderId exceeds the Binance length limit"
        }
        check(value.matches(BINANCE_CLIENT_ORDER_ID)) {
            "Generated clientOrderId is not Binance-safe"
        }
        return value
    }

    private fun levelIdentity(levelId: UUID): String {
        val checksum = CRC32()
        checksum.update(levelId.toString().toByteArray(Charsets.US_ASCII))
        return checksum.value.toString(BASE_36).padStart(LEVEL_ID_LENGTH, '0')
    }

    private fun validate(request: OrderIntentRequest) {
        if (request.symbol.isBlank()) {
            throw OrderExecutionException("symbol must not be blank")
        }
        if (request.attemptNumber <= 0) {
            throw OrderExecutionException("attemptNumber must be positive")
        }
        if (request.slot < 0) {
            throw OrderExecutionException("slot must not be negative")
        }
        if (request.confirmedQuantity?.signum()?.let { it <= 0 } == true) {
            throw OrderExecutionException("confirmedQuantity must be positive")
        }
        if (request.price?.signum()?.let { it <= 0 } == true) {
            throw OrderExecutionException("price must be positive")
        }
        if (request.stopPrice?.signum()?.let { it <= 0 } == true) {
            throw OrderExecutionException("stopPrice must be positive")
        }
        validateOrderShape(request)
        validateClosingIntent(request)
    }

    private fun validateOrderShape(request: OrderIntentRequest) {
        if (request.closePosition) {
            if (request.confirmedQuantity != null) {
                throw OrderExecutionException(
                    "close-position orders must not specify a quantity",
                )
            }
        } else if (request.confirmedQuantity == null) {
            throw OrderExecutionException("confirmedQuantity is required")
        }
        if (request.type == OrderType.LIMIT) {
            if (request.price == null || request.timeInForce == null) {
                throw OrderExecutionException(
                    "limit orders require price and timeInForce",
                )
            }
        }
        if (request.type == OrderType.MARKET && request.timeInForce != null) {
            throw OrderExecutionException(
                "market orders must not specify timeInForce",
            )
        }
    }

    private fun validateClosingIntent(request: OrderIntentRequest) {
        if (!request.role.closesExposure) {
            if (request.reduceOnly || request.closePosition) {
                throw OrderExecutionException(
                    "entry and addition intents cannot be reduce-only or close-position",
                )
            }
            return
        }
        if (!request.reduceOnly && !request.closePosition) {
            throw OrderExecutionException(
                "closing intents must be reduce-only or close-position",
            )
        }
        if (request.reduceOnly && request.closePosition) {
            throw OrderExecutionException(
                "closing intents cannot be both reduce-only and close-position",
            )
        }
        val positionAmount = request.confirmedPositionAmount
            ?: throw OrderExecutionException(
                "closing intents require confirmedPositionAmount",
            )
        if (positionAmount.signum() == 0) {
            throw OrderExecutionException(
                "closing intents require non-zero confirmed exposure",
            )
        }
        val expectedSide = if (positionAmount.signum() > 0) {
            OrderSide.SELL
        } else {
            OrderSide.BUY
        }
        if (request.side != expectedSide) {
            throw OrderExecutionException(
                "closing side must oppose confirmed exposure",
            )
        }
        if (
            request.reduceOnly &&
            checkNotNull(request.confirmedQuantity) > positionAmount.abs()
        ) {
            throw OrderExecutionException(
                "reduce-only quantity cannot exceed confirmed exposure",
            )
        }
    }
}

private const val BASE_36 = 36
private const val LEVEL_ID_LENGTH = 7
private const val MAX_CLIENT_ORDER_ID_LENGTH = 36
private val BINANCE_CLIENT_ORDER_ID = Regex("^[.A-Za-z0-9_:/-]{1,36}$")
