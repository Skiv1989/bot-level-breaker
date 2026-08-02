package com.scalpsecta.breakoutbot.level

import com.scalpsecta.breakoutbot.signal.BurstStatus
import com.scalpsecta.breakoutbot.signal.LevelSignalSnapshot
import com.scalpsecta.breakoutbot.signal.SignalGate
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant

internal data class BreakoutObservation(
    val observedAt: Instant,
    val signal: LevelSignalSnapshot,
    val publicDataHealthy: Boolean,
    val privateDataHealthy: Boolean,
)

internal enum class BreakoutConfirmationStatus {
    PENDING,
    PASSED,
    FAILED,
}

internal data class BreakoutConfirmationEvaluation(
    val status: BreakoutConfirmationStatus,
    val terminalReason: LevelReasonCode? = null,
)

internal data class PositionManagementEvaluation(
    val exitScore: Int,
    val activePointReasons: List<ExitPointReason>,
    val terminalReason: LevelReasonCode? = null,
)

internal class BreakoutStateMachine {
    private var direction: LevelDirection? = null
    private var levelPrice: BigDecimal? = null
    private var frozenNpu: BigDecimal? = null
    private var preEntryFilledAt: Instant? = null
    private var confirmationStartedAt: Instant? = null
    private var breakoutConfirmedAt: Instant? = null
    private var firstObservationAnchor: Instant? = null
    private var bestPostEntryPrice: BigDecimal? = null
    private var lastTradePrice: BigDecimal? = null
    private var oppositeFlowSince: Instant? = null
    private var collapsedAccelerationSince: Instant? = null
    private var publicDataUnhealthySince: Instant? = null
    private var privateDataUnhealthySince: Instant? = null
    private var oppositeDeltaSince: Instant? = null
    private var crossed = false

    fun startPreEntry(
        direction: LevelDirection,
        levelPrice: BigDecimal,
        frozenNpu: BigDecimal,
        filledAt: Instant,
        observedTradePrices: List<BigDecimal>,
    ) {
        require(frozenNpu.signum() > 0) { "frozenNpu must be positive" }
        this.direction = direction
        this.levelPrice = levelPrice
        this.frozenNpu = frozenNpu
        preEntryFilledAt = filledAt
        firstObservationAnchor = filledAt
        bestPostEntryPrice = favorableExtreme(direction, observedTradePrices)
        lastTradePrice = observedTradePrices.lastOrNull()
        confirmationStartedAt = null
        breakoutConfirmedAt = null
        oppositeFlowSince = null
        collapsedAccelerationSince = null
        publicDataUnhealthySince = null
        privateDataUnhealthySince = null
        oppositeDeltaSince = null
        crossed = false
    }

    fun markCrossed() {
        requireStarted()
        crossed = true
    }

    fun startConfirmation(startedAt: Instant) {
        require(crossed) { "crossing must be recorded before confirmation" }
        confirmationStartedAt = startedAt
    }

    fun startPositionManagement(confirmedAt: Instant) {
        requireStarted()
        breakoutConfirmedAt = confirmedAt
    }

    fun evaluatePreBreak(
        observation: BreakoutObservation,
        includeNoCrossTimeout: Boolean,
    ): LevelReasonCode? {
        update(observation)
        return when {
            adverseRetreatExceeded() -> LevelReasonCode.PRE_ENTRY_INVALIDATED
            persisted(oppositeFlowSince, observation.observedAt, FLOW_FAILURE_DURATION) ->
                LevelReasonCode.PRE_ENTRY_INVALIDATED

            persisted(
                collapsedAccelerationSince,
                observation.observedAt,
                ACCELERATION_FAILURE_DURATION,
            ) -> LevelReasonCode.PRE_ENTRY_INVALIDATED

            observation.signal.burst.status == BurstStatus.ACTIVE ->
                LevelReasonCode.PRE_ENTRY_INVALIDATED

            persisted(
                publicDataUnhealthySince,
                observation.observedAt,
                DATA_FAILURE_DURATION,
            ) -> LevelReasonCode.MARKET_DATA_FAILURE

            persisted(
                privateDataUnhealthySince,
                observation.observedAt,
                DATA_FAILURE_DURATION,
            ) -> LevelReasonCode.PRIVATE_STREAM_FAILURE

            includeNoCrossTimeout &&
                !crossed &&
                reached(
                    checkNotNull(preEntryFilledAt),
                    observation.observedAt,
                    NO_CROSS_TIMEOUT,
                ) -> LevelReasonCode.PRE_ENTRY_TIMEOUT

            else -> null
        }
    }

    fun evaluateConfirmation(
        observation: BreakoutObservation,
    ): BreakoutConfirmationEvaluation {
        update(observation)
        val confirmationFailure = when {
            !observation.publicDataHealthy ||
                !observation.privateDataHealthy -> true

            behindLevel() > checkNotNull(frozenNpu) -> true
            !directionalPressureValid(observation.signal) -> true
            observation.signal.burst.status == BurstStatus.ACTIVE -> true
            exitEvaluation(observation).exitScore >= EXIT_SCORE_THRESHOLD -> true
            else -> false
        }
        if (confirmationFailure) {
            return BreakoutConfirmationEvaluation(
                status = BreakoutConfirmationStatus.FAILED,
                terminalReason = LevelReasonCode.BREAK_CONFIRM_FAILED,
            )
        }
        val preBreakFailure = when {
            adverseRetreatExceeded() -> true
            persisted(oppositeFlowSince, observation.observedAt, FLOW_FAILURE_DURATION) ->
                true

            persisted(
                collapsedAccelerationSince,
                observation.observedAt,
                ACCELERATION_FAILURE_DURATION,
            ) -> true

            else -> false
        }
        if (preBreakFailure) {
            return BreakoutConfirmationEvaluation(
                status = BreakoutConfirmationStatus.FAILED,
                terminalReason = LevelReasonCode.PRE_ENTRY_INVALIDATED,
            )
        }
        return if (
            reached(
                checkNotNull(confirmationStartedAt),
                observation.observedAt,
                CONFIRMATION_DURATION,
            )
        ) {
            BreakoutConfirmationEvaluation(BreakoutConfirmationStatus.PASSED)
        } else {
            BreakoutConfirmationEvaluation(BreakoutConfirmationStatus.PENDING)
        }
    }

    fun evaluatePositionManagement(
        observation: BreakoutObservation,
    ): PositionManagementEvaluation {
        update(observation)
        val evaluation = exitEvaluation(observation)
        val confirmedAt = checkNotNull(breakoutConfirmedAt) {
            "breakout confirmation must start position management"
        }
        val reason = when {
            !observation.observedAt.isBefore(confirmedAt) &&
                !observation.observedAt.isAfter(
                    confirmedAt.plus(SNAPBACK_WINDOW),
                ) &&
                behindLevel() > checkNotNull(frozenNpu).multiply(TWO) ->
                LevelReasonCode.SNAPBACK

            reached(confirmedAt, observation.observedAt, MAXIMUM_HOLDING_TIME) ->
                LevelReasonCode.MAX_HOLD_TIME

            evaluation.exitScore >= EXIT_SCORE_THRESHOLD ->
                LevelReasonCode.EXIT_SCORE

            else -> null
        }
        return evaluation.copy(terminalReason = reason)
    }

    private fun update(observation: BreakoutObservation) {
        requireStarted()
        val anchor = firstObservationAnchor ?: observation.observedAt
        val signal = observation.signal
        val currentPrice = signal.latestTradePrice
        if (currentPrice != null) {
            lastTradePrice = currentPrice
            bestPostEntryPrice = when (checkNotNull(direction)) {
                LevelDirection.LONG -> maxOf(bestPostEntryPrice ?: currentPrice, currentPrice)
                LevelDirection.SHORT -> minOf(bestPostEntryPrice ?: currentPrice, currentPrice)
            }
        }
        val directionalShare = when (checkNotNull(direction)) {
            LevelDirection.LONG -> signal.windows.fast.buyShare
            LevelDirection.SHORT -> signal.windows.fast.sellShare
        }
        val deltaOpposite = when (checkNotNull(direction)) {
            LevelDirection.LONG -> signal.windows.fast.deltaRate.signum() < 0
            LevelDirection.SHORT -> signal.windows.fast.deltaRate.signum() > 0
        }
        oppositeFlowSince = continuousSince(
            condition = directionalShare < HALF && deltaOpposite,
            existing = oppositeFlowSince,
            anchor = anchor,
        )
        collapsedAccelerationSince = continuousSince(
            condition =
                signal.acceleration.tradesPerSecondRatio < BigDecimal.ONE &&
                    signal.acceleration.volumeRateRatio < BigDecimal.ONE,
            existing = collapsedAccelerationSince,
            anchor = anchor,
        )
        publicDataUnhealthySince = continuousSince(
            condition = !observation.publicDataHealthy,
            existing = publicDataUnhealthySince,
            anchor = anchor,
        )
        privateDataUnhealthySince = continuousSince(
            condition = !observation.privateDataHealthy,
            existing = privateDataUnhealthySince,
            anchor = anchor,
        )
        oppositeDeltaSince = continuousSince(
            condition = deltaOpposite,
            existing = oppositeDeltaSince,
            anchor = anchor,
        )
        firstObservationAnchor = null
    }

    private fun adverseRetreatExceeded(): Boolean {
        val best = bestPostEntryPrice ?: return false
        val current = lastTradePrice ?: return false
        val retreat = when (checkNotNull(direction)) {
            LevelDirection.LONG -> best.subtract(current)
            LevelDirection.SHORT -> current.subtract(best)
        }
        return retreat > checkNotNull(frozenNpu).multiply(TWO)
    }

    private fun behindLevel(): BigDecimal {
        val current = lastTradePrice ?: return BigDecimal.ZERO
        return when (checkNotNull(direction)) {
            LevelDirection.LONG -> checkNotNull(levelPrice).subtract(current)
            LevelDirection.SHORT -> current.subtract(checkNotNull(levelPrice))
        }.max(BigDecimal.ZERO)
    }

    private fun directionalPressureValid(signal: LevelSignalSnapshot): Boolean =
        signal.mandatoryGates.gates
            .single { result -> result.gate == SignalGate.DIRECTIONAL_FLOW }
            .passed

    private fun exitEvaluation(
        observation: BreakoutObservation,
    ): PositionManagementEvaluation {
        val signal = observation.signal
        val npu = checkNotNull(frozenNpu)
        val lowProgress = signal.windows.mid.signedPriceProgress <
            npu.multiply(QUARTER)
        val directionalShare = when (checkNotNull(direction)) {
            LevelDirection.LONG -> signal.windows.fast.buyShare
            LevelDirection.SHORT -> signal.windows.fast.sellShare
        }
        val oppositeFastSize = when (checkNotNull(direction)) {
            LevelDirection.LONG -> signal.windows.fast.averageSellSize
            LevelDirection.SHORT -> signal.windows.fast.averageBuySize
        }
        val oppositeSlowSize = when (checkNotNull(direction)) {
            LevelDirection.LONG -> signal.windows.slow.averageSellSize
            LevelDirection.SHORT -> signal.windows.slow.averageBuySize
        }
        val activeReasons = mutableListOf<ExitPointReason>()
        if (directionalShare >= DIRECTIONAL_ABSORPTION_SHARE && lowProgress) {
            activeReasons += ExitPointReason.DIRECTIONAL_ABSORPTION
        }
        if (
            oppositeSlowSize.signum() > 0 &&
            oppositeFastSize >= oppositeSlowSize.multiply(TWO)
        ) {
            activeReasons += ExitPointReason.OPPOSITE_AVERAGE_TRADE_SIZE
        }
        if (
            persisted(
                oppositeDeltaSince,
                observation.observedAt,
                FLOW_FAILURE_DURATION,
            )
        ) {
            activeReasons += ExitPointReason.PERSISTENT_OPPOSITE_DELTA
        }
        if (behindLevel() > npu) {
            activeReasons += ExitPointReason.PRICE_BEHIND_LEVEL
        }
        val activityRatio = maxOf(
            signal.acceleration.tradesPerSecondRatio,
            signal.acceleration.volumeRateRatio,
        )
        if (activityRatio >= ABSORPTION_ACTIVITY_RATIO && lowProgress) {
            activeReasons += ExitPointReason.HIGH_ACTIVITY_LOW_PROGRESS
        }
        return PositionManagementEvaluation(
            exitScore = activeReasons.sumOf(ExitPointReason::points),
            activePointReasons = activeReasons,
        )
    }

    private fun requireStarted() {
        check(
            direction != null &&
                levelPrice != null &&
                frozenNpu != null &&
                preEntryFilledAt != null,
        ) {
            "pre-entry must be started before breakout evaluation"
        }
    }
}

private fun favorableExtreme(
    direction: LevelDirection,
    prices: List<BigDecimal>,
): BigDecimal? = when (direction) {
    LevelDirection.LONG -> prices.maxOrNull()
    LevelDirection.SHORT -> prices.minOrNull()
}

private fun continuousSince(
    condition: Boolean,
    existing: Instant?,
    anchor: Instant,
): Instant? = if (condition) existing ?: anchor else null

private fun persisted(
    since: Instant?,
    now: Instant,
    duration: Duration,
): Boolean = since != null && reached(since, now, duration)

private fun reached(
    since: Instant,
    now: Instant,
    duration: Duration,
): Boolean = !now.isBefore(since.plus(duration))

private val FLOW_FAILURE_DURATION: Duration = Duration.ofMillis(500)
private val ACCELERATION_FAILURE_DURATION: Duration = Duration.ofMillis(500)
private val DATA_FAILURE_DURATION: Duration = Duration.ofSeconds(3)
private val NO_CROSS_TIMEOUT: Duration = Duration.ofSeconds(5)
private val CONFIRMATION_DURATION: Duration = Duration.ofSeconds(1)
private val SNAPBACK_WINDOW: Duration = Duration.ofMillis(500)
private val MAXIMUM_HOLDING_TIME: Duration = Duration.ofMinutes(10)
private val HALF = BigDecimal("0.50")
private val QUARTER = BigDecimal("0.25")
private val TWO = BigDecimal("2")
private val DIRECTIONAL_ABSORPTION_SHARE = BigDecimal("0.62")
private val ABSORPTION_ACTIVITY_RATIO = BigDecimal("1.5")
private const val EXIT_SCORE_THRESHOLD = 3
