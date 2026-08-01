package com.scalpsecta.breakoutbot.risk

import com.scalpsecta.breakoutbot.evidence.AuditEventType
import com.scalpsecta.breakoutbot.evidence.AuditRecordDraft
import com.scalpsecta.breakoutbot.evidence.DecisionEvidence
import com.scalpsecta.breakoutbot.evidence.EvidenceRecorder
import com.scalpsecta.breakoutbot.evidence.NoOpEvidenceRecorder
import com.scalpsecta.breakoutbot.evidence.PriceEvidence
import com.scalpsecta.breakoutbot.evidence.QuantityEvidence
import com.scalpsecta.breakoutbot.evidence.RiskEvidence
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelState
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import java.time.Clock
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.min

@Service
class AttemptRiskService internal constructor(
    private val clock: Clock,
    private val scheduler: Scheduler,
    private val evidenceRecorder: EvidenceRecorder = NoOpEvidenceRecorder,
) {
    @Autowired
    constructor(
        clock: Clock,
        evidenceRecorder: EvidenceRecorder,
    ) : this(
        clock = clock,
        scheduler = Schedulers.newSingle("global-risk-events"),
        evidenceRecorder = evidenceRecorder,
    )

    private val attempts = linkedMapOf<UUID, MutableRiskAttempt>()
    private val reservations = linkedMapOf<UUID, MutableRiskReservation>()
    private var latestAccountState: RiskAccountState? = null
    private var nextSequence = 1L
    private val publishedState = AtomicReference(emptySnapshot(clock.instant()))
    private val queue = OrderedGlobalRiskQueue<RiskEvent>(scheduler, ::handle)

    fun admit(
        request: AttemptAdmissionRequest,
        accountState: RiskAccountState,
    ): Mono<AttemptAdmissionDecision> =
        queue
            .submit(RiskEvent.Admit(request, accountState, clock.instant()))
            .map { result -> result as AttemptAdmissionDecision }

    fun recordConfirmedExposure(
        levelId: UUID,
        confirmedPositionQuantity: BigDecimal,
    ): Mono<GlobalRiskSnapshot> =
        queue
            .submit(
                RiskEvent.ConfirmedExposure(
                    levelId = levelId,
                    confirmedPositionQuantity = confirmedPositionQuantity,
                ),
            )
            .map { result -> result as GlobalRiskSnapshot }

    fun recordConfirmedReducingFill(
        levelId: UUID,
        confirmedRemainingQuantity: BigDecimal,
    ): Mono<GlobalRiskSnapshot> =
        queue
            .submit(
                RiskEvent.ConfirmedReducingFill(
                    levelId = levelId,
                    confirmedRemainingQuantity = confirmedRemainingQuantity,
                ),
            )
            .map { result -> result as GlobalRiskSnapshot }

    fun recordConfirmedFlat(levelId: UUID): Mono<GlobalRiskSnapshot> =
        queue
            .submit(RiskEvent.ConfirmedFlat(levelId, clock.instant()))
            .map { result -> result as GlobalRiskSnapshot }

    fun currentState(): GlobalRiskSnapshot = publishedState.get()

    @PreDestroy
    fun close() {
        queue.close()
        scheduler.dispose()
    }

    private fun handle(event: RiskEvent): Any =
        when (event) {
            is RiskEvent.Admit -> admit(event)
            is RiskEvent.ConfirmedExposure -> confirmExposure(event)
            is RiskEvent.ConfirmedReducingFill -> confirmReducingFill(event)
            is RiskEvent.ConfirmedFlat -> confirmFlat(event)
        }

    private fun admit(event: RiskEvent.Admit): AttemptAdmissionDecision {
        validate(event.request, event.accountState)
        latestAccountState = event.accountState
        val plan = plan(event.request)
        val blockers = blockers(event.request, event.accountState, plan)
        if (blockers.isEmpty()) {
            val sequence = nextSequence++
            attempts[event.request.levelId] = MutableRiskAttempt(
                sequence = sequence,
                levelId = event.request.levelId,
                symbol = event.request.symbol.normalizedSymbol(),
                status = RiskAttemptStatus.PENDING_ENTRY,
                admittedAt = event.admittedAt,
                completedAt = null,
                confirmedPositionQuantity = BigDecimal.ZERO,
                plan = plan,
            )
            reservations[event.request.levelId] = MutableRiskReservation(
                sequence = sequence,
                levelId = event.request.levelId,
                symbol = event.request.symbol.normalizedSymbol(),
                status = RiskReservationStatus.PENDING_ATTEMPT,
                levelRiskBudget = plan.levelRiskBudget,
                reservedRisk = plan.levelRiskBudget,
                plannedQuantity = event.request.plannedQuantity,
            )
        }
        val state = publishState()
        val decision = AttemptAdmissionDecision(
            admitted = blockers.isEmpty(),
            blockers = blockers,
            plan = plan,
            state = state,
        )
        evidenceRecorder.recordAudit(
            AuditRecordDraft(
                timestamp = event.admittedAt,
                symbol = event.request.symbol.normalizedSymbol(),
                levelId = event.request.levelId,
                stateBefore = LevelState.APPROACH,
                stateAfter = LevelState.APPROACH,
                eventType = AuditEventType.DECISION,
                decision = if (decision.admitted) {
                    "ATTEMPT_ADMITTED"
                } else {
                    "ATTEMPT_BLOCKED"
                },
                blockerReasons = blockers.map(Enum<*>::name),
                evidence = DecisionEvidence(
                    prices = PriceEvidence(
                        levelPrice = event.request.levelPrice,
                        bidPrice = event.request.bestBidPrice,
                        askPrice = event.request.bestAskPrice,
                        spread = event.request.bestAskPrice
                            .subtract(event.request.bestBidPrice),
                        npu = event.request.frozenNpu,
                        stopPrice = plan.structuralStopPrice,
                        takeProfitPrices = plan.takeProfits.map { takeProfit ->
                            takeProfit.price
                        },
                    ),
                    quantity = QuantityEvidence(
                        plannedQuantity = event.request.plannedQuantity,
                    ),
                    risk = RiskEvidence(
                        plan = plan,
                        reservedRisk = if (decision.admitted) {
                            plan.levelRiskBudget
                        } else {
                            BigDecimal.ZERO
                        },
                    ),
                ),
            ),
        )
        return decision
    }

    private fun confirmExposure(
        event: RiskEvent.ConfirmedExposure,
    ): GlobalRiskSnapshot {
        require(event.confirmedPositionQuantity.signum() > 0) {
            "confirmedPositionQuantity must be positive"
        }
        val attempt = activeAttempt(event.levelId)
        val reservation = activeReservation(event.levelId)
        require(event.confirmedPositionQuantity <= reservation.plannedQuantity) {
            "confirmedPositionQuantity cannot exceed planned quantity"
        }
        attempt.status = RiskAttemptStatus.OPEN_POSITION
        attempt.confirmedPositionQuantity = event.confirmedPositionQuantity
        reservation.status = RiskReservationStatus.OPEN_POSITION
        return publishState().also {
            evidenceRecorder.recordAudit(
                AuditRecordDraft(
                    timestamp = clock.instant(),
                    symbol = attempt.symbol,
                    levelId = attempt.levelId,
                    stateBefore = LevelState.APPROACH,
                    stateAfter = LevelState.APPROACH,
                    eventType = AuditEventType.RISK_UPDATED,
                    decision = "EXPOSURE_CONFIRMED",
                    evidence = DecisionEvidence(
                        quantity = QuantityEvidence(
                            plannedQuantity = reservation.plannedQuantity,
                            filledQuantity = event.confirmedPositionQuantity,
                            remainingQuantity = event.confirmedPositionQuantity,
                        ),
                        risk = RiskEvidence(
                            plan = attempt.plan,
                            reservedRisk = reservation.reservedRisk,
                            remainingReservedRisk = reservation.reservedRisk,
                        ),
                    ),
                ),
            )
        }
    }

    private fun confirmReducingFill(
        event: RiskEvent.ConfirmedReducingFill,
    ): GlobalRiskSnapshot {
        require(event.confirmedRemainingQuantity.signum() > 0) {
            "Use recordConfirmedFlat when confirmed remaining quantity is zero"
        }
        val attempt = activeAttempt(event.levelId)
        val reservation = activeReservation(event.levelId)
        check(attempt.status == RiskAttemptStatus.OPEN_POSITION) {
            "A reducing fill requires confirmed open exposure"
        }
        require(event.confirmedRemainingQuantity < attempt.confirmedPositionQuantity) {
            "confirmedRemainingQuantity must reflect a reducing fill"
        }
        val previousReservedRisk = reservation.reservedRisk
        attempt.confirmedPositionQuantity = event.confirmedRemainingQuantity
        reservation.reservedRisk = reservation.levelRiskBudget
            .multiply(event.confirmedRemainingQuantity)
            .divide(reservation.plannedQuantity, MATH_CONTEXT)
        return publishState().also {
            evidenceRecorder.recordAudit(
                AuditRecordDraft(
                    timestamp = clock.instant(),
                    symbol = attempt.symbol,
                    levelId = attempt.levelId,
                    stateBefore = LevelState.APPROACH,
                    stateAfter = LevelState.APPROACH,
                    eventType = AuditEventType.RISK_UPDATED,
                    decision = "RESERVATION_REDUCED",
                    evidence = DecisionEvidence(
                        quantity = QuantityEvidence(
                            plannedQuantity = reservation.plannedQuantity,
                            remainingQuantity = event.confirmedRemainingQuantity,
                        ),
                        risk = RiskEvidence(
                            plan = attempt.plan,
                            releasedRisk = previousReservedRisk
                                .subtract(reservation.reservedRisk),
                            remainingReservedRisk = reservation.reservedRisk,
                        ),
                    ),
                ),
            )
        }
    }

    private fun confirmFlat(event: RiskEvent.ConfirmedFlat): GlobalRiskSnapshot {
        val attempt = activeAttempt(event.levelId)
        val reservation = activeReservation(event.levelId)
        reservations.remove(event.levelId)
        attempt.status = RiskAttemptStatus.FLAT_CONFIRMED
        attempt.confirmedPositionQuantity = BigDecimal.ZERO
        attempt.completedAt = event.confirmedAt
        return publishState().also {
            evidenceRecorder.recordAudit(
                AuditRecordDraft(
                    timestamp = event.confirmedAt,
                    symbol = attempt.symbol,
                    levelId = attempt.levelId,
                    stateBefore = LevelState.APPROACH,
                    stateAfter = LevelState.APPROACH,
                    eventType = AuditEventType.RISK_UPDATED,
                    decision = "FLAT_CONFIRMED",
                    evidence = DecisionEvidence(
                        quantity = QuantityEvidence(
                            plannedQuantity = reservation.plannedQuantity,
                            remainingQuantity = BigDecimal.ZERO,
                        ),
                        risk = RiskEvidence(
                            plan = attempt.plan,
                            releasedRisk = reservation.reservedRisk,
                            remainingReservedRisk = BigDecimal.ZERO,
                        ),
                    ),
                ),
            )
            evidenceRecorder.completeAttempt(
                levelId = attempt.levelId,
                symbol = attempt.symbol,
                completedAt = event.confirmedAt,
            )
        }
    }

    private fun blockers(
        request: AttemptAdmissionRequest,
        accountState: RiskAccountState,
        plan: AttemptRiskPlan,
    ): List<RiskBlockerCode> =
        buildList {
            if (plan.estimatedWorstNetLoss > plan.levelRiskBudget) {
                add(RiskBlockerCode.STOP_RISK_TOO_HIGH)
            }
            if (plan.plannedNetR < MINIMUM_PLANNED_NET_R) {
                add(RiskBlockerCode.PLANNED_NET_R_TOO_LOW)
            }
            if (
                plan.projectedIsolatedMargin >
                accountState.availableMargin.multiply(MAXIMUM_MARGIN_USAGE)
            ) {
                add(RiskBlockerCode.BLOCKED_MARGIN_BUFFER)
            }
            val liquidationSafe = when (request.direction) {
                LevelDirection.LONG ->
                    plan.estimatedLiquidationPrice < plan.structuralStopPrice

                LevelDirection.SHORT ->
                    plan.estimatedLiquidationPrice > plan.structuralStopPrice
            }
            if (!liquidationSafe) {
                add(RiskBlockerCode.LIQUIDATION_TOO_CLOSE)
            }

            val normalizedSymbol = request.symbol.normalizedSymbol()
            val activeSymbols = reservations.values
                .map(MutableRiskReservation::symbol)
                .toSet()
            if (
                normalizedSymbol !in activeSymbols &&
                activeSymbols.size >= MAXIMUM_ACTIVE_SYMBOLS
            ) {
                add(RiskBlockerCode.BLOCKED_POSITION_CAP)
            }
            if (reservations.values.any { it.symbol == normalizedSymbol }) {
                add(RiskBlockerCode.BLOCKED_SYMBOL_ATTEMPT)
            }

            val dailyLossLimit = dailyLossLimit(accountState)
            val projectedDailyRisk = tradingDrawdown(accountState)
                .add(reservedRisk(RiskReservationStatus.OPEN_POSITION))
                .add(reservedRisk(RiskReservationStatus.PENDING_ATTEMPT))
                .add(plan.levelRiskBudget)
            if (projectedDailyRisk > dailyLossLimit) {
                add(RiskBlockerCode.BLOCKED_DAILY_RISK)
            }
        }

    private fun plan(request: AttemptAdmissionRequest): AttemptRiskPlan {
        val structuralStopPrice = structuralStopPrice(request)
        val worstCappedEntryPrice = worstCappedEntryPrice(request)
        val reservedExitPrice = reservedExitPrice(
            request = request,
            structuralStopPrice = structuralStopPrice,
        )
        val entryFee = worstCappedEntryPrice
            .multiply(request.plannedQuantity)
            .multiply(request.takerFeeRate)
        val lossExitFee = reservedExitPrice
            .multiply(request.plannedQuantity)
            .multiply(request.takerFeeRate)
        val priceLoss = when (request.direction) {
            LevelDirection.LONG ->
                worstCappedEntryPrice.subtract(reservedExitPrice)

            LevelDirection.SHORT ->
                reservedExitPrice.subtract(worstCappedEntryPrice)
        }
            .max(BigDecimal.ZERO)
            .multiply(request.plannedQuantity)
        val worstNetLoss = priceLoss.add(entryFee).add(lossExitFee)
        val takeProfits = takeProfits(request)
        val grossReward = takeProfits.fold(BigDecimal.ZERO) { total, target ->
            val unitReward = when (request.direction) {
                LevelDirection.LONG ->
                    target.price.subtract(worstCappedEntryPrice)

                LevelDirection.SHORT ->
                    worstCappedEntryPrice.subtract(target.price)
            }
            total.add(unitReward.multiply(target.quantity))
        }
        val rewardExitFees = takeProfits.fold(BigDecimal.ZERO) { total, target ->
            total.add(target.estimatedExitFee)
        }
        val estimatedNetReward = grossReward
            .subtract(entryFee)
            .subtract(rewardExitFees)
        val plannedNetR = if (worstNetLoss.signum() == 0) {
            BigDecimal.ZERO
        } else {
            estimatedNetReward.divide(worstNetLoss, MATH_CONTEXT)
        }
        val selectedLeverage = min(MAXIMUM_LEVERAGE, request.leverageBracket.maximumLeverage)
        val projectedNotional = worstCappedEntryPrice.multiply(request.plannedQuantity)
        val projectedMargin = projectedNotional.divide(
            selectedLeverage.toBigDecimal(),
            MATH_CONTEXT,
        )
        val liquidationPrice = estimatedLiquidationPrice(
            request = request,
            entryPrice = worstCappedEntryPrice,
            selectedLeverage = selectedLeverage,
        )
        return AttemptRiskPlan(
            levelRiskBudget = request.positionNotionalUsdt.multiply(ONE_PERCENT),
            structuralStopPrice = structuralStopPrice,
            worstCappedEntryPrice = worstCappedEntryPrice,
            reservedExitPrice = reservedExitPrice,
            takeProfits = takeProfits,
            estimatedEntryFee = entryFee,
            estimatedLossExitFee = lossExitFee,
            estimatedWorstNetLoss = worstNetLoss,
            estimatedNetReward = estimatedNetReward,
            plannedNetR = plannedNetR,
            selectedLeverage = selectedLeverage,
            projectedIsolatedMargin = projectedMargin,
            estimatedLiquidationPrice = liquidationPrice,
        )
    }

    private fun structuralStopPrice(
        request: AttemptAdmissionRequest,
    ): BigDecimal {
        val rawStop = when (request.direction) {
            LevelDirection.LONG -> minOf(
                request.levelPrice.subtract(request.frozenNpu.multiply(THREE)),
                request.precedingOneSecondTradePrices.min()
                    .subtract(request.frozenNpu),
            )

            LevelDirection.SHORT -> maxOf(
                request.levelPrice.add(request.frozenNpu.multiply(THREE)),
                request.precedingOneSecondTradePrices.max()
                    .add(request.frozenNpu),
            )
        }
        val roundingMode = when (request.direction) {
            LevelDirection.LONG -> RoundingMode.DOWN
            LevelDirection.SHORT -> RoundingMode.UP
        }
        return roundToIncrement(rawStop, request.tickSize, roundingMode)
    }

    private fun worstCappedEntryPrice(
        request: AttemptAdmissionRequest,
    ): BigDecimal =
        when (request.direction) {
            LevelDirection.LONG -> roundToIncrement(
                request.bestAskPrice.add(request.frozenNpu),
                request.tickSize,
                RoundingMode.UP,
            )

            LevelDirection.SHORT -> roundToIncrement(
                request.bestBidPrice.subtract(request.frozenNpu),
                request.tickSize,
                RoundingMode.DOWN,
            )
        }

    private fun reservedExitPrice(
        request: AttemptAdmissionRequest,
        structuralStopPrice: BigDecimal,
    ): BigDecimal =
        when (request.direction) {
            LevelDirection.LONG -> roundToIncrement(
                structuralStopPrice.subtract(request.frozenNpu),
                request.tickSize,
                RoundingMode.DOWN,
            )

            LevelDirection.SHORT -> roundToIncrement(
                structuralStopPrice.add(request.frozenNpu),
                request.tickSize,
                RoundingMode.UP,
            )
        }

    private fun takeProfits(
        request: AttemptAdmissionRequest,
    ): List<PlannedTakeProfit> =
        TAKE_PROFIT_SPECS.map { spec ->
            val impulse = request.levelPrice
                .multiply(request.maxImpulsePct)
                .multiply(spec.impulseFraction)
                .divide(ONE_HUNDRED, MATH_CONTEXT)
            val rawPrice = when (request.direction) {
                LevelDirection.LONG -> request.levelPrice.add(impulse)
                LevelDirection.SHORT -> request.levelPrice.subtract(impulse)
            }
            val price = when (request.direction) {
                LevelDirection.LONG -> roundToIncrement(
                    rawPrice,
                    request.tickSize,
                    RoundingMode.DOWN,
                )

                LevelDirection.SHORT -> roundToIncrement(
                    rawPrice,
                    request.tickSize,
                    RoundingMode.UP,
                )
            }
            val quantity = request.plannedQuantity
                .multiply(spec.allocationPercent.toBigDecimal())
                .divide(ONE_HUNDRED, MATH_CONTEXT)
            PlannedTakeProfit(
                allocationPercent = spec.allocationPercent,
                impulseFraction = spec.impulseFraction,
                price = price,
                quantity = quantity,
                estimatedExitFee = price
                    .multiply(quantity)
                    .multiply(request.takerFeeRate),
            )
        }.also { targets ->
            require(targets.all { target -> target.price.signum() > 0 }) {
                "Take-profit prices must be positive"
            }
        }

    private fun estimatedLiquidationPrice(
        request: AttemptAdmissionRequest,
        entryPrice: BigDecimal,
        selectedLeverage: Int,
    ): BigDecimal {
        val bracket = request.leverageBracket
        val leverageFraction = BigDecimal.ONE.divide(
            selectedLeverage.toBigDecimal(),
            MATH_CONTEXT,
        )
        val cumulativePerUnit = bracket.cumulativeMaintenanceAmount.divide(
            request.plannedQuantity,
            MATH_CONTEXT,
        )
        return when (request.direction) {
            LevelDirection.LONG -> entryPrice
                .multiply(BigDecimal.ONE.subtract(leverageFraction))
                .subtract(cumulativePerUnit)
                .divide(
                    BigDecimal.ONE.subtract(bracket.maintenanceMarginRatio),
                    MATH_CONTEXT,
                )

            LevelDirection.SHORT -> entryPrice
                .multiply(BigDecimal.ONE.add(leverageFraction))
                .add(cumulativePerUnit)
                .divide(
                    BigDecimal.ONE.add(bracket.maintenanceMarginRatio),
                    MATH_CONTEXT,
                )
        }
    }

    private fun validate(
        request: AttemptAdmissionRequest,
        accountState: RiskAccountState,
    ) {
        require(request.symbol.normalizedSymbol().isNotEmpty()) {
            "symbol must not be blank"
        }
        listOf(
            "levelPrice" to request.levelPrice,
            "positionNotionalUsdt" to request.positionNotionalUsdt,
            "plannedQuantity" to request.plannedQuantity,
            "maxImpulsePct" to request.maxImpulsePct,
            "frozenNpu" to request.frozenNpu,
            "bestBidPrice" to request.bestBidPrice,
            "bestAskPrice" to request.bestAskPrice,
            "tickSize" to request.tickSize,
        ).forEach { (name, value) ->
            require(value.signum() > 0) { "$name must be positive" }
        }
        require(request.bestAskPrice >= request.bestBidPrice) {
            "bestAskPrice must not be below bestBidPrice"
        }
        require(request.takerFeeRate.signum() >= 0) {
            "takerFeeRate must not be negative"
        }
        require(request.precedingOneSecondTradePrices.isNotEmpty()) {
            "precedingOneSecondTradePrices must not be empty"
        }
        require(request.precedingOneSecondTradePrices.all { it.signum() > 0 }) {
            "preceding one-second trade prices must be positive"
        }
        require(request.leverageBracket.maximumLeverage > 0) {
            "maximumLeverage must be positive"
        }
        require(
            request.leverageBracket.maintenanceMarginRatio.signum() >= 0 &&
                request.leverageBracket.maintenanceMarginRatio < BigDecimal.ONE,
        ) {
            "maintenanceMarginRatio must be in [0, 1)"
        }
        require(request.leverageBracket.cumulativeMaintenanceAmount.signum() >= 0) {
            "cumulativeMaintenanceAmount must not be negative"
        }
        require(accountState.dailyAnchorEquity.signum() > 0) {
            "dailyAnchorEquity must be positive"
        }
        require(accountState.currentTotalAccountEquity.signum() >= 0) {
            "currentTotalAccountEquity must not be negative"
        }
        require(accountState.availableMargin.signum() >= 0) {
            "availableMargin must not be negative"
        }
        require(accountState.depositsSinceAnchor.signum() >= 0) {
            "depositsSinceAnchor must not be negative"
        }
        require(accountState.withdrawalsSinceAnchor.signum() >= 0) {
            "withdrawalsSinceAnchor must not be negative"
        }
    }

    private fun activeAttempt(levelId: UUID): MutableRiskAttempt {
        val attempt = attempts[levelId]
            ?: error("No admitted attempt exists for level $levelId")
        check(attempt.status != RiskAttemptStatus.FLAT_CONFIRMED) {
            "Attempt for level $levelId is already flat"
        }
        return attempt
    }

    private fun activeReservation(levelId: UUID): MutableRiskReservation =
        reservations[levelId]
            ?: error("No active reservation exists for level $levelId")

    private fun publishState(): GlobalRiskSnapshot =
        snapshot(clock.instant()).also(publishedState::set)

    private fun snapshot(now: Instant): GlobalRiskSnapshot {
        val accountState = latestAccountState
        val openRisk = reservedRisk(RiskReservationStatus.OPEN_POSITION)
        val pendingRisk = reservedRisk(RiskReservationStatus.PENDING_ATTEMPT)
        val totalReservedRisk = openRisk.add(pendingRisk)
        val drawdown = accountState?.let(::tradingDrawdown)
        val lossLimit = accountState?.let(::dailyLossLimit)
        val remainingCapacity = if (drawdown == null || lossLimit == null) {
            null
        } else {
            lossLimit.subtract(drawdown).subtract(totalReservedRisk)
                .max(BigDecimal.ZERO)
        }
        return GlobalRiskSnapshot(
            observedAt = now,
            dailyAnchorEquity = accountState?.dailyAnchorEquity,
            currentTotalAccountEquity =
                accountState?.currentTotalAccountEquity,
            dailyLossLimit = lossLimit,
            tradingDrawdown = drawdown,
            reservedRiskForOpenPositions = openRisk,
            reservedRiskForPendingAttempts = pendingRisk,
            totalReservedRisk = totalReservedRisk,
            remainingDailyCapacity = remainingCapacity,
            openSymbolCount = reservations.values
                .filter { it.status == RiskReservationStatus.OPEN_POSITION }
                .map(MutableRiskReservation::symbol)
                .toSet()
                .size,
            activeAttemptSymbolCount = reservations.values
                .map(MutableRiskReservation::symbol)
                .toSet()
                .size,
            attempts = attempts.values
                .sortedBy(MutableRiskAttempt::sequence)
                .map(MutableRiskAttempt::snapshot),
            reservations = reservations.values
                .sortedBy(MutableRiskReservation::sequence)
                .map(MutableRiskReservation::snapshot),
        )
    }

    private fun reservedRisk(status: RiskReservationStatus): BigDecimal =
        reservations.values
            .asSequence()
            .filter { reservation -> reservation.status == status }
            .fold(BigDecimal.ZERO) { total, reservation ->
                total.add(reservation.reservedRisk)
            }

    private fun tradingDrawdown(accountState: RiskAccountState): BigDecimal =
        accountState.dailyAnchorEquity
            .subtract(accountState.currentTotalAccountEquity)
            .add(accountState.depositsSinceAnchor)
            .subtract(accountState.withdrawalsSinceAnchor)

    private fun dailyLossLimit(accountState: RiskAccountState): BigDecimal =
        accountState.dailyAnchorEquity.multiply(DAILY_LOSS_PERCENT)

    private fun roundToIncrement(
        value: BigDecimal,
        increment: BigDecimal,
        roundingMode: RoundingMode,
    ): BigDecimal {
        val incrementScale = increment.stripTrailingZeros().scale().coerceAtLeast(0)
        return value
            .divide(increment, 0, roundingMode)
            .multiply(increment)
            .setScale(incrementScale)
    }
}

private sealed interface RiskEvent {
    data class Admit(
        val request: AttemptAdmissionRequest,
        val accountState: RiskAccountState,
        val admittedAt: Instant,
    ) : RiskEvent

    data class ConfirmedExposure(
        val levelId: UUID,
        val confirmedPositionQuantity: BigDecimal,
    ) : RiskEvent

    data class ConfirmedReducingFill(
        val levelId: UUID,
        val confirmedRemainingQuantity: BigDecimal,
    ) : RiskEvent

    data class ConfirmedFlat(
        val levelId: UUID,
        val confirmedAt: Instant,
    ) : RiskEvent
}

private data class MutableRiskAttempt(
    val sequence: Long,
    val levelId: UUID,
    val symbol: String,
    var status: RiskAttemptStatus,
    val admittedAt: Instant,
    var completedAt: Instant?,
    var confirmedPositionQuantity: BigDecimal,
    val plan: AttemptRiskPlan,
) {
    fun snapshot(): RiskAttemptSnapshot =
        RiskAttemptSnapshot(
            sequence = sequence,
            levelId = levelId,
            symbol = symbol,
            status = status,
            admittedAt = admittedAt,
            completedAt = completedAt,
            confirmedPositionQuantity = confirmedPositionQuantity,
            plan = plan,
        )
}

private data class MutableRiskReservation(
    val sequence: Long,
    val levelId: UUID,
    val symbol: String,
    var status: RiskReservationStatus,
    val levelRiskBudget: BigDecimal,
    var reservedRisk: BigDecimal,
    val plannedQuantity: BigDecimal,
) {
    fun snapshot(): RiskReservationSnapshot =
        RiskReservationSnapshot(
            sequence = sequence,
            levelId = levelId,
            symbol = symbol,
            status = status,
            levelRiskBudget = levelRiskBudget,
            reservedRisk = reservedRisk,
            plannedQuantity = plannedQuantity,
        )
}

private data class TakeProfitSpec(
    val allocationPercent: Int,
    val impulseFraction: BigDecimal,
)

private fun String.normalizedSymbol(): String = trim().uppercase()

private fun emptySnapshot(now: Instant): GlobalRiskSnapshot =
    GlobalRiskSnapshot(
        observedAt = now,
        dailyAnchorEquity = null,
        currentTotalAccountEquity = null,
        dailyLossLimit = null,
        tradingDrawdown = null,
        reservedRiskForOpenPositions = BigDecimal.ZERO,
        reservedRiskForPendingAttempts = BigDecimal.ZERO,
        totalReservedRisk = BigDecimal.ZERO,
        remainingDailyCapacity = null,
        openSymbolCount = 0,
        activeAttemptSymbolCount = 0,
        attempts = emptyList(),
        reservations = emptyList(),
    )

private val MATH_CONTEXT = MathContext.DECIMAL128
private const val MAXIMUM_LEVERAGE = 20
private const val MAXIMUM_ACTIVE_SYMBOLS = 5
private val ONE_HUNDRED = BigDecimal("100")
private val THREE = BigDecimal("3")
private val ONE_PERCENT = BigDecimal("0.01")
private val DAILY_LOSS_PERCENT = BigDecimal("0.05")
private val MAXIMUM_MARGIN_USAGE = BigDecimal("0.80")
private val MINIMUM_PLANNED_NET_R = BigDecimal("1.5")
private val TAKE_PROFIT_SPECS = listOf(
    TakeProfitSpec(33, BigDecimal("0.35")),
    TakeProfitSpec(33, BigDecimal("0.70")),
    TakeProfitSpec(34, BigDecimal.ONE),
)
