package com.scalpsecta.breakoutbot.risk

import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

class AttemptRiskServiceTest {
    private val now = Instant.parse("2026-08-01T12:00:00Z")
    private val clock = MutableRiskClock(now)
    private val scheduler = Schedulers.newSingle("attempt-risk-test")
    private val service = AttemptRiskService(
        clock = clock,
        scheduler = scheduler,
    )

    @AfterEach
    fun closeService() {
        service.close()
    }

    @Test
    fun `plans and atomically reserves exactly one percent only at admission`() {
        assertThat(service.currentState().reservations).isEmpty()

        val decision = service.admit(request(), account()).block()!!

        assertThat(decision.admitted).isTrue()
        assertThat(decision.blockers).isEmpty()
        assertThat(decision.plan.levelRiskBudget)
            .isEqualByComparingTo(BigDecimal("10"))
        assertThat(decision.plan.structuralStopPrice)
            .isEqualByComparingTo(BigDecimal("99.7"))
        assertThat(decision.plan.worstCappedEntryPrice)
            .isEqualByComparingTo(BigDecimal("100.0"))
        assertThat(decision.plan.reservedExitPrice)
            .isEqualByComparingTo(BigDecimal("99.6"))
        assertThat(decision.plan.takeProfits.map(PlannedTakeProfit::price))
            .usingElementComparator(BigDecimal::compareTo)
            .containsExactly(
                BigDecimal("100.7"),
                BigDecimal("101.4"),
                BigDecimal("102.0"),
            )
        assertThat(decision.plan.estimatedWorstNetLoss)
            .isEqualByComparingTo(BigDecimal("4.7984"))
        assertThat(decision.plan.plannedNetR)
            .isGreaterThanOrEqualTo(BigDecimal("1.5"))
        assertThat(decision.plan.selectedLeverage).isEqualTo(20)
        assertThat(decision.plan.projectedIsolatedMargin)
            .isEqualByComparingTo(BigDecimal("50"))
        assertThat(decision.plan.estimatedLiquidationPrice)
            .isLessThan(decision.plan.structuralStopPrice)

        assertThat(decision.state.dailyLossLimit)
            .isEqualByComparingTo(BigDecimal("50"))
        assertThat(decision.state.tradingDrawdown)
            .isEqualByComparingTo(BigDecimal.ZERO)
        assertThat(decision.state.reservedRiskForPendingAttempts)
            .isEqualByComparingTo(BigDecimal("10"))
        assertThat(decision.state.reservedRiskForOpenPositions)
            .isEqualByComparingTo(BigDecimal.ZERO)
        assertThat(decision.state.remainingDailyCapacity)
            .isEqualByComparingTo(BigDecimal("40"))
        assertThat(decision.state.attempts.single().status)
            .isEqualTo(RiskAttemptStatus.PENDING_ENTRY)
    }

    @Test
    fun `safe mode is serialized with global admission and blocks new attempts`() {
        val safe = service.enterSafeMode("ORDER_OUTCOME_UNKNOWN").block()!!
        val decision = service.admit(request(), account()).block()!!

        assertThat(safe.globalTradingState)
            .isEqualTo(GlobalTradingState.SAFE_MODE)
        assertThat(safe.stateReason).isEqualTo("ORDER_OUTCOME_UNKNOWN")
        assertThat(decision.admitted).isFalse()
        assertThat(decision.blockers).contains(RiskBlockerCode.BLOCKED_SAFE_MODE)
        assertThat(decision.state.reservations).isEmpty()
    }

    @Test
    fun `mirrors structural prices and respects a bracket below twenty times`() {
        val decision = service.admit(
            request(
                symbol = "ETHUSDT",
                direction = LevelDirection.SHORT,
                precedingTradePrices = listOf("100.20", "100.25"),
                bestBidPrice = "100.1",
                bestAskPrice = "100.2",
                maximumLeverage = 7,
            ),
            account(),
        ).block()!!

        assertThat(decision.admitted).isTrue()
        assertThat(decision.plan.structuralStopPrice)
            .isEqualByComparingTo(BigDecimal("100.4"))
        assertThat(decision.plan.worstCappedEntryPrice)
            .isEqualByComparingTo(BigDecimal("100.0"))
        assertThat(decision.plan.reservedExitPrice)
            .isEqualByComparingTo(BigDecimal("100.5"))
        assertThat(decision.plan.takeProfits.map(PlannedTakeProfit::price))
            .usingElementComparator(BigDecimal::compareTo)
            .containsExactly(
                BigDecimal("99.3"),
                BigDecimal("98.6"),
                BigDecimal("98.0"),
            )
        assertThat(decision.plan.selectedLeverage).isEqualTo(7)
        assertThat(decision.plan.estimatedLiquidationPrice)
            .isGreaterThan(decision.plan.structuralStopPrice)
    }

    @Test
    fun `reports every economic margin liquidation and daily blocker together`() {
        val decision = service.admit(
            request(
                positionNotionalUsdt = "2000",
                plannedQuantity = "20",
                maxImpulsePct = "0.1",
                frozenNpu = "0.5",
                precedingTradePrices = listOf("90"),
            ),
            account(
                currentEquity = "960",
                availableMargin = "50",
            ),
        ).block()!!

        assertThat(decision.admitted).isFalse()
        assertThat(decision.blockers).containsExactly(
            RiskBlockerCode.STOP_RISK_TOO_HIGH,
            RiskBlockerCode.PLANNED_NET_R_TOO_LOW,
            RiskBlockerCode.BLOCKED_MARGIN_BUFFER,
            RiskBlockerCode.LIQUIDATION_TOO_CLOSE,
            RiskBlockerCode.BLOCKED_DAILY_RISK,
        )
        assertThat(decision.state.reservations).isEmpty()
        assertThat(decision.state.attempts).isEmpty()
        assertThat(decision.plan.structuralStopPrice)
            .isEqualByComparingTo(BigDecimal("89.5"))
    }

    @Test
    fun `serializes competing admissions without overbooking daily capacity`() {
        val constrainedAccount = account(
            anchorEquity = "1000",
            currentEquity = "1000",
            availableMargin = "10000",
        )

        val result = Mono.zip(
            service.admit(
                request(
                    levelId = UUID.fromString("00000000-0000-0000-0000-000000000001"),
                    symbol = "BTCUSDT",
                    positionNotionalUsdt = "3000",
                    plannedQuantity = "30",
                ),
                constrainedAccount,
            ),
            service.admit(
                request(
                    levelId = UUID.fromString("00000000-0000-0000-0000-000000000002"),
                    symbol = "ETHUSDT",
                    positionNotionalUsdt = "3000",
                    plannedQuantity = "30",
                ),
                constrainedAccount,
            ),
        ).block()!!
        val decisions = listOf(result.t1, result.t2)

        assertThat(decisions.count { it.admitted }).isOne()
        assertThat(decisions.filterNot { it.admitted }.single().blockers)
            .contains(RiskBlockerCode.BLOCKED_DAILY_RISK)
        assertThat(service.currentState().totalReservedRisk)
            .isEqualByComparingTo(BigDecimal("30"))
        assertThat(service.currentState().reservations).hasSize(1)
    }

    @Test
    fun `serializes same-symbol attempts and admits only one owner`() {
        val roomyAccount = account(
            anchorEquity = "100000",
            currentEquity = "100000",
            availableMargin = "100000",
        )

        val result = Mono.zip(
            service.admit(
                request(
                    levelId = UUID.fromString("00000000-0000-0000-0000-000000000011"),
                ),
                roomyAccount,
            ),
            service.admit(
                request(
                    levelId = UUID.fromString("00000000-0000-0000-0000-000000000012"),
                ),
                roomyAccount,
            ),
        ).block()!!
        val decisions = listOf(result.t1, result.t2)

        assertThat(decisions.count { it.admitted }).isOne()
        assertThat(decisions.filterNot { it.admitted }.single().blockers)
            .contains(RiskBlockerCode.BLOCKED_SYMBOL_ATTEMPT)
        assertThat(service.currentState().activeAttemptSymbolCount).isOne()
    }

    @Test
    fun `reserves no more than five potential exposure symbols`() {
        val roomyAccount = account(
            anchorEquity = "100000",
            currentEquity = "100000",
            availableMargin = "100000",
        )
        repeat(5) { index ->
            val decision = service.admit(
                request(
                    levelId = UUID.nameUUIDFromBytes("level-$index".toByteArray()),
                    symbol = "SYMBOL${index}USDT",
                ),
                roomyAccount,
            ).block()!!
            assertThat(decision.admitted).isTrue()
        }

        val blocked = service.admit(
            request(
                levelId = UUID.nameUUIDFromBytes("level-5".toByteArray()),
                symbol = "SYMBOL5USDT",
            ),
            roomyAccount,
        ).block()!!

        assertThat(blocked.admitted).isFalse()
        assertThat(blocked.blockers)
            .containsExactly(RiskBlockerCode.BLOCKED_POSITION_CAP)
        assertThat(blocked.state.activeAttemptSymbolCount).isEqualTo(5)
        assertThat(blocked.state.reservations).hasSize(5)
    }

    @Test
    fun `shrinks only after a confirmed reducing fill and releases only when flat`() {
        val levelId = UUID.fromString("00000000-0000-0000-0000-000000000021")
        service.admit(request(levelId = levelId), account()).block()

        val open = service.recordConfirmedExposure(
            levelId = levelId,
            confirmedPositionQuantity = BigDecimal("10"),
        ).block()!!
        assertThat(open.reservedRiskForPendingAttempts)
            .isEqualByComparingTo(BigDecimal.ZERO)
        assertThat(open.reservedRiskForOpenPositions)
            .isEqualByComparingTo(BigDecimal("10"))

        val notReducing = catchThrowable {
            service.recordConfirmedReducingFill(
                levelId = levelId,
                confirmedRemainingQuantity = BigDecimal("10"),
            ).block()
        }
        assertThat(notReducing).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(service.currentState().reservedRiskForOpenPositions)
            .isEqualByComparingTo(BigDecimal("10"))

        val reduced = service.recordConfirmedReducingFill(
            levelId = levelId,
            confirmedRemainingQuantity = BigDecimal("4"),
        ).block()!!
        assertThat(reduced.reservedRiskForOpenPositions)
            .isEqualByComparingTo(BigDecimal("4"))
        assertThat(reduced.reservations.single().reservedRisk)
            .isEqualByComparingTo(BigDecimal("4"))

        val flat = service.recordConfirmedFlat(levelId).block()!!
        assertThat(flat.totalReservedRisk)
            .isEqualByComparingTo(BigDecimal.ZERO)
        assertThat(flat.reservations).isEmpty()
        assertThat(flat.attempts.single().status)
            .isEqualTo(RiskAttemptStatus.FLAT_CONFIRMED)
    }

    @Test
    fun `includes transfers in the trading drawdown formula`() {
        val decision = service.admit(
            request(),
            account(
                currentEquity = "970",
                depositsSinceAnchor = "20",
                withdrawalsSinceAnchor = "5",
            ),
        ).block()!!

        assertThat(decision.state.tradingDrawdown)
            .isEqualByComparingTo(BigDecimal("45"))
        assertThat(decision.admitted).isFalse()
        assertThat(decision.blockers)
            .containsExactly(RiskBlockerCode.BLOCKED_DAILY_RISK)
    }

    @Test
    fun `labels the startup anchor as temporary until the next utc boundary`() {
        val evaluation = service.observeDailyAccount(
            DailyRiskAccountObservation(
                totalAccountEquity = BigDecimal("1000"),
                availableMargin = BigDecimal("900"),
            ),
        ).block()!!

        assertThat(evaluation.startupAnchorEstablished).isTrue()
        assertThat(evaluation.state.dailyAnchorKind)
            .isEqualTo(DailyAnchorKind.TEMPORARY_RESTART)
        assertThat(evaluation.state.dailyAnchorEstablishedAt).isEqualTo(now)
        assertThat(evaluation.state.nextDailyBoundary)
            .isEqualTo(Instant.parse("2026-08-02T03:00:00Z"))
    }

    @Test
    fun `locks at the exact five percent drawdown boundary`() {
        service.observeDailyAccount(observation(equity = "1000")).block()

        val evaluation = service.observeDailyAccount(
            observation(equity = "950"),
        ).block()!!

        assertThat(evaluation.dailyBreachTriggered).isTrue()
        assertThat(evaluation.state.tradingDrawdown)
            .isEqualByComparingTo(BigDecimal("50"))
        assertThat(evaluation.state.globalTradingState)
            .isEqualTo(GlobalTradingState.DAILY_LOCKED)
    }

    @Test
    fun `transfer deltas are removed from monitored trading performance`() {
        service.observeDailyAccount(observation(equity = "1000")).block()
        service.recordDailyTransfer(
            DailyRiskTransfer(
                deposits = BigDecimal("20"),
                withdrawals = BigDecimal("5"),
            ),
        ).block()

        val evaluation = service.observeDailyAccount(
            observation(equity = "970"),
        ).block()!!

        assertThat(evaluation.dailyBreachTriggered).isFalse()
        assertThat(evaluation.state.tradingDrawdown)
            .isEqualByComparingTo(BigDecimal("45"))
        assertThat(evaluation.state.depositsSinceAnchor)
            .isEqualByComparingTo(BigDecimal("20"))
        assertThat(evaluation.state.withdrawalsSinceAnchor)
            .isEqualByComparingTo(BigDecimal("5"))
    }

    @Test
    fun `daily rollover preserves open risk and clears a daily lock`() {
        service.observeDailyAccount(observation(equity = "1000")).block()
        val levelId = UUID.fromString(
            "00000000-0000-0000-0000-000000000031",
        )
        service.admit(request(levelId = levelId), account()).block()
        service.recordConfirmedExposure(levelId, BigDecimal("10")).block()
        service.observeDailyAccount(observation(equity = "950")).block()
        assertThat(service.currentState().globalTradingState)
            .isEqualTo(GlobalTradingState.DAILY_LOCKED)

        clock.advance(Duration.ofHours(15))
        val rollover = service.observeDailyAccount(
            observation(equity = "940"),
        ).block()!!

        assertThat(rollover.dailyRollover).isTrue()
        assertThat(rollover.state.globalTradingState)
            .isEqualTo(GlobalTradingState.RUNNING)
        assertThat(rollover.state.dailyAnchorKind)
            .isEqualTo(DailyAnchorKind.SCHEDULED_03_00_UTC)
        assertThat(rollover.state.dailyAnchorEstablishedAt)
            .isEqualTo(Instant.parse("2026-08-02T03:00:00Z"))
        assertThat(rollover.state.dailyAnchorEquity)
            .isEqualByComparingTo(BigDecimal("940"))
        assertThat(rollover.state.reservedRiskForOpenPositions)
            .isEqualByComparingTo(BigDecimal("10"))
        assertThat(rollover.state.attempts.single().admittedAt).isEqualTo(now)
    }

    @Test
    fun `profitable result resets losses and three later losses cool entries`() {
        val openLevelId = UUID.fromString(
            "00000000-0000-0000-0000-000000000040",
        )
        service.admit(
            request(levelId = openLevelId, symbol = "OPENUSDT"),
            account(),
        ).block()
        service.recordConfirmedExposure(openLevelId, BigDecimal("10")).block()

        closeAttempt(41, "-1")
        closeAttempt(42, "-2")
        closeAttempt(43, "1")
        assertThat(service.currentState().consecutiveLossCount).isZero()

        closeAttempt(44, "-1")
        closeAttempt(45, "-1")
        val cooled = closeAttempt(46, "-1")

        assertThat(cooled.globalTradingState)
            .isEqualTo(GlobalTradingState.ENTRY_COOLDOWN)
        assertThat(cooled.consecutiveLossCount).isEqualTo(3)
        assertThat(cooled.entryCooldownUntil)
            .isEqualTo(now.plus(Duration.ofMinutes(15)))
        assertThat(cooled.reservations.map(RiskReservationSnapshot::levelId))
            .contains(openLevelId)
        assertThat(cooled.reservedRiskForOpenPositions)
            .isEqualByComparingTo(BigDecimal("10"))
        val blocked = service.admit(
            request(
                levelId = UUID.fromString(
                    "00000000-0000-0000-0000-000000000047",
                ),
            ),
            account(),
        ).block()!!
        assertThat(blocked.blockers)
            .contains(RiskBlockerCode.BLOCKED_ENTRY_COOLDOWN)

        clock.advance(Duration.ofMinutes(15))
        assertThat(service.currentState().globalTradingState)
            .isEqualTo(GlobalTradingState.RUNNING)
    }

    private fun closeAttempt(
        suffix: Int,
        netPnl: String,
    ): GlobalRiskSnapshot {
        val levelId = UUID.fromString(
            "00000000-0000-0000-0000-${suffix.toString().padStart(12, '0')}",
        )
        service.admit(request(levelId = levelId), account()).block()
        return service.recordConfirmedFlat(
            levelId = levelId,
            netPnl = BigDecimal(netPnl),
        ).block()!!
    }

    private fun observation(
        equity: String,
        availableMargin: String = "1000",
    ): DailyRiskAccountObservation =
        DailyRiskAccountObservation(
            totalAccountEquity = BigDecimal(equity),
            availableMargin = BigDecimal(availableMargin),
        )

    private fun request(
        levelId: UUID = UUID.fromString("00000000-0000-0000-0000-000000000001"),
        symbol: String = "BTCUSDT",
        direction: LevelDirection = LevelDirection.LONG,
        levelPrice: String = "100",
        positionNotionalUsdt: String = "1000",
        plannedQuantity: String = "10",
        maxImpulsePct: String = "2",
        frozenNpu: String = "0.1",
        precedingTradePrices: List<String> = listOf("99.85", "99.9"),
        bestBidPrice: String = "99.8",
        bestAskPrice: String = "99.9",
        maximumLeverage: Int = 50,
    ): AttemptAdmissionRequest =
        AttemptAdmissionRequest(
            levelId = levelId,
            symbol = symbol,
            direction = direction,
            levelPrice = BigDecimal(levelPrice),
            positionNotionalUsdt = BigDecimal(positionNotionalUsdt),
            plannedQuantity = BigDecimal(plannedQuantity),
            maxImpulsePct = BigDecimal(maxImpulsePct),
            frozenNpu = BigDecimal(frozenNpu),
            precedingOneSecondTradePrices = precedingTradePrices.map(::BigDecimal),
            bestBidPrice = BigDecimal(bestBidPrice),
            bestAskPrice = BigDecimal(bestAskPrice),
            tickSize = BigDecimal("0.1"),
            takerFeeRate = BigDecimal("0.0004"),
            leverageBracket = RiskLeverageBracket(
                maximumLeverage = maximumLeverage,
                maintenanceMarginRatio = BigDecimal("0.004"),
                cumulativeMaintenanceAmount = BigDecimal.ZERO,
            ),
        )

    private fun account(
        anchorEquity: String = "1000",
        currentEquity: String = "1000",
        availableMargin: String = "1000",
        depositsSinceAnchor: String = "0",
        withdrawalsSinceAnchor: String = "0",
    ): RiskAccountState =
        RiskAccountState(
            dailyAnchorEquity = BigDecimal(anchorEquity),
            currentTotalAccountEquity = BigDecimal(currentEquity),
            availableMargin = BigDecimal(availableMargin),
            depositsSinceAnchor = BigDecimal(depositsSinceAnchor),
            withdrawalsSinceAnchor = BigDecimal(withdrawalsSinceAnchor),
        )
}

private class MutableRiskClock(
    initialInstant: Instant,
) : Clock() {
    private val currentInstant = AtomicReference(initialInstant)

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = currentInstant.get()

    fun advance(duration: Duration) {
        currentInstant.updateAndGet { instant -> instant.plus(duration) }
    }
}
