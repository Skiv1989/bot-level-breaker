package com.scalpsecta.breakoutbot.risk

import com.scalpsecta.breakoutbot.level.LevelDirection
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID

class AttemptRiskServiceTest {
    private val now = Instant.parse("2026-08-01T12:00:00Z")
    private val scheduler = Schedulers.newSingle("attempt-risk-test")
    private val service = AttemptRiskService(
        clock = Clock.fixed(now, ZoneOffset.UTC),
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
