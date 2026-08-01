package com.scalpsecta.breakoutbot.level

import com.scalpsecta.breakoutbot.signal.AccelerationSnapshot
import com.scalpsecta.breakoutbot.signal.BurstSnapshot
import com.scalpsecta.breakoutbot.signal.BurstStatus
import com.scalpsecta.breakoutbot.signal.LevelSignalSnapshot
import com.scalpsecta.breakoutbot.signal.MandatoryGatesSnapshot
import com.scalpsecta.breakoutbot.signal.MetricWindowsSnapshot
import com.scalpsecta.breakoutbot.signal.NpuSnapshot
import com.scalpsecta.breakoutbot.signal.PressureScoreSnapshot
import com.scalpsecta.breakoutbot.signal.RampSnapshot
import com.scalpsecta.breakoutbot.signal.SignalGate
import com.scalpsecta.breakoutbot.signal.SignalGateResult
import com.scalpsecta.breakoutbot.signal.WindowMetricsSnapshot
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

class BreakoutStateMachineTest {
    private val startedAt = Instant.parse("2026-08-01T10:00:00Z")

    @Test
    fun `pre-break invalidations use exact virtual-clock boundaries`() {
        val oppositeFlow = machine()
        assertThat(
            oppositeFlow.evaluatePreBreak(
                observation(
                    millis = 0,
                    signal = signal(
                        directionalShare = "0.49",
                        deltaRate = "-1",
                    ),
                ),
                includeNoCrossTimeout = true,
            ),
        ).isNull()
        assertThat(
            oppositeFlow.evaluatePreBreak(
                observation(
                    millis = 499,
                    signal = signal(
                        directionalShare = "0.49",
                        deltaRate = "-1",
                    ),
                ),
                includeNoCrossTimeout = true,
            ),
        ).isNull()
        assertThat(
            oppositeFlow.evaluatePreBreak(
                observation(
                    millis = 500,
                    signal = signal(
                        directionalShare = "0.49",
                        deltaRate = "-1",
                    ),
                ),
                includeNoCrossTimeout = true,
            ),
        ).isEqualTo(LevelReasonCode.PRE_ENTRY_INVALIDATED)

        val collapsedAcceleration = machine()
        collapsedAcceleration.evaluatePreBreak(
            observation(
                millis = 0,
                signal = signal(acceleration = "0.99"),
            ),
            includeNoCrossTimeout = true,
        )
        assertThat(
            collapsedAcceleration.evaluatePreBreak(
                observation(
                    millis = 499,
                    signal = signal(acceleration = "0.99"),
                ),
                includeNoCrossTimeout = true,
            ),
        ).isNull()
        assertThat(
            collapsedAcceleration.evaluatePreBreak(
                observation(
                    millis = 500,
                    signal = signal(acceleration = "0.99"),
                ),
                includeNoCrossTimeout = true,
            ),
        ).isEqualTo(LevelReasonCode.PRE_ENTRY_INVALIDATED)

        val timeout = machine()
        assertThat(
            timeout.evaluatePreBreak(
                observation(4_999),
                includeNoCrossTimeout = true,
            ),
        ).isNull()
        assertThat(
            timeout.evaluatePreBreak(
                observation(5_000),
                includeNoCrossTimeout = true,
            ),
        ).isEqualTo(LevelReasonCode.PRE_ENTRY_TIMEOUT)
    }

    @Test
    fun `every immediate and data pre-break invalidation has a stable reason`() {
        assertThat(
            machine().evaluatePreBreak(
                observation(0, signal(price = "99.79")),
                includeNoCrossTimeout = true,
            ),
        ).isEqualTo(LevelReasonCode.PRE_ENTRY_INVALIDATED)
        assertThat(
            machine(LevelDirection.SHORT).evaluatePreBreak(
                observation(
                    millis = 0,
                    signal = signal(
                        direction = LevelDirection.SHORT,
                        price = "100.21",
                    ),
                ),
                includeNoCrossTimeout = true,
            ),
        ).isEqualTo(LevelReasonCode.PRE_ENTRY_INVALIDATED)
        assertThat(
            machine().evaluatePreBreak(
                observation(
                    millis = 0,
                    signal = signal(burstStatus = BurstStatus.ACTIVE),
                ),
                includeNoCrossTimeout = true,
            ),
        ).isEqualTo(LevelReasonCode.PRE_ENTRY_INVALIDATED)

        val marketFailure = machine()
        marketFailure.evaluatePreBreak(
            observation(0, publicHealthy = false),
            includeNoCrossTimeout = true,
        )
        assertThat(
            marketFailure.evaluatePreBreak(
                observation(2_999, publicHealthy = false),
                includeNoCrossTimeout = true,
            ),
        ).isNull()
        assertThat(
            marketFailure.evaluatePreBreak(
                observation(3_000, publicHealthy = false),
                includeNoCrossTimeout = true,
            ),
        ).isEqualTo(LevelReasonCode.MARKET_DATA_FAILURE)

        val privateFailure = machine()
        privateFailure.evaluatePreBreak(
            observation(0, privateHealthy = false),
            includeNoCrossTimeout = true,
        )
        assertThat(
            privateFailure.evaluatePreBreak(
                observation(3_000, privateHealthy = false),
                includeNoCrossTimeout = true,
            ),
        ).isEqualTo(LevelReasonCode.PRIVATE_STREAM_FAILURE)
    }

    @Test
    fun `one-second confirmation mirrors directions and tolerates one NPU of noise`() {
        listOf(LevelDirection.LONG, LevelDirection.SHORT).forEach { direction ->
            val machine = machine(direction)
            machine.markCrossed()
            machine.startConfirmation(startedAt)
            val boundaryPrice = when (direction) {
                LevelDirection.LONG -> "99.9"
                LevelDirection.SHORT -> "100.1"
            }

            assertThat(
                machine.evaluateConfirmation(
                    observation(
                        millis = 999,
                        signal = signal(direction, boundaryPrice),
                    ),
                ).status,
            ).isEqualTo(BreakoutConfirmationStatus.PENDING)
            assertThat(
                machine.evaluateConfirmation(
                    observation(
                        millis = 1_000,
                        signal = signal(direction, boundaryPrice),
                    ),
                ).status,
            ).isEqualTo(BreakoutConfirmationStatus.PASSED)
        }
    }

    @Test
    fun `confirmation fails beyond one NPU and for every validity gate`() {
        listOf(
            signal(price = "99.899"),
            signal(directionalGatePassed = false),
            signal(burstStatus = BurstStatus.ACTIVE),
            signal(
                directionalShare = "0.62",
                midProgress = "0.02",
                acceleration = "1.5",
            ),
        ).forEach { invalidSignal ->
            val machine = confirmingMachine()
            val evaluation = machine.evaluateConfirmation(
                observation(100, signal = invalidSignal),
            )
            assertThat(evaluation.status)
                .isEqualTo(BreakoutConfirmationStatus.FAILED)
            assertThat(evaluation.terminalReason)
                .isEqualTo(LevelReasonCode.BREAK_CONFIRM_FAILED)
        }

        listOf(
            observation(100, publicHealthy = false),
            observation(100, privateHealthy = false),
        ).forEach { unhealthy ->
            val evaluation = confirmingMachine().evaluateConfirmation(unhealthy)
            assertThat(evaluation.status)
                .isEqualTo(BreakoutConfirmationStatus.FAILED)
            assertThat(evaluation.terminalReason)
                .isEqualTo(LevelReasonCode.BREAK_CONFIRM_FAILED)
        }
    }

    private fun machine(
        direction: LevelDirection = LevelDirection.LONG,
    ): BreakoutStateMachine =
        BreakoutStateMachine().also { machine ->
            machine.startPreEntry(
                direction = direction,
                levelPrice = BigDecimal("100"),
                frozenNpu = BigDecimal("0.1"),
                filledAt = startedAt,
                observedTradePrices = listOf(BigDecimal("100")),
            )
        }

    private fun confirmingMachine(): BreakoutStateMachine =
        machine().also { machine ->
            machine.markCrossed()
            machine.startConfirmation(startedAt)
        }

    private fun observation(
        millis: Long,
        signal: LevelSignalSnapshot = signal(),
        publicHealthy: Boolean = true,
        privateHealthy: Boolean = true,
    ): BreakoutObservation =
        BreakoutObservation(
            observedAt = startedAt.plusMillis(millis),
            signal = signal,
            publicDataHealthy = publicHealthy,
            privateDataHealthy = privateHealthy,
        )

    private fun signal(
        direction: LevelDirection = LevelDirection.LONG,
        price: String = "100",
        directionalShare: String = "0.70",
        deltaRate: String = if (direction == LevelDirection.LONG) "1" else "-1",
        midProgress: String = "0.1",
        acceleration: String = "2",
        burstStatus: BurstStatus = BurstStatus.NONE,
        directionalGatePassed: Boolean = true,
    ): LevelSignalSnapshot {
        val share = BigDecimal(directionalShare)
        val fast = metrics(
            buyShare = if (direction == LevelDirection.LONG) share else BigDecimal.ONE.subtract(share),
            sellShare = if (direction == LevelDirection.SHORT) share else BigDecimal.ONE.subtract(share),
            deltaRate = BigDecimal(deltaRate),
        )
        return LevelSignalSnapshot(
            observedAt = startedAt,
            latestTradePrice = BigDecimal(price),
            bidPrice = BigDecimal(price).subtract(BigDecimal("0.01")),
            askPrice = BigDecimal(price).add(BigDecimal("0.01")),
            midPrice = BigDecimal(price),
            spread = BigDecimal("0.02"),
            distanceToLevel = BigDecimal("0"),
            npu = NpuSnapshot(
                absolute = BigDecimal("0.1"),
                percentage = BigDecimal("0.1"),
                frozen = true,
                lastRecomputedAt = startedAt,
                retainedSampleCount = 1,
            ),
            windows = MetricWindowsSnapshot(
                fast = fast,
                mid = metrics(
                    buyShare = fast.buyShare,
                    sellShare = fast.sellShare,
                    deltaRate = fast.deltaRate,
                    signedProgress = BigDecimal(midProgress),
                ),
                slow = metrics(
                    buyShare = fast.buyShare,
                    sellShare = fast.sellShare,
                    deltaRate = fast.deltaRate,
                ),
            ),
            acceleration = AccelerationSnapshot(
                tradesPerSecondRatio = BigDecimal(acceleration),
                volumeRateRatio = BigDecimal(acceleration),
                averageTradeSizeRatio = BigDecimal.ONE,
            ),
            ramp = RampSnapshot(
                bins = emptyList(),
                nonNegativeChangeCount = 7,
                finalToInitialActivityRatio = BigDecimal("1.5"),
                latestToStrongestActivityRatio = BigDecimal("0.7"),
                score = BigDecimal.ONE,
            ),
            burst = BurstSnapshot(
                status = burstStatus,
                aggregateTradeId = null,
                largestFastTradeShare = BigDecimal.ZERO,
                followingActivityRatio = null,
                postBurstSignedProgress = null,
                detectedAt = null,
                expiresAt = null,
            ),
            pressureScore = PressureScoreSnapshot(
                value = BigDecimal.ONE,
                diagnosticOnly = true,
                components = emptyList(),
            ),
            mandatoryGates = MandatoryGatesSnapshot(
                entryEligible = directionalGatePassed,
                blockerReasons = emptyList(),
                gates = listOf(
                    SignalGateResult(
                        gate = SignalGate.DIRECTIONAL_FLOW,
                        passed = directionalGatePassed,
                        blockerReasons = emptyList(),
                    ),
                ),
                actualSpread = BigDecimal("0.02"),
                npuSpreadLimit = BigDecimal("0.1"),
                percentageSpreadLimit = BigDecimal("0.1"),
                effectiveSpreadLimit = BigDecimal("0.1"),
                requiredMarketEventAgeMillis = 0,
            ),
        )
    }

    private fun metrics(
        buyShare: BigDecimal,
        sellShare: BigDecimal,
        deltaRate: BigDecimal,
        signedProgress: BigDecimal = BigDecimal("0.1"),
    ): WindowMetricsSnapshot =
        WindowMetricsSnapshot(
            durationMillis = 250,
            tradeCount = 4,
            aggressiveVolume = BigDecimal("4"),
            tradesPerSecond = BigDecimal("16"),
            volumeRate = BigDecimal("16"),
            averageTradeSize = BigDecimal.ONE,
            buyShare = buyShare,
            sellShare = sellShare,
            deltaRate = deltaRate,
            averageBuySize = BigDecimal.ONE,
            averageSellSize = BigDecimal.ONE,
            signedPriceProgress = signedProgress,
            adversePullback = BigDecimal.ZERO,
            flowEfficiency = BigDecimal.ONE,
        )
}
