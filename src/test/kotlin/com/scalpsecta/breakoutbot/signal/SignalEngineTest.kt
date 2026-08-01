package com.scalpsecta.breakoutbot.signal

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeGapStatus
import com.scalpsecta.breakoutbot.marketdata.AggressorSide
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.MarketEventAgeSnapshot
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import com.scalpsecta.breakoutbot.marketdata.PublicStreamConnectionState
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

class SignalEngineTest {
    private val start = Instant.parse("2026-08-01T12:00:00Z")

    @Test
    fun `samples ten seconds and freezes tick-ceiled nearest-rank NPU`() {
        val engine = engine()
        val initialPrices = listOf("100", "100.01", "100.12", "100.23", "100.54")
        initialPrices.forEachIndexed { index, price ->
            val sampledAt = start.plusMillis(index * 100L)
            engine.record(bookTicker(sampledAt, price))
            engine.tick(
                now = sampledAt,
                mode = if (index == initialPrices.lastIndex) {
                    NpuMode.ARMED
                } else {
                    NpuMode.WARMING_UP
                },
            )
        }

        val initial = snapshot(engine, start.plusMillis(400))
        assertThat(initial.npu.absolute)
            .isEqualByComparingTo(BigDecimal("0.2"))
        assertThat(initial.npu.percentage).isNotNull()
        assertThat(initial.npu.frozen).isFalse()
        assertThat(initial.npu.retainedSampleCount).isEqualTo(5)

        repeat(9) { index ->
            val sampledAt = start.plusMillis(500L + index * 100L)
            val price = if (index % 2 == 0) "101" else "100"
            engine.record(bookTicker(sampledAt, price))
            engine.tick(sampledAt, NpuMode.ARMED)
        }
        assertThat(snapshot(engine, start.plusMillis(1_300)).npu.absolute)
            .isEqualByComparingTo(BigDecimal("0.2"))

        val recomputedAt = start.plusMillis(1_400)
        engine.record(bookTicker(recomputedAt, "101"))
        engine.tick(recomputedAt, NpuMode.ARMED)
        val recomputed = snapshot(engine, recomputedAt)
        assertThat(recomputed.npu.absolute)
            .isEqualByComparingTo(BigDecimal("1.0"))
        assertThat(recomputed.npu.lastRecomputedAt).isEqualTo(recomputedAt)

        engine.tick(recomputedAt, NpuMode.FROZEN)
        repeat(11) { index ->
            val sampledAt = start.plusMillis(1_500L + index * 100L)
            val price = if (index % 2 == 0) "105" else "100"
            engine.record(bookTicker(sampledAt, price))
            engine.tick(sampledAt, NpuMode.FROZEN)
        }
        val frozen = snapshot(engine, start.plusMillis(2_500))
        assertThat(frozen.npu.absolute)
            .isEqualByComparingTo(BigDecimal("1.0"))
        assertThat(frozen.npu.lastRecomputedAt).isEqualTo(recomputedAt)
        assertThat(frozen.npu.frozen).isTrue()
    }

    @Test
    fun `retains exactly the rolling ten-second sample window`() {
        val engine = engine()
        repeat(101) { index ->
            val sampledAt = start.plusMillis(index * 100L)
            engine.record(bookTicker(sampledAt, "100"))
            engine.tick(sampledAt, NpuMode.WARMING_UP)
        }

        val signal = snapshot(engine, start.plusSeconds(10))
        assertThat(signal.npu.retainedSampleCount).isEqualTo(100)
    }

    @Test
    fun `calculates rates sizes shares delta and exact directional mirrors`() {
        val now = start.plusSeconds(5)
        val longEngine = engine(LevelDirection.LONG)
        longEngine.record(trade(1, now.minusMillis(240), "100", "2", AggressorSide.BUY))
        longEngine.record(trade(2, now.minusMillis(200), "103", "1", AggressorSide.BUY))
        longEngine.record(trade(3, now.minusMillis(100), "101", "1", AggressorSide.SELL))

        val shortEngine = engine(LevelDirection.SHORT)
        shortEngine.record(trade(1, now.minusMillis(240), "100", "2", AggressorSide.SELL))
        shortEngine.record(trade(2, now.minusMillis(200), "97", "1", AggressorSide.SELL))
        shortEngine.record(trade(3, now.minusMillis(100), "99", "1", AggressorSide.BUY))

        val long = snapshot(longEngine, now)
        val short = snapshot(shortEngine, now)
        val fast = long.windows.fast
        assertThat(fast.tradeCount).isEqualTo(3)
        assertDecimal(fast.tradesPerSecond, "12")
        assertDecimal(fast.aggressiveVolume, "4")
        assertDecimal(fast.volumeRate, "16")
        assertDecimal(fast.averageTradeSize, "1.333333333333333333333333333333333")
        assertDecimal(fast.buyShare, "0.75")
        assertDecimal(fast.sellShare, "0.25")
        assertDecimal(fast.deltaRate, "8")
        assertDecimal(fast.averageBuySize, "1.5")
        assertDecimal(fast.averageSellSize, "1")
        assertDecimal(long.windows.slow.tradesPerSecond, "0.6")
        assertDecimal(long.windows.slow.volumeRate, "0.8")
        assertDecimal(long.acceleration.tradesPerSecondRatio, "20")
        assertDecimal(long.acceleration.volumeRateRatio, "20")
        assertDecimal(long.acceleration.averageTradeSizeRatio, "1")
        assertDecimal(fast.signedPriceProgress, "1")
        assertDecimal(fast.adversePullback, "2")
        assertDecimal(fast.flowEfficiency, "0.25")

        assertDecimal(short.windows.fast.buyShare, long.windows.fast.sellShare)
        assertDecimal(short.windows.fast.sellShare, long.windows.fast.buyShare)
        assertDecimal(short.windows.fast.deltaRate, long.windows.fast.deltaRate.negate())
        assertDecimal(
            short.windows.fast.signedPriceProgress,
            long.windows.fast.signedPriceProgress,
        )
        assertDecimal(
            short.windows.fast.adversePullback,
            long.windows.fast.adversePullback,
        )
        assertDecimal(
            short.windows.fast.flowEfficiency,
            long.windows.fast.flowEfficiency,
        )
    }

    @Test
    fun `empty windows use epsilon without fabricating activity`() {
        val engine = engine()
        engine.tick(start, NpuMode.ARMED)
        val signal = snapshot(engine, start)

        assertThat(signal.npu.absolute).isEqualByComparingTo(BigDecimal("0.1"))
        assertThat(signal.windows.fast.tradeCount).isZero()
        assertDecimal(signal.windows.fast.tradesPerSecond, "0")
        assertDecimal(signal.windows.fast.buyShare, "0")
        assertDecimal(signal.windows.fast.flowEfficiency, "0")
        assertDecimal(signal.acceleration.tradesPerSecondRatio, "0")
        assertDecimal(signal.acceleration.volumeRateRatio, "0")
        assertDecimal(signal.acceleration.averageTradeSizeRatio, "0")
        assertThat(signal.mandatoryGates.entryEligible).isFalse()
        assertThat(signal.mandatoryGates.blockerReasons)
            .contains(SignalBlockerReason.RAMP_ACTIVITY_MISSING)
    }

    @Test
    fun `window and ramp bins use start-exclusive end-inclusive boundaries`() {
        val now = start.plusSeconds(5)
        val engine = engine()
        engine.record(trade(1, now.minusSeconds(2), "100", "1"))
        engine.record(trade(2, now.minusMillis(1_751), "100", "1"))
        engine.record(trade(3, now.minusMillis(1_750), "100", "1"))
        engine.record(trade(4, now.minusMillis(250), "100", "1"))
        engine.record(trade(5, now.minusMillis(249), "100", "1"))
        engine.record(trade(6, now, "100", "1"))

        val signal = snapshot(engine, now)
        assertThat(signal.ramp.bins.map(RampBinSnapshot::tradeCount))
            .containsExactly(1, 1, 0, 0, 0, 0, 0, 3)
        assertThat(signal.windows.fast.tradeCount).isEqualTo(2)
    }

    @Test
    fun `mandatory gate threshold edges are inclusive where the PRD requires`() {
        val accelerationAtEdge = AccelerationSnapshot(
            tradesPerSecondRatio = BigDecimal("1.5"),
            volumeRateRatio = BigDecimal("2.0"),
            averageTradeSizeRatio = BigDecimal.ONE,
        )
        assertThat(
            SignalGateEvaluator.accelerationGate(accelerationAtEdge).passed,
        ).isTrue()
        assertThat(
            SignalGateEvaluator.accelerationGate(
                accelerationAtEdge.copy(
                    tradesPerSecondRatio = BigDecimal("1.4999999999999999"),
                ),
            ).passed,
        ).isFalse()
        assertThat(
            SignalGateEvaluator.accelerationGate(
                accelerationAtEdge.copy(
                    volumeRateRatio = BigDecimal("1.5"),
                ),
            ).blockerReasons,
        ).containsExactly(SignalBlockerReason.ACCEL_STRONG_IMPULSE_MISSING)

        val directionalFast = metrics(
            buyShare = "0.62",
            sellShare = "0.38",
            deltaRate = "0.0000000000000001",
        )
        val directionalMid = metrics(deltaRate = "0.0000000000000001")
        assertThat(
            SignalGateEvaluator.directionalFlowGate(
                LevelDirection.LONG,
                directionalFast,
                directionalMid,
            ).passed,
        ).isTrue()
        assertThat(
            SignalGateEvaluator.directionalFlowGate(
                LevelDirection.SHORT,
                directionalFast.copy(
                    buyShare = BigDecimal("0.38"),
                    sellShare = BigDecimal("0.62"),
                    deltaRate = BigDecimal("-0.0000000000000001"),
                ),
                directionalMid.copy(
                    deltaRate = BigDecimal("-0.0000000000000001"),
                ),
            ).passed,
        ).isTrue()
        assertThat(
            SignalGateEvaluator.directionalFlowGate(
                LevelDirection.LONG,
                directionalFast.copy(deltaRate = BigDecimal.ZERO),
                directionalMid,
            ).passed,
        ).isFalse()

        val priceAtEdge = metrics(
            signedProgress = "0.5",
            adversePullback = "2.0",
        )
        assertThat(
            SignalGateEvaluator.priceResponseGate(
                priceAtEdge,
                BigDecimal.ONE,
            ).passed,
        ).isTrue()
        assertThat(
            SignalGateEvaluator.priceResponseGate(
                priceAtEdge.copy(
                    signedPriceProgress = BigDecimal("0.4999999999999999"),
                    adversePullback = BigDecimal("2.0000000000000001"),
                ),
                BigDecimal.ONE,
            ).blockerReasons,
        ).containsExactly(
            SignalBlockerReason.MID_PROGRESS_BELOW_MINIMUM,
            SignalBlockerReason.MID_PULLBACK_ABOVE_MAXIMUM,
        )

        val rampAtEdge = RampSnapshot(
            bins = List(8) { index -> rampBin(index, "1") },
            nonNegativeChangeCount = 5,
            finalToInitialActivityRatio = BigDecimal("1.5"),
            latestToStrongestActivityRatio = BigDecimal("0.70"),
            score = BigDecimal.ONE,
        )
        assertThat(SignalGateEvaluator.rampGate(rampAtEdge).passed).isTrue()
    }

    @Test
    fun `burst stays blocking while unresolved and active then expires`() {
        val engine = armedEngine(start)
        val burstAt = start.plusMillis(100)
        engine.record(trade(1, burstAt, "100", "10"))
        engine.tick(burstAt, NpuMode.ARMED)

        val unresolved = snapshot(engine, burstAt)
        assertThat(unresolved.burst.status).isEqualTo(BurstStatus.UNRESOLVED)
        assertThat(gate(unresolved, SignalGate.ONE_SHOT_BURST).passed).isFalse()

        val resolvedAt = burstAt.plusMillis(250)
        engine.tick(resolvedAt, NpuMode.ARMED)
        val active = snapshot(engine, resolvedAt)
        assertThat(active.burst.status).isEqualTo(BurstStatus.ACTIVE)
        assertDecimal(active.burst.followingActivityRatio!!, "0")
        assertDecimal(active.burst.postBurstSignedProgress!!, "0")
        assertThat(gate(active, SignalGate.ONE_SHOT_BURST).blockerReasons)
            .containsExactly(SignalBlockerReason.ONE_SHOT_BURST_ACTIVE)

        val expiredAt = burstAt.plusSeconds(2)
        engine.tick(expiredAt, NpuMode.ARMED)
        assertThat(snapshot(engine, expiredAt).burst.status)
            .isEqualTo(BurstStatus.NONE)
    }

    @Test
    fun `following activity clears a dominant burst after the persistence window`() {
        val engine = armedEngine(start)
        val burstAt = start.plusMillis(100)
        engine.record(trade(1, burstAt, "100", "10"))
        engine.tick(burstAt, NpuMode.ARMED)
        engine.record(trade(2, burstAt.plusMillis(100), "100.1", "10"))
        engine.tick(burstAt.plusMillis(100), NpuMode.ARMED)
        engine.tick(burstAt.plusMillis(250), NpuMode.ARMED)

        val signal = snapshot(engine, burstAt.plusMillis(250))
        assertThat(signal.burst.status).isEqualTo(BurstStatus.CLEARED)
        assertThat(signal.burst.followingActivityRatio)
            .isGreaterThan(BigDecimal("0.50"))
        assertThat(gate(signal, SignalGate.ONE_SHOT_BURST).passed).isTrue()
    }

    @Test
    fun `burst persistence and price-response equality are not rejected`() {
        val persistenceEngine = armedEngine(start)
        persistenceEngine.record(trade(1, start.plusMillis(10), "100", "4"))
        persistenceEngine.record(trade(2, start.plusMillis(100), "100", "6"))
        persistenceEngine.tick(start.plusMillis(100), NpuMode.ARMED)
        assertDecimal(
            snapshot(persistenceEngine, start.plusMillis(100))
                .burst.largestFastTradeShare,
            "0.6",
        )
        persistenceEngine.record(trade(3, start.plusMillis(200), "100", "5"))
        persistenceEngine.tick(start.plusMillis(350), NpuMode.ARMED)
        val persisted = snapshot(persistenceEngine, start.plusMillis(350))
        assertThat(persisted.burst.status).isEqualTo(BurstStatus.CLEARED)
        assertDecimal(persisted.burst.followingActivityRatio!!, "0.5")

        val responseEngine = armedEngine(start)
        responseEngine.record(trade(1, start.plusMillis(10), "100", "2"))
        responseEngine.record(trade(2, start.plusMillis(20), "100", "2"))
        responseEngine.record(trade(3, start.plusMillis(100), "100", "6"))
        responseEngine.tick(start.plusMillis(100), NpuMode.ARMED)
        responseEngine.record(
            trade(4, start.plusMillis(200), "100.05", "0.1"),
        )
        responseEngine.tick(start.plusMillis(350), NpuMode.ARMED)
        val responsive = snapshot(responseEngine, start.plusMillis(350))
        assertThat(responsive.burst.status).isEqualTo(BurstStatus.CLEARED)
        assertThat(responsive.burst.followingActivityRatio)
            .isLessThan(BigDecimal("0.5"))
        assertDecimal(responsive.burst.postBurstSignedProgress!!, "0.05")
    }

    @Test
    fun `latency spread and data integrity report precise blockers`() {
        val atEdge = marketSnapshot(
            bid = "99.95",
            ask = "100.05",
            ageMillis = 250,
        )
        assertThat(SignalGateEvaluator.latencyGate(atEdge).result.passed).isTrue()
        assertThat(
            SignalGateEvaluator.spreadGate(atEdge, BigDecimal("0.1")).result.passed,
        ).isTrue()

        val npuStricter = marketSnapshot(bid = "99.97495", ask = "100.02505")
        val npuSpread = SignalGateEvaluator.spreadGate(
            npuStricter,
            BigDecimal("0.05"),
        )
        assertThat(npuSpread.result.blockerReasons)
            .containsExactly(SignalBlockerReason.SPREAD_ABOVE_NPU)
        assertDecimal(npuSpread.effectiveLimit!!, "0.05")

        val percentageStricter = marketSnapshot(
            bid = "99.94995",
            ask = "100.05005",
        )
        val percentageSpread = SignalGateEvaluator.spreadGate(
            percentageStricter,
            BigDecimal("0.2"),
        )
        assertThat(percentageSpread.result.blockerReasons)
            .containsExactly(SignalBlockerReason.SPREAD_ABOVE_PERCENTAGE_LIMIT)
        assertDecimal(percentageSpread.effectiveLimit!!, "0.1")

        val staleAndBroken = marketSnapshot(
            ageMillis = 251,
            connectionState = PublicStreamConnectionState.DISCONNECTED,
            gapStatus = AggregateTradeGapStatus.GAP_DETECTED,
            heartbeatHealthy = false,
        )
        assertThat(SignalGateEvaluator.latencyGate(staleAndBroken).result.blockerReasons)
            .containsExactly(SignalBlockerReason.REQUIRED_MARKET_EVENT_TOO_OLD)
        assertThat(
            SignalGateEvaluator.dataIntegrityGate(
                publicMarketData = staleAndBroken,
                privateStreamReadiness = BinanceReadiness.NOT_READY,
                hasUnresolvedOrder = true,
            ).blockerReasons,
        ).containsExactly(
            SignalBlockerReason.PUBLIC_STREAM_DISCONNECTED,
            SignalBlockerReason.AGGREGATE_TRADE_GAP_UNRESOLVED,
            SignalBlockerReason.BID_ASK_STALE,
            SignalBlockerReason.PRIVATE_ORDER_OUTCOME_UNRESOLVED,
            SignalBlockerReason.PRIVATE_STREAM_UNHEALTHY,
        )
    }

    @Test
    fun `PressureScore remains diagnostic when a mandatory gate fails`() {
        val now = start.plusSeconds(5)
        val engine = armedEngine(now)
        listOf(
            Triple(200L, "99.8", "1"),
            Triple(150L, "99.9", "1"),
            Triple(100L, "100.0", "1"),
            Triple(50L, "100.1", "1"),
        ).forEachIndexed { index, (age, price, quantity) ->
            engine.record(
                trade(
                    id = index.toLong() + 1,
                    receivedAt = now.minusMillis(age),
                    price = price,
                    quantity = quantity,
                    side = AggressorSide.BUY,
                ),
            )
        }
        engine.tick(now, NpuMode.ARMED)

        val signal = engine.snapshot(
            now = now,
            publicMarketData = marketSnapshot(),
            privateStreamReadiness = BinanceReadiness.NOT_READY,
            hasUnresolvedOrder = false,
        )
        assertThat(signal.pressureScore.diagnosticOnly).isTrue()
        assertThat(
            signal.pressureScore.components.map(
                PressureScoreComponentSnapshot::component,
            ),
        )
            .containsExactlyElementsOf(PressureScoreComponent.entries)
        assertThat(signal.pressureScore.value).isGreaterThan(BigDecimal("0.9"))
        assertThat(signal.mandatoryGates.entryEligible).isFalse()
        assertThat(signal.mandatoryGates.blockerReasons)
            .containsExactly(SignalBlockerReason.PRIVATE_STREAM_UNHEALTHY)
    }

    private fun armedEngine(now: Instant): SignalEngine =
        engine().also { engine ->
            engine.record(bookTicker(now, "100"))
            engine.tick(now, NpuMode.ARMED)
        }

    private fun engine(
        direction: LevelDirection = LevelDirection.LONG,
    ): SignalEngine =
        SignalEngine(
            symbol = "BTCUSDT",
            direction = direction,
            levelPrice = BigDecimal("101"),
            tickSize = BigDecimal("0.1"),
        )

    private fun snapshot(
        engine: SignalEngine,
        now: Instant,
    ): LevelSignalSnapshot =
        engine.snapshot(
            now = now,
            publicMarketData = marketSnapshot(),
            privateStreamReadiness = BinanceReadiness.READY,
            hasUnresolvedOrder = false,
        )

    private fun trade(
        id: Long,
        receivedAt: Instant,
        price: String,
        quantity: String,
        side: AggressorSide = AggressorSide.BUY,
    ): AggregateTradeEvent =
        AggregateTradeEvent(
            symbol = "BTCUSDT",
            aggregateTradeId = id,
            eventTime = receivedAt,
            tradeTime = receivedAt,
            price = BigDecimal(price),
            quantity = BigDecimal(quantity),
            buyerIsMaker = side == AggressorSide.SELL,
            aggressorSide = side,
            receivedAt = receivedAt,
        )

    private fun bookTicker(
        receivedAt: Instant,
        midPrice: String,
    ): BookTickerEvent {
        val mid = BigDecimal(midPrice)
        return BookTickerEvent(
            symbol = "BTCUSDT",
            updateId = receivedAt.toEpochMilli(),
            eventTime = receivedAt,
            transactionTime = receivedAt,
            bidPrice = mid.subtract(BigDecimal("0.01")),
            bidQuantity = BigDecimal.ONE,
            askPrice = mid.add(BigDecimal("0.01")),
            askQuantity = BigDecimal.ONE,
            receivedAt = receivedAt,
        )
    }

    private fun marketSnapshot(
        bid: String = "99.95",
        ask: String = "100.05",
        ageMillis: Long = 0,
        connectionState: PublicStreamConnectionState =
            PublicStreamConnectionState.CONNECTED,
        gapStatus: AggregateTradeGapStatus = AggregateTradeGapStatus.CONTINUOUS,
        heartbeatHealthy: Boolean = true,
    ): PublicMarketDataSnapshot =
        PublicMarketDataSnapshot(
            symbol = "BTCUSDT",
            connectionState = connectionState,
            healthy =
                connectionState == PublicStreamConnectionState.CONNECTED &&
                    gapStatus == AggregateTradeGapStatus.CONTINUOUS &&
                    heartbeatHealthy,
            bidAskHeartbeatHealthy = heartbeatHealthy,
            gapStatus = gapStatus,
            latestAggregateTradeId = null,
            latestBidPrice = BigDecimal(bid),
            latestBidQuantity = BigDecimal.ONE,
            latestAskPrice = BigDecimal(ask),
            latestAskQuantity = BigDecimal.ONE,
            spread = BigDecimal(ask).subtract(BigDecimal(bid)),
            aggregateTradeAge = null,
            bookTickerAge = MarketEventAgeSnapshot(
                receiveAgeMillis = ageMillis,
                exchangeAgeMillis = ageMillis,
            ),
        )

    private fun metrics(
        buyShare: String = "0",
        sellShare: String = "0",
        deltaRate: String = "0",
        signedProgress: String = "0",
        adversePullback: String = "0",
    ): WindowMetricsSnapshot =
        WindowMetricsSnapshot(
            durationMillis = 250,
            tradeCount = 0,
            aggressiveVolume = BigDecimal.ZERO,
            tradesPerSecond = BigDecimal.ZERO,
            volumeRate = BigDecimal.ZERO,
            averageTradeSize = BigDecimal.ZERO,
            buyShare = BigDecimal(buyShare),
            sellShare = BigDecimal(sellShare),
            deltaRate = BigDecimal(deltaRate),
            averageBuySize = BigDecimal.ZERO,
            averageSellSize = BigDecimal.ZERO,
            signedPriceProgress = BigDecimal(signedProgress),
            adversePullback = BigDecimal(adversePullback),
            flowEfficiency = BigDecimal.ZERO,
        )

    private fun rampBin(
        index: Int,
        activity: String,
    ): RampBinSnapshot =
        RampBinSnapshot(
            index = index,
            tradeCount = 1,
            tradesPerSecond = BigDecimal.ONE,
            aggressiveQuoteVolumeRate = BigDecimal.ONE,
            normalizedTradesPerSecond = BigDecimal.ONE,
            normalizedQuoteVolumeRate = BigDecimal.ONE,
            activity = BigDecimal(activity),
        )

    private fun gate(
        signal: LevelSignalSnapshot,
        gate: SignalGate,
    ): SignalGateResult =
        signal.mandatoryGates.gates.single { result -> result.gate == gate }

    private fun assertDecimal(
        actual: BigDecimal,
        expected: String,
    ) {
        assertThat(actual).isEqualByComparingTo(BigDecimal(expected))
    }

    private fun assertDecimal(
        actual: BigDecimal,
        expected: BigDecimal,
    ) {
        assertThat(actual).isEqualByComparingTo(expected)
    }
}
