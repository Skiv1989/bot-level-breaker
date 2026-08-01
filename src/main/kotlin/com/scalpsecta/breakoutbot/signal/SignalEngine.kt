package com.scalpsecta.breakoutbot.signal

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.AggressorSide
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import java.time.Duration
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

internal class SignalEngine(
    private val symbol: String,
    private val direction: LevelDirection,
    private val levelPrice: BigDecimal,
    private val tickSize: BigDecimal,
) {
    private val lock = ReentrantLock()
    private val trades = mutableListOf<AggregateTradeEvent>()
    private val midPriceSamples = ArrayDeque<MidPriceSample>()
    private var latestBookTicker: BookTickerEvent? = null
    private var latestNpu: BigDecimal? = null
    private var lastNpuRecomputedAt: Instant? = null
    private var npuMode = NpuMode.WARMING_UP
    private var burstCandidate: BurstCandidate? = null

    init {
        require(symbol.isNotBlank()) { "symbol must not be blank" }
        require(levelPrice.signum() > 0) { "levelPrice must be positive" }
        require(tickSize.signum() > 0) { "tickSize must be positive" }
    }

    fun record(event: AggregateTradeEvent) {
        if (event.symbol != symbol) {
            return
        }
        lock.withLock {
            trades += event
        }
    }

    fun record(event: BookTickerEvent) {
        if (event.symbol != symbol) {
            return
        }
        lock.withLock {
            latestBookTicker = event
        }
    }

    fun tick(
        now: Instant,
        mode: NpuMode,
    ) {
        lock.withLock {
            prune(now)
            sampleMidPrice(now)
            updateNpu(now, mode)
            updateBurst(now)
        }
    }

    fun snapshot(
        now: Instant,
        publicMarketData: PublicMarketDataSnapshot?,
        privateStreamReadiness: BinanceReadiness,
        hasUnresolvedOrder: Boolean,
    ): LevelSignalSnapshot =
        lock.withLock {
            prune(now)
            updateBurst(now)

            val fast = calculateWindow(now, FAST_WINDOW)
            val mid = calculateWindow(now, MID_WINDOW)
            val slow = calculateWindow(now, SLOW_WINDOW)
            val windows = MetricWindowsSnapshot(
                fast = fast,
                mid = mid,
                slow = slow,
            )
            val acceleration = AccelerationSnapshot(
                tradesPerSecondRatio = ratio(
                    fast.tradesPerSecond,
                    slow.tradesPerSecond,
                ),
                volumeRateRatio = ratio(
                    fast.volumeRate,
                    slow.volumeRate,
                ),
                averageTradeSizeRatio = ratio(
                    fast.averageTradeSize,
                    slow.averageTradeSize,
                ),
            )
            val ramp = calculateRamp(now)
            val burst = burstSnapshot()
            val currentNpu = latestNpu
            val mandatoryGates = SignalGateEvaluator.evaluate(
                direction = direction,
                windows = windows,
                acceleration = acceleration,
                ramp = ramp,
                burst = burst,
                npu = currentNpu,
                publicMarketData = publicMarketData,
                privateStreamReadiness = privateStreamReadiness,
                hasUnresolvedOrder = hasUnresolvedOrder,
            )
            val currentBook = latestBookTicker
            val currentBidPrice = publicMarketData?.latestBidPrice
                ?: currentBook?.bidPrice
            val currentAskPrice = publicMarketData?.latestAskPrice
                ?: currentBook?.askPrice
            val currentMidPrice = if (
                currentBidPrice != null &&
                currentAskPrice != null
            ) {
                currentBidPrice.add(currentAskPrice).divide(TWO, MATH_CONTEXT)
            } else {
                null
            }
            val latestTrade = windowTrades(now, SLOW_WINDOW).lastOrNull()

            LevelSignalSnapshot(
                observedAt = now,
                latestTradePrice = latestTrade?.price,
                bidPrice = currentBidPrice,
                askPrice = currentAskPrice,
                midPrice = currentMidPrice,
                spread = if (
                    currentBidPrice != null &&
                    currentAskPrice != null
                ) {
                    currentAskPrice.subtract(currentBidPrice)
                } else {
                    null
                },
                distanceToLevel = currentMidPrice?.let { midPrice ->
                    levelPrice.subtract(midPrice).abs()
                },
                npu = NpuSnapshot(
                    absolute = currentNpu,
                    percentage = npuPercentage(currentNpu, currentMidPrice),
                    frozen = npuMode == NpuMode.FROZEN && currentNpu != null,
                    lastRecomputedAt = lastNpuRecomputedAt,
                    retainedSampleCount = midPriceSamples.size,
                ),
                windows = windows,
                acceleration = acceleration,
                ramp = ramp,
                burst = burst,
                pressureScore = pressureScore(
                    direction = direction,
                    windows = windows,
                    acceleration = acceleration,
                    ramp = ramp,
                    npu = currentNpu,
                ),
                mandatoryGates = mandatoryGates,
            )
        }

    private fun sampleMidPrice(now: Instant) {
        val lastSampleAt = midPriceSamples.lastOrNull()?.sampledAt
        if (
            lastSampleAt != null &&
            now.isBefore(lastSampleAt.plus(MID_PRICE_SAMPLE_INTERVAL))
        ) {
            return
        }
        val bookTicker = latestBookTicker ?: return
        val receiveAge = Duration.between(bookTicker.receivedAt, now)
        if (
            bookTicker.bidPrice.signum() <= 0 ||
            bookTicker.askPrice.signum() <= 0 ||
            bookTicker.askPrice < bookTicker.bidPrice ||
            receiveAge.isNegative ||
            receiveAge > MAX_SAMPLE_EVENT_AGE
        ) {
            return
        }
        midPriceSamples.addLast(
            MidPriceSample(
                sampledAt = now,
                price = bookTicker.midPrice(),
            ),
        )
        pruneMidPriceSamples(now)
    }

    private fun updateNpu(
        now: Instant,
        mode: NpuMode,
    ) {
        when (mode) {
            NpuMode.WARMING_UP -> {
                if (npuMode != NpuMode.WARMING_UP) {
                    latestNpu = null
                    lastNpuRecomputedAt = null
                }
            }

            NpuMode.ARMED -> {
                val recomputationDue =
                    npuMode != NpuMode.ARMED ||
                        lastNpuRecomputedAt == null ||
                        !now.isBefore(
                            checkNotNull(lastNpuRecomputedAt)
                                .plus(NPU_RECOMPUTE_INTERVAL),
                        )
                if (recomputationDue) {
                    latestNpu = calculateNpu()
                    lastNpuRecomputedAt = now
                }
            }

            NpuMode.FROZEN -> {
                if (npuMode != NpuMode.FROZEN && latestNpu == null) {
                    latestNpu = calculateNpu()
                    lastNpuRecomputedAt = now
                }
            }
        }
        npuMode = mode
    }

    private fun calculateNpu(): BigDecimal {
        val moves = midPriceSamples
            .zipWithNext { first, second ->
                second.price.subtract(first.price).abs()
            }
            .sorted()
        val percentile = if (moves.isEmpty()) {
            BigDecimal.ZERO
        } else {
            val nearestRank = (PERCENTILE_NUMERATOR * moves.size + 3) / 4
            moves[nearestRank - 1]
        }
        val rawNpu = maxOf(tickSize, percentile)
        val tickScale = tickSize.stripTrailingZeros().scale().coerceAtLeast(0)
        return rawNpu
            .divide(tickSize, 0, RoundingMode.CEILING)
            .multiply(tickSize)
            .setScale(tickScale)
    }

    private fun calculateWindow(
        now: Instant,
        duration: Duration,
    ): WindowMetricsSnapshot {
        val windowTrades = windowTrades(now, duration)
        val durationSeconds = duration.toMillis().toBigDecimal()
            .divide(ONE_THOUSAND, MATH_CONTEXT)
        val totalVolume = windowTrades.sumOfDecimal { trade -> trade.quantity }
        val buyTrades = windowTrades.filter { trade ->
            trade.aggressorSide == AggressorSide.BUY
        }
        val sellTrades = windowTrades.filter { trade ->
            trade.aggressorSide == AggressorSide.SELL
        }
        val buyVolume = buyTrades.sumOfDecimal { trade -> trade.quantity }
        val sellVolume = sellTrades.sumOfDecimal { trade -> trade.quantity }
        val signedProgress = if (windowTrades.size >= 2) {
            signed(
                windowTrades.last().price.subtract(windowTrades.first().price),
            )
        } else {
            BigDecimal.ZERO
        }

        return WindowMetricsSnapshot(
            durationMillis = duration.toMillis(),
            tradeCount = windowTrades.size,
            aggressiveVolume = totalVolume,
            tradesPerSecond = divide(
                windowTrades.size.toBigDecimal(),
                durationSeconds,
            ),
            volumeRate = divide(totalVolume, durationSeconds),
            averageTradeSize = divide(
                totalVolume,
                windowTrades.size.toBigDecimal(),
            ),
            buyShare = divide(buyVolume, totalVolume),
            sellShare = divide(sellVolume, totalVolume),
            deltaRate = divide(
                buyVolume.subtract(sellVolume),
                durationSeconds,
            ),
            averageBuySize = divide(
                buyVolume,
                buyTrades.size.toBigDecimal(),
            ),
            averageSellSize = divide(
                sellVolume,
                sellTrades.size.toBigDecimal(),
            ),
            signedPriceProgress = signedProgress,
            adversePullback = adversePullback(windowTrades),
            flowEfficiency = divide(signedProgress, totalVolume),
        )
    }

    private fun adversePullback(
        windowTrades: List<AggregateTradeEvent>,
    ): BigDecimal {
        if (windowTrades.size < 2) {
            return BigDecimal.ZERO
        }
        var favorableExtreme = windowTrades.first().price
        var maximumPullback = BigDecimal.ZERO
        windowTrades.drop(1).forEach { trade ->
            when (direction) {
                LevelDirection.LONG -> {
                    if (trade.price > favorableExtreme) {
                        favorableExtreme = trade.price
                    } else {
                        maximumPullback = maxOf(
                            maximumPullback,
                            favorableExtreme.subtract(trade.price),
                        )
                    }
                }

                LevelDirection.SHORT -> {
                    if (trade.price < favorableExtreme) {
                        favorableExtreme = trade.price
                    } else {
                        maximumPullback = maxOf(
                            maximumPullback,
                            trade.price.subtract(favorableExtreme),
                        )
                    }
                }
            }
        }
        return maximumPullback
    }

    private fun calculateRamp(now: Instant): RampSnapshot {
        val start = now.minus(RAMP_WINDOW)
        val mutableBins = List(RAMP_BIN_COUNT) { MutableRampBin() }
        trades.forEach { trade ->
            if (
                !trade.receivedAt.isAfter(start) ||
                trade.receivedAt.isAfter(now)
            ) {
                return@forEach
            }
            val elapsedNanos = Duration.between(start, trade.receivedAt).toNanos()
            val index = (elapsedNanos / RAMP_BIN_DURATION.toNanos())
                .toInt()
                .coerceAtMost(RAMP_BIN_COUNT - 1)
            mutableBins[index].tradeCount += 1
            mutableBins[index].quoteVolume = mutableBins[index].quoteVolume.add(
                trade.price.multiply(trade.quantity),
            )
        }
        val durationSeconds = RAMP_BIN_DURATION.toMillis().toBigDecimal()
            .divide(ONE_THOUSAND, MATH_CONTEXT)
        val tradesPerSecond = mutableBins.map { bin ->
            divide(bin.tradeCount.toBigDecimal(), durationSeconds)
        }
        val quoteVolumeRates = mutableBins.map { bin ->
            divide(bin.quoteVolume, durationSeconds)
        }
        val maximumTradesPerSecond = tradesPerSecond.maxOrNull()
            ?: BigDecimal.ZERO
        val maximumQuoteVolumeRate = quoteVolumeRates.maxOrNull()
            ?: BigDecimal.ZERO
        val bins = mutableBins.indices.map { index ->
            val normalizedTradesPerSecond = divide(
                tradesPerSecond[index],
                maximumTradesPerSecond,
            )
            val normalizedQuoteVolumeRate = divide(
                quoteVolumeRates[index],
                maximumQuoteVolumeRate,
            )
            RampBinSnapshot(
                index = index,
                tradeCount = mutableBins[index].tradeCount,
                tradesPerSecond = tradesPerSecond[index],
                aggressiveQuoteVolumeRate = quoteVolumeRates[index],
                normalizedTradesPerSecond = normalizedTradesPerSecond,
                normalizedQuoteVolumeRate = normalizedQuoteVolumeRate,
                activity = normalizedTradesPerSecond
                    .add(normalizedQuoteVolumeRate)
                    .divide(TWO, MATH_CONTEXT),
            )
        }
        val nonNegativeChangeCount = bins
            .zipWithNext()
            .count { (first, second) -> second.activity >= first.activity }
        val initialAverage = bins
            .take(2)
            .sumOfDecimal(RampBinSnapshot::activity)
            .divide(TWO, MATH_CONTEXT)
        val finalAverage = bins
            .takeLast(2)
            .sumOfDecimal(RampBinSnapshot::activity)
            .divide(TWO, MATH_CONTEXT)
        val strongest = bins.maxOfOrNull(RampBinSnapshot::activity)
            ?: BigDecimal.ZERO
        val finalRatio = ratio(finalAverage, initialAverage)
        val latestRatio = ratio(bins.last().activity, strongest)
        val monotonicScore = scoreRatio(
            nonNegativeChangeCount.toBigDecimal(),
            RAMP_NON_NEGATIVE_TARGET.toBigDecimal(),
        )
        val finalScore = scoreRatio(finalRatio, RAMP_FINAL_RATIO_TARGET)
        val latestScore = scoreRatio(latestRatio, RAMP_LATEST_RATIO_TARGET)

        return RampSnapshot(
            bins = bins,
            nonNegativeChangeCount = nonNegativeChangeCount,
            finalToInitialActivityRatio = finalRatio,
            latestToStrongestActivityRatio = latestRatio,
            score = monotonicScore
                .add(finalScore)
                .add(latestScore)
                .divide(THREE, MATH_CONTEXT),
        )
    }

    private fun updateBurst(now: Instant) {
        val currentCandidate = burstCandidate
        if (
            currentCandidate != null &&
            !now.isBefore(currentCandidate.expiresAt)
        ) {
            burstCandidate = null
        }

        resolveBurstCandidate(now)

        val fastTrades = windowTrades(now, FAST_WINDOW)
        val totalFastVolume = fastTrades.sumOfDecimal { trade -> trade.quantity }
        val largestTrade = fastTrades.maxByOrNull(AggregateTradeEvent::quantity)
        val largestShare = if (largestTrade == null) {
            BigDecimal.ZERO
        } else {
            divide(largestTrade.quantity, totalFastVolume)
        }
        val existingCandidate = burstCandidate
        val canReplaceClearedCandidate =
            existingCandidate?.status == BurstStatus.CLEARED &&
                largestTrade != null &&
                largestTrade.receivedAt.isAfter(
                    existingCandidate.tradeAt.plus(BURST_FOLLOW_WINDOW),
                )
        if (
            largestTrade != null &&
            largestShare >= BURST_LARGEST_TRADE_SHARE &&
            (
                existingCandidate == null ||
                    canReplaceClearedCandidate
            )
        ) {
            burstCandidate = BurstCandidate(
                aggregateTradeId = largestTrade.aggregateTradeId,
                tradeAt = largestTrade.receivedAt,
                tradePrice = largestTrade.price,
                largestFastTradeShare = largestShare,
                status = BurstStatus.UNRESOLVED,
                followingActivityRatio = null,
                postBurstSignedProgress = null,
                detectedAt = now,
                expiresAt = largestTrade.receivedAt.plus(BURST_RETENTION),
            )
        }

        resolveBurstCandidate(now)
    }

    private fun resolveBurstCandidate(now: Instant) {
        val candidate = burstCandidate ?: return
        if (candidate.status != BurstStatus.UNRESOLVED) {
            return
        }
        if (now.isBefore(candidate.tradeAt.plus(BURST_FOLLOW_WINDOW))) {
            return
        }
        val npu = latestNpu ?: return
        val burstTrades = tradesInInterval(
            startExclusive = candidate.tradeAt.minus(BURST_FOLLOW_WINDOW),
            endInclusive = candidate.tradeAt,
        )
        val followingTrades = tradesInInterval(
            startExclusive = candidate.tradeAt,
            endInclusive = candidate.tradeAt.plus(BURST_FOLLOW_WINDOW),
        )
        val followingActivityRatio = relativeActivity(
            baseline = burstTrades,
            comparison = followingTrades,
        )
        val lastFollowingPrice = followingTrades.lastOrNull()?.price
        val postBurstProgress = if (lastFollowingPrice == null) {
            BigDecimal.ZERO
        } else {
            signed(lastFollowingPrice.subtract(candidate.tradePrice))
        }
        val suspicious =
            followingActivityRatio < BURST_FOLLOW_ACTIVITY_RATIO &&
                postBurstProgress < npu.multiply(HALF)
        burstCandidate = candidate.copy(
            status = if (suspicious) BurstStatus.ACTIVE else BurstStatus.CLEARED,
            followingActivityRatio = followingActivityRatio,
            postBurstSignedProgress = postBurstProgress,
        )
    }

    private fun relativeActivity(
        baseline: List<AggregateTradeEvent>,
        comparison: List<AggregateTradeEvent>,
    ): BigDecimal {
        val baselineQuoteVolume = baseline.sumOfDecimal { trade ->
            trade.price.multiply(trade.quantity)
        }
        val comparisonQuoteVolume = comparison.sumOfDecimal { trade ->
            trade.price.multiply(trade.quantity)
        }
        val tradeCountRatio = ratio(
            comparison.size.toBigDecimal(),
            baseline.size.toBigDecimal(),
        )
        val quoteVolumeRatio = ratio(
            comparisonQuoteVolume,
            baselineQuoteVolume,
        )
        return tradeCountRatio.add(quoteVolumeRatio).divide(TWO, MATH_CONTEXT)
    }

    private fun burstSnapshot(): BurstSnapshot {
        val candidate = burstCandidate
        return if (candidate == null) {
            BurstSnapshot(
                status = BurstStatus.NONE,
                aggregateTradeId = null,
                largestFastTradeShare = BigDecimal.ZERO,
                followingActivityRatio = null,
                postBurstSignedProgress = null,
                detectedAt = null,
                expiresAt = null,
            )
        } else {
            BurstSnapshot(
                status = candidate.status,
                aggregateTradeId = candidate.aggregateTradeId,
                largestFastTradeShare = candidate.largestFastTradeShare,
                followingActivityRatio = candidate.followingActivityRatio,
                postBurstSignedProgress = candidate.postBurstSignedProgress,
                detectedAt = candidate.detectedAt,
                expiresAt = candidate.expiresAt,
            )
        }
    }

    private fun pressureScore(
        direction: LevelDirection,
        windows: MetricWindowsSnapshot,
        acceleration: AccelerationSnapshot,
        ramp: RampSnapshot,
        npu: BigDecimal?,
    ): PressureScoreSnapshot {
        val directionalShare = when (direction) {
            LevelDirection.LONG -> windows.fast.buyShare
            LevelDirection.SHORT -> windows.fast.sellShare
        }
        val flowTarget = if (npu == null) {
            null
        } else {
            divide(
                npu.multiply(HALF),
                windows.mid.aggressiveVolume,
            )
        }
        val components = listOf(
            pressureComponent(
                component = PressureScoreComponent.ACCEL_TRADES_PER_SECOND,
                rawValue = acceleration.tradesPerSecondRatio,
                target = TWO,
                weight = BigDecimal("0.20"),
            ),
            pressureComponent(
                component = PressureScoreComponent.ACCEL_VOLUME,
                rawValue = acceleration.volumeRateRatio,
                target = TWO,
                weight = BigDecimal("0.20"),
            ),
            pressureComponent(
                component = PressureScoreComponent.SIZE_RATIO,
                rawValue = acceleration.averageTradeSizeRatio,
                target = BigDecimal("1.5"),
                weight = BigDecimal("0.15"),
            ),
            pressureComponent(
                component = PressureScoreComponent.DIRECTIONAL_SHARE,
                rawValue = directionalShare,
                target = BigDecimal("0.62"),
                weight = BigDecimal("0.20"),
            ),
            pressureComponent(
                component = PressureScoreComponent.RAMP,
                rawValue = ramp.score,
                target = BigDecimal.ONE,
                weight = BigDecimal("0.15"),
            ),
            pressureComponent(
                component = PressureScoreComponent.FLOW_EFFICIENCY,
                rawValue = windows.mid.flowEfficiency,
                target = flowTarget,
                weight = BigDecimal("0.10"),
            ),
        )
        return PressureScoreSnapshot(
            value = components.sumOfDecimal(
                PressureScoreComponentSnapshot::contribution,
            ),
            diagnosticOnly = true,
            components = components,
        )
    }

    private fun pressureComponent(
        component: PressureScoreComponent,
        rawValue: BigDecimal,
        target: BigDecimal?,
        weight: BigDecimal,
    ): PressureScoreComponentSnapshot {
        val score = if (target == null || target.signum() <= 0) {
            BigDecimal.ZERO
        } else {
            scoreRatio(rawValue, target)
        }
        return PressureScoreComponentSnapshot(
            component = component,
            rawValue = rawValue,
            normalizationTarget = target,
            normalizedScore = score,
            weight = weight,
            contribution = score.multiply(weight),
        )
    }

    private fun npuPercentage(
        npu: BigDecimal?,
        midPrice: BigDecimal?,
    ): BigDecimal? =
        if (npu == null || midPrice == null || midPrice.signum() <= 0) {
            null
        } else {
            npu.divide(midPrice, MATH_CONTEXT).multiply(ONE_HUNDRED)
        }

    private fun windowTrades(
        now: Instant,
        duration: Duration,
    ): List<AggregateTradeEvent> =
        tradesInInterval(
            startExclusive = now.minus(duration),
            endInclusive = now,
        )

    private fun tradesInInterval(
        startExclusive: Instant,
        endInclusive: Instant,
    ): List<AggregateTradeEvent> =
        trades
            .asSequence()
            .filter { trade ->
                trade.receivedAt.isAfter(startExclusive) &&
                    !trade.receivedAt.isAfter(endInclusive)
            }
            .sortedWith(
                compareBy<AggregateTradeEvent>(AggregateTradeEvent::receivedAt)
                    .thenBy(AggregateTradeEvent::aggregateTradeId),
            )
            .toList()

    private fun prune(now: Instant) {
        val tradeCutoff = now.minus(SLOW_WINDOW)
        trades.removeIf { trade -> !trade.receivedAt.isAfter(tradeCutoff) }
        pruneMidPriceSamples(now)
    }

    private fun pruneMidPriceSamples(now: Instant) {
        val cutoff = now.minus(NPU_SAMPLE_WINDOW)
        while (
            midPriceSamples.isNotEmpty() &&
            !midPriceSamples.first().sampledAt.isAfter(cutoff)
        ) {
            midPriceSamples.removeFirst()
        }
    }

    private fun signed(value: BigDecimal): BigDecimal =
        when (direction) {
            LevelDirection.LONG -> value
            LevelDirection.SHORT -> value.negate()
        }
}

private data class MidPriceSample(
    val sampledAt: Instant,
    val price: BigDecimal,
)

private data class MutableRampBin(
    var tradeCount: Int = 0,
    var quoteVolume: BigDecimal = BigDecimal.ZERO,
)

private data class BurstCandidate(
    val aggregateTradeId: Long,
    val tradeAt: Instant,
    val tradePrice: BigDecimal,
    val largestFastTradeShare: BigDecimal,
    val status: BurstStatus,
    val followingActivityRatio: BigDecimal?,
    val postBurstSignedProgress: BigDecimal?,
    val detectedAt: Instant,
    val expiresAt: Instant,
)

private fun BookTickerEvent.midPrice(): BigDecimal =
    bidPrice.add(askPrice).divide(TWO, MATH_CONTEXT)

private fun divide(
    numerator: BigDecimal,
    denominator: BigDecimal,
): BigDecimal =
    numerator.divide(maxOf(denominator, EPSILON), MATH_CONTEXT)

private fun ratio(
    numerator: BigDecimal,
    denominator: BigDecimal,
): BigDecimal = divide(numerator, denominator)

private fun scoreRatio(
    value: BigDecimal,
    target: BigDecimal,
): BigDecimal =
    when {
        value.signum() <= 0 -> BigDecimal.ZERO
        value >= target -> BigDecimal.ONE
        else -> value.divide(target, MATH_CONTEXT)
    }

private inline fun <T> Iterable<T>.sumOfDecimal(
    selector: (T) -> BigDecimal,
): BigDecimal =
    fold(BigDecimal.ZERO) { total, item -> total.add(selector(item)) }

private val MATH_CONTEXT = MathContext.DECIMAL128
private val EPSILON = BigDecimal("0.0000000000000001")
private val MID_PRICE_SAMPLE_INTERVAL: Duration = Duration.ofMillis(100)
private val MAX_SAMPLE_EVENT_AGE: Duration = Duration.ofMillis(250)
private val NPU_SAMPLE_WINDOW: Duration = Duration.ofSeconds(10)
private val NPU_RECOMPUTE_INTERVAL: Duration = Duration.ofSeconds(1)
private val FAST_WINDOW: Duration = Duration.ofMillis(250)
private val MID_WINDOW: Duration = Duration.ofSeconds(1)
private val SLOW_WINDOW: Duration = Duration.ofSeconds(5)
private val RAMP_WINDOW: Duration = Duration.ofSeconds(2)
private val RAMP_BIN_DURATION: Duration = Duration.ofMillis(250)
private const val RAMP_BIN_COUNT = 8
private const val RAMP_NON_NEGATIVE_TARGET = 5
private val RAMP_FINAL_RATIO_TARGET = BigDecimal("1.5")
private val RAMP_LATEST_RATIO_TARGET = BigDecimal("0.70")
private val BURST_FOLLOW_WINDOW: Duration = Duration.ofMillis(250)
private val BURST_RETENTION: Duration = Duration.ofSeconds(2)
private val BURST_LARGEST_TRADE_SHARE = BigDecimal("0.60")
private val BURST_FOLLOW_ACTIVITY_RATIO = BigDecimal("0.50")
private const val PERCENTILE_NUMERATOR = 3
private val ONE_THOUSAND = BigDecimal("1000")
private val ONE_HUNDRED = BigDecimal("100")
private val HALF = BigDecimal("0.5")
private val TWO = BigDecimal("2")
private val THREE = BigDecimal("3")
