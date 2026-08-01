package com.scalpsecta.breakoutbot.level

import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceClient
import com.scalpsecta.breakoutbot.binance.BinanceLeverageBracket
import com.scalpsecta.breakoutbot.binance.BinanceLotSizeFilter
import com.scalpsecta.breakoutbot.binance.BinanceMarginType
import com.scalpsecta.breakoutbot.binance.BinanceSymbolLeverageBrackets
import com.scalpsecta.breakoutbot.binance.BinanceSymbolMetadata
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataService
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSubscription
import jakarta.annotation.PreDestroy
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Clock
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.min

@Service
class LevelService(
    private val client: AuthenticatedBinanceClient,
    private val publicMarketDataService: PublicMarketDataService,
    private val clock: Clock,
) {
    private val lock = ReentrantLock()
    private val levels = linkedMapOf<UUID, StoredLevel>()
    private val symbolConfigurationPermits =
        ConcurrentHashMap<String, Semaphore>()

    fun create(command: CreateLevelCommand): Mono<LevelSnapshot> =
        Mono.defer {
            val input = validateInput(command)
            withSymbolConfigurationPermit(input.symbol) {
                client
                    .exchangeInfo()
                    .map { exchangeInfo ->
                        exchangeInfo.symbols.firstOrNull { metadata ->
                            metadata.symbol == input.symbol
                        } ?: throw levelError(
                            LevelReasonCode.INVALID_SYMBOL,
                            "Symbol ${input.symbol} is not available on Binance USD-M Futures",
                        )
                    }
                    .onErrorMap { error ->
                        mapBoundaryFailure(
                            error = error,
                            code = LevelReasonCode.INVALID_SYMBOL,
                            message = "Could not validate Binance symbol ${input.symbol}",
                        )
                    }
                    .flatMap { metadata -> createPlan(input, metadata) }
                    .flatMap { plan ->
                        checkCollectionConstraints(plan.key)
                        validateLiquidation(plan)
                        configureSymbol(plan)
                            .then(Mono.fromCallable { store(plan) })
                    }
            }
        }

    fun delete(levelId: UUID): LevelSnapshot {
        val removed = lock.withLock {
            val stored = levels[levelId] ?: throw levelError(
                LevelReasonCode.LEVEL_NOT_FOUND,
                "Level $levelId does not exist",
            )
            when {
                stored.snapshot.ownsExposure -> throw levelError(
                    LevelReasonCode.LEVEL_HAS_EXPOSURE,
                    "Level $levelId cannot be deleted while it owns exposure",
                )

                stored.snapshot.hasUnresolvedOrder -> throw levelError(
                    LevelReasonCode.LEVEL_HAS_UNRESOLVED_ORDER,
                    "Level $levelId cannot be deleted while it owns an unresolved order",
                )
            }
            checkNotNull(levels.remove(levelId)) {
                "Level $levelId disappeared while the level lock was held"
            }
        }
        removed.marketDataSubscription.close()
        return removed.snapshot
    }

    fun currentState(): List<LevelSnapshot> =
        lock.withLock {
            levels.values.map { stored -> stored.snapshot }
        }

    fun recordOwnership(
        levelId: UUID,
        ownsExposure: Boolean,
        hasUnresolvedOrder: Boolean,
    ): LevelSnapshot =
        lock.withLock {
            val stored = levels[levelId] ?: throw levelError(
                LevelReasonCode.LEVEL_NOT_FOUND,
                "Level $levelId does not exist",
            )
            val updated = stored.snapshot.copy(
                ownsExposure = ownsExposure,
                hasUnresolvedOrder = hasUnresolvedOrder,
                deleteAllowed = !ownsExposure && !hasUnresolvedOrder,
            )
            stored.snapshot = updated
            updated
        }

    @PreDestroy
    fun close() {
        val subscriptions = lock.withLock {
            levels.values
                .map(StoredLevel::marketDataSubscription)
                .also { levels.clear() }
        }
        symbolConfigurationPermits.clear()
        subscriptions.forEach(PublicMarketDataSubscription::close)
    }

    private fun validateInput(command: CreateLevelCommand): ValidatedInput {
        val symbol = command.symbol.trim().uppercase()
        if (symbol.isEmpty()) {
            throw levelError(
                LevelReasonCode.INVALID_SYMBOL,
                "Symbol must not be blank",
            )
        }
        requirePositive(command.levelPrice, "levelPrice")
        requirePositive(command.positionNotionalUsdt, "positionNotionalUsdt")
        requirePositive(command.maxImpulsePct, "maxImpulsePct")
        return ValidatedInput(
            symbol = symbol,
            direction = command.direction,
            requestedLevelPrice = command.levelPrice,
            positionNotionalUsdt = command.positionNotionalUsdt,
            maxImpulsePct = command.maxImpulsePct,
        )
    }

    private fun withSymbolConfigurationPermit(
        symbol: String,
        action: () -> Mono<LevelSnapshot>,
    ): Mono<LevelSnapshot> {
        val permit = symbolConfigurationPermits.computeIfAbsent(symbol) {
            Semaphore(1, true)
        }
        return Mono
            .fromCallable {
                permit.acquire()
                permit
            }
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap { acquired ->
                Mono.defer(action)
                    .doFinally { acquired.release() }
            }
    }

    private fun requirePositive(value: BigDecimal, field: String) {
        if (value.signum() <= 0) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "$field must be positive and finite",
            )
        }
    }

    private fun createPlan(
        input: ValidatedInput,
        metadata: BinanceSymbolMetadata,
    ): Mono<LevelPlan> {
        val filters = validateMetadata(metadata)
        val normalizedLevelPrice = roundToIncrement(
            input.requestedLevelPrice,
            filters.tickSize,
            RoundingMode.HALF_UP,
        )
        validateNormalizedPrice(
            normalizedLevelPrice = normalizedLevelPrice,
            symbol = metadata.symbol,
            filters = filters,
        )

        return client
            .markPrice(input.symbol)
            .onErrorMap { error ->
                mapBoundaryFailure(
                    error = error,
                    code = LevelReasonCode.INVALID_LEVEL,
                    message = "Could not load a current price for ${input.symbol}",
                )
            }
            .map { currentPrice ->
                requirePositive(currentPrice, "currentPrice")
                validatePreBreakSide(
                    direction = input.direction,
                    currentPrice = currentPrice,
                    normalizedLevelPrice = normalizedLevelPrice,
                )
                val plannedQuantity = roundToIncrement(
                    input.positionNotionalUsdt.divide(
                        currentPrice,
                        CALCULATION_SCALE,
                        RoundingMode.DOWN,
                    ),
                    filters.lotSize.stepSize,
                    RoundingMode.DOWN,
                )
                val allocation = planAllocation(
                    plannedQuantity = plannedQuantity,
                    minimumExecutionPrice = minOf(
                        currentPrice,
                        normalizedLevelPrice,
                    ),
                    lotSize = filters.lotSize,
                    minimumNotional = filters.minimumNotional,
                )
                PlannedSizing(
                    currentPrice = currentPrice,
                    normalizedLevelPrice = normalizedLevelPrice,
                    plannedQuantity = plannedQuantity,
                    allocation = allocation,
                )
            }
            .flatMap { sizing ->
                client
                    .leverageBrackets(input.symbol)
                    .map { brackets ->
                        val plannedNotional = sizing.plannedQuantity
                            .multiply(sizing.currentPrice)
                        val bracket = applicableBracket(brackets, plannedNotional)
                        val leverage = min(MAX_LEVERAGE, bracket.initialLeverage)
                        if (leverage <= 0) {
                            throw levelError(
                                LevelReasonCode.INVALID_LEVEL,
                                "No executable leverage bracket exists for ${input.symbol}",
                            )
                        }
                        val riskBoundaryStopPrice = riskBoundaryStopPrice(
                            input = input,
                            plannedQuantity = sizing.plannedQuantity,
                            tickSize = filters.tickSize,
                            normalizedLevelPrice = sizing.normalizedLevelPrice,
                        )
                        LevelPlan(
                            input = input,
                            key = LevelKey(
                                symbol = input.symbol,
                                direction = input.direction,
                                normalizedLevelPrice = sizing.normalizedLevelPrice,
                            ),
                            sizing = sizing,
                            leverage = leverage,
                            projectedIsolatedMargin = plannedNotional.divide(
                                leverage.toBigDecimal(),
                                CALCULATION_SCALE,
                                RoundingMode.UP,
                            ),
                            riskBoundaryStopPrice = riskBoundaryStopPrice,
                            estimatedLiquidationPrice = estimatedLiquidationPrice(
                                direction = input.direction,
                                entryPrice = sizing.normalizedLevelPrice,
                                quantity = sizing.plannedQuantity,
                                leverage = leverage,
                                bracket = bracket,
                            ),
                        )
                    }
                    .onErrorMap { error ->
                        mapBoundaryFailure(
                            error = error,
                            code = LevelReasonCode.INVALID_LEVEL,
                            message = "Could not plan leverage for ${input.symbol}",
                        )
                    }
            }
    }

    private fun validateMetadata(metadata: BinanceSymbolMetadata): ValidatedFilters {
        if (
            metadata.status != TRADING_STATUS ||
            metadata.contractType != PERPETUAL_CONTRACT ||
            metadata.quoteAsset != USDT_ASSET ||
            metadata.marginAsset != USDT_ASSET
        ) {
            throw levelError(
                LevelReasonCode.INVALID_SYMBOL,
                "Symbol ${metadata.symbol} is not a tradable Binance USD-M perpetual",
            )
        }
        val priceFilter = metadata.priceFilter
        val lotSize = metadata.lotSizeFilter
        val minimumNotional = metadata.minimumNotional
        if (
            priceFilter == null ||
            priceFilter.tickSize.signum() <= 0 ||
            priceFilter.minimumPrice.signum() < 0 ||
            priceFilter.maximumPrice.signum() < 0 ||
            (
                priceFilter.minimumPrice.signum() > 0 &&
                    priceFilter.maximumPrice.signum() > 0 &&
                    priceFilter.maximumPrice < priceFilter.minimumPrice
            ) ||
            lotSize == null ||
            lotSize.stepSize.signum() <= 0 ||
            lotSize.minimumQuantity.signum() <= 0 ||
            lotSize.maximumQuantity < lotSize.minimumQuantity ||
            minimumNotional == null ||
            minimumNotional.signum() <= 0
        ) {
            throw levelError(
                LevelReasonCode.INVALID_SYMBOL,
                "Symbol ${metadata.symbol} has incomplete or invalid execution filters",
            )
        }
        return ValidatedFilters(
            minimumPrice = priceFilter.minimumPrice,
            maximumPrice = priceFilter.maximumPrice,
            tickSize = priceFilter.tickSize,
            lotSize = lotSize,
            minimumNotional = minimumNotional,
        )
    }

    private fun validateNormalizedPrice(
        normalizedLevelPrice: BigDecimal,
        symbol: String,
        filters: ValidatedFilters,
    ) {
        val belowMinimum =
            filters.minimumPrice.signum() > 0 &&
                normalizedLevelPrice < filters.minimumPrice
        val aboveMaximum =
            filters.maximumPrice.signum() > 0 &&
                normalizedLevelPrice > filters.maximumPrice
        if (normalizedLevelPrice.signum() <= 0 || belowMinimum || aboveMaximum) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Normalized level price is outside Binance filters for $symbol",
            )
        }
    }

    private fun validatePreBreakSide(
        direction: LevelDirection,
        currentPrice: BigDecimal,
        normalizedLevelPrice: BigDecimal,
    ) {
        val crossed = when (direction) {
            LevelDirection.LONG -> currentPrice >= normalizedLevelPrice
            LevelDirection.SHORT -> currentPrice <= normalizedLevelPrice
        }
        if (crossed) {
            throw levelError(
                LevelReasonCode.LEVEL_ALREADY_CROSSED,
                "Current price must be strictly on the pre-break side of the level",
            )
        }
    }

    private fun planAllocation(
        plannedQuantity: BigDecimal,
        minimumExecutionPrice: BigDecimal,
        lotSize: BinanceLotSizeFilter,
        minimumNotional: BigDecimal,
    ): List<LevelEntryTranche> {
        if (
            plannedQuantity < lotSize.minimumQuantity ||
            plannedQuantity > lotSize.maximumQuantity
        ) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Planned quantity is outside Binance quantity filters",
            )
        }
        val firstQuantity = roundToIncrement(
            plannedQuantity.multiply(THIRTY_PERCENT),
            lotSize.stepSize,
            RoundingMode.DOWN,
        )
        val secondQuantity = roundToIncrement(
            plannedQuantity.multiply(THIRTY_PERCENT),
            lotSize.stepSize,
            RoundingMode.DOWN,
        )
        val finalQuantity = plannedQuantity
            .subtract(firstQuantity)
            .subtract(secondQuantity)
        val allocation = listOf(
            LevelEntryTranche(LevelEntryRole.PRE_BREAK, 30, firstQuantity),
            LevelEntryTranche(LevelEntryRole.CROSSING, 30, secondQuantity),
            LevelEntryTranche(LevelEntryRole.CONFIRMATION, 40, finalQuantity),
        )
        if (
            allocation.any { tranche ->
                tranche.quantity < lotSize.minimumQuantity ||
                    tranche.quantity.multiply(minimumExecutionPrice) < minimumNotional
            }
        ) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Every 30/30/40 entry tranche must satisfy Binance minimum quantity and notional filters",
            )
        }
        return allocation
    }

    private fun applicableBracket(
        brackets: BinanceSymbolLeverageBrackets,
        plannedNotional: BigDecimal,
    ): BinanceLeverageBracket {
        if (brackets.brackets.isEmpty() || brackets.notionalCoefficient.signum() <= 0) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Binance returned no valid leverage brackets for ${brackets.symbol}",
            )
        }
        val ordered = brackets.brackets.sortedBy(BinanceLeverageBracket::notionalFloor)
        return ordered.firstOrNull { bracket ->
            val floor = bracket.notionalFloor.multiply(brackets.notionalCoefficient)
            val cap = bracket.notionalCap.multiply(brackets.notionalCoefficient)
            plannedNotional >= floor && plannedNotional < cap
        } ?: throw levelError(
            LevelReasonCode.INVALID_LEVEL,
            "Planned notional is outside leverage brackets for ${brackets.symbol}",
        )
    }

    private fun riskBoundaryStopPrice(
        input: ValidatedInput,
        plannedQuantity: BigDecimal,
        tickSize: BigDecimal,
        normalizedLevelPrice: BigDecimal,
    ): BigDecimal {
        val maximumPriceLoss = input.positionNotionalUsdt
            .multiply(ONE_PERCENT)
            .divide(plannedQuantity, CALCULATION_SCALE, RoundingMode.DOWN)
        return when (input.direction) {
            LevelDirection.LONG -> roundToIncrement(
                normalizedLevelPrice.subtract(maximumPriceLoss),
                tickSize,
                RoundingMode.UP,
            )

            LevelDirection.SHORT -> roundToIncrement(
                normalizedLevelPrice.add(maximumPriceLoss),
                tickSize,
                RoundingMode.DOWN,
            )
        }
    }

    private fun estimatedLiquidationPrice(
        direction: LevelDirection,
        entryPrice: BigDecimal,
        quantity: BigDecimal,
        leverage: Int,
        bracket: BinanceLeverageBracket,
    ): BigDecimal {
        val maintenanceRatio = bracket.maintenanceMarginRatio
        if (maintenanceRatio.signum() < 0 || maintenanceRatio >= BigDecimal.ONE) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Binance returned an invalid maintenance margin ratio",
            )
        }
        val leverageFraction = BigDecimal.ONE.divide(
            leverage.toBigDecimal(),
            CALCULATION_SCALE,
            RoundingMode.HALF_UP,
        )
        val cumulativePerUnit = bracket.cumulativeMaintenanceAmount.divide(
            quantity,
            CALCULATION_SCALE,
            RoundingMode.HALF_UP,
        )
        val liquidationPrice = when (direction) {
            LevelDirection.LONG -> entryPrice
                .multiply(BigDecimal.ONE.subtract(leverageFraction))
                .subtract(cumulativePerUnit)
                .divide(
                    BigDecimal.ONE.subtract(maintenanceRatio),
                    CALCULATION_SCALE,
                    RoundingMode.HALF_UP,
                )

            LevelDirection.SHORT -> entryPrice
                .multiply(BigDecimal.ONE.add(leverageFraction))
                .add(cumulativePerUnit)
                .divide(
                    BigDecimal.ONE.add(maintenanceRatio),
                    CALCULATION_SCALE,
                    RoundingMode.HALF_UP,
                )
        }
        if (liquidationPrice.signum() <= 0) {
            throw levelError(
                LevelReasonCode.LIQUIDATION_TOO_CLOSE,
                "Estimated liquidation price is not safe",
            )
        }
        return liquidationPrice
    }

    private fun validateLiquidation(plan: LevelPlan) {
        val safe = when (plan.input.direction) {
            LevelDirection.LONG ->
                plan.estimatedLiquidationPrice < plan.riskBoundaryStopPrice

            LevelDirection.SHORT ->
                plan.estimatedLiquidationPrice > plan.riskBoundaryStopPrice
        }
        if (!safe) {
            throw levelError(
                LevelReasonCode.LIQUIDATION_TOO_CLOSE,
                "Estimated liquidation must remain beyond the planned loss boundary",
            )
        }
    }

    private fun configureSymbol(plan: LevelPlan): Mono<Void> =
        client
            .symbolConfiguration(plan.input.symbol)
            .flatMap { current ->
                if (
                    current.marginType == BinanceMarginType.ISOLATED &&
                    current.autoAddMargin
                ) {
                    return@flatMap Mono.error<Void>(
                        levelError(
                            LevelReasonCode.SYMBOL_CONFIGURATION_FAILED,
                            "Auto-Add Margin is enabled for ${plan.input.symbol}",
                        ),
                    )
                }
                val isolated = if (current.marginType == BinanceMarginType.ISOLATED) {
                    Mono.empty<Void>()
                } else {
                    client.changeMarginType(
                        plan.input.symbol,
                        BinanceMarginType.ISOLATED,
                    )
                }
                isolated
                    .then(
                        client.changeInitialLeverage(
                            plan.input.symbol,
                            plan.leverage,
                        ),
                    )
                    .then(client.symbolConfiguration(plan.input.symbol))
                    .flatMap { verified ->
                        val plannedNotional = plan.sizing.plannedQuantity
                            .multiply(plan.sizing.currentPrice)
                        if (
                            verified.symbol == plan.input.symbol &&
                            verified.marginType == BinanceMarginType.ISOLATED &&
                            !verified.autoAddMargin &&
                            verified.leverage == plan.leverage &&
                            verified.maximumNotional >= plannedNotional
                        ) {
                            Mono.empty<Void>()
                        } else {
                            Mono.error<Void>(
                                levelError(
                                    LevelReasonCode.SYMBOL_CONFIGURATION_FAILED,
                                    "Binance symbol configuration verification failed for ${plan.input.symbol}",
                                ),
                            )
                        }
                    }
            }
            .onErrorMap { error ->
                mapBoundaryFailure(
                    error = error,
                    code = LevelReasonCode.SYMBOL_CONFIGURATION_FAILED,
                    message = "Could not establish safe symbol configuration for ${plan.input.symbol}",
                )
            }

    private fun checkCollectionConstraints(key: LevelKey) {
        lock.withLock {
            if (levels.values.any { stored -> stored.key == key }) {
                throw levelError(
                    LevelReasonCode.DUPLICATE_LEVEL,
                    "An exact normalized level already exists",
                )
            }
            if (levels.size >= MAX_LEVELS) {
                throw levelError(
                    LevelReasonCode.LEVEL_CAPACITY_REACHED,
                    "At most $MAX_LEVELS levels may be stored",
                )
            }
        }
    }

    private fun store(plan: LevelPlan): LevelSnapshot {
        val marketDataSubscription = try {
            publicMarketDataService.observe(plan.input.symbol)
        } catch (error: RuntimeException) {
            throw levelError(
                LevelReasonCode.INVALID_LEVEL,
                "Could not start public market data for ${plan.input.symbol}",
                error,
            )
        }
        try {
            return lock.withLock {
                if (levels.values.any { stored -> stored.key == plan.key }) {
                    throw levelError(
                        LevelReasonCode.DUPLICATE_LEVEL,
                        "An exact normalized level already exists",
                    )
                }
                if (levels.size >= MAX_LEVELS) {
                    throw levelError(
                        LevelReasonCode.LEVEL_CAPACITY_REACHED,
                        "At most $MAX_LEVELS levels may be stored",
                    )
                }
                val snapshot = LevelSnapshot(
                    id = UUID.randomUUID(),
                    createdAt = clock.instant(),
                    symbol = plan.input.symbol,
                    direction = plan.input.direction,
                    requestedLevelPrice = plan.input.requestedLevelPrice,
                    normalizedLevelPrice = plan.sizing.normalizedLevelPrice,
                    positionNotionalUsdt = plan.input.positionNotionalUsdt,
                    maxImpulsePct = plan.input.maxImpulsePct,
                    sizingReferencePrice = plan.sizing.currentPrice,
                    plannedQuantity = plan.sizing.plannedQuantity,
                    entryAllocation = plan.sizing.allocation,
                    leverage = plan.leverage,
                    projectedIsolatedMargin = plan.projectedIsolatedMargin,
                    riskBoundaryStopPrice = plan.riskBoundaryStopPrice,
                    estimatedLiquidationPrice = plan.estimatedLiquidationPrice,
                    state = LevelState.WARMING_UP,
                    blockers = listOf(LevelBlocker.WARMING_UP),
                    ownsExposure = false,
                    hasUnresolvedOrder = false,
                    deleteAllowed = true,
                )
                levels[snapshot.id] = StoredLevel(
                    key = plan.key,
                    snapshot = snapshot,
                    marketDataSubscription = marketDataSubscription,
                )
                snapshot
            }
        } catch (error: RuntimeException) {
            marketDataSubscription.close()
            throw error
        }
    }

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

    private fun mapBoundaryFailure(
        error: Throwable,
        code: LevelReasonCode,
        message: String,
    ): Throwable =
        if (error is LevelException) {
            error
        } else {
            levelError(code, message, error)
        }
}

private data class StoredLevel(
    val key: LevelKey,
    var snapshot: LevelSnapshot,
    val marketDataSubscription: PublicMarketDataSubscription,
)

private data class ValidatedInput(
    val symbol: String,
    val direction: LevelDirection,
    val requestedLevelPrice: BigDecimal,
    val positionNotionalUsdt: BigDecimal,
    val maxImpulsePct: BigDecimal,
)

private data class ValidatedFilters(
    val minimumPrice: BigDecimal,
    val maximumPrice: BigDecimal,
    val tickSize: BigDecimal,
    val lotSize: BinanceLotSizeFilter,
    val minimumNotional: BigDecimal,
)

private data class PlannedSizing(
    val currentPrice: BigDecimal,
    val normalizedLevelPrice: BigDecimal,
    val plannedQuantity: BigDecimal,
    val allocation: List<LevelEntryTranche>,
)

private data class LevelPlan(
    val input: ValidatedInput,
    val key: LevelKey,
    val sizing: PlannedSizing,
    val leverage: Int,
    val projectedIsolatedMargin: BigDecimal,
    val riskBoundaryStopPrice: BigDecimal,
    val estimatedLiquidationPrice: BigDecimal,
)

private data class LevelKey(
    val symbol: String,
    val direction: LevelDirection,
    val normalizedLevelPrice: BigDecimal,
)

private fun levelError(
    code: LevelReasonCode,
    message: String,
    cause: Throwable? = null,
): LevelException = LevelException(code, message, cause)

private const val TRADING_STATUS = "TRADING"
private const val PERPETUAL_CONTRACT = "PERPETUAL"
private const val USDT_ASSET = "USDT"
private const val MAX_LEVELS = 100
private const val MAX_LEVERAGE = 20
private const val CALCULATION_SCALE = 16
private val THIRTY_PERCENT = BigDecimal("0.30")
private val ONE_PERCENT = BigDecimal("0.01")
