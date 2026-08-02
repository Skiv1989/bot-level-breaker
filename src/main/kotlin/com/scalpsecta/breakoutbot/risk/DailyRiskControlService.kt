package com.scalpsecta.breakoutbot.risk

import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceClient
import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.failure.SafeModeExecutionGateway
import com.scalpsecta.breakoutbot.failure.SignedRuntimeReconciliation
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.service.AuthenticatedBinanceReadinessService
import jakarta.annotation.PreDestroy
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.Disposables
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

@Service
class DailyRiskControlService internal constructor(
    private val clock: Clock,
    private val scheduler: Scheduler,
    private val automaticMonitoring: Boolean,
    private val riskService: AttemptRiskService,
    private val executionGateway: SafeModeExecutionGateway,
    private val accountObservation: () -> Mono<DailyRiskAccountObservation>,
    transferEvents: Flux<DailyRiskTransfer>,
    private val monitorInterval: Duration,
) {
    @Autowired
    constructor(
        clock: Clock,
        riskService: AttemptRiskService,
        executionGateway: SafeModeExecutionGateway,
        client: AuthenticatedBinanceClient,
        readinessService: AuthenticatedBinanceReadinessService,
        @Value("\${bot.binance.startup-enabled:true}")
        startupEnabled: Boolean,
        @Value("\${bot.risk.monitor-interval:1s}")
        monitorInterval: Duration,
    ) : this(
        clock = clock,
        scheduler = Schedulers.newSingle("daily-risk-monitor"),
        automaticMonitoring = startupEnabled,
        riskService = riskService,
        executionGateway = executionGateway,
        accountObservation = {
            val readiness = readinessService.snapshot()
            if (
                readiness.account.readiness != BinanceReadiness.READY ||
                readiness.clock.readiness != BinanceReadiness.READY
            ) {
                Mono.empty()
            } else {
                client.accountSummary().map { account ->
                    DailyRiskAccountObservation(
                        totalAccountEquity = account.totalMarginBalance,
                        availableMargin = account.availableBalance,
                        accountValid = account.canTrade,
                    )
                }
            }
        },
        transferEvents = readinessService
            .events()
            .ofType(BinanceUserDataEvent.AccountUpdate::class.java)
            .mapNotNull(::classifiedTransfer),
        monitorInterval = monitorInterval,
    )

    private val subscriptions = Disposables.composite()
    private val flattenAttemptSequence = AtomicLong()

    init {
        require(!monitorInterval.isZero && !monitorInterval.isNegative) {
            "monitorInterval must be positive"
        }
        if (automaticMonitoring) {
            subscriptions.add(
                transferEvents
                    .concatMap { transfer ->
                        riskService
                            .recordDailyTransfer(transfer)
                            .onErrorResume { error ->
                                logger.warn(error) {
                                    "Could not record account transfer"
                                }
                                Mono.empty()
                            }
                    }
                    .subscribe(),
            )
            subscriptions.add(
                Flux
                    .interval(Duration.ZERO, monitorInterval, scheduler)
                    .concatMap {
                        evaluateNow().onErrorResume { error ->
                            logger.warn(error) {
                                "Daily risk monitoring iteration failed"
                            }
                            Mono.empty()
                        }
                    }
                    .subscribe(),
            )
        }
    }

    internal fun evaluateNow(): Mono<GlobalRiskSnapshot> =
        accountObservation()
            .flatMap(riskService::observeDailyAccount)
            .flatMap { evaluation ->
                if (
                    !evaluation.dailyBreachTriggered &&
                    evaluation.state.globalTradingState !=
                    GlobalTradingState.DAILY_LOCKED
                ) {
                    return@flatMap Mono.just(evaluation.state)
                }
                flattenDailyBreach(evaluation)
            }

    @PreDestroy
    fun close() {
        subscriptions.dispose()
        scheduler.dispose()
    }

    private fun flattenDailyBreach(
        evaluation: DailyRiskEvaluation,
    ): Mono<GlobalRiskSnapshot> =
        executionGateway
            .reconcile()
            .flatMap { reconciliation ->
                if (
                    evaluation.dailyBreachTriggered ||
                    reconciliation.requiresFlattening()
                ) {
                    executionGateway.flattenAllAccountExposure(
                        reconciliation = reconciliation,
                        operationId = dailyLockOperationId(evaluation),
                        terminalReason = LevelReasonCode.DAILY_LOSS_LIMIT,
                    )
                } else {
                    Mono.empty()
                }
            }
            .thenReturn(evaluation.state)

    private fun dailyLockOperationId(
        evaluation: DailyRiskEvaluation,
    ): String {
        val identity = evaluation.state.dailyAnchorEstablishedAt
            ?: evaluation.state.observedAt
        return "daily-lock:${identity.toEpochMilli()}:" +
            flattenAttemptSequence.incrementAndGet()
    }
}

private fun SignedRuntimeReconciliation.requiresFlattening(): Boolean =
    positions.any { position -> position.positionAmount.signum() != 0 } ||
        openBotOrderIds.isNotEmpty()

private fun classifiedTransfer(
    event: BinanceUserDataEvent.AccountUpdate,
): DailyRiskTransfer? {
    if (event.reason.uppercase() !in TRANSFER_REASONS) {
        return null
    }
    val balanceChange = event.balances.fold(BigDecimal.ZERO) { total, balance ->
        total.add(balance.balanceChange)
    }
    if (balanceChange.signum() == 0) {
        return null
    }
    return if (balanceChange.signum() > 0) {
        DailyRiskTransfer(deposits = balanceChange)
    } else {
        DailyRiskTransfer(withdrawals = balanceChange.abs())
    }
}

private val TRANSFER_REASONS = setOf("DEPOSIT", "WITHDRAW", "TRANSFER")
private val logger = KotlinLogging.logger {}
