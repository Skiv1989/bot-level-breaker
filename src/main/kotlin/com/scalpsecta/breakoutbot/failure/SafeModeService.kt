package com.scalpsecta.breakoutbot.failure

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataService
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import com.scalpsecta.breakoutbot.service.AuthenticatedBinanceReadinessService
import jakarta.annotation.PreDestroy
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

data class SafeModeSnapshot(
    val observedAt: Instant,
    val entriesAndAdditionsBlocked: Boolean,
    val publicDataHealthy: Boolean,
    val privateStreamHealthy: Boolean,
    val accountHealthy: Boolean,
    val clockHealthy: Boolean,
    val publicDataUnhealthySince: Instant?,
    val privateStreamUnhealthySince: Instant?,
    val recoveryHealthySince: Instant?,
    val matchingReconciliationCount: Int,
    val recoveryHealthDurationSatisfied: Boolean,
    val safeModeEventCount: Int,
    val globalTradingState: GlobalTradingState,
)

internal data class FailureRuntimeHealth(
    val publicDataHealthy: Boolean,
    val privateStreamHealthy: Boolean,
    val accountHealthy: Boolean,
    val clockHealthy: Boolean,
    val privateStreamEstablished: Boolean = privateStreamHealthy,
)

@Service
class SafeModeService internal constructor(
    private val clock: Clock,
    private val scheduler: Scheduler,
    private val automaticMonitoring: Boolean,
    private val riskService: AttemptRiskService,
    private val executionGateway: SafeModeExecutionGateway,
    private val runtimeHealth: () -> FailureRuntimeHealth,
    private val monitorInterval: Duration,
    private val reconciliationInterval: Duration,
    private val publicOutageTimeout: Duration,
    private val privateOutageTimeout: Duration,
    private val recoveryHealthDuration: Duration,
) {
    @Autowired
    constructor(
        clock: Clock,
        riskService: AttemptRiskService,
        executionGateway: SafeModeExecutionGateway,
        publicMarketDataService: PublicMarketDataService,
        authenticatedBinanceReadinessService:
            AuthenticatedBinanceReadinessService,
        @Value("\${bot.failure.monitor-interval:50ms}")
        monitorInterval: Duration,
        @Value("\${bot.failure.reconciliation-interval:1s}")
        reconciliationInterval: Duration,
        @Value("\${bot.failure.public-outage-timeout:3s}")
        publicOutageTimeout: Duration,
        @Value("\${bot.failure.private-outage-timeout:5s}")
        privateOutageTimeout: Duration,
        @Value("\${bot.failure.recovery-health-duration:30s}")
        recoveryHealthDuration: Duration,
    ) : this(
        clock = clock,
        scheduler = Schedulers.newSingle("safe-mode-monitor"),
        automaticMonitoring = true,
        riskService = riskService,
        executionGateway = executionGateway,
        runtimeHealth = {
            val symbols = executionGateway.runtimeSymbols()
            val publicSnapshots = publicMarketDataService
                .snapshots()
                .associateBy { snapshot -> snapshot.symbol }
            val authenticated = authenticatedBinanceReadinessService.snapshot()
            FailureRuntimeHealth(
                publicDataHealthy =
                    symbols.all { symbol ->
                        publicSnapshots[symbol]?.healthy == true
                    },
                privateStreamHealthy =
                    authenticated.privateStream.readiness ==
                        BinanceReadiness.READY,
                privateStreamEstablished =
                    authenticated.privateStream.connectedAt != null,
                accountHealthy =
                    authenticated.account.readiness == BinanceReadiness.READY,
                clockHealthy =
                    authenticated.clock.readiness == BinanceReadiness.READY,
            )
        },
        monitorInterval = monitorInterval,
        reconciliationInterval = reconciliationInterval,
        publicOutageTimeout = publicOutageTimeout,
        privateOutageTimeout = privateOutageTimeout,
        recoveryHealthDuration = recoveryHealthDuration,
    )

    private var publicDataUnhealthySince: Instant? = null
    private var privateStreamUnhealthySince: Instant? = null
    private var recoveryHealthySince: Instant? = null
    private var lastReconciliationAt: Instant? = null
    private var lastFingerprint: RuntimeReconciliationFingerprint? = null
    private var matchingReconciliationCount = 0
    private var seenHealthyPrivateStream = false
    private var privateOutageReconciliationStarted = false
    private var publicFailureExitDispatched = false
    private var privateFailureExitDispatched = false
    private var manualLockFlattened = false
    private var lastEmergencyAttemptAt: Instant? = null
    private var lastManualLockAttemptAt: Instant? = null
    private val publishedState = AtomicReference(initialSnapshot())
    private val monitoringSubscription: Disposable?

    init {
        require(!monitorInterval.isZero && !monitorInterval.isNegative) {
            "monitorInterval must be positive"
        }
        require(
            !reconciliationInterval.isZero &&
                !reconciliationInterval.isNegative,
        ) {
            "reconciliationInterval must be positive"
        }
        require(!publicOutageTimeout.isNegative) {
            "publicOutageTimeout must not be negative"
        }
        require(!privateOutageTimeout.isNegative) {
            "privateOutageTimeout must not be negative"
        }
        require(!recoveryHealthDuration.isNegative) {
            "recoveryHealthDuration must not be negative"
        }
        monitoringSubscription = if (automaticMonitoring) {
            Flux
                .interval(Duration.ZERO, monitorInterval, scheduler)
                .concatMap {
                    evaluateNow().onErrorResume { error ->
                        logger.warn(error) {
                            "SAFE_MODE monitoring iteration failed"
                        }
                        Mono.fromSupplier { publish(runtimeHealth()) }
                    }
                }
                .subscribe()
        } else {
            null
        }
    }

    fun currentState(): SafeModeSnapshot = publishedState.get()

    internal fun evaluateNow(): Mono<SafeModeSnapshot> = Mono.defer {
        val now = clock.instant()
        val health = runtimeHealth()
        updateHealthTimers(now, health)
        val globalState = riskService.currentState().globalTradingState

        when {
            globalState == GlobalTradingState.MANUAL_LOCK &&
                !manualLockFlattened &&
                attemptDue(lastManualLockAttemptAt, now) ->
                flattenForManualLock(now, health)

            globalState == GlobalTradingState.MANUAL_LOCK &&
                manualLockFlattened &&
                health.publicDataHealthy &&
                health.privateStreamHealthy &&
                reconciliationDue(now) ->
                evaluateManualLockEvidence(now, health)

            failureReason(now) != null ->
                handleFailure(checkNotNull(failureReason(now)), now, health)

            privateStreamUnhealthySince != null &&
                !privateOutageReconciliationStarted ->
                startPrivateOutageReconciliation(health)

            globalState == GlobalTradingState.SAFE_MODE &&
                health.publicDataHealthy &&
                health.privateStreamHealthy &&
                reconciliationDue(now) -> evaluateRecovery(now, health)

            else -> Mono.just(publish(health))
        }
    }

    @PreDestroy
    fun close() {
        monitoringSubscription?.dispose()
        scheduler.dispose()
    }

    private fun updateHealthTimers(
        now: Instant,
        health: FailureRuntimeHealth,
    ) {
        if (health.publicDataHealthy) {
            publicDataUnhealthySince = null
            publicFailureExitDispatched = false
        } else if (
            executionGateway.runtimeSymbols().isNotEmpty() &&
            publicDataUnhealthySince == null
        ) {
            publicDataUnhealthySince = now
        }

        if (health.privateStreamHealthy) {
            seenHealthyPrivateStream = true
            privateStreamUnhealthySince = null
            privateOutageReconciliationStarted = false
            privateFailureExitDispatched = false
        } else if (
            (seenHealthyPrivateStream || health.privateStreamEstablished) &&
            privateStreamUnhealthySince == null
        ) {
            privateStreamUnhealthySince = now
        }

        val streamsHealthy =
            health.publicDataHealthy && health.privateStreamHealthy
        val recoveryState = riskService.currentState().globalTradingState
        if (
            recoveryState in RECOVERY_EVIDENCE_STATES &&
            streamsHealthy
        ) {
            recoveryHealthySince = recoveryHealthySince ?: now
        } else {
            resetRecoveryEvidence()
        }
    }

    private fun failureReason(now: Instant): LevelReasonCode? {
        if (!executionGateway.hasTrackedExposure()) {
            return null
        }
        if (
            !publicFailureExitDispatched &&
            exceeded(publicDataUnhealthySince, now, publicOutageTimeout) &&
            attemptDue(lastEmergencyAttemptAt, now)
        ) {
            return LevelReasonCode.MARKET_DATA_FAILURE
        }
        if (
            !privateFailureExitDispatched &&
            exceeded(privateStreamUnhealthySince, now, privateOutageTimeout) &&
            attemptDue(lastEmergencyAttemptAt, now)
        ) {
            return LevelReasonCode.PRIVATE_STREAM_FAILURE
        }
        return null
    }

    private fun handleFailure(
        reason: LevelReasonCode,
        now: Instant,
        health: FailureRuntimeHealth,
    ): Mono<SafeModeSnapshot> {
        lastEmergencyAttemptAt = now
        val outageStartedAt = when (reason) {
            LevelReasonCode.MARKET_DATA_FAILURE -> publicDataUnhealthySince
            LevelReasonCode.PRIVATE_STREAM_FAILURE -> privateStreamUnhealthySince
            else -> null
        } ?: now
        val operationId = "${reason.name}:${outageStartedAt.toEpochMilli()}"
        return executionGateway
            .reconcile()
            .flatMap { reconciliation ->
                recordReconciliation(reconciliation, now)
                riskService.enterSafeMode(reason.name).flatMap { risk ->
                    if (risk.globalTradingState == GlobalTradingState.MANUAL_LOCK) {
                        executionGateway
                            .flattenAllAccountExposure(
                                reconciliation = reconciliation,
                                operationId = manualLockOperationId(),
                            )
                            .doOnSuccess { manualLockFlattened = true }
                    } else {
                        executionGateway
                            .closeReconciledExposure(
                                reason = reason,
                                reconciliation = reconciliation,
                                operationId = operationId,
                            )
                            .doOnSuccess { markFailureDispatched(reason) }
                    }
                }
            }
            .onErrorResume { error ->
                logger.warn(error) {
                    "Could not reconcile exposure for ${reason.name}"
                }
                riskService.enterSafeMode(reason.name).then()
            }
            .then(Mono.fromSupplier { publish(health) })
    }

    private fun startPrivateOutageReconciliation(
        health: FailureRuntimeHealth,
    ): Mono<SafeModeSnapshot> {
        privateOutageReconciliationStarted = true
        return executionGateway
            .reconcile()
            .doOnNext { reconciliation ->
                recordReconciliation(reconciliation, clock.instant())
            }
            .then(Mono.fromSupplier { publish(health) })
    }

    private fun evaluateRecovery(
        now: Instant,
        health: FailureRuntimeHealth,
    ): Mono<SafeModeSnapshot> =
        executionGateway
            .reconcile()
            .flatMap { reconciliation ->
                recordReconciliation(reconciliation, now)
                if (recoveryAllowed(now, health, reconciliation)) {
                    riskService.recoverFromSafeMode().then()
                } else {
                    Mono.empty()
                }
            }
            .then(Mono.fromSupplier { publish(health) })

    private fun evaluateManualLockEvidence(
        now: Instant,
        health: FailureRuntimeHealth,
    ): Mono<SafeModeSnapshot> = executionGateway
        .reconcile()
        .doOnNext { reconciliation ->
            recordReconciliation(reconciliation, now)
        }
        .then(Mono.fromSupplier { publish(health) })

    private fun flattenForManualLock(
        now: Instant,
        health: FailureRuntimeHealth,
    ): Mono<SafeModeSnapshot> {
        lastManualLockAttemptAt = now
        return executionGateway
            .reconcile()
            .flatMap { reconciliation ->
                recordReconciliation(reconciliation, now)
                executionGateway.flattenAllAccountExposure(
                    reconciliation = reconciliation,
                    operationId = manualLockOperationId(),
                )
            }
            .doOnSuccess { manualLockFlattened = true }
            .then(Mono.fromSupplier { publish(health) })
    }

    private fun manualLockOperationId(): String {
        val risk = riskService.currentState()
        val identity = risk.safeModeEventTimes.lastOrNull()?.toEpochMilli()
            ?.toString()
            ?: risk.stateReason
            ?: "manual"
        return "manual-lock:$identity"
    }

    private fun recordReconciliation(
        reconciliation: SignedRuntimeReconciliation,
        reconciledAt: Instant,
    ) {
        lastReconciliationAt = reconciledAt
        val fingerprint = reconciliation.fingerprint()
        if (fingerprint == lastFingerprint) {
            matchingReconciliationCount += 1
        } else {
            lastFingerprint = fingerprint
            matchingReconciliationCount = 1
        }
    }

    private fun recoveryAllowed(
        now: Instant,
        health: FailureRuntimeHealth,
        reconciliation: SignedRuntimeReconciliation,
    ): Boolean {
        val healthySince = recoveryHealthySince ?: return false
        return !now.isBefore(healthySince.plus(recoveryHealthDuration)) &&
            matchingReconciliationCount >= REQUIRED_MATCHING_RECONCILIATIONS &&
            reconciliation.unexplainedPositionSymbols.isEmpty() &&
            reconciliation.orphanedBotOrderIds.isEmpty() &&
            reconciliation.unresolvedOrderIds.isEmpty() &&
            reconciliation.symbolChecksHealthy &&
            reconciliation.accountChecksHealthy &&
            health.accountHealthy &&
            health.clockHealthy &&
            health.publicDataHealthy &&
            health.privateStreamHealthy
    }

    private fun reconciliationDue(now: Instant): Boolean {
        val last = lastReconciliationAt ?: return true
        return !now.isBefore(last.plus(reconciliationInterval))
    }

    private fun attemptDue(lastAttemptAt: Instant?, now: Instant): Boolean =
        lastAttemptAt == null ||
            !now.isBefore(lastAttemptAt.plus(reconciliationInterval))

    private fun resetRecoveryEvidence() {
        recoveryHealthySince = null
        lastReconciliationAt = null
        lastFingerprint = null
        matchingReconciliationCount = 0
    }

    private fun markFailureDispatched(reason: LevelReasonCode) {
        when (reason) {
            LevelReasonCode.MARKET_DATA_FAILURE ->
                publicFailureExitDispatched = true

            LevelReasonCode.PRIVATE_STREAM_FAILURE ->
                privateFailureExitDispatched = true

            else -> Unit
        }
    }

    private fun publish(health: FailureRuntimeHealth): SafeModeSnapshot =
        SafeModeSnapshot(
            observedAt = clock.instant(),
            entriesAndAdditionsBlocked =
                !health.publicDataHealthy ||
                    !health.privateStreamHealthy ||
                    riskService.currentState().globalTradingState !=
                    GlobalTradingState.RUNNING,
            publicDataHealthy = health.publicDataHealthy,
            privateStreamHealthy = health.privateStreamHealthy,
            accountHealthy = health.accountHealthy,
            clockHealthy = health.clockHealthy,
            publicDataUnhealthySince = publicDataUnhealthySince,
            privateStreamUnhealthySince = privateStreamUnhealthySince,
            recoveryHealthySince = recoveryHealthySince,
            matchingReconciliationCount = matchingReconciliationCount,
            recoveryHealthDurationSatisfied =
                recoveryHealthySince?.let { healthySince ->
                    !clock.instant().isBefore(
                        healthySince.plus(recoveryHealthDuration),
                    )
                } == true,
            safeModeEventCount = riskService.currentState().safeModeEventCount,
            globalTradingState = riskService.currentState().globalTradingState,
        ).also(publishedState::set)

    private fun initialSnapshot(): SafeModeSnapshot =
        SafeModeSnapshot(
            observedAt = clock.instant(),
            entriesAndAdditionsBlocked = true,
            publicDataHealthy = false,
            privateStreamHealthy = false,
            accountHealthy = false,
            clockHealthy = false,
            publicDataUnhealthySince = null,
            privateStreamUnhealthySince = null,
            recoveryHealthySince = null,
            matchingReconciliationCount = 0,
            recoveryHealthDurationSatisfied = false,
            safeModeEventCount = 0,
            globalTradingState = GlobalTradingState.RUNNING,
        )
}

private fun exceeded(
    since: Instant?,
    now: Instant,
    duration: Duration,
): Boolean = since != null && now.isAfter(since.plus(duration))

private const val REQUIRED_MATCHING_RECONCILIATIONS = 3
private val RECOVERY_EVIDENCE_STATES = setOf(
    GlobalTradingState.SAFE_MODE,
    GlobalTradingState.MANUAL_LOCK,
)
private val logger = KotlinLogging.logger {}
