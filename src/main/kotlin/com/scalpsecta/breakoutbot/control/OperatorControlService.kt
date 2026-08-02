package com.scalpsecta.breakoutbot.control

import com.scalpsecta.breakoutbot.evidence.AuditEventType
import com.scalpsecta.breakoutbot.evidence.AuditRecordDraft
import com.scalpsecta.breakoutbot.evidence.EvidenceRecorder
import com.scalpsecta.breakoutbot.failure.SignedRuntimeReconciliation
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.level.LevelState
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Clock
import java.time.Instant
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Service
class OperatorControlService(
    private val clock: Clock,
    private val riskService: AttemptRiskService,
    private val executionGateway: OperatorControlExecutionGateway,
    private val evidenceRecorder: EvidenceRecorder,
) {
    private val lock = ReentrantLock()
    private val globalQueue = OrderedGlobalControlQueue()
    private val commands = linkedMapOf<UUID, RegisteredOperatorCommand>()
    private val snapshots = linkedMapOf<UUID, OperatorCommandSnapshot>()

    fun closePosition(
        symbol: String,
        commandId: UUID,
    ): Mono<OperatorCommandSnapshot> = register(
        commandId = commandId,
        type = OperatorCommandType.MANUAL_CLOSE,
        symbol = symbol.trim().uppercase(),
        globallyOrdered = false,
    ) {
        executeManualClose(symbol.trim().uppercase(), commandId)
    }

    fun kill(commandId: UUID): Mono<OperatorCommandSnapshot> = register(
        commandId = commandId,
        type = OperatorCommandType.KILL_SWITCH,
        symbol = null,
        globallyOrdered = true,
    ) {
        executeKill(commandId)
    }

    fun unlock(commandId: UUID): Mono<OperatorCommandSnapshot> = register(
        commandId = commandId,
        type = OperatorCommandType.MANUAL_UNLOCK,
        symbol = null,
        globallyOrdered = true,
    ) {
        executeUnlock()
    }

    fun currentState(): OperatorControlsSnapshot = lock.withLock {
        OperatorControlsSnapshot(
            observedAt = clock.instant(),
            commands = snapshots.values.toList(),
        )
    }

    private fun register(
        commandId: UUID,
        type: OperatorCommandType,
        symbol: String?,
        globallyOrdered: Boolean,
        action: () -> Mono<CommandOutcome>,
    ): Mono<OperatorCommandSnapshot> = lock.withLock {
        commands[commandId]?.let { existing ->
            if (existing.type != type || existing.symbol != symbol) {
                return@withLock Mono.error(
                    OperatorCommandException(
                        code = OperatorCommandCode.COMMAND_ID_CONFLICT,
                        message =
                            "Command id $commandId is already registered for " +
                                existing.type.name,
                    ),
                )
            }
            return@withLock existing.result
        }

        val requestedAt = clock.instant()
        val initial = OperatorCommandSnapshot(
            commandId = commandId,
            type = type,
            symbol = symbol,
            status = OperatorCommandStatus.IN_PROGRESS,
            code = OperatorCommandCode.COMMAND_IN_PROGRESS,
            message = "${type.name} command is in progress",
            requestedAt = requestedAt,
            completedAt = null,
            blockers = emptyList(),
            residualExposure = emptyList(),
            openBotOrderIds = emptySet(),
            globalTradingState = riskService.currentState().globalTradingState,
        )
        snapshots[commandId] = initial
        recordCommandAudit(initial)

        val execution = if (globallyOrdered) {
            globalQueue.submit(action)
        } else {
            Mono.defer(action)
        }
        val result = execution
            .map { outcome -> complete(initial, outcome) }
            .onErrorResume { error ->
                Mono.just(
                    complete(
                        initial,
                        CommandOutcome(
                            status = OperatorCommandStatus.FAILED,
                            code = OperatorCommandCode.COMMAND_FAILED,
                            message =
                                "${type.name} failed safely: " +
                                    error.javaClass.simpleName,
                        ),
                    ),
                )
            }
            .cache()
        commands[commandId] = RegisteredOperatorCommand(type, symbol, result)
        pruneCompletedCommands()
        result
    }

    private fun executeManualClose(
        symbol: String,
        commandId: UUID,
    ): Mono<CommandOutcome> = executionGateway
        .reconcile()
        .flatMap { reconciliation ->
            executionGateway.closePosition(symbol, commandId, reconciliation)
        }
        .map { execution ->
            val residual = execution.reconciliation.residualExposure(symbol)
            when {
                execution.levelId == null && residual.isNotEmpty() ->
                    CommandOutcome(
                        status = OperatorCommandStatus.BLOCKED,
                        code = OperatorCommandCode.MANUAL_CLOSE_INCOMPLETE,
                        message =
                            "$symbol has account exposure but no owning runtime level",
                        blockers = listOf(
                            OperatorCommandBlocker.POSITION_NOT_TRACKED,
                            OperatorCommandBlocker.RESIDUAL_EXPOSURE,
                        ),
                        residualExposure = residual,
                    )

                execution.levelId == null -> CommandOutcome(
                    status = OperatorCommandStatus.BLOCKED,
                    code = OperatorCommandCode.POSITION_NOT_ACTIVE,
                    message = "$symbol has no active position owned by a runtime level",
                )

                residual.isNotEmpty() || !execution.deleteAllowed ->
                    CommandOutcome(
                        status = OperatorCommandStatus.BLOCKED,
                        code = OperatorCommandCode.MANUAL_CLOSE_INCOMPLETE,
                        message =
                            "$symbol close finished with residual exposure or an unresolved order",
                        blockers = buildList {
                            if (residual.isNotEmpty()) {
                                add(OperatorCommandBlocker.RESIDUAL_EXPOSURE)
                            }
                            if (!execution.deleteAllowed) {
                                add(OperatorCommandBlocker.UNRESOLVED_ORDER)
                            }
                        },
                        residualExposure = residual,
                        openBotOrderIds =
                            execution.reconciliation.openBotOrderIds,
                    )

                !execution.closeDispatched &&
                    execution.levelState == LevelState.TERMINAL ->
                    CommandOutcome(
                        status = OperatorCommandStatus.SUCCEEDED,
                        code =
                            OperatorCommandCode.MANUAL_CLOSE_ALREADY_COMPLETED,
                        message = "$symbol was already confirmed flat",
                    )

                else -> CommandOutcome(
                    status = OperatorCommandStatus.SUCCEEDED,
                    code = OperatorCommandCode.MANUAL_CLOSE_COMPLETED,
                    message =
                        "$symbol was closed through the normal reduce-only flow and confirmed flat",
                )
            }
        }

    private fun executeKill(commandId: UUID): Mono<CommandOutcome> =
        riskService
            .enterManualLock(LevelReasonCode.KILL_SWITCH.name)
            .then(executionGateway.reconcile())
            .flatMap { reconciliation ->
                executionGateway
                    .flattenAllAccountExposure(
                        reconciliation = reconciliation,
                        operationId = "kill-switch:$commandId",
                    )
                    .then(executionGateway.reconcile())
            }
            .map { reconciliation ->
                val blockers = reconciliation.accountBlockers(
                    requireNoOpenOrders = true,
                    requireHealthyChecks = false,
                )
                if (blockers.isEmpty()) {
                    CommandOutcome(
                        status = OperatorCommandStatus.SUCCEEDED,
                        code = OperatorCommandCode.KILL_SWITCH_COMPLETED,
                        message =
                            "All account exposure is flat and trading remains manually locked",
                    )
                } else {
                    CommandOutcome(
                        status = OperatorCommandStatus.BLOCKED,
                        code = OperatorCommandCode.KILL_SWITCH_INCOMPLETE,
                        message =
                            "Kill switch remains locked with residual account state",
                        blockers = blockers,
                        residualExposure = reconciliation.residualExposure(),
                        openBotOrderIds = reconciliation.openBotOrderIds,
                    )
                }
            }

    private fun executeUnlock(): Mono<CommandOutcome> {
        if (
            riskService.currentState().globalTradingState !=
            GlobalTradingState.MANUAL_LOCK
        ) {
            return Mono.just(
                CommandOutcome(
                    status = OperatorCommandStatus.BLOCKED,
                    code = OperatorCommandCode.MANUAL_UNLOCK_REJECTED,
                    message = "The runtime is not in MANUAL_LOCK",
                    blockers = listOf(
                        OperatorCommandBlocker.NOT_MANUAL_LOCKED,
                    ),
                ),
            )
        }
        return Flux
            .range(0, REQUIRED_UNLOCK_RECONCILIATIONS)
            .concatMap { executionGateway.reconcile() }
            .collectList()
            .flatMap { reconciliations ->
                val latest = reconciliations.last()
                val health = executionGateway.runtimeHealth()
                val blockers = buildList {
                    addAll(
                        latest.accountBlockers(
                            requireNoOpenOrders = true,
                            requireHealthyChecks = true,
                        ),
                    )
                    if (
                        reconciliations
                            .map(SignedRuntimeReconciliation::fingerprint)
                            .distinct()
                            .size != 1
                    ) {
                        add(OperatorCommandBlocker.RECONCILIATION_MISMATCH)
                    }
                    if (!health.publicDataHealthy) {
                        add(OperatorCommandBlocker.PUBLIC_DATA_UNHEALTHY)
                    }
                    if (!health.privateStreamHealthy) {
                        add(OperatorCommandBlocker.PRIVATE_STREAM_UNHEALTHY)
                    }
                    if (!health.accountHealthy) {
                        add(OperatorCommandBlocker.ACCOUNT_UNHEALTHY)
                    }
                    if (!health.clockHealthy) {
                        add(OperatorCommandBlocker.CLOCK_UNHEALTHY)
                    }
                    if (!health.recoveryHealthDurationSatisfied) {
                        add(
                            OperatorCommandBlocker
                                .RECOVERY_HEALTH_WINDOW_INCOMPLETE,
                        )
                    }
                }.distinct()
                if (blockers.isNotEmpty()) {
                    Mono.just(
                        CommandOutcome(
                            status = OperatorCommandStatus.BLOCKED,
                            code = OperatorCommandCode.MANUAL_UNLOCK_REJECTED,
                            message =
                                "Manual unlock requirements are not satisfied",
                            blockers = blockers,
                            residualExposure = latest.residualExposure(),
                            openBotOrderIds = latest.openBotOrderIds,
                        ),
                    )
                } else {
                    riskService.unlockManualLock().map { state ->
                        if (
                            state.globalTradingState ==
                            GlobalTradingState.MANUAL_LOCK
                        ) {
                            CommandOutcome(
                                status = OperatorCommandStatus.BLOCKED,
                                code =
                                    OperatorCommandCode.MANUAL_UNLOCK_REJECTED,
                                message =
                                    "A higher-priority global lock prevents manual unlock",
                                blockers = listOf(
                                    OperatorCommandBlocker.GLOBAL_LOCK_REMAINS,
                                ),
                            )
                        } else {
                            CommandOutcome(
                                status = OperatorCommandStatus.SUCCEEDED,
                                code =
                                    OperatorCommandCode.MANUAL_UNLOCK_COMPLETED,
                                message =
                                    "Manual lock was removed after three matching " +
                                        "healthy flat reconciliations",
                            )
                        }
                    }
                }
            }
    }

    private fun complete(
        initial: OperatorCommandSnapshot,
        outcome: CommandOutcome,
    ): OperatorCommandSnapshot {
        val completed = initial.copy(
            status = outcome.status,
            code = outcome.code,
            message = outcome.message,
            completedAt = clock.instant(),
            blockers = outcome.blockers,
            residualExposure = outcome.residualExposure,
            openBotOrderIds = outcome.openBotOrderIds,
            globalTradingState = riskService.currentState().globalTradingState,
        )
        lock.withLock { snapshots[completed.commandId] = completed }
        recordCommandAudit(completed)
        return completed
    }

    private fun recordCommandAudit(command: OperatorCommandSnapshot) {
        evidenceRecorder.recordAudit(
            AuditRecordDraft(
                timestamp = command.completedAt ?: command.requestedAt,
                symbol = command.symbol ?: ACCOUNT_SYMBOL,
                levelId = command.commandId,
                stateBefore = null,
                stateAfter = null,
                eventType = AuditEventType.DECISION,
                decision = command.code.name,
                blockerReasons = command.blockers.map(Enum<*>::name),
                recoveryDetail = command.message,
            ),
        )
    }

    private fun pruneCompletedCommands() {
        while (commands.size > MAXIMUM_RETAINED_COMMANDS) {
            val oldest = commands.entries.firstOrNull { (id, _) ->
                snapshots[id]?.status != OperatorCommandStatus.IN_PROGRESS
            } ?: return
            commands.remove(oldest.key)
            snapshots.remove(oldest.key)
        }
    }
}

private data class RegisteredOperatorCommand(
    val type: OperatorCommandType,
    val symbol: String?,
    val result: Mono<OperatorCommandSnapshot>,
)

private data class CommandOutcome(
    val status: OperatorCommandStatus,
    val code: OperatorCommandCode,
    val message: String,
    val blockers: List<OperatorCommandBlocker> = emptyList(),
    val residualExposure: List<ResidualExposureSnapshot> = emptyList(),
    val openBotOrderIds: Set<String> = emptySet(),
)

private fun SignedRuntimeReconciliation.residualExposure(
    symbol: String? = null,
): List<ResidualExposureSnapshot> = positions
    .asSequence()
    .filter { position ->
        position.positionAmount.signum() != 0 &&
            (symbol == null || position.symbol == symbol)
    }
    .sortedBy { position -> position.symbol }
    .map { position ->
        ResidualExposureSnapshot(
            symbol = position.symbol,
            positionAmount = position.positionAmount,
            entryPrice = position.entryPrice,
        )
    }
    .toList()

private fun SignedRuntimeReconciliation.accountBlockers(
    requireNoOpenOrders: Boolean,
    requireHealthyChecks: Boolean,
): List<OperatorCommandBlocker> = buildList {
    if (positions.any { position -> position.positionAmount.signum() != 0 }) {
        add(OperatorCommandBlocker.RESIDUAL_EXPOSURE)
    }
    if (requireNoOpenOrders && openBotOrderIds.isNotEmpty()) {
        add(OperatorCommandBlocker.BOT_ORDERS_REMAIN)
    }
    if (unresolvedOrderIds.isNotEmpty()) {
        add(OperatorCommandBlocker.UNRESOLVED_ORDER)
    }
    if (unexplainedPositionSymbols.isNotEmpty()) {
        add(OperatorCommandBlocker.UNEXPLAINED_EXPOSURE)
    }
    if (orphanedBotOrderIds.isNotEmpty()) {
        add(OperatorCommandBlocker.ORPHANED_BOT_ORDER)
    }
    if (requireHealthyChecks && !symbolChecksHealthy) {
        add(OperatorCommandBlocker.SYMBOL_CHECK_FAILED)
    }
    if (requireHealthyChecks && !accountChecksHealthy) {
        add(OperatorCommandBlocker.ACCOUNT_CHECK_FAILED)
    }
}

private const val REQUIRED_UNLOCK_RECONCILIATIONS = 3
private const val MAXIMUM_RETAINED_COMMANDS = 100
private const val ACCOUNT_SYMBOL = "ACCOUNT"
