package com.scalpsecta.breakoutbot.control

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.execution.BreakoutExecutionService
import com.scalpsecta.breakoutbot.failure.SafeModeExecutionGateway
import com.scalpsecta.breakoutbot.failure.SafeModeService
import com.scalpsecta.breakoutbot.failure.SignedRuntimeReconciliation
import com.scalpsecta.breakoutbot.level.LevelException
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.level.LevelService
import com.scalpsecta.breakoutbot.level.LevelState
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataService
import com.scalpsecta.breakoutbot.service.AuthenticatedBinanceReadinessService
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.util.UUID

data class ManualCloseExecution(
    val levelId: UUID?,
    val levelState: LevelState?,
    val closeDispatched: Boolean,
    val deleteAllowed: Boolean,
    val reconciliation: SignedRuntimeReconciliation,
)

data class OperatorRuntimeHealth(
    val publicDataHealthy: Boolean,
    val privateStreamHealthy: Boolean,
    val accountHealthy: Boolean,
    val clockHealthy: Boolean,
    val recoveryHealthDurationSatisfied: Boolean,
)

interface OperatorControlExecutionGateway {
    fun reconcile(): Mono<SignedRuntimeReconciliation>

    fun closePosition(
        symbol: String,
        commandId: UUID,
        reconciliation: SignedRuntimeReconciliation,
    ): Mono<ManualCloseExecution>

    fun flattenAllAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void>

    fun runtimeHealth(): OperatorRuntimeHealth
}

@Service
class LiveOperatorControlExecutionGateway(
    private val safeModeExecutionGateway: SafeModeExecutionGateway,
    private val safeModeService: SafeModeService,
    private val levelService: LevelService,
    private val breakoutExecutionService: BreakoutExecutionService,
    private val publicMarketDataService: PublicMarketDataService,
    private val authenticatedBinanceReadinessService:
        AuthenticatedBinanceReadinessService,
) : OperatorControlExecutionGateway {
    override fun reconcile(): Mono<SignedRuntimeReconciliation> =
        safeModeExecutionGateway.reconcile()

    override fun closePosition(
        symbol: String,
        commandId: UUID,
        reconciliation: SignedRuntimeReconciliation,
    ): Mono<ManualCloseExecution> {
        val normalizedSymbol = symbol.trim().uppercase()
        val positionAmount = reconciliation.positions
            .firstOrNull { position -> position.symbol == normalizedSymbol }
            ?.positionAmount
            ?: BigDecimal.ZERO
        return levelService
            .claimManualClose(
                symbol = normalizedSymbol,
                commandId = commandId,
                reconciledPositionAmount = positionAmount,
            )
            .flatMap { claim ->
                val request = claim.request
                if (request == null) {
                    Mono.just(
                        ManualCloseExecution(
                            levelId = claim.level?.id,
                            levelState = claim.level?.state,
                            closeDispatched = false,
                            deleteAllowed = claim.level?.deleteAllowed == true,
                            reconciliation = reconciliation,
                        ),
                    )
                } else {
                    breakoutExecutionService
                        .execute(request)
                        .then(reconcile())
                        .map { confirmed ->
                            ManualCloseExecution(
                                levelId = request.levelId,
                                levelState = levelService
                                    .currentState()
                                    .firstOrNull { level ->
                                        level.id == request.levelId
                                    }
                                    ?.state,
                                closeDispatched = true,
                                deleteAllowed = levelService
                                    .currentState()
                                    .firstOrNull { level ->
                                        level.id == request.levelId
                                    }
                                    ?.deleteAllowed == true,
                                reconciliation = confirmed,
                            )
                        }
                }
            }
            .onErrorResume(LevelException::class.java) { error ->
                if (error.code != LevelReasonCode.LEVEL_NOT_FOUND) {
                    Mono.error(error)
                } else {
                    Mono.just(
                        ManualCloseExecution(
                            levelId = null,
                            levelState = null,
                            closeDispatched = false,
                            deleteAllowed = false,
                            reconciliation = reconciliation,
                        ),
                    )
                }
            }
    }

    override fun flattenAllAccountExposure(
        reconciliation: SignedRuntimeReconciliation,
        operationId: String,
    ): Mono<Void> = safeModeExecutionGateway.flattenAllAccountExposure(
        reconciliation = reconciliation,
        operationId = operationId,
        terminalReason = LevelReasonCode.KILL_SWITCH,
    )

    override fun runtimeHealth(): OperatorRuntimeHealth {
        val symbols = safeModeExecutionGateway.runtimeSymbols()
        val publicSnapshots = publicMarketDataService
            .snapshots()
            .associateBy { snapshot -> snapshot.symbol }
        val authenticated = authenticatedBinanceReadinessService.snapshot()
        return OperatorRuntimeHealth(
            publicDataHealthy = symbols.all { symbol ->
                publicSnapshots[symbol]?.healthy == true
            },
            privateStreamHealthy =
                authenticated.privateStream.readiness == BinanceReadiness.READY,
            accountHealthy =
                authenticated.account.readiness == BinanceReadiness.READY,
            clockHealthy =
                authenticated.clock.readiness == BinanceReadiness.READY,
            recoveryHealthDurationSatisfied = safeModeService
                .currentState()
                .recoveryHealthDurationSatisfied,
        )
    }
}
