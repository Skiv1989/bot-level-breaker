package com.scalpsecta.breakoutbot.evidence

import com.scalpsecta.breakoutbot.service.AuthenticatedBinanceReadinessService
import jakarta.annotation.PreDestroy
import org.springframework.stereotype.Component
import reactor.core.Disposable

@Component
class PrivateEventEvidenceBridge(
    readinessService: AuthenticatedBinanceReadinessService,
    evidenceRecorder: EvidenceRecorder,
) {
    private val subscription: Disposable = readinessService
        .events()
        .subscribe(evidenceRecorder::record)

    @PreDestroy
    fun close() {
        subscription.dispose()
    }
}
