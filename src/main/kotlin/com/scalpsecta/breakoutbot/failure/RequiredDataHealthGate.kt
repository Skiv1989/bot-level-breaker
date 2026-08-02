package com.scalpsecta.breakoutbot.failure

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataService
import com.scalpsecta.breakoutbot.service.AuthenticatedBinanceReadinessService
import org.springframework.stereotype.Service

@Service
class RequiredDataHealthGate(
    private val publicMarketDataService: PublicMarketDataService,
    private val authenticatedBinanceReadinessService:
        AuthenticatedBinanceReadinessService,
) {
    fun entriesAndAdditionsAllowed(symbol: String): Boolean {
        val normalizedSymbol = symbol.trim().uppercase()
        val publicDataHealthy = publicMarketDataService
            .snapshots()
            .firstOrNull { snapshot -> snapshot.symbol == normalizedSymbol }
            ?.healthy == true
        val privateDataHealthy = authenticatedBinanceReadinessService
            .snapshot()
            .privateStream
            .readiness == BinanceReadiness.READY
        return publicDataHealthy && privateDataHealthy
    }
}
