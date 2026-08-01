package com.scalpsecta.breakoutbot.evidence

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(EvidenceProperties::class)
class EvidenceConfiguration

@ConfigurationProperties("bot.evidence")
data class EvidenceProperties(
    val directory: String = "build/evidence",
    val recentAuditLimit: Int = 200,
    val recentTradeLimit: Int = 200,
    val rawEventLimit: Int = 100_000,
    val shutdownFlushTimeout: Duration = Duration.ofSeconds(2),
) {
    init {
        require(directory.isNotBlank()) { "bot.evidence.directory must not be blank" }
        require(recentAuditLimit > 0) { "recentAuditLimit must be positive" }
        require(recentTradeLimit > 0) { "recentTradeLimit must be positive" }
        require(rawEventLimit > 0) { "rawEventLimit must be positive" }
        require(!shutdownFlushTimeout.isZero && !shutdownFlushTimeout.isNegative) {
            "shutdownFlushTimeout must be positive"
        }
    }
}
