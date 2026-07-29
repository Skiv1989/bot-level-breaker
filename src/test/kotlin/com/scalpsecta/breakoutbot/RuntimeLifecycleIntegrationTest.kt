package com.scalpsecta.breakoutbot

import com.scalpsecta.breakoutbot.binance.BinanceGateway
import com.scalpsecta.breakoutbot.binance.BinanceOperation
import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.domain.TradingReadiness
import com.scalpsecta.breakoutbot.service.BotStateService
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import reactor.core.publisher.Mono
import java.util.concurrent.CopyOnWriteArrayList

class RuntimeLifecycleIntegrationTest {
    @Test
    fun `starts each context with a fresh empty memory-only runtime`() {
        val firstContext = startApplication()
        val firstStartedAt = firstContext
            .getBean(BotStateService::class.java)
            .currentState()
            .startedAt
        firstContext.close()

        val secondContext = startApplication()
        try {
            val snapshot = secondContext
                .getBean(BotStateService::class.java)
                .currentState()

            assertThat(snapshot.startedAt).isAfter(firstStartedAt)
            assertThat(snapshot.levelCount).isZero()
            assertThat(snapshot.recoveredAttemptCount).isZero()
        } finally {
            secondContext.close()
        }
    }

    @Test
    fun `keeps Binance and trading readiness separate`() {
        val context = startApplication()
        try {
            val health = context
                .getBean(BotStateService::class.java)
                .currentState()
                .health

            assertThat(health.publicDataReadiness).isEqualTo(BinanceReadiness.NOT_READY)
            assertThat(health.privateStreamReadiness).isEqualTo(BinanceReadiness.NOT_READY)
            assertThat(health.tradingReadiness).isEqualTo(TradingReadiness.BLOCKED)
        } finally {
            context.close()
        }
    }

    @Test
    fun `startup and shutdown issue no Binance operation`() {
        val context = startApplication()
        val gateway = context.getBean(RecordingFakeBinanceGateway::class.java)

        assertThat(gateway.operations).isEmpty()

        context.close()

        assertThat(gateway.operations).isEmpty()
    }

    private fun startApplication(): ConfigurableApplicationContext =
        SpringApplicationBuilder(
            BreakoutBotApplication::class.java,
            SafeLifecycleTestConfiguration::class.java,
        )
            .web(WebApplicationType.REACTIVE)
            .properties(
                "spring.main.banner-mode=off",
            )
            .run(
                "--server.port=0",
                "--server.ssl.enabled=false",
                "--server.ssl.key-store=classpath:unused-test-keystore.p12",
                "--server.ssl.key-store-password=unused-test-keystore-password",
                "--bot.security.username=lifecycle-test-operator",
                "--bot.security.password=lifecycle-test-password",
            )
}

@TestConfiguration(proxyBeanMethods = false)
class SafeLifecycleTestConfiguration {
    @Bean
    @Primary
    fun recordingFakeBinanceGateway(): RecordingFakeBinanceGateway =
        RecordingFakeBinanceGateway()
}

class RecordingFakeBinanceGateway : BinanceGateway {
    val operations = CopyOnWriteArrayList<BinanceOperation>()

    override fun execute(operation: BinanceOperation): Mono<Void> {
        operations += operation
        return Mono.empty()
    }
}
