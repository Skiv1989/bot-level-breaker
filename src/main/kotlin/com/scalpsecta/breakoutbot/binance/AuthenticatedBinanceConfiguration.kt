package com.scalpsecta.breakoutbot.binance

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import java.time.Clock

@Configuration(proxyBeanMethods = false)
class AuthenticatedBinanceConfiguration {
    @Bean
    @ConditionalOnMissingBean(BinanceCredentialsProvider::class)
    fun binanceCredentialsProvider(): BinanceCredentialsProvider =
        EnvironmentBinanceCredentialsProvider()

    @Bean
    @ConditionalOnMissingBean(AuthenticatedBinanceClient::class)
    fun authenticatedBinanceClient(
        webClientBuilder: WebClient.Builder,
        objectMapper: ObjectMapper,
        credentialsProvider: BinanceCredentialsProvider,
        clock: Clock,
    ): AuthenticatedBinanceClient =
        LiveAuthenticatedBinanceClient(
            webClient = liveBinanceWebClient(webClientBuilder),
            objectMapper = objectMapper,
            credentialsProvider = credentialsProvider,
            clock = clock,
        )

    @Bean
    @Primary
    @ConditionalOnMissingBean(BinanceExecutionClient::class)
    fun binanceExecutionClient(
        authenticatedBinanceClient: AuthenticatedBinanceClient,
    ): BinanceExecutionClient =
        authenticatedBinanceClient as? BinanceExecutionClient
            ?: UnavailableBinanceExecutionClient()

    @Bean
    @ConditionalOnMissingBean(BinanceUserDataEventParser::class)
    fun binanceUserDataEventParser(
        objectMapper: ObjectMapper,
        clock: Clock,
    ): BinanceUserDataEventParser =
        BinanceUserDataEventParser(objectMapper, clock)

    @Bean
    @ConditionalOnMissingBean(WebSocketClient::class)
    fun binanceWebSocketClient(): WebSocketClient =
        ReactorNettyWebSocketClient()

    @Bean
    @ConditionalOnMissingBean(BinanceUserDataStreamProvider::class)
    fun binanceUserDataStreamProvider(
        webSocketClient: WebSocketClient,
        parser: BinanceUserDataEventParser,
        clock: Clock,
    ): BinanceUserDataStreamProvider =
        LiveBinanceUserDataStreamProvider(
            webSocketClient = webSocketClient,
            parser = parser,
            clock = clock,
        )
}
