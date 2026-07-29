package com.scalpsecta.breakoutbot.security

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.Customizer
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService
import org.springframework.security.core.userdetails.User
import org.springframework.security.crypto.factory.PasswordEncoderFactories
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.security.web.server.SecurityWebFilterChain
import org.springframework.security.web.server.csrf.CookieServerCsrfTokenRepository
import org.springframework.security.web.server.csrf.CsrfToken
import org.springframework.security.web.server.csrf.ServerCsrfTokenRequestAttributeHandler
import org.springframework.web.server.WebFilter
import reactor.core.publisher.Mono

@Configuration(proxyBeanMethods = false)
@EnableWebFluxSecurity
@EnableConfigurationProperties(OperatorSecurityProperties::class)
class OperatorSecurityConfiguration {
    @Bean
    fun passwordEncoder(): PasswordEncoder =
        PasswordEncoderFactories.createDelegatingPasswordEncoder()

    @Bean
    fun operatorUserDetailsService(
        properties: OperatorSecurityProperties,
        passwordEncoder: PasswordEncoder,
    ): MapReactiveUserDetailsService {
        val operator = User
            .withUsername(properties.username)
            .password(passwordEncoder.encode(properties.password))
            .roles("OPERATOR")
            .build()

        return MapReactiveUserDetailsService(operator)
    }

    @Bean
    fun securityWebFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        val csrfTokenRepository = CookieServerCsrfTokenRepository.withHttpOnlyFalse()
        csrfTokenRepository.setCookieCustomizer { cookie ->
            cookie
                .secure(true)
                .sameSite("Strict")
        }

        return http
            .authorizeExchange { exchanges ->
                exchanges.anyExchange().authenticated()
            }
            .httpBasic(Customizer.withDefaults())
            .formLogin { formLogin -> formLogin.disable() }
            .logout { logout -> logout.disable() }
            .cors { cors -> cors.disable() }
            .csrf { csrf ->
                csrf
                    .csrfTokenRepository(csrfTokenRepository)
                    .csrfTokenRequestHandler(
                        ServerCsrfTokenRequestAttributeHandler(),
                    )
            }
            .build()
    }

    @Bean
    fun csrfCookieWebFilter(): WebFilter =
        WebFilter { exchange, chain ->
            val csrfToken = exchange.getAttribute<Mono<CsrfToken>>(
                CsrfToken::class.java.name,
            )

            if (csrfToken == null) {
                chain.filter(exchange)
            } else {
                csrfToken.then(Mono.defer { chain.filter(exchange) })
            }
        }
}

@ConfigurationProperties("bot.security")
class OperatorSecurityProperties(
    val username: String,
    val password: String,
) {
    init {
        require(username.isNotBlank()) {
            "BOT_BASIC_USERNAME must not be blank"
        }
        require(password.isNotBlank()) {
            "BOT_BASIC_PASSWORD must not be blank"
        }
    }
}
