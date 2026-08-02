package com.scalpsecta.breakoutbot.security

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.security.config.Customizer
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity
import org.springframework.security.config.web.server.SecurityWebFiltersOrder
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService
import org.springframework.security.core.userdetails.User
import org.springframework.security.crypto.factory.PasswordEncoderFactories
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.security.web.server.SecurityWebFilterChain
import org.springframework.security.web.server.authorization.ServerAccessDeniedHandler
import org.springframework.security.web.server.csrf.CookieServerCsrfTokenRepository
import org.springframework.security.web.server.csrf.CsrfToken
import org.springframework.security.web.server.csrf.ServerCsrfTokenRequestAttributeHandler
import org.springframework.web.server.WebFilter
import reactor.core.publisher.Mono
import java.net.URI
import java.nio.charset.StandardCharsets

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
        val mutationDeniedHandler = ServerAccessDeniedHandler { exchange, _ ->
            writeSecurityError(
                exchange = exchange,
                code = "SECURITY_POLICY_VIOLATION",
                message =
                    "Mutating requests require valid authentication and CSRF protection",
            )
        }
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
            .exceptionHandling { exceptions ->
                exceptions.accessDeniedHandler(mutationDeniedHandler)
            }
            .csrf { csrf ->
                csrf
                    .csrfTokenRepository(csrfTokenRepository)
                    .accessDeniedHandler(mutationDeniedHandler)
                    .csrfTokenRequestHandler(
                        ServerCsrfTokenRequestAttributeHandler(),
                    )
            }
            .addFilterBefore(
                sameOriginMutationWebFilter(),
                SecurityWebFiltersOrder.AUTHORIZATION,
            )
            .build()
    }

    private fun sameOriginMutationWebFilter(): WebFilter = WebFilter { exchange, chain ->
        if (exchange.request.method in SAFE_METHODS) {
            return@WebFilter chain.filter(exchange)
        }
        val requestOrigin = exchange.request.headers.origin
            ?: exchange.request.headers.getFirst("Referer")
                ?.let(::refererOrigin)
        val expectedOrigin = requestOrigin(exchange.request.uri)
        if (requestOrigin == expectedOrigin) {
            chain.filter(exchange)
        } else {
            writeSecurityError(
                exchange = exchange,
                code = "SAME_ORIGIN_REQUIRED",
                message =
                    "Mutating requests require a same-origin Origin or Referer header",
            )
        }
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

private fun writeSecurityError(
    exchange: org.springframework.web.server.ServerWebExchange,
    code: String,
    message: String,
): Mono<Void> {
    val response = exchange.response
    response.statusCode = HttpStatus.FORBIDDEN
    response.headers.contentType = MediaType.APPLICATION_JSON
    val json = "{\"code\":\"$code\",\"message\":\"$message\"}"
    val buffer: DataBuffer = response.bufferFactory().wrap(
        json.toByteArray(StandardCharsets.UTF_8),
    )
    return response.writeWith(Mono.just(buffer))
}

private fun refererOrigin(referer: String): String? = runCatching {
    requestOrigin(URI.create(referer))
}.getOrNull()

private fun requestOrigin(uri: URI): String {
    val defaultPort = when (uri.scheme.lowercase()) {
        "https" -> 443
        "http" -> 80
        else -> -1
    }
    val port = uri.port.takeIf { candidate ->
        candidate != -1 && candidate != defaultPort
    }
    return buildString {
        append(uri.scheme.lowercase())
        append("://")
        append(uri.host.lowercase())
        if (port != null) {
            append(':')
            append(port)
        }
    }
}

private val SAFE_METHODS = setOf(
    HttpMethod.GET,
    HttpMethod.HEAD,
    HttpMethod.OPTIONS,
)

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
