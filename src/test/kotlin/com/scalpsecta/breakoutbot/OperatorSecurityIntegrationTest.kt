package com.scalpsecta.breakoutbot

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.boot.web.context.WebServerApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import java.net.URI
import java.net.URLDecoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.security.SecureRandom
import java.time.Instant
import java.util.Base64
import java.util.UUID
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

@ExtendWith(OutputCaptureExtension::class)
class OperatorSecurityIntegrationTest {
    @TempDir
    lateinit var tempDirectory: Path

    @Test
    fun `unauthenticated HTTPS snapshot access is rejected`() {
        withHttpsApplication { application ->
            val response = application.getSnapshot()

            assertThat(response.statusCode()).isEqualTo(401)
            assertThat(response.headers().firstValue("www-authenticate").orElse(""))
                .startsWith("Basic")
            assertThat(response.body()).doesNotContainCredentials()
        }
    }

    @Test
    fun `invalid Basic credentials are rejected without leaking secrets`(
        output: CapturedOutput,
    ) {
        withHttpsApplication { application ->
            val response = application.getSnapshot(
                username = "invalid-operator",
                password = INVALID_PASSWORD,
            )

            assertThat(response.statusCode()).isEqualTo(401)
            assertThat(response.body())
                .doesNotContainCredentials()
                .doesNotContain(INVALID_PASSWORD)
        }

        assertThat(output.all)
            .doesNotContain(OPERATOR_USERNAME)
            .doesNotContain(OPERATOR_PASSWORD)
            .doesNotContain(KEYSTORE_PASSWORD)
            .doesNotContain(INVALID_PASSWORD)
    }

    @Test
    fun `valid Basic credentials read the consolidated snapshot over HTTPS`() {
        withHttpsApplication { application ->
            val response = application.getSnapshot(
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
            )

            assertThat(response.statusCode()).isEqualTo(200)
            assertThat(response.headers().firstValue("content-type").orElse(""))
                .startsWith("application/json")
            assertThat(response.headers().firstValue("strict-transport-security").orElse(""))
                .isNotBlank()

            val snapshot = application.objectMapper.readTree(response.body())
            assertThat(Instant.parse(snapshot["startedAt"].asText())).isBeforeOrEqualTo(Instant.now())
            assertThat(snapshot["health"]["publicDataReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(snapshot["publicMarketData"]).isEmpty()
            assertThat(snapshot["health"]["privateStreamReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(snapshot["health"]["clockReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(snapshot["health"]["accountReadiness"].asText())
                .isEqualTo("NOT_READY")
            assertThat(snapshot["health"]["tradingReadiness"].asText())
                .isEqualTo("BLOCKED")
            assertThat(
                snapshot["authenticatedBinance"]["clock"]["readiness"].asText(),
            ).isEqualTo("NOT_READY")
            assertThat(
                snapshot["authenticatedBinance"]["account"]["readiness"].asText(),
            ).isEqualTo("NOT_READY")
            assertThat(
                snapshot["authenticatedBinance"]["privateStream"]["readiness"].asText(),
            ).isEqualTo("NOT_READY")
            assertThat(snapshot["authenticatedBinance"]["currentEquity"].isNull)
                .isTrue()
            assertThat(
                snapshot["authenticatedBinance"]["temporaryDailyAnchorEquity"].isNull,
            ).isTrue()
            assertThat(response.body()).doesNotContainCredentials()
        }
    }

    @Test
    fun `authenticated browser receives a secure CSRF token and CORS remains disabled`() {
        withHttpsApplication { application ->
            val snapshotResponse = application.getSnapshot(
                username = OPERATOR_USERNAME,
                password = OPERATOR_PASSWORD,
                origin = "https://untrusted.example",
            )

            assertThat(snapshotResponse.statusCode()).isEqualTo(200)
            assertThat(snapshotResponse.headers().allValues("access-control-allow-origin"))
                .isEmpty()

            val csrfCookie = snapshotResponse
                .headers()
                .allValues("set-cookie")
                .single { cookie -> cookie.startsWith("$CSRF_COOKIE_NAME=") }
            assertThat(csrfCookie)
                .containsIgnoringCase("Secure")
                .contains("SameSite=Strict")
                .doesNotContain("HttpOnly")

            val cookiePair = csrfCookie.substringBefore(';')
            val csrfToken = URLDecoder.decode(
                cookiePair.substringAfter('='),
                StandardCharsets.UTF_8,
            )

            val missingTokenResponse = application.post(
                path = "/api/future-control",
            )
            assertThat(missingTokenResponse.statusCode()).isEqualTo(403)

            val validTokenResponse = application.post(
                path = "/api/future-control",
                cookie = cookiePair,
                csrfToken = csrfToken,
            )
            assertThat(validTokenResponse.statusCode()).isEqualTo(404)
            assertThat(validTokenResponse.body()).doesNotContainCredentials()
        }
    }

    private fun withHttpsApplication(
        block: (HttpsApplication) -> Unit,
    ) {
        val keyStorePath = createTestKeyStore()
        val context = SpringApplicationBuilder(BreakoutBotApplication::class.java)
            .web(WebApplicationType.REACTIVE)
            .properties(
                "spring.main.banner-mode=off",
            )
            .run(
                "--server.port=0",
                "--server.ssl.enabled=true",
                "--server.ssl.key-store=${keyStorePath.toUri()}",
                "--server.ssl.key-store-password=$KEYSTORE_PASSWORD",
                "--server.ssl.key-store-type=PKCS12",
                "--bot.security.username=$OPERATOR_USERNAME",
                "--bot.security.password=$OPERATOR_PASSWORD",
                "--bot.binance.startup-enabled=false",
            )

        try {
            val webContext = context as WebServerApplicationContext
            val application = HttpsApplication(
                context = context,
                client = createHttpsClient(keyStorePath),
                baseUri = URI.create("https://localhost:${webContext.webServer.port}"),
            )
            block(application)
        } finally {
            context.close()
        }
    }

    private fun createTestKeyStore(): Path {
        val keyStorePath = tempDirectory.resolve(
            "operator-${UUID.randomUUID()}.p12",
        )
        val executableName = if (System.getProperty("os.name").startsWith("Windows")) {
            "keytool.exe"
        } else {
            "keytool"
        }
        val keytool = Path.of(
            System.getProperty("java.home"),
            "bin",
            executableName,
        )
        val process = ProcessBuilder(
            keytool.toString(),
            "-genkeypair",
            "-alias",
            KEY_ALIAS,
            "-keyalg",
            "RSA",
            "-keysize",
            "2048",
            "-storetype",
            "PKCS12",
            "-keystore",
            keyStorePath.toString(),
            "-storepass",
            KEYSTORE_PASSWORD,
            "-keypass",
            KEYSTORE_PASSWORD,
            "-dname",
            "CN=localhost",
            "-validity",
            "1",
            "-ext",
            "SAN=dns:localhost,ip:127.0.0.1",
            "-noprompt",
        )
            .redirectErrorStream(true)
            .start()
        val output = process.inputStream.bufferedReader().use { reader ->
            reader.readText()
        }

        assertThat(process.waitFor())
            .describedAs("keytool output: %s", output)
            .isZero()
        assertThat(keyStorePath).isRegularFile()
        return keyStorePath
    }

    private fun createHttpsClient(keyStorePath: Path): HttpClient {
        val serverKeyStore = KeyStore.getInstance("PKCS12")
        Files.newInputStream(keyStorePath).use { input ->
            serverKeyStore.load(input, KEYSTORE_PASSWORD.toCharArray())
        }

        val trustStore = KeyStore.getInstance(KeyStore.getDefaultType())
        trustStore.load(null)
        trustStore.setCertificateEntry(
            KEY_ALIAS,
            serverKeyStore.getCertificate(KEY_ALIAS),
        )

        val trustManagerFactory = TrustManagerFactory.getInstance(
            TrustManagerFactory.getDefaultAlgorithm(),
        )
        trustManagerFactory.init(trustStore)

        val sslContext = SSLContext.getInstance("TLS")
        sslContext.init(null, trustManagerFactory.trustManagers, SecureRandom())

        return HttpClient
            .newBuilder()
            .sslContext(sslContext)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build()
    }

    private fun HttpsApplication.getSnapshot(
        username: String? = null,
        password: String? = null,
        origin: String? = null,
    ): HttpResponse<String> {
        val request = HttpRequest
            .newBuilder(baseUri.resolve("/api/snapshot"))
            .GET()
        if (username != null && password != null) {
            request.header("Authorization", basicAuthorization(username, password))
        }
        if (origin != null) {
            request.header("Origin", origin)
        }

        return client.send(
            request.build(),
            HttpResponse.BodyHandlers.ofString(),
        )
    }

    private fun HttpsApplication.post(
        path: String,
        cookie: String? = null,
        csrfToken: String? = null,
    ): HttpResponse<String> {
        val request = HttpRequest
            .newBuilder(baseUri.resolve(path))
            .header(
                "Authorization",
                basicAuthorization(OPERATOR_USERNAME, OPERATOR_PASSWORD),
            )
            .POST(HttpRequest.BodyPublishers.noBody())
        if (cookie != null) {
            request.header("Cookie", cookie)
        }
        if (csrfToken != null) {
            request.header(CSRF_HEADER_NAME, csrfToken)
        }

        return client.send(
            request.build(),
            HttpResponse.BodyHandlers.ofString(),
        )
    }

    private fun basicAuthorization(
        username: String,
        password: String,
    ): String {
        val credentials = "$username:$password".toByteArray(StandardCharsets.UTF_8)
        return "Basic ${Base64.getEncoder().encodeToString(credentials)}"
    }

    private fun org.assertj.core.api.AbstractStringAssert<*>.doesNotContainCredentials() =
        doesNotContain(
            OPERATOR_USERNAME,
            OPERATOR_PASSWORD,
            KEYSTORE_PASSWORD,
        )

    private data class HttpsApplication(
        val context: ConfigurableApplicationContext,
        val client: HttpClient,
        val baseUri: URI,
    ) {
        val objectMapper: ObjectMapper = context.getBean(ObjectMapper::class.java)
    }

    companion object {
        private const val OPERATOR_USERNAME = "security-test-operator"
        private const val OPERATOR_PASSWORD = "security-test-basic-password"
        private const val INVALID_PASSWORD = "invalid-password-do-not-log"
        private const val KEYSTORE_PASSWORD = "security-test-keystore-password"
        private const val KEY_ALIAS = "operator"
        private const val CSRF_COOKIE_NAME = "XSRF-TOKEN"
        private const val CSRF_HEADER_NAME = "X-XSRF-TOKEN"
    }
}
