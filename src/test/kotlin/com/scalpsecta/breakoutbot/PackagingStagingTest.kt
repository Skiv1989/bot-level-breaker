package com.scalpsecta.breakoutbot

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import kotlin.io.path.createDirectories
import kotlin.io.path.createFile
import kotlin.io.path.isRegularFile
import kotlin.io.path.writeText

class PackagingStagingTest {
    @TempDir
    lateinit var tempDirectory: Path

    @Test
    fun `staging copies three immutable source snapshots and excludes sensitive data`() {
        assumeTrue(System.getProperty("os.name").startsWith("Windows"))
        val powershell = Path.of(
            System.getenv("WINDIR"),
            "System32",
            "WindowsPowerShell",
            "v1.0",
            "powershell.exe",
        )
        assumeTrue(powershell.isRegularFile())

        val sourceRoot = tempDirectory.resolve("sources")
        val botSource = sourceRoot.resolve("bot")
        val starterSource = sourceRoot.resolve("liner-starter")
        val dtoSource = sourceRoot.resolve("liner-dto")
        createBotFixture(botSource)
        createDependencyFixture(starterSource, "Starter.kt")
        createDependencyFixture(dtoSource, "Dto.kt")
        val before = listOf(botSource, starterSource, dtoSource)
            .associateWith(::snapshot)
        val context = tempDirectory.resolve("context")
        val script = Path.of(System.getProperty("user.dir"))
            .resolve("scripts")
            .resolve("package-image.ps1")
            .toAbsolutePath()

        val process = ProcessBuilder(
            powershell.toString(),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            script.toString(),
            "-BotSource",
            botSource.toString(),
            "-LinerStarterSource",
            starterSource.toString(),
            "-LinerDtoSource",
            dtoSource.toString(),
            "-ContextDirectory",
            context.toString(),
            "-StageOnly",
        )
            .redirectErrorStream(true)
            .start()
        val output = process.inputStream.bufferedReader().use { it.readText() }

        assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue()
        assertThat(process.exitValue())
            .describedAs("packaging output: %s", output)
            .isZero()
        assertThat(context.resolve("Dockerfile")).isRegularFile()
        assertThat(context.resolve(".dockerignore")).isRegularFile()
        assertThat(context.resolve("bot/src/main/kotlin/Keep.kt")).isRegularFile()
        assertThat(
            context.resolve(
                "bot/src/main/kotlin/com/example/evidence/Recorder.kt",
            ),
        ).isRegularFile()
        assertThat(context.resolve("liner-starter/src/main/kotlin/Starter.kt"))
            .isRegularFile()
        assertThat(context.resolve("liner-dto/src/main/kotlin/Dto.kt"))
            .isRegularFile()

        val stagedPaths = Files.walk(context).use { paths ->
            paths.iterator().asSequence()
                .map { path ->
                    context.relativize(path).toString().replace('\\', '/')
                }
                .toList()
        }
        assertThat(stagedPaths).noneMatch { path ->
                path.contains("/.git/") ||
                path.contains("/.idea/") ||
                path.contains("/.gradle-user-home/") ||
                path.contains("/.ssh/") ||
                path.contains("/build/") ||
                path.contains("/tls/") ||
                path.contains("/audit/") ||
                path.contains("/event-data/") ||
                path.endsWith("runtime.env") ||
                path.endsWith("secret.pem")
        }
        before.forEach { (source, sourceSnapshot) ->
            assertThat(snapshot(source)).isEqualTo(sourceSnapshot)
        }
    }

    private fun createBotFixture(root: Path) {
        write(root.resolve("Dockerfile"), "FROM scratch\n")
        write(root.resolve(".dockerignore"), "**/.git\n")
        write(root.resolve("src/main/kotlin/Keep.kt"), "class Keep\n")
        write(
            root.resolve("src/main/kotlin/com/example/evidence/Recorder.kt"),
            "class Recorder\n",
        )
        write(root.resolve(".git/config"), "private repository metadata\n")
        write(root.resolve(".idea/workspace.xml"), "private IDE metadata\n")
        write(
            root.resolve(".gradle-user-home/caches/private.bin"),
            "local Gradle cache\n",
        )
        write(root.resolve("build/output.jar"), "build output\n")
        write(root.resolve("tls/server.p12"), "private key material\n")
        write(root.resolve("audit/audit.jsonl"), "audit record\n")
        write(root.resolve("event-data/attempt.jsonl.gz"), "event record\n")
        write(root.resolve("runtime.env"), "BINANCE_API_SECRET=secret\n")
        write(root.resolve("secret.pem"), "private key\n")
        write(root.resolve(".ssh/id_ed25519"), "private SSH key\n")
    }

    private fun createDependencyFixture(root: Path, sourceName: String) {
        write(root.resolve("src/main/kotlin/$sourceName"), "class Fixture\n")
        write(root.resolve(".git/config"), "private repository metadata\n")
        write(root.resolve("build/output.jar"), "build output\n")
    }

    private fun write(path: Path, content: String) {
        path.parent.createDirectories()
        path.createFile()
        path.writeText(content)
    }

    private fun snapshot(root: Path): Map<String, String> =
        Files.walk(root).use { paths ->
            paths.iterator().asSequence()
                .filter(Files::isRegularFile)
                .associate { file ->
                    root.relativize(file).toString() to sha256(file)
                }
        }

    private fun sha256(path: Path): String {
        val digest = MessageDigest.getInstance("SHA-256")
        Files.newInputStream(path).use { input ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val count = input.read(buffer)
                if (count < 0) {
                    break
                }
                digest.update(buffer, 0, count)
            }
        }
        return digest.digest().joinToString("") { byte -> "%02x".format(byte) }
    }
}
