package com.scalpsecta.breakoutbot.evidence

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import mu.KotlinLogging
import java.io.BufferedOutputStream
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPOutputStream

internal class EvidenceFileWriter(
    directory: Path,
    objectMapper: ObjectMapper,
) {
    val directory: Path = directory.toAbsolutePath().normalize()
    val auditPath: Path = this.directory.resolve(AUDIT_FILE_NAME)

    private val logger = KotlinLogging.logger {}
    private val objectMapper = objectMapper
        .copy()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    private val closing = AtomicBoolean()
    private val lastFailure = AtomicReference<Throwable?>()
    private val executor: ExecutorService = Executors.newSingleThreadExecutor { task ->
        Thread(task, "evidence-writer").apply { isDaemon = true }
    }
    private val auditWriter: BufferedWriter
    private val attemptWriters = mutableMapOf<UUID, BufferedWriter>()

    init {
        Files.createDirectories(this.directory)
        auditWriter = Files.newBufferedWriter(
            auditPath,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND,
            StandardOpenOption.WRITE,
        )
    }

    fun healthy(): Boolean = lastFailure.get() == null

    fun lastError(): String? = lastFailure.get()?.let { error ->
        "${error.javaClass.simpleName}: ${error.message ?: "evidence write failed"}"
    }

    fun attemptFileName(
        levelId: UUID,
        symbol: String,
        startedAt: Instant,
    ): String =
        "attempt-${startedAt.toEpochMilli()}-${safeFilePart(symbol)}-$levelId.jsonl.gz"

    fun appendAudit(record: AuditRecord) {
        val json = try {
            objectMapper.writeValueAsString(record)
        } catch (error: Exception) {
            recordFailure(error)
            return
        }
        submit {
            writeJsonLine(auditWriter, json)
            auditWriter.flush()
        }
    }

    fun startAttempt(
        levelId: UUID,
        symbol: String,
        startedAt: Instant,
        initialEvents: List<AttemptEvidenceEvent>,
    ) {
        val fileName = attemptFileName(levelId, symbol, startedAt)
        submit {
            check(levelId !in attemptWriters) {
                "Attempt evidence writer already exists for $levelId"
            }
            val output = Files.newOutputStream(
                directory.resolve(fileName),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
            )
            val writer = BufferedWriter(
                OutputStreamWriter(
                    GZIPOutputStream(BufferedOutputStream(output)),
                    StandardCharsets.UTF_8,
                ),
            )
            attemptWriters[levelId] = writer
            initialEvents.forEach { event -> writeJsonLine(writer, event) }
            writer.flush()
        }
    }

    fun appendAttempt(levelId: UUID, event: AttemptEvidenceEvent) {
        submit {
            val writer = attemptWriters[levelId]
                ?: error("Attempt evidence writer does not exist for $levelId")
            writeJsonLine(writer, event)
            writer.flush()
        }
    }

    fun finishAttempt(levelId: UUID) {
        submit {
            attemptWriters.remove(levelId)?.close()
        }
    }

    fun flush(timeout: Duration): Boolean {
        if (closing.get()) {
            return false
        }
        val barrier = executor.submit {
            auditWriter.flush()
            attemptWriters.values.forEach(BufferedWriter::flush)
        }
        return await(barrier, timeout)
    }

    fun close(timeout: Duration): Boolean {
        if (!closing.compareAndSet(false, true)) {
            return true
        }
        val closeTask = executor.submit {
            attemptWriters.values.forEach(BufferedWriter::close)
            attemptWriters.clear()
            auditWriter.close()
        }
        val completed = await(closeTask, timeout)
        if (completed) {
            executor.shutdown()
        } else {
            closeTask.cancel(true)
            executor.shutdownNow()
            logger.warn { "Evidence writer did not flush within $timeout" }
        }
        return completed
    }

    private fun submit(action: () -> Unit) {
        if (closing.get()) {
            return
        }
        executor.submit {
            try {
                action()
            } catch (error: Exception) {
                recordFailure(error)
            }
        }
    }

    private fun await(
        future: java.util.concurrent.Future<*>,
        timeout: Duration,
    ): Boolean =
        try {
            future.get(timeout.toMillis().coerceAtLeast(1), TimeUnit.MILLISECONDS)
            true
        } catch (error: InterruptedException) {
            Thread.currentThread().interrupt()
            lastFailure.compareAndSet(null, error)
            false
        } catch (error: Exception) {
            lastFailure.compareAndSet(null, error)
            false
        }

    private fun writeJsonLine(writer: BufferedWriter, value: Any) {
        writer.write(objectMapper.writeValueAsString(value))
        writer.newLine()
    }

    private fun writeJsonLine(writer: BufferedWriter, json: String) {
        writer.write(json)
        writer.newLine()
    }

    private fun recordFailure(error: Exception) {
        lastFailure.compareAndSet(null, error)
        logger.error(error) { "Evidence write failed" }
    }

    private fun safeFilePart(value: String): String =
        value.lowercase().replace(UNSAFE_FILE_PART, "-")
}

private const val AUDIT_FILE_NAME = "audit.jsonl"
private val UNSAFE_FILE_PART = Regex("[^a-z0-9_-]")
