package com.scalpsecta.breakoutbot.risk

import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.util.concurrent.CompletableFuture
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A linearizable global mailbox. The handler is the sole mutation boundary for
 * attempt and reservation state and is never invoked concurrently.
 */
internal class OrderedGlobalRiskQueue<E : Any>(
    private val scheduler: Scheduler,
    private val handler: (E) -> Any,
) : AutoCloseable {
    private val lock = ReentrantLock()
    private val pending = ArrayDeque<GlobalRiskEnvelope<E>>()
    private var draining = false
    private var closed = false

    fun submit(event: E): Mono<Any> {
        val completion = CompletableFuture<Any>()
        val scheduleDrain = lock.withLock {
            check(!closed) { "Global risk event queue is closed" }
            pending.addLast(GlobalRiskEnvelope(event, completion))
            if (draining) {
                false
            } else {
                draining = true
                true
            }
        }
        if (scheduleDrain) {
            scheduler.schedule(::drain)
        }
        return Mono.fromFuture(completion, true)
    }

    override fun close() {
        val abandoned = lock.withLock {
            if (closed) {
                return
            }
            closed = true
            pending.toList().also { pending.clear() }
        }
        val error = IllegalStateException("Global risk event queue is closed")
        abandoned.forEach { envelope ->
            envelope.completion.completeExceptionally(error)
        }
    }

    private fun drain() {
        while (true) {
            val envelope = lock.withLock {
                if (pending.isEmpty()) {
                    draining = false
                    null
                } else {
                    pending.removeFirst()
                }
            } ?: return

            try {
                envelope.completion.complete(handler(envelope.event))
            } catch (error: Exception) {
                envelope.completion.completeExceptionally(error)
            }
        }
    }
}

private data class GlobalRiskEnvelope<E : Any>(
    val event: E,
    val completion: CompletableFuture<Any>,
)

