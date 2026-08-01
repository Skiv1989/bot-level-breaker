package com.scalpsecta.breakoutbot.level

import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.util.concurrent.CompletableFuture
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A linearizable, symbol-local mailbox. Producers only append immutable events;
 * the supplied handler is the sole mutation boundary and is never invoked
 * concurrently for this queue.
 */
internal class OrderedSymbolEventQueue<E : Any>(
    private val symbol: String,
    private val scheduler: Scheduler,
    private val handler: (E) -> Any,
) : AutoCloseable {
    private val lock = ReentrantLock()
    private val pending = ArrayDeque<Envelope<E>>()
    private var draining = false
    private var closed = false

    fun submit(
        event: E,
        afterProcessed: () -> Unit = {},
    ): Mono<Any> {
        val completion = CompletableFuture<Any>()
        enqueue(Envelope(event, completion, afterProcessed))
        return Mono.fromFuture(completion, true)
    }

    fun publish(event: E) {
        enqueue(Envelope(event, null, {}))
    }

    override fun close() {
        val abandoned = lock.withLock {
            if (closed) {
                return
            }
            closed = true
            pending.toList().also { pending.clear() }
        }
        val error = IllegalStateException("Event queue for $symbol is closed")
        abandoned.forEach { envelope ->
            envelope.completion?.completeExceptionally(error)
            envelope.afterProcessed()
        }
    }

    private fun enqueue(envelope: Envelope<E>) {
        val scheduleDrain = lock.withLock {
            check(!closed) { "Event queue for $symbol is closed" }
            pending.addLast(envelope)
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

            val result = try {
                handler(envelope.event)
            } catch (error: Exception) {
                try {
                    envelope.afterProcessed()
                } catch (cleanupError: Exception) {
                    error.addSuppressed(cleanupError)
                }
                envelope.completion?.completeExceptionally(error)
                continue
            }
            try {
                envelope.afterProcessed()
            } catch (error: Exception) {
                envelope.completion?.completeExceptionally(error)
                continue
            }
            envelope.completion?.complete(result)
        }
    }
}

private data class Envelope<E : Any>(
    val event: E,
    val completion: CompletableFuture<Any>?,
    val afterProcessed: () -> Unit,
)
