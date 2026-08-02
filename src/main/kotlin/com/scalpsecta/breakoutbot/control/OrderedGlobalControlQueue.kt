package com.scalpsecta.breakoutbot.control

import reactor.core.publisher.Mono
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/** Serializes global operator commands without holding a lock across I/O. */
internal class OrderedGlobalControlQueue {
    private val lock = ReentrantLock()
    private var tail: Mono<Void> = Mono.empty()

    fun <T : Any> submit(action: () -> Mono<T>): Mono<T> = lock.withLock {
        val predecessor = tail
        lateinit var successor: Mono<Void>
        val result = predecessor
            .onErrorResume { Mono.empty() }
            .then(Mono.defer(action))
            .doFinally {
                lock.withLock {
                    if (tail === successor) {
                        tail = Mono.empty()
                    }
                }
            }
            .cache()
        successor = result.then().onErrorResume { Mono.empty() }
        tail = successor
        result
    }
}
