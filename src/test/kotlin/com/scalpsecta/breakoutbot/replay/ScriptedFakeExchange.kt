package com.scalpsecta.breakoutbot.replay

import com.scalpsecta.breakoutbot.binance.BinanceAccountReconciliation
import com.scalpsecta.breakoutbot.binance.BinanceExecutionClient
import com.scalpsecta.breakoutbot.binance.BinanceOrderAcknowledgement
import com.scalpsecta.breakoutbot.binance.BinanceOrderReconciliation
import com.scalpsecta.breakoutbot.binance.BinanceOrderRequest
import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataStreamProvider
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.util.ArrayDeque
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

sealed interface ScriptedExchangeResult<out T> {
    data class Success<T>(val value: T) : ScriptedExchangeResult<T>

    data class Rejection(val reason: String) : ScriptedExchangeResult<Nothing>

    data object Timeout : ScriptedExchangeResult<Nothing>
}

data class RecordedCancellation(
    val symbol: String,
    val clientOrderId: String,
)

data class StreamAvailability(
    val publicSymbols: Map<String, Boolean>,
    val privateStreamConnected: Boolean,
)

/**
 * Strict, in-memory Binance boundary for replay tests. Every REST response is
 * dequeued from a script; an unexpected request fails instead of reaching a
 * live transport.
 */
class ScriptedFakeExchange :
    BinanceExecutionClient,
    PublicMarketDataStreamProvider,
    AutoCloseable {
    private val lock = ReentrantLock()
    private val placementResults =
        ArrayDeque<ScriptedExchangeResult<BinanceOrderAcknowledgement>>()
    private val cancellationResults = ArrayDeque<ScriptedExchangeResult<Unit>>()
    private val orderReconciliations =
        ArrayDeque<ScriptedExchangeResult<BinanceOrderReconciliation>>()
    private val accountReconciliations =
        ArrayDeque<ScriptedExchangeResult<BinanceAccountReconciliation>>()
    private val tradeSinks = mutableMapOf<String, Sinks.Many<AggregateTradeEvent>>()
    private val bookSinks = mutableMapOf<String, Sinks.Many<BookTickerEvent>>()
    private val publicAvailability = mutableMapOf<String, Boolean>()
    private val privateSink = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer<BinanceUserDataEvent>()
    private var privateConnected = true
    private var closed = false

    val placements: MutableList<BinanceOrderRequest> = CopyOnWriteArrayList()
    val cancellations: MutableList<RecordedCancellation> = CopyOnWriteArrayList()
    val orderReconciliationRequests: MutableList<Pair<String, String>> =
        CopyOnWriteArrayList()
    var accountReconciliationRequests: Int = 0
        private set

    fun scriptPlacement(
        result: ScriptedExchangeResult<BinanceOrderAcknowledgement>,
    ) = enqueue(placementResults, result)

    fun scriptCancellation(result: ScriptedExchangeResult<Unit>) =
        enqueue(cancellationResults, result)

    fun scriptOrderReconciliation(
        result: ScriptedExchangeResult<BinanceOrderReconciliation>,
    ) = enqueue(orderReconciliations, result)

    fun scriptAccountReconciliation(
        result: ScriptedExchangeResult<BinanceAccountReconciliation>,
    ) = enqueue(accountReconciliations, result)

    fun privateEvents(): Flux<BinanceUserDataEvent> = privateSink.asFlux()

    fun emit(event: AggregateTradeEvent) {
        val sink = lock.withLock {
            requireOpen()
            check(publicAvailability.getOrDefault(event.symbol, true)) {
                "Public stream for ${event.symbol} is disconnected"
            }
            tradeSinks.getOrPut(event.symbol, ::replaySink)
        }
        check(sink.tryEmitNext(event).isSuccess) {
            "Could not emit aggregate trade ${event.aggregateTradeId}"
        }
    }

    fun emit(event: BookTickerEvent) {
        val sink = lock.withLock {
            requireOpen()
            check(publicAvailability.getOrDefault(event.symbol, true)) {
                "Public stream for ${event.symbol} is disconnected"
            }
            bookSinks.getOrPut(event.symbol, ::replaySink)
        }
        check(sink.tryEmitNext(event).isSuccess) {
            "Could not emit book ticker ${event.updateId}"
        }
    }

    fun emit(event: BinanceUserDataEvent) {
        lock.withLock {
            requireOpen()
            check(privateConnected) { "Private stream is disconnected" }
        }
        check(privateSink.tryEmitNext(event).isSuccess) {
            "Could not emit private event at ${event.eventTime}"
        }
    }

    fun setPublicStreamConnected(symbol: String, connected: Boolean) {
        lock.withLock {
            requireOpen()
            publicAvailability[symbol.trim().uppercase()] = connected
        }
    }

    fun setPrivateStreamConnected(connected: Boolean) {
        lock.withLock {
            requireOpen()
            privateConnected = connected
        }
    }

    fun streamAvailability(): StreamAvailability = lock.withLock {
        StreamAvailability(
            publicSymbols = publicAvailability.toSortedMap(),
            privateStreamConnected = privateConnected,
        )
    }

    fun assertExhausted() {
        lock.withLock {
            check(placementResults.isEmpty()) {
                "Unused placement scripts: ${placementResults.size}"
            }
            check(cancellationResults.isEmpty()) {
                "Unused cancellation scripts: ${cancellationResults.size}"
            }
            check(orderReconciliations.isEmpty()) {
                "Unused order reconciliation scripts: ${orderReconciliations.size}"
            }
            check(accountReconciliations.isEmpty()) {
                "Unused account reconciliation scripts: ${accountReconciliations.size}"
            }
        }
    }

    override fun aggregateTrades(symbol: String): Flux<AggregateTradeEvent> =
        lock.withLock {
            requireOpen()
            tradeSinks.getOrPut(symbol.trim().uppercase(), ::replaySink).asFlux()
        }

    override fun bookTickers(symbol: String): Flux<BookTickerEvent> =
        lock.withLock {
            requireOpen()
            bookSinks.getOrPut(symbol.trim().uppercase(), ::replaySink).asFlux()
        }

    override fun placeOrder(
        request: BinanceOrderRequest,
    ): Mono<BinanceOrderAcknowledgement> = Mono.defer {
        placements += request
        scripted(placementResults, "order placement")
    }

    override fun cancelOrder(
        symbol: String,
        clientOrderId: String,
    ): Mono<Void> = Mono.defer {
        cancellations += RecordedCancellation(symbol, clientOrderId)
        scripted(cancellationResults, "order cancellation").then()
    }

    override fun reconcileOrder(
        symbol: String,
        clientOrderId: String,
    ): Mono<BinanceOrderReconciliation> = Mono.defer {
        orderReconciliationRequests += symbol to clientOrderId
        scripted(orderReconciliations, "order reconciliation")
    }

    override fun reconcileAccount(): Mono<BinanceAccountReconciliation> = Mono.defer {
        lock.withLock { accountReconciliationRequests += 1 }
        scripted(accountReconciliations, "account reconciliation")
    }

    override fun close() {
        val sinks = lock.withLock {
            if (closed) {
                return
            }
            closed = true
            (tradeSinks.values + bookSinks.values).also {
                tradeSinks.clear()
                bookSinks.clear()
            }
        }
        sinks.forEach { sink -> sink.tryEmitComplete() }
        privateSink.tryEmitComplete()
    }

    private fun <T : Any> enqueue(
        queue: ArrayDeque<ScriptedExchangeResult<T>>,
        result: ScriptedExchangeResult<T>,
    ) {
        lock.withLock {
            requireOpen()
            queue.addLast(result)
        }
    }

    private fun <T : Any> scripted(
        queue: ArrayDeque<ScriptedExchangeResult<T>>,
        operation: String,
    ): Mono<T> {
        val result = lock.withLock {
            requireOpen()
            check(queue.isNotEmpty()) {
                "Unexpected fake exchange $operation; no script remains"
            }
            queue.removeFirst()
        }
        return when (result) {
            is ScriptedExchangeResult.Success -> Mono.just(result.value)
            is ScriptedExchangeResult.Rejection -> Mono.error(
                FakeExchangeRejectionException(operation, result.reason),
            )

            ScriptedExchangeResult.Timeout -> Mono.never()
        }
    }

    private fun requireOpen() {
        check(!closed) { "Fake exchange is closed" }
    }
}

class FakeExchangeRejectionException(
    operation: String,
    reason: String,
) : RuntimeException("Fake exchange rejected $operation: $reason")

private fun <T> replaySink(): Sinks.Many<T> = Sinks
    .many()
    .multicast()
    .onBackpressureBuffer()
