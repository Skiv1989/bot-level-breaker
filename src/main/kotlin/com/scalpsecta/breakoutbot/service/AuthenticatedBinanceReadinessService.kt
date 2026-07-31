package com.scalpsecta.breakoutbot.service

import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceClient
import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceSnapshot
import com.scalpsecta.breakoutbot.binance.BinanceAccountReadinessSnapshot
import com.scalpsecta.breakoutbot.binance.BinanceAccountSummary
import com.scalpsecta.breakoutbot.binance.BinanceAssetMode
import com.scalpsecta.breakoutbot.binance.BinanceClockMeasurement
import com.scalpsecta.breakoutbot.binance.BinanceClockSnapshot
import com.scalpsecta.breakoutbot.binance.BinanceExchangeInfo
import com.scalpsecta.breakoutbot.binance.BinancePositionMode
import com.scalpsecta.breakoutbot.binance.BinancePrivateStreamConnectionState
import com.scalpsecta.breakoutbot.binance.BinancePrivateStreamMessage
import com.scalpsecta.breakoutbot.binance.BinancePrivateStreamSnapshot
import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.binance.BinanceUserDataStreamProvider
import com.scalpsecta.breakoutbot.binance.authenticatedBinanceNotReady
import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.Disposables
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Clock
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.abs

@Service
class AuthenticatedBinanceReadinessService(
    private val client: AuthenticatedBinanceClient,
    private val streamProvider: BinanceUserDataStreamProvider,
    private val clock: Clock,
    @param:Value("\${bot.binance.startup-enabled:true}")
    private val startupEnabled: Boolean,
) {
    private val started = AtomicBoolean()
    private val closed = AtomicBoolean()
    private val clockMonitorStarted = AtomicBoolean()
    private val state = AtomicReference(authenticatedBinanceNotReady())
    private val subscriptions = Disposables.composite()
    private val privateStreamSubscription = AtomicReference<Disposable?>()
    private val keepAliveSubscription = AtomicReference<Disposable?>()
    private val privateStreamRestartScheduled = AtomicBoolean()
    private val eventSink = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer<BinanceUserDataEvent>(EVENT_BUFFER_SIZE, false)

    fun snapshot(): AuthenticatedBinanceSnapshot = state.get()

    fun events(): Flux<BinanceUserDataEvent> = eventSink.asFlux()

    @EventListener(ApplicationReadyEvent::class)
    fun onApplicationReady() {
        start()
    }

    fun start() {
        if (!startupEnabled || closed.get() || !started.compareAndSet(false, true)) {
            return
        }

        val startup = client
            .synchronizeClock()
            .doOnNext { measurement ->
                recordClockMeasurement(measurement)
                startClockMonitoring()
            }
            .flatMap {
                Mono.zip(
                    client.accountSummary(),
                    client.positionMode(),
                    client.assetMode(),
                    client.exchangeInfo(),
                )
            }
            .doOnNext { readiness ->
                recordAccountReadiness(
                    account = readiness.t1,
                    positionMode = readiness.t2,
                    assetMode = readiness.t3,
                    exchangeInfo = readiness.t4,
                )
            }
            .flatMap { client.startUserDataStream() }
            .subscribe(
                ::connectPrivateStream,
                { markPrivateStreamDisconnected() },
            )
        subscriptions.add(startup)
    }

    @PreDestroy
    fun close() {
        if (!closed.compareAndSet(false, true)) {
            return
        }
        subscriptions.dispose()
        eventSink.tryEmitComplete()
    }

    private fun recordClockMeasurement(measurement: BinanceClockMeasurement) {
        val readiness = if (
            abs(measurement.serverOffsetMillis) <= MAX_HEALTHY_CLOCK_OFFSET_MILLIS &&
            measurement.roundTripMillis <= MAX_HEALTHY_CLOCK_ROUND_TRIP_MILLIS
        ) {
            BinanceReadiness.READY
        } else {
            BinanceReadiness.NOT_READY
        }
        state.updateAndGet { current ->
            current.copy(
                clock = BinanceClockSnapshot(
                    readiness = readiness,
                    checkedAt = measurement.checkedAt,
                    serverOffsetMillis = measurement.serverOffsetMillis,
                    roundTripMillis = measurement.roundTripMillis,
                ),
            )
        }
    }

    private fun startClockMonitoring() {
        if (!clockMonitorStarted.compareAndSet(false, true)) {
            return
        }
        val monitor = Flux
            .interval(CLOCK_SYNC_INTERVAL, CLOCK_SYNC_INTERVAL)
            .concatMap {
                client
                    .synchronizeClock()
                    .doOnNext(::recordClockMeasurement)
                    .onErrorResume {
                        markClockNotReady()
                        Mono.empty()
                    }
            }
            .subscribe()
        subscriptions.add(monitor)
    }

    private fun markClockNotReady() {
        state.updateAndGet { current ->
            current.copy(
                clock = BinanceClockSnapshot(
                    readiness = BinanceReadiness.NOT_READY,
                    checkedAt = clock.instant(),
                    serverOffsetMillis = null,
                    roundTripMillis = null,
                ),
            )
        }
    }

    private fun recordAccountReadiness(
        account: BinanceAccountSummary,
        positionMode: BinancePositionMode,
        assetMode: BinanceAssetMode,
        exchangeInfo: BinanceExchangeInfo,
    ) {
        val readiness = if (
            account.canTrade &&
            positionMode == BinancePositionMode.ONE_WAY &&
            assetMode == BinanceAssetMode.SINGLE_ASSET
        ) {
            BinanceReadiness.READY
        } else {
            BinanceReadiness.NOT_READY
        }
        state.updateAndGet { current ->
            current.copy(
                account = BinanceAccountReadinessSnapshot(
                    readiness = readiness,
                    checkedAt = clock.instant(),
                    canTrade = account.canTrade,
                    positionMode = positionMode,
                    assetMode = assetMode,
                    loadedSymbolCount = exchangeInfo.symbols.size,
                ),
                currentEquity = account.totalMarginBalance,
                temporaryDailyAnchorEquity =
                    current.temporaryDailyAnchorEquity
                        ?: account.totalMarginBalance,
            )
        }
    }

    private fun connectPrivateStream(listenKey: String) {
        if (closed.get()) {
            return
        }
        privateStreamRestartScheduled.set(false)
        markPrivateStreamConnecting()
        val stream = streamProvider
            .connect(listenKey)
            .doOnSubscribe { markPrivateStreamConnecting() }
            .doOnNext(::handlePrivateStreamMessage)
            .subscribe(
                {},
                { schedulePrivateStreamRestart() },
                ::schedulePrivateStreamRestart,
            )
        privateStreamSubscription.getAndSet(stream)?.dispose()
        subscriptions.add(stream)
        val keepAlive = keepListenKeyAlive(listenKey)
        keepAliveSubscription.getAndSet(keepAlive)?.dispose()
        subscriptions.add(keepAlive)
    }

    private fun keepListenKeyAlive(listenKey: String): Disposable =
        Flux
            .interval(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_INTERVAL)
            .concatMap { client.keepAliveUserDataStream(listenKey) }
            .subscribe(
                {},
                { schedulePrivateStreamRestart() },
            )

    private fun handlePrivateStreamMessage(message: BinancePrivateStreamMessage) {
        when (message) {
            is BinancePrivateStreamMessage.Connected ->
                state.updateAndGet { current ->
                    current.copy(
                        privateStream = BinancePrivateStreamSnapshot(
                            readiness = BinanceReadiness.READY,
                            connectionState =
                                BinancePrivateStreamConnectionState.CONNECTED,
                            connectedAt = message.connectedAt,
                            lastEventAt = current.privateStream.lastEventAt,
                        ),
                    )
                }

            is BinancePrivateStreamMessage.Event -> {
                if (message.event is BinanceUserDataEvent.ListenKeyExpired) {
                    schedulePrivateStreamRestart()
                } else {
                    state.updateAndGet { current ->
                        current.copy(
                            privateStream = current.privateStream.copy(
                                lastEventAt = message.event.receivedAt,
                            ),
                        )
                    }
                }
                eventSink.tryEmitNext(message.event)
            }
        }
    }

    private fun markPrivateStreamConnecting() {
        state.updateAndGet { current ->
            current.copy(
                privateStream = current.privateStream.copy(
                    readiness = BinanceReadiness.NOT_READY,
                    connectionState =
                        BinancePrivateStreamConnectionState.CONNECTING,
                ),
            )
        }
    }

    private fun markPrivateStreamDisconnected() {
        state.updateAndGet { current ->
            current.copy(
                privateStream = current.privateStream.copy(
                    readiness = BinanceReadiness.NOT_READY,
                    connectionState =
                        BinancePrivateStreamConnectionState.DISCONNECTED,
                ),
            )
        }
    }

    private fun schedulePrivateStreamRestart() {
        if (
            closed.get() ||
            !privateStreamRestartScheduled.compareAndSet(false, true)
        ) {
            return
        }
        markPrivateStreamDisconnected()
        privateStreamSubscription.getAndSet(null)?.dispose()
        keepAliveSubscription.getAndSet(null)?.dispose()

        val restart = Mono
            .delay(RECONNECT_DELAY)
            .then(client.startUserDataStream())
            .subscribe(
                ::connectPrivateStream,
                {
                    privateStreamRestartScheduled.set(false)
                    schedulePrivateStreamRestart()
                },
            )
        subscriptions.add(restart)
    }
}

private const val EVENT_BUFFER_SIZE = 1_024
private const val MAX_HEALTHY_CLOCK_OFFSET_MILLIS = 1_000L
private const val MAX_HEALTHY_CLOCK_ROUND_TRIP_MILLIS = 1_000L
private val CLOCK_SYNC_INTERVAL: Duration = Duration.ofMinutes(1)
private val KEEP_ALIVE_INTERVAL: Duration = Duration.ofMinutes(30)
private val RECONNECT_DELAY: Duration = Duration.ofSeconds(1)
