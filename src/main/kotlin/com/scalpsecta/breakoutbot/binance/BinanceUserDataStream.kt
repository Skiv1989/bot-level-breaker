package com.scalpsecta.breakoutbot.binance

import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Instant

fun interface BinanceUserDataStreamProvider {
    fun connect(listenKey: String): Flux<BinancePrivateStreamMessage>
}

sealed interface BinancePrivateStreamMessage {
    data class Connected(
        val connectedAt: Instant,
    ) : BinancePrivateStreamMessage

    data class Event(
        val event: BinanceUserDataEvent,
    ) : BinancePrivateStreamMessage
}

class LiveBinanceUserDataStreamProvider(
    private val webSocketClient: WebSocketClient,
    private val parser: BinanceUserDataEventParser,
    private val clock: Clock,
) : BinanceUserDataStreamProvider {
    override fun connect(listenKey: String): Flux<BinancePrivateStreamMessage> {
        require(listenKey.isNotBlank()) {
            "listenKey must not be blank"
        }
        return Flux.create(
            { sink -> connect(listenKey, sink) },
            FluxSink.OverflowStrategy.BUFFER,
        )
    }

    private fun connect(
        listenKey: String,
        sink: FluxSink<BinancePrivateStreamMessage>,
    ) {
        val uri = URI.create(
            "$LIVE_PRIVATE_STREAM_BASE_URL/${urlEncodePathSegment(listenKey)}",
        )
        val connection = webSocketClient
            .execute(uri) { session ->
                sink.next(
                    BinancePrivateStreamMessage.Connected(clock.instant()),
                )
                session
                    .receive()
                    .map { message -> message.payloadAsText }
                    .doOnNext { payload ->
                        parser.parse(payload)?.let { event ->
                            sink.next(BinancePrivateStreamMessage.Event(event))
                        }
                    }
                    .then()
            }
            .subscribe(
                {},
                {
                    sink.error(
                        BinanceClientException(
                            "Binance private user-data stream disconnected",
                        ),
                    )
                },
                sink::complete,
            )
        sink.onDispose(connection)
    }
}

private fun urlEncodePathSegment(value: String): String =
    URLEncoder
        .encode(value, StandardCharsets.UTF_8)
        .replace("+", "%20")

private const val LIVE_PRIVATE_STREAM_BASE_URL =
    "wss://fstream.binance.com/private/ws"
