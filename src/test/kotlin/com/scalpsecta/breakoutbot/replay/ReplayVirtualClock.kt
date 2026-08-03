package com.scalpsecta.breakoutbot.replay

import reactor.test.scheduler.VirtualTimeScheduler
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit

/**
 * One time source for replayed production code and Reactor deadlines.
 * Advancing the scheduler also advances every injected [Clock] read.
 */
class ReplayVirtualClock private constructor(
    private val origin: Instant,
    private val scheduler: VirtualTimeScheduler,
    private val replayZone: ZoneId,
) : Clock(), AutoCloseable {
    constructor(
        origin: Instant,
        scheduler: VirtualTimeScheduler = VirtualTimeScheduler.create(),
    ) : this(origin, scheduler, ZoneOffset.UTC)

    fun scheduler(): VirtualTimeScheduler = scheduler

    override fun getZone(): ZoneId = replayZone

    override fun withZone(zone: ZoneId): Clock =
        ReplayVirtualClock(origin, scheduler, zone)

    override fun instant(): Instant = origin.plusNanos(
        scheduler.now(TimeUnit.NANOSECONDS),
    )

    fun advanceTo(timestamp: Instant) {
        require(!timestamp.isBefore(instant())) {
            "Replay time cannot move backwards from ${instant()} to $timestamp"
        }
        scheduler.advanceTimeBy(Duration.between(instant(), timestamp))
    }

    fun advanceBy(duration: Duration) {
        require(!duration.isNegative) { "Replay duration must not be negative" }
        scheduler.advanceTimeBy(duration)
    }

    override fun close() {
        scheduler.dispose()
    }
}
