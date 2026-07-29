# 01 — Bootstrap a safe live-only application

**What to build:** A Kotlin/JVM 17 Spring Boot WebFlux application that starts as an empty, non-trading bot, loads the two read-only local dependency JARs without importing their source builds, and provides a testable boundary between runtime orchestration and Binance.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§2.2, 22, 23, 30.7, 33.

## Acceptance criteria

- [ ] The Gradle wrapper builds and tests the bot with JVM 17 and loads `liner-dto-1.0.0.jar` and `liner-spring-boot-starter-1.1.0.jar` from configurable local paths.
- [ ] The application records a fresh start timestamp and reports no levels or recovered attempts without introducing placeholder state holders.
- [ ] Exchange access is behind a replaceable boundary, and automated tests use a fake implementation that cannot send live Binance orders.
- [ ] Startup and graceful-shutdown tests prove that no position/open-order discovery, order cancellation, order placement, exposure close, or account-wide mode change occurs.
- [ ] Verification confirms that `liner-starter` and `liner-dto` remain unchanged.
