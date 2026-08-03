# syntax=docker/dockerfile:1.7

FROM eclipse-temurin:17-jdk-jammy AS builder

WORKDIR /workspace
ENV GRADLE_USER_HOME=/workspace/.gradle

COPY liner-dto/ /workspace/liner-dto/
COPY liner-starter/ /workspace/liner-starter/
COPY bot/ /workspace/bot/

RUN chmod +x \
        /workspace/liner-dto/gradlew \
        /workspace/liner-starter/gradlew \
        /workspace/bot/gradlew

RUN --mount=type=cache,target=/workspace/.gradle \
    /workspace/liner-dto/gradlew \
        -p /workspace/liner-dto \
        --no-daemon \
        -Dmaven.repo.local=/workspace/local-maven \
        clean publishToMavenLocal

RUN --mount=type=cache,target=/workspace/.gradle \
    /workspace/liner-starter/gradlew \
        -p /workspace/liner-starter \
        --no-daemon \
        -Dmaven.repo.local=/workspace/local-maven \
        clean jar

RUN --mount=type=cache,target=/workspace/.gradle \
    /workspace/bot/gradlew \
        -p /workspace/bot \
        --no-daemon \
        -PlinerDtoJar=/workspace/liner-dto/build/libs/liner-dto-1.0.0.jar \
        -PlinerStarterJar=/workspace/liner-starter/build/libs/liner-spring-boot-starter-1.1.0.jar \
        clean bootJar

RUN mkdir -p /workspace/liveness-probe \
    && javac \
        --release 17 \
        -d /workspace/liveness-probe \
        /workspace/bot/deployment/LivenessProbe.java \
    && jar \
        --create \
        --file /workspace/liveness-probe.jar \
        -C /workspace/liveness-probe .

FROM eclipse-temurin:17-jre-jammy AS runtime

RUN groupadd --gid 10001 breakout-bot \
    && useradd \
        --uid 10001 \
        --gid 10001 \
        --no-create-home \
        --shell /usr/sbin/nologin \
        breakout-bot \
    && mkdir -p /app /var/lib/breakout-bot/audit \
    && chown -R 10001:10001 /app /var/lib/breakout-bot

WORKDIR /app
COPY --from=builder /workspace/bot/build/libs/application.jar /app/application.jar
COPY --from=builder /workspace/liveness-probe.jar /app/liveness-probe.jar

ENV AUDIT_DIRECTORY=/var/lib/breakout-bot/audit

USER 10001:10001
EXPOSE 443

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["java", "-cp", "/app/liveness-probe.jar", "LivenessProbe"]

ENTRYPOINT ["java", "-jar", "/app/application.jar"]
