# ── 1단계: 빌드 ─────────────────────────────────────────────────
FROM gradle:8.7-jdk17 AS builder

WORKDIR /app

# 의존성 캐시 레이어 분리 (소스 변경 시 재다운로드 방지)
COPY build.gradle settings.gradle ./
COPY gradle ./gradle
RUN gradle dependencies --no-daemon

COPY src ./src
RUN gradle bootJar --no-daemon

# ── 2단계: 실행 이미지 (경량) ────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY --from=builder /app/build/libs/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
