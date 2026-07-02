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
# eclipse-temurin:17-jre-alpine은 arm64 매니페스트가 없어 Apple Silicon 등에서 빌드가 깨진다.
# jammy(Ubuntu 기반)는 amd64/arm64 모두 공식 지원한다.
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

COPY --from=builder /app/build/libs/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
