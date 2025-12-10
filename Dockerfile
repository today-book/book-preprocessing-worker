# ============================
# 1. Build Stage (Gradle)
# ============================
FROM gradle:8.7-jdk21 AS builder
WORKDIR /app

# 필요한 파일만 복사 (캐시 최적화)
COPY build.gradle settings.gradle ./
COPY gradle gradle
RUN gradle dependencies --no-daemon || true

# 전체 프로젝트 복사
COPY . .

# JAR 빌드
RUN gradle clean bootJar --no-daemon

# ============================
# 2. Runtime Stage (Distroless)
# ============================
FROM gcr.io/distroless/java21

WORKDIR /app

# 빌드된 JAR 복사
COPY --from=builder /app/build/libs/*.jar app.jar

# Java 실행
ENTRYPOINT ["java", "-jar", "/app/app.jar"]