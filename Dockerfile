# =========================
# 1️⃣ Build Stage
# =========================
FROM gradle:8.7-jdk21 AS builder
WORKDIR /app

COPY build.gradle settings.gradle ./
COPY gradle gradle
RUN gradle dependencies --no-daemon || true

COPY . .
RUN gradle clean bootJar --no-daemon


# =========================
# 2️⃣ Runtime Stage
# =========================
FROM eclipse-temurin:21-jdk
WORKDIR /app

# --- 타임존 설정 (OS + JVM 모두 KST) ---
RUN apt-get update && \
    apt-get install -y tzdata && \
    ln -snf /usr/share/zoneinfo/Asia/Seoul /etc/localtime && \
    echo "Asia/Seoul" > /etc/timezone && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Seoul
ENV JAVA_TOOL_OPTIONS="-Duser.timezone=Asia/Seoul"

# --- Spring Profile 기본값 (docker run -e 로 override 가능) ---
ENV SPRING_PROFILES_ACTIVE=dev

# --- JAR 복사 ---
COPY --from=builder /app/build/libs/*.jar app.jar

# --- 문서용 포트 명시 ---
EXPOSE 9002

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
