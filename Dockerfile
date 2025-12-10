FROM gradle:8.7-jdk21 AS builder
WORKDIR /app

COPY build.gradle settings.gradle ./
COPY gradle gradle
RUN gradle dependencies --no-daemon || true

COPY . .

RUN gradle clean bootJar --no-daemon

FROM eclipse-temurin:21-jdk

WORKDIR /app

ENV SPRING_PROFILES_ACTIVE=dev

COPY --from=builder /app/build/libs/*.jar app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]