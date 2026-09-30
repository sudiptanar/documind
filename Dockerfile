# syntax=docker/dockerfile:1.7
# One Dockerfile for every service:  docker build --build-arg MODULE=auth-service -t documind/auth-service .

# ---- build ----
FROM maven:3.9-eclipse-temurin-17 AS build
ARG MODULE
WORKDIR /src
COPY . .
# The cache mount keeps ~/.m2 between builds, so only changed code is recompiled and nothing is re-downloaded.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl ${MODULE} -am package -DskipTests \
 && JAR=$(ls ${MODULE}/target/*.jar | grep -v original | head -1) \
 && java -Djarmode=tools -jar "$JAR" extract --layers --launcher --destination /extracted

# ---- run ----
FROM eclipse-temurin:17-jre-alpine
RUN addgroup -S app && adduser -S app -G app
USER app
WORKDIR /app
# Layers ordered from least to most frequently changed: a code change only rebuilds the last one.
COPY --from=build /extracted/dependencies/ ./
COPY --from=build /extracted/spring-boot-loader/ ./
COPY --from=build /extracted/snapshot-dependencies/ ./
COPY --from=build /extracted/application/ ./
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
