FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=builder /app/target/producer-1.0-SNAPSHOT-jar-with-dependencies.jar /app/producer.jar
ENTRYPOINT ["java", "-jar", "/app/producer.jar"]