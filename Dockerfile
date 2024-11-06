# BUILD 
FROM maven:3.9.9-ibm-semeru-23-jammy AS build

WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests


# RUNTIME
FROM openjdk:24-slim-bullseye

WORKDIR /app
COPY --from=build /app/target/Ingestor-1.0.jar /app/Ingestor-1.0.jar

ENV INGESTOR_DB_URL="jdbc:postgresql://localhost:5432/inventory?charSet=UTF-8"
ENV INGESTOR_DB_USER="ingestor"
ENV INGESTOR_DB_AUTOCOMMIT=false
ENV INGESTOR_DB_CSV_INPUT="input.csv"

ENTRYPOINT ["java", "-jar", "Ingestor-1.0.jar"]

