# One Dockerfile for all services. Pick with: --build-arg MODULE=api|scheduler|worker
FROM maven:3.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /src
COPY pom.xml .
COPY common/pom.xml common/
COPY api/pom.xml api/
COPY scheduler/pom.xml scheduler/
COPY worker/pom.xml worker/
RUN mvn -q -B dependency:go-offline -pl ${MODULE} -am || true
COPY . .
RUN mvn -q -B package -pl ${MODULE} -am -DskipTests \
 && cp ${MODULE}/target/${MODULE}-*.jar /app.jar

FROM eclipse-temurin:21-jre
COPY --from=build /app.jar /app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app.jar"]
