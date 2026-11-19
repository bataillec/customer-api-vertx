# One image for the three verticles: the container's command picks which one runs.
FROM maven:3-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
# dependency:go-offline misses the Scala compiler bridge, so the build stays online and the
# local repository is kept in a cache mount instead
# The integration tests need Docker for Testcontainers, which a build stage doesn't have: run mvn verify in CI instead
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /build/target/customer-api-vertx-*-SNAPSHOT.jar app.jar
# Hazelcast can't find its peers on 127.0.0.1 in containers: the members are the compose service names
COPY docker/cluster.xml docker/entrypoint.sh ./
RUN chmod +x entrypoint.sh && useradd --system --no-create-home app
USER app
EXPOSE 8080
ENTRYPOINT ["./entrypoint.sh"]
# Overridden per service: HttpServerVerticle, CustomerVerticle or AuditVerticle
CMD ["me.bataillec.portfolio.customer.HttpServerVerticle"]
