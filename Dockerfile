#
# Build Application image
#
FROM openjdk:15-slim

#
# Resources from build image
#
COPY target/libs /app/lib/
COPY target/dapla-data-maintenance*.jar /app/lib/
COPY target/classes/logback.xml /app/conf/
COPY target/classes/logback-bip.xml /app/conf/
COPY target/classes/application.yaml /app/conf/

WORKDIR /app

CMD ["java", "-cp", "/app/lib/*", "no.ssb.dapla.datamaintenance.DataMaintenanceApplication"]

EXPOSE 10200
