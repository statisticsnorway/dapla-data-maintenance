#
# Build Application image
#
FROM openjdk:15-slim

# CVE-2019-25013
RUN apt-get update && apt-get install -y \
    libc6 \
    && rm -rf /var/lib/apt/lists/*

#
# Resources from build image
#
COPY target/libs /app/lib/
COPY target/dapla-data-maintenance*.jar /app/lib/
COPY target/classes/logback.xml /app/conf/
COPY target/classes/logback-bip.xml /app/conf/
COPY target/classes/application.yaml /app/conf/

WORKDIR /app

CMD ["java", "--enable-preview", "-cp", "/app/lib/*", "no.ssb.dapla.datamaintenance.DataMaintenanceApplication"]

EXPOSE 10200