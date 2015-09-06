FROM java:8
MAINTAINER Ivan Ramirez - ivan.ramirez@gmail.com

RUN mkdir /opt/bitacora

ONBUILD ADD target/bitacora-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/bitacora/app.jar
WORKDIR /opt/bitacora

ENV API_PORT 8080
ENV AVRO_PORT 8081

EXPOSE 8080
EXPOSE 8081
EXPOSE 8082

ENTRYPOINT ["java", "-Dcom.sun.management.jmxremote", "-Dcom.sun.management.jmxremote.port=8082", "-Dcom.sun.management.jmxremote.local.only=false", "-Dcom.sun.management.jmxremote.authenticate=false", "-Dcom.sun.management.jmxremote.rmi.port=8082", "-Dcom.sun.management.jmxremote.ssl=false", "-jar", "app.jar"]