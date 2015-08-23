FROM java:8
MAINTAINER Ivan Ramirez - ivan.ramirez@gmail.com

RUN mkdir /opt/bitacora

ONBUILD ADD target/bitacora-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/bitacora/app.jar
WORKDIR /opt/bitacora

ENV API_PORT 8080
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]