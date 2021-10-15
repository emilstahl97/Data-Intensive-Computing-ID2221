FROM openjdk:8
COPY ./target /usr/bin/app
WORKDIR /usr/bin/app
ENTRYPOINT java -jar producer-1.0-SNAPSHOT-uber.jar com.id2221.recentchanges.producer.DataTransporter
