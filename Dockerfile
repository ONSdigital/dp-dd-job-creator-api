FROM java:8

WORKDIR /app/

ADD ./target/job-creator-*.jar .

ENTRYPOINT java -jar ./job-creator-*.jar
