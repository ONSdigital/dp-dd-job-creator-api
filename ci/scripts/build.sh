#!/bin/bash -eux

pushd dp-dd-job-creator-api
  mvn clean package -DskipTests=true
popd

cp -r dp-dd-job-creator-api/target/* target/
