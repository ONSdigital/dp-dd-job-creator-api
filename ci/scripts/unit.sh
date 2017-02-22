#!/bin/bash -eux

pushd dp-dd-job-creator-api
  mvn clean surefire:test
popd
