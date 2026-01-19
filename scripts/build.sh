#!/bin/bash
set -ex

mvn clean package
cd target
zip -r BoostKit-omniruntime-omniStateStore.zip flink-alg-falcon.jar
