#!/bin/bash
set -ex

mvn clean package
cd target
zip -r BoostKit-omniruntime-omniStateStore-1.2.0.zip flink-alg-falcon.jar
