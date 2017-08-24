#!/bin/bash

# Convenience script to run test spark-api using Apache Spark local mode

fatjar="target/scala-2.11/spark-api-assembly-0.0.1.jar"
build_command="./sbt assembly"

hash java >/dev/null 2>&1 || { echo "Please install Java" >&2; exit 1; }

if [[ ! -f "${fatjar}" ]]; then
    echo "${fatjar} not found. Running build ${build_command}"
    $build_command
fi

exec java -jar "${fatjar}" $@