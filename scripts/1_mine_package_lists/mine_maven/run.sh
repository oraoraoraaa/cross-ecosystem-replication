#!/bin/bash
# Run script for Maven Central Miner (Java version)

set -e

JAR_FILE="target/maven-miner-1.0.0-jar-with-dependencies.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found: $JAR_FILE"
    echo "Please build the project first:"
    echo "  ./build.sh"
    exit 1
fi

echo "Running Maven Central Miner..."
echo

# Run with increased memory (8GB heap, optimized GC)
java -Xmx8G -Xms2G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar "$JAR_FILE"
