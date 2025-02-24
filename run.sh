#!/bin/bash

# Set the environment variables for Spark and Java
export SPARK_HOME="/opt/spark"
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Add Spark and Hadoop binaries to PATH
export PATH=$SPARK_HOME/bin:$PATH
export PATH=$JAVA_HOME/bin:$PATH

echo "Building..."
mvn clean package -q -DskipTests
echo "Finished"

cd "$(dirname "$0")" || return

echo "Running Spark job..."
# Run the Spark job
$SPARK_HOME/bin/spark-submit \
  --class nl.tue.bdm.Main \
  --master local[*] \
  --deploy-mode client \
  target/app.jar
