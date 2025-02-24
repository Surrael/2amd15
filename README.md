# 2AMD15 Project

## Prerequisites

> Java 11
> Spark
> Maven
> Downloaded `plays.csv` dataset and put it in the root directory of the project.

## Building and running

The project can be built using Maven:

```bash
mvn clean package
```

Using the `run.sh` script, the project can be locally built and run automatically.
Ensure that `JAVA_HOME` and `SPARK_HOME` are set correctly.
Make the script executable via

```bash
chmod +x run.sh
```

The script can then be executed via

```bash
./run.sh
```
