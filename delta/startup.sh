#!/bin/bash

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab --ip=0.0.0.0'
export DELTA_SPARK_VERSION='4.1.0'
export UNITYCATALOG_VERSION='0.4.0'
export DELTA_PACKAGE_VERSION=delta-spark_2.13:${DELTA_SPARK_VERSION}
export UNITYCATALOG_PACKAGE_VERSION=unitycatalog-spark_2.13:${UNITYCATALOG_VERSION}

echo "SparkSession:initalizing: cores:${PYSPARK_TOTAL_CORES}, memory:${PYSPARK_DRIVER_MEMORY}"

$SPARK_HOME/bin/pyspark \
  --packages io.delta:${DELTA_PACKAGE_VERSION},io.unitycatalog:${UNITYCATALOG_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.4.1 \
  --driver-memory ${PYSPARK_DRIVER_MEMORY} \
  --driver-cores ${PYSPARK_TOTAL_CORES} \
  --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp" \
  --conf "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=io.unitycatalog.spark.UCSingleCatalog" \
  --conf "spark.sql.catalog.unity=io.unitycatalog.spark.UCSingleCatalog" \
  --conf "spark.sql.catalog.unity.uri=http://unitycatalog:8080/" \
  --conf "spark.sql.catalog.unity.token=" \
  --conf "spark.sql.defaultCatalog=unity"
