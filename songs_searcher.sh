#!/bin/sh

set -o nounset
set -o errexit

work_dir="$(cd "$(dirname -- "$0")" ; pwd)"

$SPARK_HOME/bin/spark-submit \
--master 'local[*]' \
--driver-java-options "-Dlog4j.configuration=file://$work_dir/src/test/resources/log4j.xml" \
--driver-memory 2G \
--class com.github.tashoyan.tfidf.Main \
"$work_dir/target/scala-2.11/spark-sql-tfidf-assembly-0.1-SNAPSHOT.jar" songs_db
