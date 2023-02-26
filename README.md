# pyspark-scala-Connector

cd pyspark-scala-Connector/

## Create Python Environment
1. sh makePython.sh

## Build Scala Class
2. sbt package 

## Run Shell Script 
cd scripts/
sh main.sh $jar_file_path $spark_submit_path $inputPath $outputPath
