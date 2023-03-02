package jsl.tasks

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._

class SimpleClass(sqlContext: SQLContext, inputDir: String) {
  def exe(): DataFrame = {
    val spark = SparkSession.builder.getOrCreate()
    import sqlContext.implicits._
    import spark.implicits._

    val dataType = inputDir.split("/").last
    val jsonFileName = if (dataType == "val") { "val_v1.0.json" } else { "train_v1.0.json"}
    val jsonFile = inputDir + jsonFileName

    val df = spark.read.option("inferSchema", "true").json(jsonFile).drop("dataset_name","dataset_split","dataset_version")

    val dfData = df.withColumn("data",explode(col("data")))

    val dfExtract = dfData.withColumn("questionID",col("data").getItem("questionId"))
    .withColumn("question",col("data").getItem("question"))
    .withColumn("image",col("data").getItem("image"))
    .withColumn("docId",col("data").getItem("docId"))
    .withColumn("ucsf_document_id",col("data").getItem("ucsf_document_id"))
    .withColumn("ucsf_document_page_no",col("data").getItem("ucsf_document_page_no"))
    .withColumn("answers",col("data").getItem("answers"))
    .withColumn("data_split",col("data").getItem("data_split")).drop("data","data_split","ucsf_document_page_no","ucsf_document_id","docId","questionID")

    val dfCollectAllByImage = dfExtract.groupBy("image").agg(collect_list("question").alias("questions"),collect_list("answers").alias("answers"))

    val dfAddModificationTime = dfCollectAllByImage.withColumn("modificationTime",current_timestamp())

    val dfAddDocumentPath = dfAddModificationTime.withColumn("path",concat(lit(inputDir),col("image"))).drop("image")

    val dfAddLength = dfAddDocumentPath.withColumn("length",size(col("questions")))

    val contentCheck = when(col("length") >= 1, "1").otherwise("0")

    val dfAddContent = dfAddLength.withColumn("content",contentCheck)

    val returnDf = dfAddContent.select("path","modificationTime","length","content","questions","answers").repartition(120)

    returnDf
  }
}
