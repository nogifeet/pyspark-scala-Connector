from pyspark import StorageLevel, SparkFiles
from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys 

class DocVQA:

    @staticmethod
    def readDataset(spark,path):
        sc = spark._sc
        jvm = sc._jvm
        sqlContext = SQLContext(sc)
        ssqlContext = sqlContext._ssql_ctx
        simpleObject = jvm.jsl.tasks.SimpleClass(ssqlContext,path)
        inputDf = DataFrame(simpleObject.exe(), ssqlContext)
        return inputDf 

if __name__ == "__main__":

    inputPath = sys.argv[1] #HDFS 
    targetPath = sys.argv[2] #HDFS
    
    spark = SparkSession.builder.appName("jsl-submission").getOrCreate()
    df = DocVQA().readDataset(spark,inputPath)

    df.show()

    df.printSchema()

    questions = df.select(explode("questions"))

    questions.count()

    df.rdd.getNumPartitions()

    df.write.format("orc").option("headers","true").save(targetPath)
