import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.pipeline import PipelineModel

spark_home = 'C:/Prog/Spark/spark-3.1.2-bin-hadoop3.2'
os.environ['SPARK_HOME'] = spark_home
os.environ['HADOOP_HOME'] = spark_home
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
spark_jar_package = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2'

# Constants
PATH = os.getcwd()
PATH_DATA = PATH + '/data/'
PATH_MODEL = PATH + '/src/model/'
TOPIC_SRC = 'leroy-db-final'
TOPIC_TGT = 'leroy-index-final'

# SparkSession
spark = SparkSession.builder\
    .appName("leroy")\
        .config("spark.master", "local")\
        .config("spark.local.dir", PATH+'/sparktmp')\
        .config('spark.jars.packages', spark_jar_package)\
        .getOrCreate() 
print(f"Spark version = {spark.version}")
print(f"Hadoop version = {spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")


# Read streaming data from kafka
dfs = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", TOPIC_SRC)\
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()\
    .select(\
        F.col('key'),\
        F.json_tuple(F.col("value").cast("string"),\
             "product", "pricem2")\
                 .alias("product_name", "pricem2")
             )


# Match category
model = PipelineModel.load(PATH_MODEL)
predictions = model.transform(dfs)


# Calculate average price for each category
result = predictions\
            .groupBy('prediction')\
            .agg(F.max('key').alias('insert_dt'), F.mean('pricem2'))

calc_time = datetime.now().strftime('%Y/%m/%d %H:%M:%S')

# Save to kafka
query =  result \
  .withColumn("category", F.when(F.col('prediction') > 0, F.lit("Paper"))
                            .otherwise(F.lit("Non-paper")))\
  .withColumn("key", \
      F.date_format(F.current_timestamp(),"yyyy/MM/dd HH:mm:ss").cast("string"))\
  .select(
        F.col('key'), \
        F.concat(F.col('insert_dt'), F.lit("\t"), F.col("category"), \
            F.lit("\t"), "avg(pricem2)")\
            .alias("value")
      )\
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers",  "localhost:9092") \
  .option("topic", TOPIC_TGT) \
  .outputMode('complete')\
  .option("checkpointLocation", PATH_DATA)\
  .start()

query.awaitTermination()