import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.pipeline import Pipeline, PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator

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

# SparkSession
spark = SparkSession.builder\
    .appName("leroy-model")\
        .config("spark.master", "local")\
        .config('spark.jars.packages', spark_jar_package)\
        .getOrCreate() # config('spark.jars', spark_jar_file)
print(f"Spark version = {spark.version}")
print(f"Hadoop version = {spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")


# Load data
dfs = spark\
        .read\
        .format("csv")\
        .option("header", "true")\
        .load(PATH_DATA + "dataset_matched.csv")\
        .selectExpr('product_name', 'cast(target as int)','dataset_type')


# Train and test set
train = dfs.where(F.col('dataset_type')=='train')\
        .select(['product_name','target'])
test = dfs.where(F.col('dataset_type')=='test')\
        .select(['product_name'])
test_check = dfs.where(F.col('dataset_type')=='test')\
        .select(['product_name','target'])


# Fit pipeline
tokenizer = Tokenizer(inputCol='product_name', outputCol='words')
hashingTF = HashingTF(inputCol='words', outputCol='rawFeatures')
idf = IDF(inputCol='rawFeatures', outputCol='features')
lr = LogisticRegression(featuresCol='features', labelCol='target')
pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])
fitted_model = pipeline.fit(train)
fitted_model.write().overwrite().save(PATH_MODEL)


# Predict on the test set demo
model = PipelineModel.load(PATH_MODEL)
predictions = model.transform(test)
predictions.limit(10).show()


# Predict check
predictions_check = model.transform(test_check)
predictions_check\
    .select(['product_name','target','prediction'])\
    .limit(10)\
    .show()
evaluator = BinaryClassificationEvaluator(labelCol='target')
print('Test ROC AUC =', evaluator.evaluate(predictions_check))