import sys
import os

import win32api
import win32con
import win32job

import subprocess
import altair as alt 
import streamlit as st
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark_home = 'C:/Prog/Spark/spark-3.1.2-bin-hadoop3.2'
os.environ['SPARK_HOME'] = spark_home
os.environ['HADOOP_HOME'] = spark_home
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
spark_jar_package = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2'

# Constants
PATH = os.getcwd()
PATH_SRC = PATH + '/src/'
PATH_DATA = PATH + '/data/'
PATH_MODEL = PATH + '/src/model/'
TOPIC_SRC = 'leroy-db-final'
TOPIC_TGT = 'leroy-index-final'

# launch 
f = open(PATH+"/log_data.txt", "w")
p1 = subprocess.Popen([sys.executable, PATH_SRC + 'get_data.py'], 
                                    stdout=f, 
                                    stderr=subprocess.STDOUT)

fm = open(PATH+"/log_model.txt", "w")
p2 = subprocess.Popen([sys.executable, PATH_SRC + 'calc_index.py'], 
                                    stdout=fm, 
                                    stderr=subprocess.STDOUT)


# associate child processes with the main script - stop on close of main
perms = win32con.PROCESS_TERMINATE | win32con.PROCESS_SET_QUOTA

hJob1 = win32job.CreateJobObject(None, "")
extended_info = win32job.QueryInformationJobObject(hJob1, win32job.JobObjectExtendedLimitInformation)
extended_info['BasicLimitInformation']['LimitFlags'] = win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
win32job.SetInformationJobObject(hJob1, win32job.JobObjectExtendedLimitInformation, extended_info)

# Convert process id to process handle:
hProcess1 = win32api.OpenProcess(perms, False, p1.pid)
win32job.AssignProcessToJobObject(hJob1, hProcess1)

hJob2 = win32job.CreateJobObject(None, "")
extended_info = win32job.QueryInformationJobObject(hJob1, win32job.JobObjectExtendedLimitInformation)

extended_info['BasicLimitInformation']['LimitFlags'] = win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
win32job.SetInformationJobObject(hJob2, win32job.JobObjectExtendedLimitInformation, extended_info)
hProcess2 = win32api.OpenProcess(perms, False, p2.pid)
win32job.AssignProcessToJobObject(hJob2, hProcess2)



# SparkSession
spark = SparkSession.builder\
    .appName("leroy-main")\
        .config("spark.master", "local")\
        .config("spark.local.dir", PATH+'/sparktmp')\
        .config('spark.jars.packages', spark_jar_package)\
        .getOrCreate() 



# Read batch data from kafka
dfs = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", TOPIC_TGT)\
    .option("startingOffsets", "earliest") \
    .load()\
    .select(\
        F.col('key').cast("string").alias('report_dt'),\
        F.col("value").cast("string"))\
    .withColumn("_tmp", F.split("value", "\t"))\
    .select(
        F.col("_tmp").getItem(0).alias("report_dt"),
        F.col("_tmp").getItem(1).alias("category"),
        F.col("_tmp").getItem(2).alias("avg_price"))

#dfs.show()

# Dataframe for reporting
dfs = dfs\
    .join(dfs.select(F.max('report_dt'))).alias('dfs1')\
    .groupBy(['report_dt','category'])\
    .agg(F.max('avg_price').alias('avg_price'))

#.where(F.col('category')=='Paper')
df = dfs\
    .select(F.to_timestamp(F.col('dfs1.report_dt'), "yyyy/MM/dd HH:mm:ss").alias('report_dt'),\
        F.col('category'),
        F.col('avg_price').cast('double'))\
    .toPandas()

df = df.sort_values(by=['report_dt','category'])

df_chart = df.copy()
df_chart.report_dt = df_chart.report_dt.apply(lambda x: x.tz_localize(tz='Europe/Moscow'))
df_pivot = df.pivot_table(index='report_dt', columns='category', values='avg_price')    

c = alt.Chart(df_chart).mark_line(point=True).encode(
  alt.Y('avg_price:Q', axis=alt.Axis()),
  x='report_dt:T',
  color='category:N'
)

# layout
st.title('Индекс цен на обои')
st.write('Источник: Leroy Merlin')
st.altair_chart(c)


df_pivot
