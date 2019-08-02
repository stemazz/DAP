import os
import sys
import datetime
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Window, WindowSpec
from pyspark.sql.functions import *
import pyspark.sql.functions as func

spark_framework_user = "stefano.mazzucca"


os.environ["SPARK_HOME"] = "/usr/local/spark-2.1.0-bin-hadoop2.7/"
os.environ["SPARK_HOME_VERSION"] = "2.1.0"
os.environ["HADOOP_CONF_DIR"] = "/opt/hadoop/"
os.environ["SPARK_USER"] = spark_framework_user
#os.environ["PACKAGES"] = "com.databricks:spark-cassandra-connector_2.11-2.0.2.jar"
#os.environ["PACKAGES"] = "com.databricks:spark-csv_2.10:1.5.0"
#os.environ["PACKAGES"] = "com.databricks:spark-avro_2.11:3.2.0"
#os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages " + os.environ["PACKAGES"] + " pyspark-shell"

## impostare le configurazioni
config = SparkConf()\
        .setAppName(spark_framework_user+".Jupyter")\
        .setMaster("mesos://leader.mesos:5050")
  
 
config.set("spark.mesos.executor.docker.image", "artifacts.slu.bskyb.com:5003/mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.7")
config.set("spark.mesos.executor.home", "/opt/spark/dist")
config.set("spark.executor.cores", "2")
config.set("spark.executor.memory", "15G")
config.set("spark.executor.heartbeat", "60s")
config.set("spark.cores.max", "10")
config.set("spark.driver.maxResultSize", "40G")
config.set("spark.driver.memory", "6G")
config.set("spark.driver.cores", "2")
#config.set("spark.cassandra.connection.host", "node-0-server.cassandra.mesos")
#config.set("spark.cassandra.connection.port", "9042")
#config.set("spark.cassandra.input.consistency.level","TWO")

config.set("spark.mesos.driver.failoverTimeout","5.0")
config.set("spark.ui.port", "22001")
# spark.ui.port = 22001

# inizializzare lo spark context
sc = SparkContext(conf = config)

# inizializzare 
spark = SparkSession(sc)