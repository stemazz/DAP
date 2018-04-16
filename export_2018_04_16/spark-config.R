#rm(list=ls())

Sys.setenv(SPARK_HOME="/home/rstudio/spark/spark-2.1.0-bin-hadoop2.7/")
Sys.setenv(SPARK_HOME_VERSION="2.1.0")
Sys.setenv(SPARK_HDFS_CONFIG_URL="/usr/lib/rstudio-server/hadoop/")
Sys.setenv(SPARK_USER="stefano.mazzucca")
#Sys.setenv(HADOOP_USER_NAME='hdfs')
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.stop()

# Conf obbligatorie
conf <- list()
conf$spark.mesos.executor.docker.image <- "mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.7"
conf$spark.mesos.executor.home <- "/opt/spark/dist"

# Puoi aggiungere le opzioni aggiuntive ad es.:

conf$spark.executor.cores <-"2"
conf$spark.executor.memory <- "15G"
conf$spark.executor.heartbeat <- "60s"
conf$spark.cores.max <- "4"
conf$spark.driver.maxResultSize <- "40G" 

sparkR.session(master='mesos://leader.mesos:5050',sparkConfig = conf,appName = 'Spark_SM')

# "/"

# "/user/stefano.mazzucca/"

