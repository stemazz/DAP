#USER
SPARK_USER="stefano.mazzucca"

# connection #
Sys.setenv(SPARK_HOME="/home/rstudio/spark/spark-2.1.0-bin-hadoop2.7/")
Sys.setenv(SPARK_HOME_VERSION="2.1.0")
Sys.setenv(SPARK_HDFS_CONFIG_URL="/usr/lib/rstudio-server/hadoop/")
Sys.setenv(SPARK_USER=SPARK_USER)

##########################################################
# da capire come utilizzarlo per connetterci a cassandra #
##########################################################

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))






#Sys.setenv(SPARKR_SUBMIT_ARGS='"--packages" "/jar/spark-cassandra-connector_2.11-2.0.2.jar" "sparkr-shell"' )

# Conf obbligatorie
conf <- list()
#conf$spark.mesos.executor.docker.image <- "mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.7"
#conf$spark.mesos.executor.docker.image <- "master.mesos:5000/mesosphere/spark:1.0.7-2.1.0-hadoop-2.6"
#conf$spark.mesos.executor.docker.image <- "master.mesos:5000/mesosphere/spark:2.1.0-2.2.1-1-hadoop-2.6"
conf$spark.mesos.executor.docker.image = "artifacts.slu.bskyb.com:5003/mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.7"

conf$spark.mesos.executor.home <- "/opt/spark/dist"

# Puoi aggiungere le opzioni aggiuntive ad es.:

conf$spark.executor.cores <-"2"
conf$spark.executor.memory <- "15G"
conf$spark.executor.heartbeat <- "60s"
conf$spark.cores.max <- "10"
conf$spark.driver.maxResultSize <- "40G"
conf$spark.driver.memory <- "6G"
conf$spark.driver.cores <- "2"

conf$spark.mesos.driver.failoverTimeout=5.0

sparkR.session(master='mesos://leader.mesos:5050',sparkConfig = conf,appName = 'Spark_SM')

############################################
# configurations for cassandra connections #
############################################

# conf$spark.cassandra.connection.host <- "node-0-server.cassandra.mesos"
# conf$spark.cassandra.connection.port <- "9042"
# conf$spark.cassandra.input.consistency.level <- "TWO"
