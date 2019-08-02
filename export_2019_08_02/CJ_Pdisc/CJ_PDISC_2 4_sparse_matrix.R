
## CJ_PDISC_2 
## sparse_matrix + group_by


#USER
SPARK_USER="stefano.mazzucca"

# connection #
Sys.setenv(SPARK_HOME="/home/rstudio/spark/spark-2.1.0-bin-hadoop2.7/")
Sys.setenv(SPARK_HOME_VERSION="2.1.0")
Sys.setenv(SPARK_HDFS_CONFIG_URL="/usr/lib/rstudio-server/hadoop/")
Sys.setenv(SPARK_USER=SPARK_USER)

# Conf obbligatorie
conf <- list()
conf$spark.mesos.executor.docker.image <- "mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.7"
conf$spark.mesos.executor.home <- "/opt/spark/dist"

# Puoi aggiungere le opzioni aggiuntive ad es.:

conf$spark.executor.cores <-"2"
conf$spark.executor.memory <- "15G"
conf$spark.executor.heartbeat <- "60s"
conf$spark.cores.max <- "10"
conf$spark.driver.maxResultSize <- "40G"
conf$spark.driver.memory <- "6G"
conf$spark.driver.cores <- "2"

library(sparklyr)
sc <- spark_connect(master = 'mesos://leader.mesos:5050', config = conf, app_name = 'sparklyr_SM')

#library("dplyr", lib.loc="/usr/local/lib/R/site-library")
#library("dplyr", lib.loc="~/R/x86_64-pc-linux-gnu-library/3.3")
library(dplyr)
library(ggplot2)



## create sparse matrix ########################################################################################################################################################

nav_sky_tot <- spark_read_parquet(sc, "nav_sky_tot", "/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_completo_20180406.parquet")
# nav_sky_tot_2 <- filter(nav_sky_tot, "sequenza NOT LIKE '%skyatlantic_guidatv%'")
View(head(nav_sky_tot,100))
nrow(nav_sky_tot)
# 2.162.983

# columns <- c("sequenza")
# 
# for (column in columns){
#   df <- ml_create_dummy_variables(nav_sky_tot, column)
# }

df <- ml_create_dummy_variables(nav_sky_tot, "sequenza")
View(head(df,100))

df_sparse <- select(df, -c(sequenza))
#df_sparse <- select(df, -one_of(c("sequenza")))
View(head(df_sparse,100))
nrow(df_sparse)
# 2.162.983






## group by extenral_id #########################################################################################################################################################

df_1 <- nav_sky_tot %>% group_by(COD_CLIENTE_CIFRATO) %>% summarise(n = n())

View(head(df_1))
df_1 <- summarize(group_by(df, "COD_CLIENTE_CIFRATO"), n = sum(df$visit_num))

##???????

##
# 
# sqlfunction <- function(sc, block) {
#   spark_session(sc) %>% invoke("sql", block)
# }
# 
# tmp <- sdf_register(df_sparse,"df_sparse2")
# 
# tmp2 <- sqlfunction(sc, "SELECT * from df_sparse2 LIMIT 10")
# collect(tmp2)
# str(tmp2)
# View(head(tmp2,100))
# 
# 
# tmp3 <- sqlfunction(sc, "SELECT COD_CLIENTE_CIFRATO , count(*) from df_sparse2 group by COD_CLIENTE_CIFRATO")
# collect(tmp3)
# str(tmp3)
# View(head(tmp3,100))
# 
# 
###

tmp <- sdf_register(df,"df")

library(DBI)

df_1 <- dbGetQuery(sc, 'SELECT COD_CLIENTE_CIFRATO, sum(visit_num) as n
                        FROM df
                        GROUP BY COD_CLIENTE_CIFRATO')
df_1 %>% collect()

View(head(df_1,1000))
nrow(df_1)
# 22.435









spark_disconnect(sc)
