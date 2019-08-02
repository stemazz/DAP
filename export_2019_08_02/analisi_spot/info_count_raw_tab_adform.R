
## Info conteggio righe (per Silvia) tabelle IMPRESSION E TRACKING POINT

#apri sessione
source("connection_R.R")
options(scipen = 1000)


impression <- read.parquet('/STAGE/adform/table=Impression/Impression.parquet')

impression_2 <- withColumn(impression, "data_nav", cast(impression$yyyymmdd, 'string'))
impression_3 <- withColumn(impression_2, "data_nav_dt", cast(cast(unix_timestamp(impression_2$data_nav, 'yyyyMMdd'), 'timestamp'), 'date'))
impression_4 <- filter(impression_3, "data_nav_dt >= '2018-07-01' and data_nav_dt <= '2018-07-31'")


write.parquet(impression_4, "/user/stefano.mazzucca/info_count_raw_impression_tab.parquet")




trackingpoint <- read.parquet("/STAGE/adform/table=Trackingpoint/Trackingpoint.parquet")

trackingpoint_2 <- withColumn(trackingpoint, "data", cast(trackingpoint$yyyymmdd, 'string'))
trackingpoint_3 <- withColumn(trackingpoint_2, "data_dt", cast(cast(unix_timestamp(trackingpoint_2$data, 'yyyyMMdd'), 'timestamp'), 'date'))
trackingpoint_4 <- filter(trackingpoint_3, "data_dt >= '2018-07-01' and data_dt <= '2018-07-31'")


write.parquet(trackingpoint_4, "/user/stefano.mazzucca/info_count_raw_tp_tab.parquet")




impression_tab <- read.parquet("/user/stefano.mazzucca/info_count_raw_impression_tab.parquet")
View(head(impression_tab,100))
nrow(impression_tab)
# 559.217.865

tp_tab <- read.parquet("/user/stefano.mazzucca/info_count_raw_tp_tab.parquet")
View(head(tp_tab,100))
nrow(tp_tab)
# 305.220.655


#chiudi sessione
sparkR.stop()
