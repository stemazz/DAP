
# Calcolo ext_id su APP SC e sito WSC

source("connection_R.R")
options(scipen = 10000)



#### APP FAI DA TE

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")
# seleziona campi
skyappwsc_1 <- select(skyappwsc,
                      "external_id_post_evar",
                      "site_section",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "post_pagename",
                      "date_time",
                      "visit_num", 
                      "post_visid_concatenated")

skyappwsc_2 <- filter(skyappwsc_1,"external_id_post_evar is not NULL")

skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_dt", cast(skyappwsc_2$date_time, "date"))
skyappwsc_4 <- withColumn(skyappwsc_3, "date_time_ts", cast(skyappwsc_3$date_time, "timestamp"))
# filtro temporale
skyappwsc_5 <- filter(skyappwsc_4,"date_time_dt >= '2017-07-01'")
skyappwsc_6 <- filter(skyappwsc_5,"date_time_dt <= '2017-09-30'")


write.parquet(skyappwsc_6, "/user/stefano.mazzucca/count_extid_app_sc_2017.parquet")



#### Sito WSC

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
# seleziona campi
skyitdev <- select(skyitdev_df, "external_id_post_evar", 
                       "post_channel",
                       "post_pagename",
                       "post_visid_high",
                       "post_visid_low",
                       "post_visid_concatenated",
                       "date_time",
                       "page_url_post_evar","page_url_post_prop",
                       "secondo_livello_post_prop",
                       "terzo_livello_post_prop",
                       "visit_num",
                       "hit_source",
                       "exclude_hit",
                       "external_id_con_NL_post_evar",
                       "post_campaign",
                       "va_closer_id",
                       "canale_corporate_post_prop")
# filtro temporale
skyitdev <- withColumn(skyitdev, "date_time_dt", cast(skyitdev$date_time, "date"))
skyitdev <- withColumn(skyitdev, "date_time_ts", cast(skyitdev$date_time, "timestamp"))

skyitdev <- filter(skyitdev, "date_time_dt >= '2017-07-01'")
skyitdev <- filter(skyitdev, "date_time_dt <= '2017-09-30'")

navigazioni <- filter(skyitdev, "post_channel = 'corporate' or post_channel = 'Guidatv'")


write.parquet(navigazioni, "/user/stefano.mazzucca/count_extid_sito_wsc_2017.parquet", mode = "overwrite")



## Elaborazioni APP ##

app <- read.parquet("/user/stefano.mazzucca/count_extid_app_sc.parquet")
# "/user/stefano.mazzucca/count_extid_app_sc_2017.parquet
View(head(app,100))
nrow(app)
# 51.162.756  <- 2018
# 30.072.046  <- 2017

# ## verifrica sull'ext_id APP
# valdist_utenti_appwsc <- distinct(select(app, "external_id_post_evar"))
# nrow(valdist_utenti_appwsc)
# # 952.484
# createOrReplaceTempView(valdist_utenti_appwsc, "valdist_utenti_appwsc")
# ver_valdist_utenti_appwsc <-sql("select *
#                                 from valdist_utenti_appwsc
#                                 where length(external_id_post_evar) == 48")
# nrow(ver_valdist_utenti_appwsc)
# # 952.480 (4 in meno)


appwsc_lug <- filter(app, "date_time_dt >= '2018-07-01' and date_time_dt <= '2018-07-31'")
extid_lug_appwsc <- distinct(select(appwsc_lug, "external_id_post_evar"))
nrow(extid_lug_appwsc)
# 431.838   <- 2018
# 376.375   <- 2017

appwsc_ago <- filter(app, "date_time_dt >= '2018-08-01' and date_time_dt <= '2018-08-31'")
extid_ago_appwsc <- distinct(select(appwsc_ago, "external_id_post_evar"))
nrow(extid_ago_appwsc)
# 651.640   <- 2018
# 416.587   <- 2017

appwsc_set <- filter(app, "date_time_dt >= '2018-09-01' and date_time_dt <= '2018-09-30'")
extid_set_appwsc <- distinct(select(appwsc_set, "external_id_post_evar"))
nrow(extid_set_appwsc)
# 568.634   <- 2018
# 438.655   <- 2017



## Elaborazioni SITO ##

sito <- read.parquet("/user/stefano.mazzucca/count_extid_sito_wsc.parquet")
# "/user/stefano.mazzucca/count_extid_sito_wsc_2017.parquet"
View(head(sito,100))
nrow(sito)
# 142.229.820   <- 2018
# 84.575.879    <- 2017

sito_wsc <- filter(sito, "post_channel = 'corporate' and terzo_livello_post_prop LIKE '%faidate%'")

# ## verifrica sull'ext_id APP
# valdist_utenti_sito_wsc <- distinct(select(sito_wsc, "external_id_post_evar"))
# nrow(valdist_utenti_sito_wsc)
# # 952.484   # 1.499.132
# createOrReplaceTempView(valdist_utenti_sito_wsc, "valdist_utenti_sito_wsc")
# ver_valdist_utenti_sito_wsc <-sql("select *
#                                 from valdist_utenti_sito_wsc
#                                 where length(external_id_post_evar) == 48")
# nrow(ver_valdist_utenti_sito_wsc)
# # 1.499.129 (3 in meno)

extid_sito_wsc <- distinct(select(sito_wsc, "external_id_post_evar"))
nrow(extid_sito_wsc)
# 1.499.138   <- 2018
# 1.081.241   <- 2017
cookie_sito_wsc <- distinct(select(sito_wsc, "post_visid_concatenated"))
nrow(cookie_sito_wsc)
# 2.630.827   <- 2018
# <- 2017


sito_wsc_lug <- filter(sito_wsc, "date_time_dt >= '2018-07-01' and date_time_dt <= '2018-07-31'")
extid_lug_sito_wsc <- distinct(select(sito_wsc_lug, "external_id_post_evar"))
nrow(extid_lug_sito_wsc)
# 542.623   # 542.622   <- 2018
# 469.519   <- 2017
createOrReplaceTempView(sito_wsc_lug,"sito_wsc_lug")
skywsc_lug <- sql("SELECT date_time_dt, 
                          COUNT(DISTINCT external_id_post_evar)
                  FROM sito_wsc_lug
                  WHERE (external_id_post_evar IS NOT NULL)
                        AND (secondo_livello_post_prop LIKE 'fai da te')
                  GROUP BY 1
                  ")
View(head(skywsc_lug,100))
# 
cookie_lug_sito_wsc <- distinct(select(sito_wsc_lug, "post_visid_concatenated"))
nrow(cookie_lug_sito_wsc)
# 713.275   <- 2018
# <- 2017

sito_wsc_ago <- filter(sito_wsc, "date_time_dt >= '2018-08-01' and date_time_dt <= '2018-08-31'")
extid_ago_sito_wsc <- distinct(select(sito_wsc_ago, "external_id_post_evar"))
nrow(extid_ago_sito_wsc)
# 938.560   # 938.558   <- 2018
# 508.008   <- 2017
createOrReplaceTempView(sito_wsc_ago,"sito_wsc_ago")
skywsc_ago <- sql("SELECT date_time_dt, 
                  COUNT(DISTINCT external_id_post_evar)
                  FROM sito_wsc_ago
                  WHERE (external_id_post_evar IS NOT NULL)
                  AND (secondo_livello_post_prop LIKE 'fai da te')
                  GROUP BY 1
                  ")
View(head(skywsc_ago,100))
# 
cookie_ago_sito_wsc <- distinct(select(sito_wsc_ago, "post_visid_concatenated"))
nrow(cookie_ago_sito_wsc)
# 1.360.487   <- 2018
# <- 2017

sito_wsc_set <- filter(sito_wsc, "date_time_dt >= '2018-09-01' and date_time_dt <= '2018-09-30'")
extid_set_sito_wsc <- distinct(select(sito_wsc_set, "external_id_post_evar"))
nrow(extid_set_sito_wsc)
# 743.307   # 743.301   <- 2018
# 552.100   <- 2017
createOrReplaceTempView(sito_wsc_set,"sito_wsc_set")
skywsc_set <- sql("SELECT date_time_dt, 
                  COUNT(DISTINCT external_id_post_evar)
                  FROM sito_wsc_set
                  WHERE (external_id_post_evar IS NOT NULL)
                  AND (secondo_livello_post_prop LIKE 'fai da te')
                  GROUP BY 1
                  ")
skywsc_set <- arrange(skywsc_set, asc(skywsc_set$date_time_dt))
View(head(skywsc_set,100))
# 
cookie_set_sito_wsc <- distinct(select(sito_wsc_set, "post_visid_concatenated"))
nrow(cookie_set_sito_wsc)
# 1.006.163   <- 2018
# <- 2017



## Elaborazioni APP or SITO ##

extid_tot_lug <- rbind(extid_lug_appwsc, extid_lug_sito_wsc)
extid_tot_lug <- distinct(select(extid_tot_lug, "external_id_post_evar"))
nrow(extid_tot_lug)
# 841.315   <- 2018
# 732.821   <- 2017

extid_tot_ago <- rbind(extid_ago_appwsc, extid_ago_sito_wsc)
extid_tot_ago <- distinct(select(extid_tot_ago, "external_id_post_evar"))
nrow(extid_tot_ago)
# 1.306.791   <- 2018
# 797.660     <- 2017

extid_tot_set <- rbind(extid_set_appwsc, extid_set_sito_wsc)
extid_tot_set <- distinct(select(extid_tot_set, "external_id_post_evar"))
nrow(extid_tot_set)
# 1.125.806   <- 2018
# 852.148     <- 2017




#chiudi sessione
sparkR.stop()
