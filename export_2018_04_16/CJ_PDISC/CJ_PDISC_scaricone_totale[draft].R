
# CJ_PDISC scaricone totale


#apri sessione
source("connection_R.R")
options(scipen = 1000)


#################################################################################################################################################################################
#################################################################################################################################################################################
## Base PDISC
#################################################################################################################################################################################
#################################################################################################################################################################################

base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
nrow(base_pdisc)
# 37.976

valdist_base <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_base)
# 37.753


base_pdisc_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc.parquet")
nrow(base_pdisc_coo)
# 161.766
ver_clienti <- distinct(select(base_pdisc_coo, "COD_CLIENTE_CIFRATO"))
nrow(ver_clienti)
# 24.340 clienti pdisc on line (su device)


createOrReplaceTempView(base_pdisc, "base_pdisc")
createOrReplaceTempView(base_pdisc_coo, "base_pdisc_coo")

base_pdisc_coo_2 <- sql("select t1.*, t2.post_visid_concatenated
                        from base_pdisc t1
                        left join base_pdisc_coo t2
                          on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
nrow(base_pdisc_coo_2)
# 176.511



#################################################################################################################################################################################
#################################################################################################################################################################################
#### Scarico pag sezione pubblica del sito
#################################################################################################################################################################################
#################################################################################################################################################################################

# skyitdev
skyitdev_path <- "hdfs:///STAGE/adobe/reportsuite=skyitdev"
skyitdev_df <- read.parquet(skyitdev_path)


# filtri ----
skyit_v1 <- select(skyitdev_df, "external_id_post_evar",
                   "post_visid_high",
                   "post_visid_low",
                   "post_visid_concatenated",
                   "date_time",
                   "page_url_post_evar","page_url_post_prop","download_name_post_prop",
                   "secondo_livello_post_evar",
                   "terzo_livello_post_evar",
                   "visit_num",
                   "hit_source",
                   "exclude_hit")


skyit_v2 <- withColumn(skyit_v1, "date_time_dt", cast(skyit_v1$date_time, "date"))
skyit_v3 <- withColumn(skyit_v2, "date_time_ts", cast(skyit_v2$date_time, "timestamp"))


#filtro temporale
# skyit_v4 <- filter(skyit_v3,"date_time_dt>='2017-03-01'")
# skyit_v5 <- filter(skyit_v4,"date_time_dt <= '2018-02-10'")


#unisco alla base
createOrReplaceTempView(skyit_v3,"skyit_v3")
createOrReplaceTempView(base_pdisc_coo_2,"base_pdisc_coo_2")

join_base_sez_pubblica <- sql("SELECT t1.COD_CONTRATTO, t1.COD_CLIENTE_FRUITORE,t1.COD_CLIENTE_CIFRATO,
                                     t1.data_ingr_pdisc_formdate, t1.post_visid_concatenated,
                                     t2.visit_num,t2.hit_source,t2.exclude_hit,
                                     t2.date_time, t2.date_time_dt, t2.date_time_ts,
                                     t2.page_url_post_evar,t2.page_url_post_prop,t2.download_name_post_prop,t2.secondo_livello_post_evar,t2.terzo_livello_post_evar
                             FROM base_pdisc_coo_2 t1
                             INNER JOIN skyit_v3 t2
                              ON t1.post_visid_concatenated = t2.post_visid_concatenated")

#salvataggio
write.parquet(join_base_sez_pubblica,"/user/stefano.mazzucca/CJ_PDISC_scarico_skyitdev.parquet")


#leggi
CJ_PDISC_skyitdev <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_skyitdev.parquet")
View(head(CJ_PDISC_skyitdev,100))
nrow(CJ_PDISC_skyitdev)


#################################################################################################################################################################################
#################################################################################################################################################################################
#### Scarico pag viste APP
#################################################################################################################################################################################
#################################################################################################################################################################################

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")

#seleziona campi
skyappwsc_1 <- select(skyappwsc,
                      "external_id_post_evar",
                      "site_section",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "post_pagename",
                      "date_time",
                      "visit_num", "post_visid_concatenated", "hit_source", "exclude_hit")

skyappwsc_2 <- filter(skyappwsc_1,"external_id_post_evar is not NULL")

skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_dt", cast(skyappwsc_2$date_time, "date"))
skyappwsc_4 <- withColumn(skyappwsc_3, "date_time_ts", cast(skyappwsc_3$date_time, "timestamp"))


#filtro temporale
skyappwsc_5 <- filter(skyappwsc_4,"date_time_dt>='2017-03-01'")
skyappwsc_6 <- filter(skyappwsc_5,"date_time_dt <= '2018-02-10'")


#unisco alla base
createOrReplaceTempView(skyappwsc_6,"skyappwsc_6")
createOrReplaceTempView(base_pdisc,"base_pdisc")

join_base_app <- sql("SELECT t1.COD_CONTRATTO,t1.COD_CLIENTE_FRUITORE,t1.COD_CLIENTE_CIFRATO,t1.data_ingr_pdisc_formdate,
                                 t2.site_section,t2.page_name_post_evar,t2.page_name_post_prop,t2.post_pagename,
                                 t2.visit_num,t2.post_visid_concatenated,t2.hit_source,t2.exclude_hit,t2.date_time,t2.date_time_dt,t2.date_time_ts
                             FROM base_pdisc t1
                             INNER JOIN skyappwsc_6 t2
                              ON t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

#salvataggio
write.parquet(join_base_app,"/user/valentina/CJ_PDISC_scarico_app.parquet")



#################################################################################################################################################################################
#################################################################################################################################################################################
#################################################################################################################################################################################
#################################################################################################################################################################################











#chiudi sessione
sparkR.stop()
