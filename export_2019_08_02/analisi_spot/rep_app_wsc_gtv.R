
# lavoro per Paolo Fiordi
# utenti su GUIDA TV e FAI DA TE
# filtro temporale Mar17 - Feb18

#apri sessione
source("connection_R.R")
options(scipen = 1000)



#### APP FAI DA TE

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")

#View(head(skyappwsc,100))

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
# nrow(skyappwsc_1)
# # 341.254.758
skyappwsc_2 <- filter(skyappwsc_1,"external_id_post_evar is not NULL")
# nrow(skyappwsc_2)
# # 191.293.227

skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_dt", cast(skyappwsc_2$date_time, "date"))
skyappwsc_4 <- withColumn(skyappwsc_3, "date_time_ts", cast(skyappwsc_3$date_time, "timestamp"))

# filtro temporale
skyappwsc_5 <- filter(skyappwsc_4,"date_time_dt >= '2017-03-01'")
skyappwsc_6 <- filter(skyappwsc_5,"date_time_dt <= '2018-02-28'")


valdist_utenti_appwsc <- distinct(select(skyappwsc_6, "external_id_post_evar"))
nrow(valdist_utenti_appwsc)
# 1.367.149


## verifrica sull'ext_id
createOrReplaceTempView(valdist_utenti_appwsc, "valdist_utenti_appwsc")
ver_valdist_utenti_appwsc <-sql("select *
                                 from valdist_utenti_appwsc
                                 where length(external_id_post_evar) == 48")
nrow(ver_valdist_utenti_appwsc)
# 1.367.145 (4 in meno)


# ## verifica sui cookies (se coincide con Adobe Analytics)
# 
# skyappwsc_3a <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
# skyappwsc_4a <- withColumn(skyappwsc_3a, "date_time_ts", cast(skyappwsc_3a$date_time, "timestamp"))
# 
# #filtro temporale
# skyappwsc_5a <- filter(skyappwsc_4a,"date_time_dt >= '2017-03-01'")
# skyappwsc_6a <- filter(skyappwsc_5a,"date_time_dt <= '2018-02-28'")
# 
# 
# valdist_cookies_appwsc <- distinct(select(skyappwsc_6a, "post_visid_concatenated"))
# nrow(valdist_cookies_appwsc)
# # 2.966.719 (vs 2.821.323 di Adobe)


# export 
write.df(repartition( ver_valdist_utenti_appwsc, 1),path = "/user/stefano.mazzucca/valdist_utenti_appwsc.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





#### APP GUIDA TV

skyappgtv <- read.parquet("/STAGE/adobe/reportsuite=skyappguidatv.prod")

#View(head(skyappgtv,1000))

# seleziona campi
skyappgtv_1 <- select(skyappgtv,
                      "external_id_post_evar",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "post_pagename",
                      "date_time",
                      "visit_num", 
                      "post_visid_concatenated")

skyappgtv_2 <- filter(skyappgtv_1,"external_id_post_evar is not NULL")

skyappgtv_3 <- withColumn(skyappgtv_2, "date_time_dt", cast(skyappgtv_1$date_time, "date"))
skyappgtv_4 <- withColumn(skyappgtv_3, "date_time_ts", cast(skyappgtv_3$date_time, "timestamp"))

# filtro temporale
skyappgtv_5 <- filter(skyappgtv_4,"date_time_dt >= '2017-03-01'")
skyappgtv_6 <- filter(skyappgtv_5,"date_time_dt <= '2018-02-28'")


valdist_utenti_appgtv <- distinct(select(skyappgtv_6, "external_id_post_evar"))
nrow(valdist_utenti_appgtv)
# 420.295


## verifrica sull'ext_id
createOrReplaceTempView(valdist_utenti_appgtv, "valdist_utenti_appgtv")
ver_valdist_utenti_appgtv <-sql("select *
                                 from valdist_utenti_appgtv
                                 where length(external_id_post_evar) == 48")
nrow(ver_valdist_utenti_appgtv)
# 420.295


# ## verifica sui cookies (se coincide con Adobe Analytics)
# 
# skyappgtv_3a <- withColumn(skyappgtv_1, "date_time_dt", cast(skyappgtv_1$date_time, "date"))
# skyappgtv_4a <- withColumn(skyappgtv_3a, "date_time_ts", cast(skyappgtv_3a$date_time, "timestamp"))
# 
# #filtro temporale
# skyappgtv_5a <- filter(skyappgtv_4a,"date_time_dt >= '2017-03-01'")
# skyappgtv_6a <- filter(skyappgtv_5a,"date_time_dt <= '2018-02-28'")
# 
# 
# valdist_cookies_appgtv <- distinct(select(skyappgtv_6a, "post_visid_concatenated"))
# nrow(valdist_cookies_appgtv)
# # 1.000.389 (vs 567.064 di Adobe)


# export 
write.df(repartition( valdist_utenti_appgtv, 1),path = "/user/stefano.mazzucca/valdist_utenti_appgtv.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





#### utenti su entrambe le APP

createOrReplaceTempView(ver_valdist_utenti_appwsc, "ver_valdist_utenti_appwsc")
createOrReplaceTempView(valdist_utenti_appgtv, "valdist_utenti_appgtv")

join_app <- sql("select t1.*
                from valdist_utenti_appgtv t1
                inner join ver_valdist_utenti_appwsc t2
                  on t1.external_id_post_evar = t2.external_id_post_evar")
nrow(join_app)
# 329.272
View(head(join_app,100))


# export 
write.df(repartition( join_app, 1),path = "/user/stefano.mazzucca/join_utenti_app_wsc_gtv.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





## ANTI JOIN (dove t1 e' la tabella di riferimento)
utenti_solo_appwsc <- sql("select t1.*
                            from ver_valdist_utenti_appwsc t1
                            left join valdist_utenti_appgtv t2
                              on t1.external_id_post_evar = t2.external_id_post_evar where t2.external_id_post_evar is NULL")
nrow(utenti_solo_appwsc)
# 1.037.873


# export 
write.df(repartition( utenti_solo_appwsc, 1),path = "/user/stefano.mazzucca/utenti_solo_appwsc.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





## ANTI JOIN (dove t1 e' la tabella di riferimento)
utenti_solo_appgtv <- sql("select t1.*
                            from valdist_utenti_appgtv t1
                            left join ver_valdist_utenti_appwsc t2
                              on t1.external_id_post_evar = t2.external_id_post_evar where t2.external_id_post_evar is NULL")
nrow(utenti_solo_appgtv)
# 91.023


# export 
write.df(repartition( utenti_solo_appgtv, 1),path = "/user/stefano.mazzucca/utenti_solo_appgtv.csv", "csv", sep=";", mode = "overwrite", header=TRUE)







