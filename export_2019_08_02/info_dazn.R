
## Estrazione info DAZN da WEB + APP


#apri sessione
source("connection_R.R")
options(scipen = 1000)



## Creo dizionario cookies Adobe ##############################################################################################################################

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated")
diz_coo_id <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated"))

write.parquet(diz_coo_id,"/user/stefano.mazzucca/info_dazn/diz_cookie.parquet", mode = "overwrite")



## Info DAZN da Sky.it ##########################################################################################################################################

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
                   "canale_corporate_post_prop"
)

## FILTRO DATE
skyitdev <- withColumn(skyitdev, "date_time_dt", cast(skyitdev$date_time, "date"))
skyitdev <- withColumn(skyitdev, "date_time_ts", cast(skyitdev$date_time, "timestamp"))

skyitdev <- filter(skyitdev,"date_time_dt >= '2018-07-30'")

## Scarico navigazioni
navigazioni <- filter(skyitdev, "post_channel = 'corporate'")
navigazioni <- filter(navigazioni, "terzo_livello NOT LIKE '%faidate%'")
navigazioni_dazn <- filter(navigazioni, "page_url_post_prop LIKE '%sky.it/pacchetti-offerte/sky-calcio/dazn%' OR 
                                        page_url_post_prop LIKE '%sky.it/assistenza/conosci/programmazione%' OR 
                                        page_url_post_prop LIKE '%sky.it/assistenza/conosci/supporto-dazn%' ")


write.parquet(navigazioni,"/user/stefano.mazzucca/info_dazn/scarico_navigazioni_sitopubb.parquet", mode = "overwrite")
write.parquet(navigazioni_dazn,"/user/stefano.mazzucca/info_dazn/scarico_navigazioni_sitopubb_DAZN.parquet", mode = "overwrite")


navigazioni_dazn <- read.parquet("/user/stefano.mazzucca/info_dazn/scarico_navigazioni_sitopubb_DAZN.parquet")
View(head(navigazioni_dazn,100))
nrow(navigazioni_dazn)
# 2.443.098


## Join ext_id cookie
navigazioni <- read.parquet("/user/stefano.mazzucca/info_dazn/scarico_navigazioni_sitopubb.parquet")
View(head(navigazioni,100))
nrow(navigazioni)
# 40.693.123
diz_coo <- read.parquet("/user/stefano.mazzucca/info_dazn/diz_cookie.parquet")
View(head(diz_coo,100))
nrow(diz_coo)
# 364.729.241

createOrReplaceTempView(navigazioni,"navigazioni")
createOrReplaceTempView(diz_coo,"diz_coo")
navigazioni_extid <- sql("select distinct t1.*, t2.external_id_post_evar as COD_CLIENTE_CIFRATO
                          from navigazioni t1
                          inner join diz_coo t2
                          on t1.post_visid_concatenated = t2.post_visid_concatenated ")


write.parquet(navigazioni_extid,"/user/stefano.mazzucca/info_dazn/navigazioni_sitopubb_extid.parquet", mode = "overwrite")


navigazioni_sitopubb <- read.parquet("/user/stefano.mazzucca/info_dazn/navigazioni_sitopubb_extid.parquet")
View(head(navigazioni_sitopubb,100))
nrow(navigazioni_sitopubb)
# 67.247.324


## Info DAZN da APP WSC #########################################################################################################################################

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod/table=hitdata/hitdata.parquet")
skyappwsc_1 <- select(skyappwsc, "external_id_post_evar",
                      "post_visid_high",
                      "post_visid_low",
                      "post_visid_concatenated",
                      "visit_num",
                      "date_time",
                      "post_channel", 
                      "page_name_post_evar",
                      "page_url_post_prop",
                      "site_section",
                      "hit_source",
                      "exclude_hit"
)
skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_ts", cast(skyappwsc_2$date_time, "timestamp"))
skyappwsc_4 <- filter(skyappwsc_3, "external_id_post_evar is not NULL")

# filtro temporale
skyappwsc_5 <- filter(skyappwsc_4,"date_time_dt >= '2018-07-30' ")

## Scarico navigazioni
navigazioni_app_dazn <- filter(skyappwsc_5, "page_name_post_evar LIKE 'arricchisci abbonamento:lista voucher'")


write.parquet(navigazioni_app_dazn,"/user/stefano.mazzucca/info_dazn/scarico_navigazioni_appwsc_DAZN.parquet",mode = "overwrite")



#################################################################################################################################################################
## Riepilogo ##
#################################################################################################################################################################

## Accessi sito pubblico ######################################################################################################################################
navigazioni_sitopubb <- read.parquet("/user/stefano.mazzucca/info_dazn/navigazioni_sitopubb_extid.parquet")
View(head(navigazioni_sitopubb,100))
nrow(navigazioni_sitopubb)
# 67.247.324

createOrReplaceTempView(navigazioni_sitopubb, "navigazioni_sitopubb")
valdist_sitopubb <- sql("select distinct external_id_post_evar, date_time_ts, post_pagename
                         from navigazioni_sitopubb
                         group by external_id_post_evar, date_time_ts, post_pagename")
valdist_sitopubb <- filter(valdist_sitopubb, "external_id_post_evar is NOT NULL")
valdist_sitopubb <- arrange(valdist_sitopubb, asc(valdist_sitopubb$external_id_post_evar), desc(valdist_sitopubb$date_time_ts))
View(head(valdist_sitopubb,100))
nrow(valdist_sitopubb)
# 20.701.296 hit di utenti riconosciuti su sito

write.df(repartition( valdist_sitopubb, 1), path = "/user/stefano.mazzucca/info_dazn/navigazioni_sitopubb_coo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


extid_sitopubb <- distinct(select(valdist_sitopubb, "external_id_post_evar"))
nrow(extid_sitopubb)
# 1.053.935   # (5.673.007 cookies)


# valdist_sitopubb <- read.df("/user/stefano.mazzucca/info_dazn/navigazioni_sitopubb_coo.csv", source = "csv", header = "true", delimiter = ";")
# View(head(valdist_sitopubb,100))
# nrow(valdist_sitopubb)
# # 5.673.007

# ## Join con gli extid
# diz_coo <- read.parquet("/user/stefano.mazzucca/info_dazn/diz_cookie.parquet")
# View(head(diz_coo,100))
# nrow(diz_coo)
# # 364.729.241
# createOrReplaceTempView(navigazioni_sitopubb,"navigazioni_sitopubb")
# createOrReplaceTempView(diz_coo,"diz_coo")
# navigazioni_sitopubb_coo <- sql("select distinct t1.*, t2.external_id_post_evar
#                                   from navigazioni_sitopubb t1
#                                   inner join diz_coo t2
#                                   on t1.post_visid_concatenated = t2.post_visid_concatenated ")
# View(head(navigazioni_sitopubb_coo,100))
# nrow(navigazioni_sitopubb_coo)
# # 160.595.543



## Visite a info_dazn #########################################################################################################################################
navigazioni_sito_dazn <- read.parquet("/user/stefano.mazzucca/info_dazn/scarico_navigazioni_sitopubb_DAZN.parquet")
View(head(navigazioni_sito_dazn,100))
nrow(navigazioni_sito_dazn)
# 2.443.098

# ## Join ext_id cookie
# diz_coo <- read.parquet("/user/stefano.mazzucca/info_dazn/diz_cookie.parquet")
# View(head(diz_coo,100))
# nrow(diz_coo)
# # 364.729.241
# 
# createOrReplaceTempView(navigazioni_sito_dazn,"navigazioni_sito_dazn")
# createOrReplaceTempView(diz_coo,"diz_coo")
# navigazioni_sito_dazn_coo <- sql("select distinct t1.*, t2.external_id_post_evar as COD_CLIENTE_CIFRATO
#                                   from navigazioni_sito_dazn t1
#                                   inner join diz_coo t2
#                                   on t1.post_visid_concatenated = t2.post_visid_concatenated ")
# View(head(navigazioni_sito_dazn_coo,100))
# nrow(navigazioni_sito_dazn_coo)
# # 4.086.866

createOrReplaceTempView(navigazioni_sito_dazn, "navigazioni_sito_dazn")
valdist_sito_dazn <- sql("select distinct external_id_post_evar, date_time_ts, post_pagename
                         from navigazioni_sito_dazn
                         group by external_id_post_evar, date_time_ts, post_pagename")
valdist_sito_dazn <- filter(valdist_sito_dazn, "external_id_post_evar is NOT NULL")
valdist_sito_dazn <- withColumn(valdist_sito_dazn, "flg_sito_app", lit("sito"))
View(head(valdist_sito_dazn,100))
nrow(valdist_sito_dazn)
# (2.167.338 hit totali)   # 1.320.561 con extid loggato
extid_sito <- distinct(select(valdist_sito_dazn, "external_id_post_evar"))
nrow(extid_sito)
# 339.406   # (994.469 cookies)



navigazioni_app_dazn <- read.parquet("/user/stefano.mazzucca/info_dazn/scarico_navigazioni_appwsc_DAZN.parquet")
View(head(navigazioni_app_dazn,100))
nrow(navigazioni_app_dazn)
# 347.625

createOrReplaceTempView(navigazioni_app_dazn, "navigazioni_app_dazn")
valdist_app_dazn <- sql("select distinct external_id_post_evar, date_time_ts, page_name_post_evar as post_pagename
                         from navigazioni_app_dazn
                         group by external_id_post_evar, date_time_ts, page_name_post_evar")
valdist_app_dazn <- withColumn(valdist_app_dazn, "flg_sito_app", lit("app"))
View(head(valdist_app_dazn,100))
nrow(valdist_app_dazn)
# 311.306
extid_app <- distinct(select(valdist_app_dazn, "external_id_post_evar"))
nrow(extid_app)
# 138.774


union_extid <- union(extid_sito, extid_app)
union_extid <- distinct(select(union_extid, "external_id_post_evar"))
nrow(union_extid)
# 424.545


## union 
valdist_app_sito_dazn <- rbind(valdist_sito_dazn, valdist_app_dazn)
valdist_app_sito_dazn <- arrange(valdist_app_sito_dazn, asc(valdist_app_sito_dazn$external_id_post_evar), desc(valdist_app_sito_dazn$date_time_ts))
View(head(valdist_app_sito_dazn,100))
nrow(valdist_app_sito_dazn)
# 1.631.867

write.df(repartition( valdist_app_sito_dazn, 1), path = "/user/stefano.mazzucca/info_dazn/navigazioni_info_dazn.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


valdist_app_sito_dazn <- read.df("/user/stefano.mazzucca/info_dazn/navigazioni_info_dazn.csv", source = "csv", header = "true", delimiter = ";")
View(head(valdist_app_sito_dazn,100))
nrow(valdist_app_sito_dazn)
# 1.631.867





#################################################################################################################################################################
## Distribuzione DAILY dei non-loggati (con focus su "info_serieA/DAZN") ##
#################################################################################################################################################################

navigazioni_sitopubb <- read.parquet("/user/stefano.mazzucca/info_dazn/navigazioni_sitopubb_extid.parquet")
View(head(navigazioni_sitopubb,100))
nrow(navigazioni_sitopubb)
# 67.247.324

## da fare/finire !!







#chiudi sessione
sparkR.stop()