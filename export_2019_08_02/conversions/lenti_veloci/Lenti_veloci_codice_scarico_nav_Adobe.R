
########## prendi lista ###################################################################################################
conversion_target_filtered <- read.parquet("/user/stefano.mazzucca/conversion_target_filtered.parquet")
nrow(conversion_target_filtered) #28505
printSchema(conversion_target_filtered)

# |-- COD_CLIENTE_CIFRATO: string (nullable = true)
# |-- data_stipula_dt: date (nullable = true)



######################################################################################################################################################################
########################## associo cookies ed external_id ADOBE #####################################################################################################

base_ext_id <- distinct(select(conversion_target_filtered,"COD_CLIENTE_CIFRATO"))
nrow(base_ext_id) # 26631

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated"))

#join
createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
createOrReplaceTempView(base_ext_id,"base_ext_id")

base_ext_id_2 <- sql("select *
                     from base_ext_id  t1 left join skyitdev_df_3 t2
                     on t1.COD_CLIENTE_CIFRATO=t2.external_id_post_evar")


write.parquet(base_ext_id_2,"/user/valentina/estrai_lista_IBRIDI_CookiesADOBE.parquet")



#########################################################################################################################################################
####### creo data-set per estrazioni successive

base_cookies <- read.parquet("/user/valentina/estrai_lista_IBRIDI_CookiesADOBE.parquet")
nrow(base_cookies) #55476

base_cookies_2 <- filter(base_cookies,"external_id_post_evar is not NULL")
nrow(base_cookies_2) #40858

## seleziono la max data di attivaz associata all'ext-id
createOrReplaceTempView(conversion_target_filtered,"conversion_target_filtered")
max_data_stipula <- sql("select COD_CLIENTE_CIFRATO, max(data_stipula_dt) as max_Data_stipula_dt
                        from conversion_target_filtered
                        group by COD_CLIENTE_CIFRATO")
nrow(conversion_target_filtered)  #28505
nrow(max_data_stipula) #26631


#join 
createOrReplaceTempView(base_cookies_2,"base_cookies_2")
createOrReplaceTempView(max_data_stipula,"max_data_stipula")

base_cookies_3 <- sql("select t1.*, t2.max_Data_stipula_dt
                      from base_cookies_2 t1 inner join max_data_stipula t2
                      on t1.COD_CLIENTE_CIFRATO=t2.COD_CLIENTE_CIFRATO")
nrow(base_cookies_3)  #40858
#########################################################################################################################################################
# scarico tutta la navigazione sul sito da prima dell'attivazione
#########################################################################################################################################################

skyitdev <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")

#names(skyitdev)
# post_campaign: ha persistenza di 7gg (trackingcode)
# va_closer_id: ha persistenza legata alla singola visita, non rimane nel tempo

skyitdev <- select(skyitdev, "external_id_post_evar",
                   "post_pagename",
                   "post_visid_high",
                   "post_visid_low",
                   "post_visid_concatenated",
                   "date_time",
                   "page_url_post_evar","page_url_post_prop",
                   "secondo_livello_post_evar",
                   "terzo_livello_post_evar",
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

skyitdev <- filter(skyitdev,"date_time_dt >= '2017-04-01'")


#### associa e scarica navigazioni
createOrReplaceTempView(skyitdev,"skyitdev")
createOrReplaceTempView(base_cookies_3,"base_cookies_3")

scarico_naigazione_1 <- sql("select t1.*,
                           t2.COD_CLIENTE_CIFRATO, t2.max_Data_stipula_dt
                           from skyitdev t1 inner join base_cookies_3 t2
                           on t1.post_visid_concatenated=t2.post_visid_concatenated
                           ")

scarico_naigazione_2 <- filter(scarico_naigazione_1,"date_time_dt<=max_Data_stipula_dt")


write.parquet(scarico_naigazione_2,"/user/valentina/estrai_lista_IBRIDI_scarico_naigazione.parquet")


#chiudi sessione
sparkR.stop()