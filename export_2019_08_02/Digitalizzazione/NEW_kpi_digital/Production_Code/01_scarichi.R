
## 01_scarichi ##

source("connection_R.R")
source("Digitalizzazione/NEW_kpi_digital/Production_Code/00_Utils_kpi_digital.R")

options(scipen = 10000)



# Scarico Corporate da SKYITDEV:

skyitdev <- read.parquet(path_skyitdev) # read reportsuite

# variabili utilizzate per lo scarico

skyit_1 <- select(skyitdev, "external_id_post_evar",
                  "post_visid_high",
                  "post_visid_low",
                  "post_visid_concatenated",
                  "visit_num",
                  "date_time",
                  "post_channel", 
                  "page_name_post_evar",
                  "page_url_post_evar",
                  "secondo_livello_post_prop",
                  "terzo_livello_post_prop",
                  "hit_source",
                  "exclude_hit", 
                  "post_page_event_value",
                  "post_mobiledevice",
                  "post_campaign",
                  "va_closer_id", 
                  "mapped_connection_type_value",
                  "mapped_browser_type_value",
                  "mapped_country_value",
                  "mapped_os_value",
                  "mapped_resolution_value",
                  "mapped_browser_value"
)

skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date")) # cast date_time in formato date
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))  # cast date_time in formato timestamp

# filtro temporale

skyit_4 <- filter(skyit_3, skyit_3$date_time_dt >= data_inizio) 
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt <= data_fine)

# traduzione last touch label con dizionario 

skyit_5$last_touch_label <- ifelse(skyit_5$va_closer_id == 1, "editorial_recommendation",
                                   ifelse(skyit_5$va_closer_id == 2, "search_engine_organic",
                                          ifelse(skyit_5$va_closer_id == 3, "internal_referrer_w",
                                                 ifelse(skyit_5$va_closer_id == 4, "social_network",
                                                        ifelse(skyit_5$va_closer_id == 5, "direct_bookmark",
                                                               ifelse(skyit_5$va_closer_id == 6, "referring_domains",
                                                                      ifelse(skyit_5$va_closer_id == 7, "paid_display",
                                                                             ifelse(skyit_5$va_closer_id == 8, "paid_search",
                                                                                    ifelse(skyit_5$va_closer_id == 9, "paid_other",
                                                                                           ifelse(skyit_5$va_closer_id == 10, "retargeting",
                                                                                                  ifelse(skyit_5$va_closer_id == 11, "social_player",
                                                                                                         ifelse(skyit_5$va_closer_id == 12, "direct_email_marketing",
                                                                                                                ifelse(skyit_5$va_closer_id == 13, "paid_social",
                                                                                                                       ifelse(skyit_5$va_closer_id == 14, "newsletter",
                                                                                                                              ifelse(skyit_5$va_closer_id == 15, "acquisition",
                                                                                                                                     ifelse(skyit_5$va_closer_id == 16, "AMP_w",
                                                                                                                                            ifelse(skyit_5$va_closer_id == 17, "web_push_notification",
                                                                                                                                                   ifelse(skyit_5$va_closer_id == 18, "DEM_w",
                                                                                                                                                          ifelse(skyit_5$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                 ifelse(skyit_5$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                        ifelse(skyit_5$va_closer_id == 21, "internal",
                                                                                                                                                                               ifelse(skyit_5$va_closer_id == 22, "affiliation",
                                                                                                                                                                                      ifelse(skyit_5$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                             ifelse(skyit_5$va_closer_id == 24, "short_url",
                                                                                                                                                                                                    ifelse(skyit_5$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

write.parquet(skyit_5, path_scarico_skyitdev_senza_diz)
skyit_5 <- read.parquet(path_scarico_skyitdev_senza_diz) # 3.992.853.089

## Creo le chiavi ext_id con cookies (dizionario)
diz_1 <- distinct(select(skyitdev, "external_id_post_evar", "post_visid_concatenated", "date_time"))
diz_2 <- filter(diz_1, "external_id_post_evar is NOT NULL")
diz_3 <- filter(diz_2, "post_visid_concatenated is NOT NULL")
diz_4 <- withColumn(diz_3, "date_time_dt", cast(diz_3$date_time, "date"))

diz_5 <- filter(diz_4, diz_4$date_time_dt >= (data_inizio - 185) & diz_4$date_time_dt <= data_fine)

diz_6 <- select(diz_5, "external_id_post_evar", "post_visid_concatenated")

write.parquet(diz_6, path_diz_skyitdev)

diz_6 <- read.parquet(path_diz_skyitdev) # 824.670.691
diz_7 <- distinct(select(diz_6, "external_id_post_evar", "post_visid_concatenated")) # 1.202.2554
write.parquet(diz_7, path_diz_skyitdev_2)

diz_7 <- read.parquet(path_diz_skyitdev)

## join tra diz e navigazioni (skyit_5)
createOrReplaceTempView(diz_7,"diz_7")
createOrReplaceTempView(skyit_5,"skyit_5")

# skyitdev_diz  <- sql("SELECT DISTINCT t2.external_id_post_evar as skyid, t1.*
#                      FROM skyit_5 t1
#                      LEFT JOIN diz_7 t2
#                      ON t1.post_visid_concatenated = t2.post_visid_concatenated")

skyitdev_diz <- merge(skyit_5, diz_7, by.x = "post_visid_concatenated", by.y = "post_visid_concatenated")

write.parquet(skyitdev_diz, path_scarico_skyitdev, mode = "overwrite")

############################## ############################## ############################## ############################## ############################## 
############################## ############################## ############################## ############################## ############################## 
## ATTENZIONE alla riconduzione COOKIE - EXT ID con il dizionario su Skyitdev ##
############################## ############################## ############################## ############################## ############################## 
############################## ############################## ############################## ############################## ############################## 


# scarico <- read.parquet(path_scarico_skyitdev) # read scarico skyitdev  
# scarico$external_id_post_evar <- NULL
# scarico <- withColumnRenamed(scarico,  "skyid", "external_id_post_evar")

scarico <- read.parquet(path_scarico_skyitdev_senza_diz)



###### CORPORATE ######

corporate <- filter(scarico, 'post_channel=="corporate" OR post_channel=="Guidatv"') # filtro primo livello corporate e guida tv

write.parquet(corporate, path_scarico_corporate) # write scarico corporate

corporate <- read.parquet(path_scarico_corporate) # read scarico corporate

############################## TG24 ###########################################

tg24 <- filter(scarico, 'post_channel == "tg24"')

write.parquet(tg24, path_scarico_tg24)

tg24 <- read.parquet(path_scarico_tg24)

############################## SPORT ###########################################

sport <- filter(scarico, 'post_channel == "sport"')

write.parquet(sport, path_scarico_sport)

sport <- read.parquet(path_scarico_sport)

############################## APP WSC ###########################################

skyappwsc <- read.parquet(path_appwsc)

skyappwsc_1 <- select(skyappwsc, "external_id_post_evar", #external id
                      "post_visid_high", #first part of cookie
                      "post_visid_low", #second part of cookie
                      "post_visid_concatenated", #cookie
                      "visit_num", #?
                      "date_time", #date of the visit
                      "post_channel",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "site_section",
                      "hit_source",
                      "exclude_hit",
                      "post_page_event_value",
                      "post_mobiledevice",
                      "post_campaign",
                      "va_closer_id",
                      "mapped_connection_type_value",
                      "mapped_browser_type_value",
                      "mapped_country_value",
                      "mapped_language_value",
                      "mapped_os_value",
                      "mapped_resolution_value",
                      "mapped_browser_value",
                      "paid_search"
)

skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_ts", cast(skyappwsc_2$date_time, "timestamp"))
skyappwsc_4 <- filter(skyappwsc_3, "external_id_post_evar is not NULL")

skyappwsc_5 <- filter(skyappwsc_4, skyappwsc_4$date_time_dt >= data_inizio) 
skyappwsc_6 <- filter(skyappwsc_5, skyappwsc_5$date_time_dt <= data_fine)

#label colonna last_touch_point:
skyappwsc_6$last_touch_label <- ifelse(skyappwsc_6$va_closer_id == 1, "editorial_recommendation",
                                       ifelse(skyappwsc_6$va_closer_id == 2, "search_engine_organic",
                                              ifelse(skyappwsc_6$va_closer_id == 3, "internal_referrer_w",
                                                     ifelse(skyappwsc_6$va_closer_id == 4, "social_network",
                                                            ifelse(skyappwsc_6$va_closer_id == 5, "direct_bookmark",
                                                                   ifelse(skyappwsc_6$va_closer_id == 6, "referring_domains",
                                                                          ifelse(skyappwsc_6$va_closer_id == 7, "paid_display",
                                                                                 ifelse(skyappwsc_6$va_closer_id == 8, "paid_search",
                                                                                        ifelse(skyappwsc_6$va_closer_id == 9, "paid_other",
                                                                                               ifelse(skyappwsc_6$va_closer_id == 10, "retargeting",
                                                                                                      ifelse(skyappwsc_6$va_closer_id == 11, "social_player",
                                                                                                             ifelse(skyappwsc_6$va_closer_id == 12, "direct_email_marketing",
                                                                                                                    ifelse(skyappwsc_6$va_closer_id == 13, "paid_social",
                                                                                                                           ifelse(skyappwsc_6$va_closer_id == 14, "newsletter",
                                                                                                                                  ifelse(skyappwsc_6$va_closer_id == 15, "acquisition",
                                                                                                                                         ifelse(skyappwsc_6$va_closer_id == 16, "AMP_w",
                                                                                                                                                ifelse(skyappwsc_6$va_closer_id == 17, "web_push_notification",
                                                                                                                                                       ifelse(skyappwsc_6$va_closer_id == 18, "DEM_w",
                                                                                                                                                              ifelse(skyappwsc_6$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                     ifelse(skyappwsc_6$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                            ifelse(skyappwsc_6$va_closer_id == 21, "internal",
                                                                                                                                                                                   ifelse(skyappwsc_6$va_closer_id == 22, "affiliation",
                                                                                                                                                                                          ifelse(skyappwsc_6$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                                 ifelse(skyappwsc_6$va_closer_id == 24, "short_url",
                                                                                                                                                                                                        ifelse(skyappwsc_6$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

write.parquet(skyappwsc_6, path_scarico_skyappwsc) # write scarico app wsc

appwsc <- read.parquet(path_scarico_skyappwsc) 

############################# APP GUIDA TV ################################

skyappgtv <- read.parquet(path_appgtv)

skyappgtv_1 <- select(skyappgtv, "external_id_post_evar", #external id
                      "post_visid_concatenated", #cookie
                      "visit_num", 
                      "date_time", #date of the visit
                      "post_channel",
                      "site_section_post_evar",
                      "post_page_event_value",
                      "post_mobiledevice",
                      "post_campaign",
                      "va_closer_id",
                      "mapped_connection_type_value",
                      "mapped_browser_type_value",
                      "mapped_country_value",
                      "mapped_language_value",
                      "mapped_os_value",
                      "mapped_resolution_value",
                      "mapped_browser_value",
                      "paid_search"
)

skyappgtv_2 <- withColumn(skyappgtv_1, "date_time_dt", cast(skyappgtv_1$date_time, "date"))
skyappgtv_3 <- withColumn(skyappgtv_2, "date_time_ts", cast(skyappgtv_2$date_time, "timestamp"))
# skyappgtv_4 <- filter(skyappgtv_3, "external_id_post_evar is not NULL")

skyappgtv_5 <- filter(skyappgtv_3, skyappgtv_3$date_time_dt >= data_inizio) 
skyappgtv_6 <- filter(skyappgtv_5, skyappgtv_5$date_time_dt <= data_fine)

#label colonna last_touch_point:
skyappgtv_6$last_touch_label <- ifelse(skyappgtv_6$va_closer_id == 1, "editorial_recommendation",
                                       ifelse(skyappgtv_6$va_closer_id == 2, "search_engine_organic",
                                              ifelse(skyappgtv_6$va_closer_id == 3, "internal_referrer_w",
                                                     ifelse(skyappgtv_6$va_closer_id == 4, "social_network",
                                                            ifelse(skyappgtv_6$va_closer_id == 5, "direct_bookmark",
                                                                   ifelse(skyappgtv_6$va_closer_id == 6, "referring_domains",
                                                                          ifelse(skyappgtv_6$va_closer_id == 7, "paid_display",
                                                                                 ifelse(skyappgtv_6$va_closer_id == 8, "paid_search",
                                                                                        ifelse(skyappgtv_6$va_closer_id == 9, "paid_other",
                                                                                               ifelse(skyappgtv_6$va_closer_id == 10, "retargeting",
                                                                                                      ifelse(skyappgtv_6$va_closer_id == 11, "social_player",
                                                                                                             ifelse(skyappgtv_6$va_closer_id == 12, "direct_email_marketing",
                                                                                                                    ifelse(skyappgtv_6$va_closer_id == 13, "paid_social",
                                                                                                                           ifelse(skyappgtv_6$va_closer_id == 14, "newsletter",
                                                                                                                                  ifelse(skyappgtv_6$va_closer_id == 15, "acquisition",
                                                                                                                                         ifelse(skyappgtv_6$va_closer_id == 16, "AMP_w",
                                                                                                                                                ifelse(skyappgtv_6$va_closer_id == 17, "web_push_notification",
                                                                                                                                                       ifelse(skyappgtv_6$va_closer_id == 18, "DEM_w",
                                                                                                                                                              ifelse(skyappgtv_6$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                     ifelse(skyappgtv_6$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                            ifelse(skyappgtv_6$va_closer_id == 21, "internal",
                                                                                                                                                                                   ifelse(skyappgtv_6$va_closer_id == 22, "affiliation",
                                                                                                                                                                                          ifelse(skyappgtv_6$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                                 ifelse(skyappgtv_6$va_closer_id == 24, "short_url",
                                                                                                                                                                                                        ifelse(skyappgtv_6$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))



# PER APP GUIDA TV VA FATTO IL DIZIONARIO E IL JOIN

skyappgtv_diz <- distinct(select(skyappgtv, "external_id_post_evar", "post_visid_concatenated", "date_time"))

skyappgtv_diz2 <- filter(skyappgtv_diz, isNotNull(skyappgtv_diz$external_id_post_evar))
skyappgtv_diz3 <- filter(skyappgtv_diz2, isNotNull(skyappgtv_diz2$post_visid_concatenated))

skyappgtv_diz4 <- withColumn(skyappgtv_diz3, "date_time_dt", cast(skyappgtv_diz3$date_time, "date"))

skyappgtv_diz5 <- filter(skyappgtv_diz4, skyappgtv_diz4$date_time_dt >= (data_inizio - 185))
skyappgtv_diz6 <- distinct(select(skyappgtv_diz5, "external_id_post_evar", "post_visid_concatenated"))

write.parquet(skyappgtv_diz6, path_diz_appgtv)

skyappgtv_diz6 <- read.parquet(path_diz_appgtv)

skyappgtv_diz7 <- withColumnRenamed(skyappgtv_diz6, "external_id_post_evar", "skyid")
skyappgtv_diz8 <- withColumnRenamed(skyappgtv_diz7, "post_visid_concatenated", "cookieid")

createOrReplaceTempView(skyappgtv_diz8, "skyappgtv_diz8")
createOrReplaceTempView(skyappgtv_6, "skyappgtv_6")

join_skyappgtv_diz <- sql("select DISTINCT skyappgtv_diz8.skyid, skyappgtv_6.*
                          from skyappgtv_6
                          left join skyappgtv_diz8 
                          on skyappgtv_6.post_visid_concatenated = skyappgtv_diz8.cookieid ")

join_skyappgtv_diz_2 <-  drop(join_skyappgtv_diz,  "external_id_post_evar")
join_skyappgtv_diz_2 <-  withColumnRenamed(join_skyappgtv_diz_2, "skyid", "external_id_post_evar")

write.parquet(join_skyappgtv_diz_2, path_scarico_skyappgtv) # write scarico app wsc

appgtv <- read.parquet(path_scarico_skyappgtv) 

############################# APP SPORT ################################

skyappsport <- read.parquet(path_appsport)

skyappsport_1 <- select(skyappsport, "external_id_post_evar", 
                        "post_visid_concatenated", 
                        "visit_num", 
                        "date_time",
                        "post_channel",
                        "site_section_post_evar",
                        "post_page_event_value",
                        "post_mobiledevice",
                        "post_campaign",
                        "va_closer_id",
                        "mapped_connection_type_value",
                        "mapped_browser_type_value",
                        "mapped_country_value",
                        "mapped_language_value",
                        "mapped_os_value",
                        "mapped_resolution_value",
                        "mapped_browser_value",
                        "paid_search"
)

skyappsport_2 <- withColumn(skyappsport_1, "date_time_dt", cast(skyappsport_1$date_time, "date"))
skyappsport_3 <- withColumn(skyappsport_2, "date_time_ts", cast(skyappsport_2$date_time, "timestamp"))
# skyappsport_4 <- filter(skyappsport_3, "external_id_post_evar is not NULL")

# filtro temporale:

skyappsport_5 <- filter(skyappsport_3, skyappsport_3$date_time_dt >= data_inizio) 
skyappsport_6 <- filter(skyappsport_5, skyappsport_5$date_time_dt <= data_fine)

#label colonna last_touch_point:
skyappsport_6$last_touch_label <- ifelse(skyappsport_6$va_closer_id == 1, "editorial_recommendation",
                                         ifelse(skyappsport_6$va_closer_id == 2, "search_engine_organic",
                                                ifelse(skyappsport_6$va_closer_id == 3, "internal_referrer_w",
                                                       ifelse(skyappsport_6$va_closer_id == 4, "social_network",
                                                              ifelse(skyappsport_6$va_closer_id == 5, "direct_bookmark",
                                                                     ifelse(skyappsport_6$va_closer_id == 6, "referring_domains",
                                                                            ifelse(skyappsport_6$va_closer_id == 7, "paid_display",
                                                                                   ifelse(skyappsport_6$va_closer_id == 8, "paid_search",
                                                                                          ifelse(skyappsport_6$va_closer_id == 9, "paid_other",
                                                                                                 ifelse(skyappsport_6$va_closer_id == 10, "retargeting",
                                                                                                        ifelse(skyappsport_6$va_closer_id == 11, "social_player",
                                                                                                               ifelse(skyappsport_6$va_closer_id == 12, "direct_email_marketing",
                                                                                                                      ifelse(skyappsport_6$va_closer_id == 13, "paid_social",
                                                                                                                             ifelse(skyappsport_6$va_closer_id == 14, "newsletter",
                                                                                                                                    ifelse(skyappsport_6$va_closer_id == 15, "acquisition",
                                                                                                                                           ifelse(skyappsport_6$va_closer_id == 16, "AMP_w",
                                                                                                                                                  ifelse(skyappsport_6$va_closer_id == 17, "web_push_notification",
                                                                                                                                                         ifelse(skyappsport_6$va_closer_id == 18, "DEM_w",
                                                                                                                                                                ifelse(skyappsport_6$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                       ifelse(skyappsport_6$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                              ifelse(skyappsport_6$va_closer_id == 21, "internal",
                                                                                                                                                                                     ifelse(skyappsport_6$va_closer_id == 22, "affiliation",
                                                                                                                                                                                            ifelse(skyappsport_6$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                                   ifelse(skyappsport_6$va_closer_id == 24, "short_url",
                                                                                                                                                                                                          ifelse(skyappsport_6$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

# PER APP Sport VA FATTO IL DIZIONARIO E IL JOIN

skyappsport_diz <- distinct(select(skyappsport, "external_id_post_evar", "post_visid_concatenated", "date_time"))

skyappsport_diz2 <- filter(skyappsport_diz, isNotNull(skyappsport_diz$external_id_post_evar))
skyappsport_diz3 <- filter(skyappsport_diz2, isNotNull(skyappsport_diz2$post_visid_concatenated))

skyappsport_diz4 <- withColumn(skyappsport_diz3, "date_time_dt", cast(skyappsport_diz3$date_time, "date"))

skyappsport_diz5 <- filter(skyappsport_diz4, skyappsport_diz4$date_time_dt >= (data_inizio - 185))
skyappsport_diz6 <- distinct(select(skyappsport_diz5, "external_id_post_evar", "post_visid_concatenated"))

write.parquet(skyappsport_diz6, path_diz_appsport)

skyappsport_diz6 <- read.parquet(path_diz_appsport)
skyappsport_diz7 <- withColumnRenamed(skyappsport_diz6, "external_id_post_evar", "skyid")
skyappsport_diz8 <- withColumnRenamed(skyappsport_diz7, "post_visid_concatenated", "cookieid")

createOrReplaceTempView(skyappsport_diz8, "skyappsport_diz8")
createOrReplaceTempView(skyappsport_6, "skyappsport_6")

join_skyappsport_diz <- sql("select DISTINCT skyappsport_diz8.skyid, skyappsport_6.*
                            from skyappsport_6
                            left join skyappsport_diz8 
                            on skyappsport_6.post_visid_concatenated = skyappsport_diz8.cookieid ")

join_skyappsport_diz_2 <-  drop(join_skyappsport_diz,  "external_id_post_evar")
join_skyappsport_diz_2 <-  withColumnRenamed(join_skyappsport_diz_2, "skyid", "external_id_post_evar")

write.parquet(join_skyappsport_diz_2, path_scarico_skyappsport) # write scarico app wsc

appsport <- read.parquet(path_scarico_skyappsport) 

############################# APP XFACTOR ################################

skyappxfactor <- read.parquet(path_appxfactor)

skyappxfactor_1 <- select(skyappxfactor, "external_id_post_evar", 
                          "post_visid_concatenated", 
                          "visit_num", 
                          "date_time",
                          "post_channel",
                          "site_section_post_evar",
                          "post_page_event_value",
                          "post_mobiledevice",
                          "post_campaign",
                          "va_closer_id",
                          "mapped_connection_type_value",
                          "mapped_browser_type_value",
                          "mapped_country_value",
                          "mapped_language_value",
                          "mapped_os_value",
                          "mapped_resolution_value",
                          "mapped_browser_value",
                          "paid_search"
)

skyappxfactor_2 <- withColumn(skyappxfactor_1, "date_time_dt", cast(skyappxfactor_1$date_time, "date"))
skyappxfactor_3 <- withColumn(skyappxfactor_2, "date_time_ts", cast(skyappxfactor_2$date_time, "timestamp"))
# skyappxfactor_4 <- filter(skyappxfactor_3, "external_id_post_evar is not NULL")

# filtro temporale:

skyappxfactor_5 <- filter(skyappxfactor_3, skyappxfactor_3$date_time_dt >= data_inizio) 
skyappxfactor_6 <- filter(skyappxfactor_5, skyappxfactor_5$date_time_dt <= data_fine)

#label colonna last_touch_point:
skyappxfactor_6$last_touch_label <- ifelse(skyappxfactor_6$va_closer_id == 1, "editorial_recommendation",
                                           ifelse(skyappxfactor_6$va_closer_id == 2, "search_engine_organic",
                                                  ifelse(skyappxfactor_6$va_closer_id == 3, "internal_referrer_w",
                                                         ifelse(skyappxfactor_6$va_closer_id == 4, "social_network",
                                                                ifelse(skyappxfactor_6$va_closer_id == 5, "direct_bookmark",
                                                                       ifelse(skyappxfactor_6$va_closer_id == 6, "referring_domains",
                                                                              ifelse(skyappxfactor_6$va_closer_id == 7, "paid_display",
                                                                                     ifelse(skyappxfactor_6$va_closer_id == 8, "paid_search",
                                                                                            ifelse(skyappxfactor_6$va_closer_id == 9, "paid_other",
                                                                                                   ifelse(skyappxfactor_6$va_closer_id == 10, "retargeting",
                                                                                                          ifelse(skyappxfactor_6$va_closer_id == 11, "social_player",
                                                                                                                 ifelse(skyappxfactor_6$va_closer_id == 12, "direct_email_marketing",
                                                                                                                        ifelse(skyappxfactor_6$va_closer_id == 13, "paid_social",
                                                                                                                               ifelse(skyappxfactor_6$va_closer_id == 14, "newsletter",
                                                                                                                                      ifelse(skyappxfactor_6$va_closer_id == 15, "acquisition",
                                                                                                                                             ifelse(skyappxfactor_6$va_closer_id == 16, "AMP_w",
                                                                                                                                                    ifelse(skyappxfactor_6$va_closer_id == 17, "web_push_notification",
                                                                                                                                                           ifelse(skyappxfactor_6$va_closer_id == 18, "DEM_w",
                                                                                                                                                                  ifelse(skyappxfactor_6$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                         ifelse(skyappxfactor_6$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                                ifelse(skyappxfactor_6$va_closer_id == 21, "internal",
                                                                                                                                                                                       ifelse(skyappxfactor_6$va_closer_id == 22, "affiliation",
                                                                                                                                                                                              ifelse(skyappxfactor_6$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                                     ifelse(skyappxfactor_6$va_closer_id == 24, "short_url",
                                                                                                                                                                                                            ifelse(skyappxfactor_6$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

# PER APP XFACTOR VA FATTO IL DIZIONARIO E IL JOIN

skyappxfactor_diz <- distinct(select(skyappxfactor, "external_id_post_evar", "post_visid_concatenated", "date_time"))

skyappxfactor_diz2 <- filter(skyappxfactor_diz, isNotNull(skyappxfactor_diz$external_id_post_evar))
skyappxfactor_diz3 <- filter(skyappxfactor_diz2, isNotNull(skyappxfactor_diz2$post_visid_concatenated))

skyappxfactor_diz4 <- withColumn(skyappxfactor_diz3, "date_time_dt", cast(skyappxfactor_diz3$date_time, "date"))

skyappxfactor_diz5 <- filter(skyappxfactor_diz4, skyappxfactor_diz4$date_time_dt >= (data_inizio - 185))
skyappxfactor_diz6 <- distinct(select(skyappxfactor_diz5, "external_id_post_evar", "post_visid_concatenated"))

write.parquet(skyappxfactor_diz6, path_diz_appxfactor)

skyappxfactor_diz6 <- read.parquet(path_diz_appxfactor)

skyappxfactor_diz7 <- withColumnRenamed(skyappxfactor_diz6, "external_id_post_evar", "skyid")
skyappxfactor_diz8 <- withColumnRenamed(skyappxfactor_diz7, "post_visid_concatenated", "cookieid")

createOrReplaceTempView(skyappxfactor_diz8, "skyappxfactor_diz8")
createOrReplaceTempView(skyappxfactor_6, "skyappxfactor_6")

join_skyappxfactor_diz <- sql("select DISTINCT skyappxfactor_diz8.skyid, skyappxfactor_6.*
                              from skyappxfactor_6
                              left join skyappxfactor_diz8 on skyappxfactor_6.post_visid_concatenated = skyappxfactor_diz8.cookieid ")

join_skyappxfactor_diz_2 <-  drop(join_skyappxfactor_diz,  "external_id_post_evar")
join_skyappxfactor_diz_2 <-  withColumnRenamed(join_skyappxfactor_diz_2, "skyid", "external_id_post_evar")

write.parquet(join_skyappxfactor_diz_2, path_scarico_skyappxfactor) # write scarico app wsc

appxfactor <- read.parquet(path_scarico_skyappxfactor) 




