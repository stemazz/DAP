
## Scarico skyitdev + SkyAppWSC

source("connection_R.R")
options(scipen = 10000)



skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
skyit_1 <- select(skyitdev, "external_id_post_evar",
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
                  "video_domain_post_evar",
                  "canali_video_post_evar",
                  "sottocanali_video_post_evar",
                  "terzo_livello_video_post_evar"
)
skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))

# filtro temporale
skyit_5 <- filter(skyit_3, skyit_3$date_time_dt >= "2019-05-01") 



skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod/table=hitdata/hitdata.parquet")
skyappwsc_1 <- select(skyappwsc, "external_id_post_evar",
                      "post_visid_high",
                      "post_visid_low",
                      "post_visid_concatenated",
                      "visit_num",
                      "date_time",
                      "post_channel", 
                      "page_name_post_evar",
                      "page_name_post_prop", 
                      "page_url_post_prop",
                      "site_section",
                      "hit_source",
                      "exclude_hit",
                      "post_page_event_value"
)
skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_ts", cast(skyappwsc_2$date_time, "timestamp"))
skyappwsc_4 <- filter(skyappwsc_3, "external_id_post_evar is not NULL")

# filtro temporale
skyappwsc_5 <- filter(skyappwsc_4, skyappwsc_4$date_time_dt >= "2019-05-01") 



write.parquet(skyit_5, "/user/stefano.mazzucca/scarico_skyitdev")
write.parquet(skyappwsc_5, "/user/stefano.mazzucca/scarico_appwsc")

##################################################################################################################################################################


skyitdev_scarico <- read.parquet("/user/stefano.mazzucca/scarico_skyitdev")
View(head(skyitdev_scarico,100))
nrow(skyitdev_scarico)
# 1.430.175.116


summ_date <- arrange(summarize(groupBy(skyitdev_scarico, skyitdev_scarico$date_time_dt), count_hit = count(skyitdev_scarico$post_visid_concatenated)), "date_time_dt")
View(head(summ_date,1000))


skyitdev_settimana <- filter(skyitdev_scarico, skyitdev_scarico$date_time_dt >= as.Date("2019-06-14") & skyitdev_scarico$date_time_dt <= as.Date("2019-06-16"))
View(head(skyitdev_settimana,1000))
nrow(skyitdev_settimana)
# 187.497.352

corporate_domenica <- filter(skyitdev_settimana, "(post_channel = 'corporate' OR post_channel = 'Guidatv') AND date_time_dt = '2019-06-16'")
View(head(corporate_domenica,100))
nrow(corporate_domenica)
# 485.007

# summ_1_livello <- summarize(groupBy(corporate_settimana, corporate_settimana$post_channel), count_hit = count(corporate_settimana$post_visid_concatenated))
# View(head(summ_1_livello))

corporate_domenica_2 <- withColumn(corporate_domenica, "string_app", lit("A-"))
corporate_domenica_3 <- withColumn(corporate_domenica_2, "cookieID", concat(corporate_domenica_2$string_app, corporate_domenica_2$post_visid_concatenated))
corporate_domenica_3$string_app <- NULL
corporate_domenica_3$post_visid_concatenated <- NULL
View(head(corporate_domenica_3,100))



skyappwsc_scarico <- read.parquet("/user/stefano.mazzucca/scarico_appwsc")
View(head(skyappwsc_scarico,100))
nrow(skyappwsc_scarico)
# 10.487.392

appwsc_domenica <- filter(skyappwsc_scarico, skyappwsc_scarico$date_time_dt == "2019-06-16")
View(head(appwsc_domenica,100))
nrow(appwsc_domenica)
# 106.660

appwsc_domenica_1 <- withColumn(appwsc_domenica, "string_app", lit("A-"))
appwsc_domenica_2 <- withColumn(appwsc_domenica_1, "cookieID", concat(appwsc_domenica_1$string_app, appwsc_domenica_1$post_visid_concatenated))
appwsc_domenica_2$post_visid_high <- NULL
appwsc_domenica_2$post_visid_low <- NULL
appwsc_domenica_2$string_app <- NULL


################

write.df(repartition(corporate_domenica_3, 1), "/user/stefano.mazzucca/scarico_corporate_domenica", source = "csv", header = T, delimiter = ";", mode = "overwrite")
write.df(repartition(appwsc_domenica_2, 1), "/user/stefano.mazzucca/scarico_appwsc_domenica", source = "csv", header = T, delimiter = ";", mode = "overwrite")




