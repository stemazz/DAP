
## scarico navigazione cookie


source("connection_R.R")
options(scipen = 10000)


## Navigazioni sito
skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
skyit_1 <- select(skyitdev, "external_id_post_evar",
                  "post_visid_high",
                  "post_visid_low",
                  "post_visid_concatenated",
                  "visit_num",
                  "date_time",
                  "post_channel", 
                  "page_name_post_evar",
                  "page_url_post_evar",
                  "page_url_pulita_post_evar",
                  "secondo_livello_post_prop",
                  "terzo_livello_post_prop",
                  "hit_source",
                  "exclude_hit",
                  "post_page_event_value",
                  "canale_d'acquisto_post_evar",
                  "os",
                  "post_mobiledevice",
                  "mapped_browser_type_value",
                  "mapped_os_value"
)

###########################################################################################
createOrReplaceTempView(skyit_1, "skyit_1")
valdist_os <- sql("select mapped_os_value, count(*) as count
                  from skyit_1
                  group by mapped_os_value")
valdist_os <- arrange(valdist_os, desc(valdist_os$count))
write.parquet(valdist_os, "/user/stefano.mazzucca/clustering_corporate/valdist_os.oparquet")

valdist_os <- read.parquet("/user/stefano.mazzucca/clustering_corporate/valdist_os.parquet")
valdist_os <- arrange(valdist_os, desc(valdist_os$count))
View(head(valdist_os,100))
# Mobile == "Windows Phone", "Mobile iOS", "Android", "Macintosh (iPhone)"
###########################################################################################

skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))
skyit_4 <- filter(skyit_3, "post_channel = 'corporate' or post_channel = 'Guidatv'")

# filtro temporale
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt >= '2018-05-01')
skyit_6 <- filter(skyit_5, skyit_5$date_time_dt <= '2018-12-03')


write.parquet(skyit_6, "/user/stefano.mazzucca/clustering_corporate/scarico_skyitdev.parquet", mode = "overwrite")



## Naviagazioni APP
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
skyappwsc_5 <- filter(skyappwsc_4, skyappwsc_4$date_time_dt >= '2018-05-01') 
skyappwsc_6 <- filter(skyappwsc_5, skyappwsc_5$date_time_dt <= '2018-12-03')


write.parquet(skyappwsc_6, "/user/stefano.mazzucca/clustering_corporate/scarico_skyappwsc.parquet")





scarico_skyitdev <- read.parquet("/user/stefano.mazzucca/clustering_corporate/scarico_skyitdev.parquet")
View(head(scarico_skyitdev,100))
nrow(scarico_skyitdev)
# 251.260.513
# createOrReplaceTempView(scarico_skyitdev, "scarico_skyitdev")
# valdist_post_page_event_value <- sql("select post_page_event_value, count(*) as count
#                                      from scarico_skyitdev
#                                      group by post_page_event_value")
# View(head(valdist_post_page_event_value,100))


scarico_skyappwsc <- read.parquet("/user/stefano.mazzucca/clustering_corporate/scarico_skyappwsc.parquet")
View(head(scarico_skyappwsc,100))
nrow(scarico_skyappwsc)
# 91.626.671
# createOrReplaceTempView(scarico_skyappwsc, "scarico_skyappwsc")
# valdist_post_page_event_value <- sql("select post_page_event_value, count(*) as count
#                                      from scarico_skyappwsc
#                                      group by post_page_event_value")
# View(head(valdist_post_page_event_value,100))


#chiudi sessione
sparkR.stop()
