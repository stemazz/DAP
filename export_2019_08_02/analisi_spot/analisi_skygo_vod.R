
## Analisi per Skygo VOD


source("connection_R.R")
options(scipen = 10000)


################################################################################################################################################################
################################################################################################################################################################
## Skygo APP (Adobe)
################################################################################################################################################################
################################################################################################################################################################

skygo_app <- read.parquet("/STAGE/adobe/reportsuite=skyappanywhere/table=hitdata/hitdata.parquet")
View(head(skygo_app,100))
nrow(skygo_app)
# 
skygo_app_1 <- select(skygo_app, "visitor_id_post_evar",
                      "post_visid_concatenated",
                      "visit_num",
                      "date_time",
                      "post_channel", 
                      # "page_name_post_evar",
                      # "page_name_post_prop", 
                      "page_url_post_prop",
                      # "site_section",
                      "hit_source",
                      "exclude_hit",
                      "post_page_event_value"
)
View(head(skygo_app_1,100))


createOrReplaceTempView(skygo_app_1, "skygo_app_1")
valdist_post_channel <- sql("select post_channel, count(*) as count
                            from skygo_app_1
                            group by post_channel")
View(head(valdist_post_channel,100))

write.df(repartition( valdist_post_channel, 1), path = "/user/stefano.mazzucca/export_post_channel_skygo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################
################################################################################################################################################################
## Skygo APP (Adform)
################################################################################################################################################################
################################################################################################################################################################

trackingpoint_path <- "hdfs:///STAGE/adform/table=Trackingpoint"

# CLEANING TRACKING POINT 

trackingpoint <- read.parquet(trackingpoint_path)
trackingpoint$Timestamp <- cast(trackingpoint$Timestamp, "timestamp")

trackingpoint <- select(trackingpoint, c("TransactionId",
                                         "Timestamp",
                                         "CookieID",
                                         "PageURL",
                                         "DeviceTypeId",
                                         "customvars",
                                         "page",
                                         "device-name",
                                         "city",
                                         "region",
                                         "country",
                                         "browser-name",
                                         "os-name",
                                         "yyyymmdd")
)
View(head(trackingpoint,100))

# # Filtro per ora
# trackingpoint <- filter(trackingpoint, trackingpoint$Timestamp >= "2017-10-28 18:00:00" )
# trackingpoint <- filter(trackingpoint, trackingpoint$Timestamp <= "2017-10-28 20:00:00" )

#### Aggiungo Skyid a tp
trackingpoint$skyid <- regexp_extract(trackingpoint$customvars.systemvariables, '"13":"(\\w+)"', 1)

# Tolgo nulli
createOrReplaceTempView(trackingpoint, "trackingpoint")
trackingpoint <- filter(trackingpoint, "skyid IS NOT NULL")
trackingpoint$customvars <- NULL

# Rinomino device-name
trackingpoint <- withColumnRenamed(trackingpoint, "device-name", "devicename")

# Tengo solo trackingpoint Sky Go
tp_skygo_list <- c("A105266:Home Page SkyGo - Post Login",
                   "A110746:PostLogin",
                   "A110761:PostLogin"
)

trackingpoint_sg <- trackingpoint[trackingpoint$page %in% tp_skygo_list,]
View(head(trackingpoint_sg,100))



# trackingpoint_sg$Timestamp <- cast(trackingpoint_sg$Timestamp, "timestamp")
# trackingpoint_sg <- filter(trackingpoint_sg, trackingpoint_sg$yyyymmdd == "20171028")
# trackingpoint_sg$hour <- hour(trackingpoint_sg$Timestamp)
# trackingpoint_sg <- filter(trackingpoint_sg, trackingpoint_sg$hour == 18 | trackingpoint_sg$hour == 19)



################################################################################################################################################################
################################################################################################################################################################
## Skygo WEB (Adobe)
################################################################################################################################################################
################################################################################################################################################################

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
                  "secondo_livello_post_prop",
                  "terzo_livello_post_prop",
                  "hit_source",
                  "exclude_hit",
                  "post_page_event_value"
)
skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))
skyit_4 <- filter(skyit_3, "post_channel = 'skygo'")


## to be continued...










#chiudi sessione
sparkR.stop()
