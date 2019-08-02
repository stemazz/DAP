
## Metriche e dimensioni legate ai VIDEO

source("connection_R.R")
options(scipen = 10000)

skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
video_metrics <- select(skyitdev, "external_id_post_evar",
                        "post_visid_high",
                        "post_visid_low",
                        "post_visid_concatenated",
                        "visit_num",
                        "date_time",
                        "videos_post_evar", # nome video
                        "video_domain_post_evar",
                        "video_url_post_evar",
                        "video_lenght_post_evar",
                        "canali_video_post_evar",
                        "sottocanali_video_post_evar",
                        "terzo_livello_video_post_evar",
                        "post_evar88" # tipologia player video
                        # eventi legati alla visione del video: 
                        # event76 (25% fruizione), event79 (50% fruizione), event74 (75% fruizione) e event73 (fruizione completa)
                        # event142 (secondi spesi sul video) SOLO PER VIDEO FLUID
                        # event72 (video time viewd)secondi spedi sul video) PER I VIDEO jwp SUI SITI SKY
)

video_metrics_2 <- withColumn(video_metrics, "date_time_dt", cast(video_metrics$date_time, "date"))
video_metrics_3 <- withColumn(video_metrics_2, "date_time_ts", cast(video_metrics_2$date_time, "timestamp"))



#chiudi sessione
sparkR.stop()
