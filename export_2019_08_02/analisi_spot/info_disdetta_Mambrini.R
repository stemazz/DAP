
## Analisi spot (x Mambrini) info-disdetta


source("connection_R.R")
options(scipen = 10000)


## Navigazioni SITO

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
skyit_4 <- filter(skyit_3, "post_channel = 'corporate' or post_channel = 'Guidatv'")

# filtro temporale
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt >= '2018-08-01') 
skyit_6 <- filter(skyit_5, skyit_5$date_time_dt <= '2018-10-29')

info_disdetta_target <- filter(skyit_6, "page_name_post_evar LIKE 'sky:assistenza:info disdetta' OR page_name_post_evar LIKE 'sky:assistenza:info-disdetta' OR 
                         page_name_post_evar LIKE 'sky:assistenza:info disdetta:tim sky' OR page_name_post_evar LIKE 'sky:assistenza:info-disdetta:sky' OR
                         page_name_post_evar LIKE 'sky:assistenza:info disdetta:sky'")


write.parquet(info_disdetta_target, "/user/stefano.mazzucca/scarico_info_disdetta_Mambrini.parquet", mode = "overwrite")


info_disdetta_target <- read.parquet("/user/stefano.mazzucca/scarico_info_disdetta_Mambrini.parquet")
View(head(info_disdetta_target,100))
nrow(info_disdetta_target)
# 878.857


write.df(repartition( info_disdetta_target, 1), path = "/user/stefano.mazzucca/export_info_disdetta_Mambrini.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



createOrReplaceTempView(info_disdetta_target, "info_disdetta_target")
valdist_extid <- sql("select distinct external_id_post_evar, count(*) as count
                     from info_disdetta_target
                     group by external_id_post_evar")
View(head(valdist_extid,100))
nrow(valdist_extid)
# 126.993

valdist_page <- sql("select distinct page_name_post_evar, count(*) as count
                     from info_disdetta_target
                     group by page_name_post_evar")
View(head(valdist_page,100))
# page_name_post_evar                       count
# sky:assistenza:info disdetta:sky          577917
# sky:assistenza:info disdetta:tim sky      21509
# sky:assistenza:info-disdetta:sky          17
# sky:assistenza:info-disdetta              263208
# sky:assistenza:info disdetta              16206





#chiudi sessione
sparkR.stop()

