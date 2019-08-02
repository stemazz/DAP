
## Analisi spot (x Mambrini) landing page


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
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt >= '2018-07-02') 
skyit_6 <- filter(skyit_5, skyit_5$date_time_dt <= '2018-10-28')

landing_target <- filter(skyit_6, "(page_name_post_evar LIKE 'sky:landing:clienti:ups%' and page_name_post_evar LIKE '%cmn%') or 
                         page_name_post_evar LIKE 'sky:landing:clienti:ups:aggiungi-pacchetto%'")


write.parquet(landing_target, "/user/stefano.mazzucca/scarico_landing_Mambrini.parquet", mode = "overwrite")


landing_target <- read.parquet("/user/stefano.mazzucca/scarico_landing_Mambrini.parquet")
View(head(landing_target,100))
nrow(landing_target)
# 1.233.672

# prova <- filter(landing_target, "page_name_post_evar LIKE 'sky:landing:clienti:ups%'")
# View(head(prova,100))

write.df(repartition( landing_target, 1), path = "/user/stefano.mazzucca/export_landing_Mambrini.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



createOrReplaceTempView(landing_target, "landing_target")
valdist_extid <- sql("select distinct external_id_post_evar, count(*) as count
                     from landing_target
                     group by external_id_post_evar")
View(head(valdist_extid,100))
nrow(valdist_extid)
# 343.183




#chiudi sessione
sparkR.stop()

