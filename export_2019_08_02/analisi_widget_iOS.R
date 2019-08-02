
## Analisi Widget iOS: Sport vs TG24


source("connection_R.R")
options(scipen = 10000)


# dal 1 maggio 2019
# sport e tg24 traffico solo mobile, direct, sistema operativo iOS, (prop27) entry page tempale = foglia articolo oppure photogallery oppure liveblog


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
                  
                  "mapped_os_value",

                  "page_template_post_prop",
                  
                  "va_closer_id"
)
skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))

# filtro temporale
skyit_4 <- filter(skyit_3, skyit_3$date_time_dt >= "2019-05-01") 

skyit_4$last_touch_label <- ifelse(skyit_4$va_closer_id == 1, "editorial_recommendation",
                                   ifelse(skyit_4$va_closer_id == 2, "search_engine_organic",
                                          ifelse(skyit_4$va_closer_id == 3, "internal_referrer_w",
                                                 ifelse(skyit_4$va_closer_id == 4, "social_network",
                                                        ifelse(skyit_4$va_closer_id == 5, "direct_bookmark",
                                                               ifelse(skyit_4$va_closer_id == 6, "referring_domains",
                                                                      ifelse(skyit_4$va_closer_id == 7, "paid_display",
                                                                             ifelse(skyit_4$va_closer_id == 8, "paid_search",
                                                                                    ifelse(skyit_4$va_closer_id == 9, "paid_other",
                                                                                           ifelse(skyit_4$va_closer_id == 10, "retargeting",
                                                                                                  ifelse(skyit_4$va_closer_id == 11, "social_player",
                                                                                                         ifelse(skyit_4$va_closer_id == 12, "direct_email_marketing",
                                                                                                                ifelse(skyit_4$va_closer_id == 13, "paid_social",
                                                                                                                       ifelse(skyit_4$va_closer_id == 14, "newsletter",
                                                                                                                              ifelse(skyit_4$va_closer_id == 15, "acquisition",
                                                                                                                                     ifelse(skyit_4$va_closer_id == 16, "AMP_w",
                                                                                                                                            ifelse(skyit_4$va_closer_id == 17, "web_push_notification",
                                                                                                                                                   ifelse(skyit_4$va_closer_id == 18, "DEM_w",
                                                                                                                                                          ifelse(skyit_4$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                 ifelse(skyit_4$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                        ifelse(skyit_4$va_closer_id == 21, "internal",
                                                                                                                                                                               ifelse(skyit_4$va_closer_id == 22, "affiliation",
                                                                                                                                                                                      ifelse(skyit_4$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                             ifelse(skyit_4$va_closer_id == 24, "short_url",
                                                                                                                                                                                                    ifelse(skyit_4$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))


createOrReplaceTempView(skyit_4,'skyit_4')
skyit_5 <- sql('SELECT *,
                  CASE WHEN (mapped_os_value LIKE "%iOS%") THEN 1 ELSE 0 END AS flag_iOS,
                  CASE WHEN (mapped_os_value LIKE "%Android%") THEN 1 ELSE 0 END AS flag_Android
                FROM skyit_4')

write.parquet(skyit_5, "/user/stefano.mazzucca/scarico_analisi_widget_IOS.parquet")

##################################################################################################################################################################

df <- read.parquet("/user/stefano.mazzucca/scarico_analisi_widget_IOS.parquet")
View(head(df,100))
nrow(df)
# 1.531.353.548


df_1 <- filter(df, df$last_touch_label == "direct_bookmark")

template = summarize(groupBy(df_1, df_1$page_template_post_prop), count_hit = count(df_1$post_visid_concatenated))
View(head(template,100))

df_2 <- filter(df_1, df_1$page_template_post_prop == "photogallery" | df_1$page_template_post_prop == "liveblog" | df_1$page_template_post_prop == "foglia articolo")
View(head(df_2,100))
nrow(df_2)
# 31.440.082

df_3 <- filter(df_2, df_2$flag_iOS == 1 | df_2$flag_Android == 1)
nrow(df_3)
# 28.245.328

df_4 <- filter(df_3, (df_3$post_channel == "sport" | (df_3$post_channel == "tg24" & df_3$flag_iOS == 1)))
View(head(df_4,100))
nrow(df_4)
# 26.938.843

df_5 <-withColumn(df_4, "visita", concat(df_4$post_visid_concatenated, df_4$visit_num))
View(head(df_5,100))

# prova <- filter(df_5, df_5$visita == "9137377246090934059840954837789334069424")
# View(head(prova,100))

df_6 <- summarize(groupBy(df_5, df_5$visita), first_visit = min(df_5$date_time_ts), count_hit = count(df_5$post_visid_concatenated), 
                  date_time_dt = min(df_5$date_time_dt),
                  external_id = first(df_5$external_id_post_evar), 
                  cookieID = first(df_5$post_visid_concatenated), 
                  primo_livello = first(df_5$post_channel), 
                  page_name = first(df_5$page_name_post_evar),
                  secondo_livello = first(df_5$secondo_livello_post_prop),
                  terzo_livello = first(df_5$terzo_livello_post_prop),
                  page_template = first(df_5$page_template_post_prop),
                  last_touch_label = first(df_5$last_touch_label),
                  flg_iOS = max(df_5$flag_iOS),
                  flg_Android = max(df_5$flag_Android))
View(head(df_6,100))

write.parquet(df_6, "/user/stefano.mazzucca/df_analisi_widget_IOS.parquet", mode = "overwrite")


analisi <- read.parquet("/user/stefano.mazzucca/df_analisi_widget_IOS.parquet")
View(head(analisi,100))
nrow(analisi)
# 12.380.725

## Focus 1 giugno: #############################################################################

analisi_1 <- filter(analisi, analisi$date_time_dt == "2019-06-01")
View(head(analisi_1,100))
nrow(analisi_1)
# 400.763

analisi_1 <- withColumn(analisi_1, "timestamp", unix_timestamp(analisi_1$first_visit, 'yyyy-MM-dd HH:mm:ss'))

analisi_1_sport <- filter(analisi_1, analisi_1$primo_livello == "sport")
nrow(analisi_1_sport)
# 382.441

analisi_1_tg24 <- filter(analisi_1, analisi_1$primo_livello == "tg24")
nrow(analisi_1_tg24)
# 18.322


gp_analisi_1_sport <- summarize(groupBy(analisi_1_sport, analisi_1_sport$timestamp), count_visits = count(analisi_1_sport$visita),
                                flg_iOS = sum(analisi_1_sport$flg_iOS),
                                flg_Android = sum(analisi_1_sport$flg_Android))
View(head(gp_analisi_1_sport,100))


gp_analisi_1_tg24 <- summarize(groupBy(analisi_1_tg24, analisi_1_tg24$timestamp), count_visits = count(analisi_1_tg24$visita),
                                flg_iOS = sum(analisi_1_tg24$flg_iOS),
                                flg_Android = sum(analisi_1_tg24$flg_Android))
View(head(gp_analisi_1_tg24,100))



write.df(repartition(gp_analisi_1_sport, 1), "/user/stefano.mazzucca/export_sport_analisi_widget_IOS.parquet", source = "csv", header = T, delimiter = ";")
write.df(repartition(gp_analisi_1_tg24, 1), "/user/stefano.mazzucca/export_tg24_analisi_widget_IOS.parquet", source = "csv", header = T, delimiter = ";")


## Dataset completo #############################################################################

analisi <- withColumn(analisi, "timestamp", unix_timestamp(analisi$first_visit, 'yyyy-MM-dd HH:mm:ss'))


analisi_sport <- filter(analisi, analisi$primo_livello == "sport")
nrow(analisi_sport)
# 9.188.701

analisi_tg24 <- filter(analisi, analisi$primo_livello == "tg24")
nrow(analisi_tg24)
# 3.192.024


gp_analisi_sport <- summarize(groupBy(analisi_sport, analisi_sport$timestamp), count_visits = count(analisi_sport$visita),
                                flg_iOS = sum(analisi_sport$flg_iOS),
                                flg_Android = sum(analisi_sport$flg_Android))
View(head(gp_analisi_sport,100))


gp_analisi_tg24 <- summarize(groupBy(analisi_tg24, analisi_tg24$timestamp), count_visits = count(analisi_tg24$visita),
                               flg_iOS = sum(analisi_tg24$flg_iOS),
                               flg_Android = sum(analisi_tg24$flg_Android))
View(head(gp_analisi_tg24,100))



write.df(repartition(gp_analisi_sport, 1), "/user/stefano.mazzucca/export_completo_sport_analisi_widget_IOS.parquet", source = "csv", header = T, delimiter = ";")
write.df(repartition(gp_analisi_tg24, 1), "/user/stefano.mazzucca/export_completo_tg24_analisi_widget_IOS.parquet", source = "csv", header = T, delimiter = ";")






