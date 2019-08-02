
## CJ_UPSELLING_new 1
## Raccolta kpi dal campione


#apri sessione
source("connection_R.R")
options(scipen = 1000)




source("CJ_UP_Utils.R")





base_attivi <- read.df("/user/stefano.mazzucca/attivi_periodo_anno_def_join_v2.csv", source = "csv", header = "true", delimiter = ",")
View(head(base_attivi,100))
nrow(base_attivi)
# 2.367.931


base_up <- filter(base_attivi, "flag_up_periodo = 1")
View(head(base_up, 100))
nrow(base_up)
# 517.136

base_up_digital <- filter(base_attivi, "flag_up_periodo = 1 and FLG_DIGITAL = 1")
View(head(base_up_digital,100))
nrow(base_up_digital)
# 390.273

write.parquet(base_up_digital, "/user/stefano.mazzucca/CJ_UP_base_up_digital.parquet")


base_up_digital <- read.parquet("/user/stefano.mazzucca/CJ_UP_base_up_digital.parquet")
View(head(base_up_digital,100))
nrow(base_up_digital)
# 390.273

valdist_extid_dat <- distinct(select(base_up_digital, "COD_CLIENTE_CIFRATO", "DAT_EVENTO"))
nrow(valdist_extid_dat)
# 390.116
valdist_codcntr_dat <- distinct(select(base_up_digital, "COD_CONTRATTO", "DAT_EVENTO"))
nrow(valdist_codcntr_dat)
# 390.273


## Navigazioni corporate Sky.it ################################################################################################################################

skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")

skyit_1 <- select(skyitdev, "external_id_post_evar",
                  "post_visid_high",
                  "post_visid_low",
                  "post_visid_concatenated",
                  "visit_num",
                  "date_time",
                  "post_channel", 
                  "page_name_post_evar",
                  "page_url_pulita_post_prop",
                  "secondo_livello_post_prop",
                  "terzo_livello_post_prop",
                  "post_campaign", 
                  "tracking_code_30_giorni_post_evar", 
                  "va_closer_id", # last touch
                  "va_finder_id", # first touch
                  "hit_source",
                  "exclude_hit"
)

skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))

skyit_4 <- withColumn(skyit_3, "secondo_livello_prop", lower(skyit_3$secondo_livello_post_prop))
skyit_5 <- withColumn(skyit_4, "terzo_livello_prop", lower(skyit_4$terzo_livello_post_prop))

skyit_6 <- filter(skyit_5, "post_channel = 'corporate' or post_channel = 'Guidatv'")

skyit_6$last_touch_label <- ifelse(skyit_6$va_closer_id == 1, "editorial_reccomendation",
                                   ifelse(skyit_6$va_closer_id == 2, "search_engine_organic",
                                          ifelse(skyit_6$va_closer_id == 3, "internal_referrer_w",
                                                 ifelse(skyit_6$va_closer_id == 4, "social_network",
                                                        ifelse(skyit_6$va_closer_id == 5, "direct_bookmark",
                                                               ifelse(skyit_6$va_closer_id == 6, "referring_domains",
                                                                      ifelse(skyit_6$va_closer_id == 7, "paid_display",
                                                                             ifelse(skyit_6$va_closer_id == 8, "paid_search",
                                                                                    ifelse(skyit_6$va_closer_id == 9, "paid_other",
                                                                                           ifelse(skyit_6$va_closer_id == 10, "retargeting",
                                                                                                  ifelse(skyit_6$va_closer_id == 11, "social_player",
                                                                                                         ifelse(skyit_6$va_closer_id == 12, "direct_email_marketing",
                                                                                                                ifelse(skyit_6$va_closer_id == 13, "paid_social",
                                                                                                                       ifelse(skyit_6$va_closer_id == 14, "newsletter",
                                                                                                                              ifelse(skyit_6$va_closer_id == 15, "acquisition",
                                                                                                                                     ifelse(skyit_6$va_closer_id == 16, "AMP_w",
                                                                                                                                            ifelse(skyit_6$va_closer_id == 17, "web_push_notification",
                                                                                                                                                   ifelse(skyit_6$va_closer_id == 18, "DEM_w",
                                                                                                                                                          ifelse(skyit_6$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                 ifelse(skyit_6$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                        ifelse(skyit_6$va_closer_id == 21, "internal",
                                                                                                                                                                               ifelse(skyit_6$va_closer_id == 22, "affiliation",
                                                                                                                                                                                      ifelse(skyit_6$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                             ifelse(skyit_6$va_closer_id == 24, "short_url",
                                                                                                                                                                                                    ifelse(skyit_6$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

skyit_6$first_touch_label <- ifelse(skyit_6$va_finder_id == 1, "editorial_reccomendation",
                                    ifelse(skyit_6$va_finder_id == 2, "search_engine_organic",
                                           ifelse(skyit_6$va_finder_id == 3, "internal_referrer_w",
                                                  ifelse(skyit_6$va_finder_id == 4, "social_network",
                                                         ifelse(skyit_6$va_finder_id == 5, "direct_bookmark",
                                                                ifelse(skyit_6$va_finder_id == 6, "referring_domains",
                                                                       ifelse(skyit_6$va_finder_id == 7, "paid_display",
                                                                              ifelse(skyit_6$va_finder_id == 8, "paid_search",
                                                                                     ifelse(skyit_6$va_finder_id == 9, "paid_other",
                                                                                            ifelse(skyit_6$va_finder_id == 10, "retargeting",
                                                                                                   ifelse(skyit_6$va_finder_id == 11, "social_player",
                                                                                                          ifelse(skyit_6$va_finder_id == 12, "direct_email_marketing",
                                                                                                                 ifelse(skyit_6$va_finder_id == 13, "paid_social",
                                                                                                                        ifelse(skyit_6$va_finder_id == 14, "newsletter",
                                                                                                                               ifelse(skyit_6$va_finder_id == 15, "acquisition",
                                                                                                                                      ifelse(skyit_6$va_finder_id == 16, "AMP_w",
                                                                                                                                             ifelse(skyit_6$va_finder_id == 17, "web_push_notification",
                                                                                                                                                    ifelse(skyit_6$va_finder_id == 18, "DEM_w",
                                                                                                                                                           ifelse(skyit_6$va_finder_id == 19, "mobile_CPC_w",
                                                                                                                                                                  ifelse(skyit_6$va_finder_id == 20, "real_time_bidding",
                                                                                                                                                                         ifelse(skyit_6$va_finder_id == 21, "internal",
                                                                                                                                                                                ifelse(skyit_6$va_finder_id == 22, "affiliation",
                                                                                                                                                                                       ifelse(skyit_6$va_finder_id == 23, "unknown_channel",
                                                                                                                                                                                              ifelse(skyit_6$va_finder_id == 24, "short_url",
                                                                                                                                                                                                     ifelse(skyit_6$va_finder_id == 25, "IOL_W", "none")))))))))))))))))))))))))

# filtro temporale
skyit_7 <- filter(skyit_6,"date_time_dt >= '2017-03-01'")
skyit_8 <- filter(skyit_7,"date_time_dt <= '2018-05-31'")

View(head(skyit_8,100))


## Creo le chiavi ext_id con cookies (dizionario)

diz_1 <- distinct(select(skyitdev, "external_id_post_evar", "post_visid_concatenated"))
diz_2 <- filter(diz_1, "external_id_post_evar is NOT NULL")

ext_id <- distinct(select(base_up_digital, "COD_CLIENTE_CIFRATO"))
nrow(ext_id)
# 388.911

createOrReplaceTempView(ext_id, "ext_id")
createOrReplaceTempView(diz_2, "diz_2")

diz_ext_id_coo <- sql("select t2.*
                      from ext_id t1
                      left join diz_2 t2
                      on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(diz_ext_id_coo,"/user/stefano.mazzucca/CJ_UP_chiavi_coo.parquet")


chiavi_coo <- read.parquet("/user/stefano.mazzucca/CJ_UP_chiavi_coo.parquet")
View(head(chiavi_coo,100))
nrow(chiavi_coo)
# 3.358.331


## join tra base_up_digital e chiavi_cookie

createOrReplaceTempView(base_up_digital, "base_up_digital")
createOrReplaceTempView(chiavi_coo, "chiavi_coo")

base_up_digital_coo <- sql("select distinct t1.*, t2.post_visid_concatenated
                            from base_up_digital t1
                            inner join chiavi_coo t2
                              on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
View(head(base_up_digital_coo,100))
nrow(base_up_digital_coo)
# 3.366.081

write.parquet(base_up_digital_coo,"/user/stefano.mazzucca/CJ_UP_base_up_digital_coo.parquet")


base_up_digital_coo <- read.parquet("/user/stefano.mazzucca/CJ_UP_base_up_digital_coo.parquet")
View(head(base_up_digital_coo,100))
nrow(base_up_digital_coo)
# 3.366.081


## join tra base_up_digital e navigazioni (skyit_8)

createOrReplaceTempView(base_up_digital_coo,"base_up_digital_coo")
createOrReplaceTempView(skyit_8,"skyit_8")

nav_sez_corporate <- sql("SELECT t1.*, 
                                  t2.visit_num, t2.post_channel,
                                  t2.date_time, t2.date_time_dt, t2.date_time_ts,
                                  t2.page_name_post_evar, t2.page_url_pulita_post_prop, t2.secondo_livello_prop, t2.terzo_livello_prop,
                                  t2.post_campaign, t2.tracking_code_30_giorni_post_evar, t2.last_touch_label, t2.first_touch_label,
                                  t2.hit_source, t2.exclude_hit
                        FROM base_up_digital_coo t1
                        INNER JOIN skyit_8 t2
                          ON t1.post_visid_concatenated = t2.post_visid_concatenated")

write.parquet(nav_sez_corporate,"/user/stefano.mazzucca/CJ_UP_scarico_nav_corporate.parquet")

nav_sez_corporate <- read.parquet("/user/stefano.mazzucca/CJ_UP_scarico_nav_corporate.parquet")
View(head(nav_sez_corporate,100))
nrow(nav_sez_corporate)
# 47.630.594

valdist_extid_dat_ <- distinct(select(nav_sez_corporate, "COD_CLIENTE_CIFRATO", "DAT_EVENTO"))
nrow(valdist_extid_dat_)
# 376.710 (vs. 390.116 dai dati di input --> 13.406 "coppiette" perse) N.B. Verificare se le troviamo in WSC/APP



nav_sez_corporate_1 <- remove_too_short(nav_sez_corporate, 10)
View(head(nav_sez_corporate_1,100))
nrow(nav_sez_corporate_1)
# 39.427.758 (vs. 47.630.594 di scarico totale --> elimino 8.202.836 records di hit "too short")

nav_sez_corporate_2 <- withColumn(nav_sez_corporate_1, "data_upselling", cast(cast(unix_timestamp(nav_sez_corporate_1$DAT_EVENTO, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))
nav_sez_corporate_3 <- withColumn(nav_sez_corporate_2, "days_from_up", datediff(nav_sez_corporate_2$data_upselling, nav_sez_corporate_2$date_time_dt))
nav_sez_corporate_3$flg_3_m <- ifelse(nav_sez_corporate_3$days_from_up <= 90 & nav_sez_corporate_3$days_from_up >= 0, 1, 0)
#View(head(nav_sez_corporate_3,1000))
nav_sez_corporate_4 <- filter(nav_sez_corporate_3, "flg_3_m == 1")

write.parquet(nav_sez_corporate_4, "/user/stefano.mazzucca/CJ_UP_nav_corporate_filt.parquet")


nav_sez_corporate_4 <- read.parquet("/user/stefano.mazzucca/CJ_UP_nav_corporate_filt.parquet")
View(head(nav_sez_corporate_4,100))
nrow(nav_sez_corporate_4)
# 11.603.454 (vs. 39.427.758 di scarico filtrato ai 10 sec di navigazione --> 27.824.304 records di navigazione eliminati perche' fuori dai 3 mesi di interesse)


navigazione_1 <- add_kpi_sito(nav_sez_corporate_4)

navigazione_2 <- withColumn(navigazione_1, "flg_app_wsc", lit(0))
View(head(navigazione_2,100))

write.parquet(navigazione_2, "/user/stefano.mazzucca/CJ_UP_navigazione_desktop.parquet")



## Navigazioni APP WSC #######################################################################################################################################

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod/table=hitdata/hitdata.parquet")
View(head(skyappwsc,100))

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
                                # "secondo_livello_post_prop",
                                # "terzo_livello_post_prop",
                                "post_campaign", 
                                # "tracking_code_30_giorni_post_evar", 
                                "va_closer_id", # last touch
                                "va_finder_id", # first touch
                                "hit_source",
                                "exclude_hit"
)

skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_ts", cast(skyappwsc_2$date_time, "timestamp"))

skyappwsc_4 <- withColumn(skyappwsc_3, "site_section_prop", lower(skyappwsc_3$site_section))

skyappwsc_5 <- filter(skyappwsc_4,"external_id_post_evar is not NULL")

skyappwsc_5$last_touch_label <- ifelse(skyappwsc_5$va_closer_id == 1, "editorial_reccomendation",
                                   ifelse(skyappwsc_5$va_closer_id == 2, "search_engine_organic",
                                          ifelse(skyappwsc_5$va_closer_id == 3, "internal_referrer_w",
                                                 ifelse(skyappwsc_5$va_closer_id == 4, "social_network",
                                                        ifelse(skyappwsc_5$va_closer_id == 5, "direct_bookmark",
                                                               ifelse(skyappwsc_5$va_closer_id == 6, "referring_domains",
                                                                      ifelse(skyappwsc_5$va_closer_id == 7, "paid_display",
                                                                             ifelse(skyappwsc_5$va_closer_id == 8, "paid_search",
                                                                                    ifelse(skyappwsc_5$va_closer_id == 9, "paid_other",
                                                                                           ifelse(skyappwsc_5$va_closer_id == 10, "retargeting",
                                                                                                  ifelse(skyappwsc_5$va_closer_id == 11, "social_player",
                                                                                                         ifelse(skyappwsc_5$va_closer_id == 12, "direct_email_marketing",
                                                                                                                ifelse(skyappwsc_5$va_closer_id == 13, "paid_social",
                                                                                                                       ifelse(skyappwsc_5$va_closer_id == 14, "newsletter",
                                                                                                                              ifelse(skyappwsc_5$va_closer_id == 15, "acquisition",
                                                                                                                                     ifelse(skyappwsc_5$va_closer_id == 16, "AMP_w",
                                                                                                                                            ifelse(skyappwsc_5$va_closer_id == 17, "web_push_notification",
                                                                                                                                                   ifelse(skyappwsc_5$va_closer_id == 18, "DEM_w",
                                                                                                                                                          ifelse(skyappwsc_5$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                 ifelse(skyappwsc_5$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                        ifelse(skyappwsc_5$va_closer_id == 21, "internal",
                                                                                                                                                                               ifelse(skyappwsc_5$va_closer_id == 22, "affiliation",
                                                                                                                                                                                      ifelse(skyappwsc_5$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                             ifelse(skyappwsc_5$va_closer_id == 24, "short_url",
                                                                                                                                                                                                    ifelse(skyappwsc_5$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

skyappwsc_5$first_touch_label <- ifelse(skyappwsc_5$va_finder_id == 1, "editorial_reccomendation",
                                    ifelse(skyappwsc_5$va_finder_id == 2, "search_engine_organic",
                                           ifelse(skyappwsc_5$va_finder_id == 3, "internal_referrer_w",
                                                  ifelse(skyappwsc_5$va_finder_id == 4, "social_network",
                                                         ifelse(skyappwsc_5$va_finder_id == 5, "direct_bookmark",
                                                                ifelse(skyappwsc_5$va_finder_id == 6, "referring_domains",
                                                                       ifelse(skyappwsc_5$va_finder_id == 7, "paid_display",
                                                                              ifelse(skyappwsc_5$va_finder_id == 8, "paid_search",
                                                                                     ifelse(skyappwsc_5$va_finder_id == 9, "paid_other",
                                                                                            ifelse(skyappwsc_5$va_finder_id == 10, "retargeting",
                                                                                                   ifelse(skyappwsc_5$va_finder_id == 11, "social_player",
                                                                                                          ifelse(skyappwsc_5$va_finder_id == 12, "direct_email_marketing",
                                                                                                                 ifelse(skyappwsc_5$va_finder_id == 13, "paid_social",
                                                                                                                        ifelse(skyappwsc_5$va_finder_id == 14, "newsletter",
                                                                                                                               ifelse(skyappwsc_5$va_finder_id == 15, "acquisition",
                                                                                                                                      ifelse(skyappwsc_5$va_finder_id == 16, "AMP_w",
                                                                                                                                             ifelse(skyappwsc_5$va_finder_id == 17, "web_push_notification",
                                                                                                                                                    ifelse(skyappwsc_5$va_finder_id == 18, "DEM_w",
                                                                                                                                                           ifelse(skyappwsc_5$va_finder_id == 19, "mobile_CPC_w",
                                                                                                                                                                  ifelse(skyappwsc_5$va_finder_id == 20, "real_time_bidding",
                                                                                                                                                                         ifelse(skyappwsc_5$va_finder_id == 21, "internal",
                                                                                                                                                                                ifelse(skyappwsc_5$va_finder_id == 22, "affiliation",
                                                                                                                                                                                       ifelse(skyappwsc_5$va_finder_id == 23, "unknown_channel",
                                                                                                                                                                                              ifelse(skyappwsc_5$va_finder_id == 24, "short_url",
                                                                                                                                                                                                     ifelse(skyappwsc_5$va_finder_id == 25, "IOL_W", "none")))))))))))))))))))))))))

# filtro temporale
skyappwsc_6 <- filter(skyappwsc_5,"date_time_dt >= '2017-03-01'")
skyappwsc_7 <- filter(skyappwsc_6,"date_time_dt <= '2018-05-31'")

View(head(skyappwsc_7,100))


## join tra base_up_digital e navigazioni (skyappwsc_7)

createOrReplaceTempView(base_up_digital_coo,"base_up_digital_coo")
createOrReplaceTempView(skyappwsc_7,"skyappwsc_7")

nav_app_wsc <- sql("SELECT t1.*, 
                          t2.visit_num, t2.post_channel,
                          t2.date_time, t2.date_time_dt, t2.date_time_ts,
                          t2.page_name_post_evar, t2.page_url_post_prop, t2.site_section_prop,
                          t2.post_campaign, t2.last_touch_label, t2.first_touch_label,
                          t2.hit_source, t2.exclude_hit
                  FROM base_up_digital_coo t1
                  INNER JOIN skyappwsc_7 t2
                    ON t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(nav_app_wsc,"/user/stefano.mazzucca/CJ_UP_scarico_nav_app_wsc.parquet")


nav_app_wsc <- read.parquet("/user/stefano.mazzucca/CJ_UP_scarico_nav_app_wsc.parquet")
View(head(nav_app_wsc,100))
nrow(nav_app_wsc)
# 509.293.743

valdist_extid_dat_ <- distinct(select(nav_app_wsc, "COD_CLIENTE_CIFRATO", "DAT_EVENTO"))
nrow(valdist_extid_dat_)
# 230.957 (vs. 390.116 dai dati di input --> 159.159 "coppiette" perse) N.B. Verificare se le troviamo in Sito Desktop



nav_app_wsc_1 <- remove_too_short(nav_app_wsc, 10)
View(head(nav_app_wsc_1,100))
nrow(nav_app_wsc_1)
# 21.612.334 (vs. 509.293.743 di scarico totale --> elimino 487.681.409 records di hit "too short")

nav_app_wsc_2 <- withColumn(nav_app_wsc_1, "data_upselling", cast(cast(unix_timestamp(nav_app_wsc_1$DAT_EVENTO, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))
nav_app_wsc_3 <- withColumn(nav_app_wsc_2, "days_from_up", datediff(nav_app_wsc_2$data_upselling, nav_app_wsc_2$date_time_dt))
nav_app_wsc_3$flg_3_m <- ifelse(nav_app_wsc_3$days_from_up <= 90 & nav_app_wsc_3$days_from_up >= 0, 1, 0)
#View(head(nav_app_wsc_3,1000))
nav_app_wsc_4 <- filter(nav_app_wsc_3, "flg_3_m == 1")

write.parquet(nav_app_wsc_4, "/user/stefano.mazzucca/CJ_UP_nav_app_wsc_filt.parquet")


nav_app_wsc_4 <- read.parquet("/user/stefano.mazzucca/CJ_UP_nav_app_wsc_filt.parquet")
View(head(nav_app_wsc_4,100))
nrow(nav_app_wsc_4)
# 6.387.802 (vs. 21.612.334 di scarico filtrato ai 10 sec di navigazione --> 15.224.532 records di navigazione eliminati perche' fuori dai 3 mesi di interesse)


navigazione_3 <- add_kpi_app(nav_app_wsc_4)

navigazione_4 <- withColumn(navigazione_3, "flg_app_wsc", lit(1))
View(head(navigazione_4,100))

write.parquet(navigazione_4, "/user/stefano.mazzucca/CJ_UP_navigazione_app.parquet")


## Unisco le navigazioni desktop e app ##########################################################################################################################

nav_desktop <- read.parquet("/user/stefano.mazzucca/CJ_UP_navigazione_desktop.parquet")
View(head(nav_desktop,100))
nrow(nav_desktop)
# 11.603.454

nav_desktop_1 <- filter(nav_desktop, "evento_digital is NOT NULL")
nrow(nav_desktop_1)
# 8.459.303

nav_desktop_2 <- select(nav_desktop_1, "COD_CONTRATTO", "COD_CLIENTE_CIFRATO", "data_upselling", "date_time_dt", "date_time_ts", 
                        "days_from_up", "evento_digital", "flg_app_wsc", "post_visid_concatenated", "visit_num", 
                        "post_campaign", "last_touch_label", "first_touch_label")
View(head(nav_desktop_2,100))


nav_app <- read.parquet("/user/stefano.mazzucca/CJ_UP_navigazione_app.parquet")
View(head(nav_app,100))
nrow(nav_app)
# 6.387.802

nav_app_1 <- filter(nav_app, "evento_digital is NOT NULL")
nrow(nav_app_1)
# 6.036.689

nav_app_2 <- select (nav_app_1, "COD_CONTRATTO", "COD_CLIENTE_CIFRATO", "data_upselling", "date_time_dt", "date_time_ts", 
                     "days_from_up", "evento_digital", "flg_app_wsc", "post_visid_concatenated", "visit_num", 
                     "post_campaign", "last_touch_label", "first_touch_label")
View(head(nav_app_2,100))


navigazioni_target <- rbind(nav_desktop_2, nav_app_2)
navigazioni_target_1 <- arrange(navigazioni_target, desc(navigazioni_target$COD_CONTRATTO), asc(navigazioni_target$date_time_ts))
View(head(navigazioni_target_1,100))
nrow(navigazioni_target_1)
# 14.495.992


valdist_extid_dat_target <- distinct(select(navigazioni_target_1, "COD_CLIENTE_CIFRATO", "data_upselling"))
nrow(valdist_extid_dat_target)
# 310.066 (vs. 390.116 inziiali)
valdist_codcntr_dat_target <- distinct(select(navigazioni_target_1, "COD_CONTRATTO", "data_upselling"))
nrow(valdist_codcntr_dat_target)
# 310.076 (vs. 390.273 iniziali)


write.parquet(navigazioni_target_1, "/user/stefano.mazzucca/CJ_UP_navigazione_hit_target.parquet")



## Navigazioni raggruppate per visite ##########################################################################################################################

navigazioni_target <- read.parquet("/user/stefano.mazzucca/CJ_UP_navigazione_hit_target.parquet")
View(head(navigazioni_target, 1000))
nrow(navigazioni_target)
# 14.495.992


navigazioni_target_1 <- withColumn(navigazioni_target, "visit_uniq", concat_ws('-', navigazioni_target$post_visid_concatenated, navigazioni_target$visit_num))
#View(head(navigazioni_target_1, 100))

createOrReplaceTempView(navigazioni_target_1, "navigazioni_target_1")
navigazioni_visits_target <- sql("select first(COD_CONTRATTO) as COD_CONTRATTO,
                                          first(COD_CLIENTE_CIFRATO) as COD_CLIENTE_CIFRATO,
                                          first(data_upselling) as data_upselling,
                                          first(date_time_dt) as date_time_dt,
                                          first(date_time_ts) as date_time_ts,
                                          first(days_from_up) as days_from_up,
                                          evento_digital, 
                                          flg_app_wsc
                                          -- first(post_campaign) as post_campaign,
                                          -- first(last_touch_label) as last_touch_label,
                                          -- first(first_touch_label) as first_touch_label
                                 from navigazioni_target_1
                                 group by visit_uniq, evento_digital, flg_app_wsc")

navigazioni_visits_target_1 <- arrange(navigazioni_visits_target, desc(navigazioni_visits_target$COD_CONTRATTO), asc(navigazioni_visits_target$date_time_ts))
View(head(navigazioni_visits_target_1,1000))
nrow(navigazioni_visits_target_1)
# 5.979.358


write.parquet(navigazioni_visits_target_1, "/user/stefano.mazzucca/CJ_UP_navigazione_visit_target.parquet")



## Ricavo DEM e SMS click da post_campaign ######################################################################################################################

navigazioni_target <- read.parquet("/user/stefano.mazzucca/CJ_UP_navigazione_hit_target.parquet")
View(head(navigazioni_target, 1000))
nrow(navigazioni_target)
# 14.495.992

navigazioni_target_1 <- withColumn(navigazioni_target, "visit_uniq", concat_ws('-', navigazioni_target$post_visid_concatenated, navigazioni_target$visit_num))
navigazioni_target_2 <- withColumn(navigazioni_target_1, "post_campaign_lowercase",  lower(navigazioni_target_1$post_campaign))
#View(head(navigazioni_target_1,100))

navigazioni_target_3 <- filter(navigazioni_target_2, "post_campaign_lowercase is NOT NULL")
nrow(navigazioni_target_3)
# 2.601.534

createOrReplaceTempView(navigazioni_target_3, "navigazioni_target_3")
dem_sms_click <- sql("select first(COD_CONTRATTO) as COD_CONTRATTO,
                             first(COD_CLIENTE_CIFRATO) as COD_CLIENTE_CIFRATO,
                             first(data_upselling) as data_upselling,
                             first(date_time_dt) as date_time_dt,
                             first(date_time_ts) as date_time_ts,
                             first(days_from_up) as days_from_up,
                             first(post_campaign_lowercase) as evento_digital,
                             first(flg_app_wsc) as flg_app_wsc
                     from navigazioni_target_3
                     group by visit_uniq")
View(head(dem_sms_click,1000))
nrow(dem_sms_click)
# 542.782

createOrReplaceTempView(dem_sms_click, "dem_sms_click")
dem_sms_click_1 <- sql("select *, 
                              case when evento_digital LIKE '%dem%' then 'dem'
                              when evento_digital LIKE '%sms%' then 'sms'
                              else Null end as flg_dem_sms
                       from dem_sms_click")
View(head(dem_sms_click_1,1000))

dem_sms_click_1$evento_digital <- ifelse(dem_sms_click_1$flg_dem_sms == 'dem' | dem_sms_click_1$flg_dem_sms == 'sms', 
                                         dem_sms_click_1$flg_dem_sms, NA)
View(head(dem_sms_click_1,1000))

dem_sms_click_2 <- filter(dem_sms_click_1, "evento_digital is NOT NULL")
dem_sms_click_2$flg_dem_sms <- NULL
dem_sms_click_3 <- arrange(dem_sms_click_2, desc(dem_sms_click_2$COD_CONTRATTO), asc(dem_sms_click_2$date_time_ts))
View(head(dem_sms_click_3,1000))
nrow(dem_sms_click_3)
# 227.649

valdist_extid <- distinct(select(dem_sms_click_2, "COD_CLIENTE_CIFRATO"))
nrow(valdist_extid)
# 88.313


write.parquet(dem_sms_click_3, "/user/stefano.mazzucca/CJ_UP_dem_sms_click_target.parquet")



## Estraggo CLICK da adform ######################################################################################################################################

click <- read.parquet('/STAGE/adform/table=Click/Click.parquet')
View(head(click,100))
# nrow(click)
# # --

click_1 <- filter(click, "IsRobot = 'No'")
click_2 <- filter(click_1, "CookieID <> 0 and CookieID is NOT NULL")
click_3 <- withColumn(click_2, "date_time", cast(click_2$yyyymmdd, 'string'))
click_4 <- withColumn(click_3, "date_time_dt", cast(cast(unix_timestamp(click_3$date_time, 'yyyyMMdd'), 'timestamp'), 'date'))
# View(head(click_4,100))
click_5 <- filter(click_4, "date_time_dt <= '2018-05-31' and date_time_dt >= '2017-03-01'")

createOrReplaceTempView(click_5, "click_5")
click_6 <- sql("select *, regexp_extract(unloadvars.visibility, '\"visible1\":(\\\\w+)',1) as visibility
                from click_5
                having (visibility <> 'false' or visibility is NOT NULL)")

click_7 <- withColumn(click_6, "date_time_ts", cast(unix_timestamp(click_6$Timestamp, 'yyyy-MM-dd HH:mm:ss'), 'timestamp'))
View(head(click_7,1000))


base_up_digital <- read.parquet("/user/stefano.mazzucca/CJ_UP_base_up_digital.parquet")
View(head(base_up_digital,100))
nrow(base_up_digital)
# 390.273


#########################################################################################################################
## Leggo tracking point per costruire dizionario cookieid-skyid  ########################################################

trackingpoint <- read.parquet("/STAGE/adform/table=Trackingpoint/Trackingpoint.parquet")
View(head(trackingpoint, 100))

trackingpoint_1 <- filter(trackingpoint, "IsRobot = 'No'")
trackingpoint_2 <- filter(trackingpoint_1, "IsNoRepeats = 'Yes'")
trackingpoint_3 <- filter(trackingpoint_2, "CookieID <> 0 and CookieID is NOT NULL")
trackingpoint_4 <- withColumn(trackingpoint_3, "data", cast(trackingpoint_3$yyyymmdd, 'string'))
trackingpoint_5 <- withColumn(trackingpoint_4, "data_dt", cast(cast(unix_timestamp(trackingpoint_4$data, 'yyyyMMdd'), 'timestamp'), 'date'))

createOrReplaceTempView(trackingpoint_5, "trackingpoint_5")

cookieid_skyid_adform <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                                      CookieID, `device-name`,`os-name`,`browser-name`
                             from trackingpoint_5
                             where customvars.systemvariables like '%13%' and CookieID <> '0' -- and 
                                    -- data_dt >= '2017-03-01' and data_dt <= '2018-05-31'
                             having (skyid <> '' and length(skyid)= 48)")

createOrReplaceTempView(cookieid_skyid_adform, "cookieid_skyid_adform")
count_ <- sql("select count(distinct skyid) 
              from cookieid_skyid_adform")
View(head(count_,100))
# --


write.parquet(cookieid_skyid_adform, "/user/stefano.mazzucca/CJ_UP_chiavi_coo_adform.parquet")


## Aggancio skyid al cookie su tutti i convertenti ###################################################################


cookieid_skyid_adform <- read.parquet("/user/stefano.mazzucca/CJ_UP_chiavi_coo_adform.parquet")
View(head(cookieid_skyid_adform,100))
nrow(cookieid_skyid_adform)
# 54.635.580

createOrReplaceTempView(base_up_digital, "base_up_digital")
createOrReplaceTempView(cookieid_skyid_adform, "cookieid_skyid_adform")

base_up_digital_coo_adform <- sql("select t1.*, t2.cookieid , t2.`device-name` , t2.`os-name` , t2.`browser-name`
                                   from base_up_digital t1
                                   left join cookieid_skyid_adform t2
                                    on t1.COD_CLIENTE_CIFRATO = t2.skyid")


write.parquet(base_up_digital_coo_adform, "/user/stefano.mazzucca/CJ_UP_base_up_digital_coo_adform.parquet")


base_up_digital_coo_adform <- read.parquet("/user/stefano.mazzucca/CJ_UP_base_up_digital_coo_adform.parquet")
View(head(base_up_digital_coo_adform,100))
nrow(base_up_digital_coo_adform)
# 8.901.271

#########################################################################################################################
#########################################################################################################################

## join tra base_up_digital e click (click_7)

createOrReplaceTempView(base_up_digital_coo_adform,"base_up_digital_coo_adform")
createOrReplaceTempView(click_7,"click_7")

click_adform_target <- sql("SELECT t1.*, 
                                    t2.date_time_dt,
                                    t2.date_time_ts,
                                    t2.`campaign-name`
                           FROM base_up_digital_coo_adform t1
                           INNER JOIN click_7 t2
                           ON t1.cookieid = t2.cookieid")
# first(flg_app_wsc) as flg_app_wsc

write.parquet(click_adform_target,"/user/stefano.mazzucca/CJ_UP_scarico_click_adform.parquet")


click_adform_target <- read.parquet("/user/stefano.mazzucca/CJ_UP_scarico_click_adform.parquet")
View(head(click_adform_target,100))
nrow(click_adform_target)
# 9.217.159

valdist_extid_dat_ <- distinct(select(click_adform_target, "COD_CLIENTE_CIFRATO", "DAT_EVENTO"))
nrow(valdist_extid_dat_)
# 281.450 (vs. 390.116 dai dati di input --> 108.666 "coppiette" perse) 


click_adform_target_1 <- withColumn(click_adform_target, "data_upselling", cast(cast(unix_timestamp(click_adform_target$DAT_EVENTO, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))

click_adform_target_2 <- withColumn(click_adform_target_1, "days_from_up", datediff(click_adform_target_1$data_upselling, click_adform_target_1$date_time_dt))

click_adform_target_2$flg_3_m <- ifelse(click_adform_target_2$days_from_up <= 90 & click_adform_target_2$days_from_up >= 0, 1, 0)

click_adform_target_3 <- filter(click_adform_target_2, "flg_3_m == 1")
View(head(click_adform_target_3,1000))
nrow(click_adform_target_3)
# 2.132.445

valdist_extid_dat_ <- distinct(select(click_adform_target_3, "COD_CLIENTE_CIFRATO", "DAT_EVENTO"))
nrow(valdist_extid_dat_)
# 190.118 (vs. 390.116 dai dati di input --> 199.998 "coppiette" perse) 

click_adform_target_4 <- select(click_adform_target_3, "COD_CONTRATTO", "COD_CLIENTE_CIFRATO", "data_upselling", "date_time_dt", "date_time_ts", "days_from_up", 
                                "`campaign-name`")
click_adform_target_5 <- withColumn(click_adform_target_4, "flg_app_wsc", lit(0))
click_adform_target_6 <- withColumn(click_adform_target_5, "evento_digital", lit('adv_click'))

write.parquet(click_adform_target_6, "/user/stefano.mazzucca/CJ_UP_click_adv_target.parquet")



## Estraggo IMPRESSION da adform ################################################################################################################################


impression <- read.parquet('/STAGE/adform/table=Impression/Impression.parquet')
View(head(impression,100))
# nrow(impression)
# # 9.310.724.907

impression_1 <- filter(impression, "IsRobot = 'No'")
impression_2 <- filter(impression_1, "CookieID <> 0 and CookieID is NOT NULL")
impression_3 <- withColumn(impression_2, "date_time", cast(impression_2$yyyymmdd, 'string'))
impression_4 <- withColumn(impression_3, "date_time_dt", cast(cast(unix_timestamp(impression_3$date_time, 'yyyyMMdd'), 'timestamp'), 'date'))
impression_5 <- filter(impression_4, "date_time_dt <= '2018-05-31' and date_time_dt >= '2017-03-01'")
# View(head(impression_5,100))

createOrReplaceTempView(impression_5, "impression_5")
impression_6 <- sql("select *, 
                    regexp_extract(unloadvars.visibility, '\"visible1\":(\\\\w+)',1) as visibility
                    from impression_5
                    having visibility LIKE 'true'")

impression_7 <- withColumn(impression_6, "date_time_ts", cast(unix_timestamp(impression_6$Timestamp, 'yyyy-MM-dd HH:mm:ss'), 'timestamp'))
View(head(impression_7,1000))


base_up_digital_coo_adform <- read.parquet("/user/stefano.mazzucca/CJ_UP_base_up_digital_coo_adform.parquet")
View(head(base_up_digital_coo_adform,100))
nrow(base_up_digital_coo_adform)
# 8.901.271

#########################################################################################################################
#########################################################################################################################

## join tra base_up_digital e impression (impression_7)

createOrReplaceTempView(base_up_digital_coo_adform,"base_up_digital_coo_adform")
createOrReplaceTempView(impression_7,"impression_7")

impression_adform_target <- sql("SELECT t1.*, 
                                         t2.date_time_dt,
                                         t2.date_time_ts,
                                         t2.`campaign-name`
                                 FROM base_up_digital_coo_adform t1
                                 INNER JOIN impression_7 t2
                                 ON t1.cookieid = t2.cookieid")

write.parquet(impression_adform_target,"/user/stefano.mazzucca/CJ_UP_scarico_impression_adform.parquet")


impression_adform_target <- read.parquet("/user/stefano.mazzucca/CJ_UP_scarico_impression_adform.parquet")
View(head(impression_adform_target,100))
nrow(impression_adform_target)
# 2.023.696.284

valdist_extid_dat_ <- distinct(select(impression_adform_target, "COD_CLIENTE_CIFRATO", "DAT_EVENTO"))
nrow(valdist_extid_dat_)
# 371.875 (vs. 390.116 dai dati di input --> 18.241 "coppiette" perse) 


impression_adform_target_1 <- withColumn(impression_adform_target, "data_upselling", cast(cast(unix_timestamp(impression_adform_target$DAT_EVENTO, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))

impression_adform_target_2 <- withColumn(impression_adform_target_1, "days_from_up", datediff(impression_adform_target_1$data_upselling, impression_adform_target_1$date_time_dt))

impression_adform_target_2$flg_3_m <- ifelse(impression_adform_target_2$days_from_up <= 90 & impression_adform_target_2$days_from_up >= 0, 1, 0)

impression_adform_target_3 <- filter(impression_adform_target_2, "flg_3_m == 1")
View(head(impression_adform_target_3,1000))
nrow(impression_adform_target_3)
# --

valdist_extid_dat_ <- distinct(select(impression_adform_target_3, "COD_CLIENTE_CIFRATO", "DAT_EVENTO"))
nrow(valdist_extid_dat_)
# -- (vs. 390.116 dai dati di input --> -- "coppiette" perse) 

impression_adform_target_4 <- select(impression_adform_target_3, "COD_CONTRATTO", "COD_CLIENTE_CIFRATO", "data_upselling", "date_time_dt", "date_time_ts", "days_from_up", 
                                "`campaign-name`")
impression_adform_target_5 <- withColumn(impression_adform_target_4, "flg_app_wsc", lit(0))
impression_adform_target_6 <- withColumn(impression_adform_target_5, "evento_digital", lit('adv_impression'))

write.parquet(impression_adform_target_6, "/user/stefano.mazzucca/CJ_UP_impression_adv_target.parquet")



##################################################################################################################################################################
##################################################################################################################################################################
## Recap e finalizzazione lista degli eventi digital ##
##################################################################################################################################################################
##################################################################################################################################################################


# Navigazioni_hit
# write.parquet(navigazioni_target_1, "/user/stefano.mazzucca/CJ_UP_navigazione_hit_target.parquet")

# Navigazioni_visite
# write.parquet(navigazioni_visits_target_1, "/user/stefano.mazzucca/CJ_UP_navigazione_visit_target.parquet")
nav_visite_target <- read.parquet("/user/stefano.mazzucca/CJ_UP_navigazione_visit_target.parquet")
View(head(nav_visite_target,100))
nrow(nav_visite_target)
# 5.979.358

# Click da DEM o SMS
# write.parquet(dem_sms_click_2, "/user/stefano.mazzucca/CJ_UP_dem_sms_click_target.parquet")
dem_sms_click <- read.parquet("/user/stefano.mazzucca/CJ_UP_dem_sms_click_target.parquet")
View(head(dem_sms_click, 100))
nrow(dem_sms_click)
# 227.649

# Click ADV (tutti)
# write.parquet(click_adform_target_6, "/user/stefano.mazzucca/CJ_UP_click_adv_target.parquet")
adv_click <- read.parquet("/user/stefano.mazzucca/CJ_UP_click_adv_target.parquet")
View(head(adv_click,100))
nrow(adv_click)
# 2.132.445


nav_visite_target_1 <- select(nav_visite_target, "COD_CONTRATTO", "COD_CLIENTE_CIFRATO", "data_upselling", "date_time_dt", "date_time_ts", "days_from_up", 
                              "evento_digital", "flg_app_wsc")
View(head(nav_visite_target_1,100))


adv_click_1 <- distinct(select(adv_click, "COD_CONTRATTO", "COD_CLIENTE_CIFRATO", "data_upselling", "date_time_dt", "date_time_ts", "days_from_up", 
                                "evento_digital", "flg_app_wsc"))
View(head(adv_click_1,1000))


lista_eventi_digital_target_upselling <- rbind(nav_visite_target_1, dem_sms_click, adv_click_1)
lista_eventi_digital_target_upselling_1 <- arrange(lista_eventi_digital_target_upselling, desc(lista_eventi_digital_target_upselling$COD_CONTRATTO), 
                                                   asc(lista_eventi_digital_target_upselling$date_time_ts))
View(head(lista_eventi_digital_target_upselling_1,100))


write.parquet(lista_eventi_digital_target_upselling_1, "/user/stefano.mazzucca/CJ_UP_lista_eventi_digital.parquet")



## Lista finale ##
lista_eventi_digital <- read.parquet("/user/stefano.mazzucca/CJ_UP_lista_eventi_digital.parquet")
View(head(lista_eventi_digital,1000))
nrow(lista_eventi_digital)
# 8.044.554

valdist_eventi_digital <- distinct(select(lista_eventi_digital, "evento_digital"))
View(head(valdist_eventi_digital,100))


write.df(repartition( lista_eventi_digital, 1), path = "/user/stefano.mazzucca/CJ_UP_lista_eventi_digital.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




# Impression ADV (tutti)
# write.parquet(impression_adform_target_6, "/user/stefano.mazzucca/CJ_UP_impression_adv_target.parquet")
adv_impression <- read.parquet("/user/stefano.mazzucca/CJ_UP_impression_adv_target.parquet")
View(head(adv_impression,100))
nrow(adv_impression)
# 425.745.997

adv_impression_1 <- distinct(select(adv_impression, "COD_CONTRATTO", "COD_CLIENTE_CIFRATO", "data_upselling", "date_time_dt", "date_time_ts", "days_from_up", 
                               "evento_digital", "flg_app_wsc"))
adv_impression_2 <- arrange(adv_impression_1, desc(adv_impression_1$COD_CONTRATTO), asc(adv_impression_1$date_time_ts))
View(head(adv_impression_2,1000))




#chiudi sessione
sparkR.stop()
