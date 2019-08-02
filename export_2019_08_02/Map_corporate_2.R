

#apri sessione
source("connection_R.R")
options(scipen = 1000)



#################################################################################################################################################################
#################################################################################################################################################################
## Report Suite = Skyitdev
#################################################################################################################################################################
#################################################################################################################################################################

skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
#View(head(skyitdev,100))

skyit_1 <- select(skyitdev, "external_id_post_evar",
                  "post_visid_high",
                  "post_visid_low",
                  "post_visid_concatenated",
                  "visit_num",
                  "date_time",
                  "post_channel", 
                  "page_name_post_evar",
                  "page_url_pulita_post_prop",
                  "page_url_pulita_post_evar",
                  #"download_name_post_prop",
                  "secondo_livello_post_prop",
                  "secondo_livello_post_evar",
                  "terzo_livello_post_prop",
                  "terzo_livello_post_evar",
                  "post_campaign", 
                  "tracking_code_30_giorni_post_evar", 
                  "va_closer_id", # last touch
                  "va_finder_id", # first touch
                  # capire di quali refferal si parla:
                  "first_hit_ref_domain",
                  "first_hit_ref_type",
                  "first_hit_referrer",
                  "visit_referrer",
                  # capire come utilizzare questi parametri:
                  "chiave_ricerca_interna_corporate_post_prop",
                  "chiave_ricerca_interna_corporate_post_evar",
                  "aes_search_post_evar",
                  "hit_source",
                  "exclude_hit"
)

skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))

skyit_4 <- withColumn(skyit_3, "secondo_livello_prop", lower(skyit_3$secondo_livello_post_prop))
skyit_5 <- withColumn(skyit_4, "terzo_livello_prop", lower(skyit_4$terzo_livello_post_prop))

skyit_6 <- withColumn(skyit_5, "secondo_livello_evar", lower(skyit_5$secondo_livello_post_evar))
skyit_7 <- withColumn(skyit_6, "terzo_livello_evar", lower(skyit_6$terzo_livello_post_evar))


skyit_8 <- filter(skyit_7, "post_channel = 'corporate' or post_channel = 'Guidatv'")
#View(head(skyit_8,1000))


skyit_8$last_touch_label <- ifelse(skyit_8$va_closer_id == 1, "editorial_reccomendation",
                                   ifelse(skyit_8$va_closer_id == 2, "search_engine_organic",
                                          ifelse(skyit_8$va_closer_id == 3, "internal_referrer_w",
                                                 ifelse(skyit_8$va_closer_id == 4, "social_network",
                                                        ifelse(skyit_8$va_closer_id == 5, "direct_bookmark",
                                                               ifelse(skyit_8$va_closer_id == 6, "referring_domains",
                                                                      ifelse(skyit_8$va_closer_id == 7, "paid_display",
                                                                             ifelse(skyit_8$va_closer_id == 8, "paid_search",
                                                                                    ifelse(skyit_8$va_closer_id == 9, "paid_other",
                                                                                           ifelse(skyit_8$va_closer_id == 10, "retargeting",
                                                                                                  ifelse(skyit_8$va_closer_id == 11, "social_player",
                                                                                                         ifelse(skyit_8$va_closer_id == 12, "direct_email_marketing",
                                                                                                                ifelse(skyit_8$va_closer_id == 13, "paid_social",
                                                                                                                       ifelse(skyit_8$va_closer_id == 14, "newsletter",
                                                                                                                              ifelse(skyit_8$va_closer_id == 15, "acquisition",
                                                                                                                                     ifelse(skyit_8$va_closer_id == 16, "AMP_w",
                                                                                                                                            ifelse(skyit_8$va_closer_id == 17, "web_push_notification",
                                                                                                                                                   ifelse(skyit_8$va_closer_id == 18, "DEM_w",
                                                                                                                                                          ifelse(skyit_8$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                 ifelse(skyit_8$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                        ifelse(skyit_8$va_closer_id == 21, "internal",
                                                                                                                                                                               ifelse(skyit_8$va_closer_id == 22, "affiliation",
                                                                                                                                                                                      ifelse(skyit_8$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                             ifelse(skyit_8$va_closer_id == 24, "short_url",
                                                                                                                                                                                                    ifelse(skyit_8$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

skyit_8$first_touch_label <- ifelse(skyit_8$va_finder_id == 1, "editorial_reccomendation",
                                    ifelse(skyit_8$va_finder_id == 2, "search_engine_organic",
                                           ifelse(skyit_8$va_finder_id == 3, "internal_referrer_w",
                                                  ifelse(skyit_8$va_finder_id == 4, "social_network",
                                                         ifelse(skyit_8$va_finder_id == 5, "direct_bookmark",
                                                                ifelse(skyit_8$va_finder_id == 6, "referring_domains",
                                                                       ifelse(skyit_8$va_finder_id == 7, "paid_display",
                                                                              ifelse(skyit_8$va_finder_id == 8, "paid_search",
                                                                                     ifelse(skyit_8$va_finder_id == 9, "paid_other",
                                                                                            ifelse(skyit_8$va_finder_id == 10, "retargeting",
                                                                                                   ifelse(skyit_8$va_finder_id == 11, "social_player",
                                                                                                          ifelse(skyit_8$va_finder_id == 12, "direct_email_marketing",
                                                                                                                 ifelse(skyit_8$va_finder_id == 13, "paid_social",
                                                                                                                        ifelse(skyit_8$va_finder_id == 14, "newsletter",
                                                                                                                               ifelse(skyit_8$va_finder_id == 15, "acquisition",
                                                                                                                                      ifelse(skyit_8$va_finder_id == 16, "AMP_w",
                                                                                                                                             ifelse(skyit_8$va_finder_id == 17, "web_push_notification",
                                                                                                                                                    ifelse(skyit_8$va_finder_id == 18, "DEM_w",
                                                                                                                                                           ifelse(skyit_8$va_finder_id == 19, "mobile_CPC_w",
                                                                                                                                                                  ifelse(skyit_8$va_finder_id == 20, "real_time_bidding",
                                                                                                                                                                         ifelse(skyit_8$va_finder_id == 21, "internal",
                                                                                                                                                                                ifelse(skyit_8$va_finder_id == 22, "affiliation",
                                                                                                                                                                                       ifelse(skyit_8$va_finder_id == 23, "unknown_channel",
                                                                                                                                                                                              ifelse(skyit_8$va_finder_id == 24, "short_url",
                                                                                                                                                                                                     ifelse(skyit_8$va_finder_id == 25, "IOL_W", "none")))))))))))))))))))))))))

write.parquet(skyit_8, "/user/stefano.mazzucca/scarico_corporate_guidatv_20180712.parquet")

sito_completo <- read.parquet("/user/stefano.mazzucca/scarico_corporate_guidatv_20180712.parquet")
View(head(sito_completo,1000))
nrow(sito_completo)
# 664.318.214


## Minima e massima data ###########################################################################

createOrReplaceTempView(sito_completo, "sito_completo")
date_sito <- sql("select min(date_time_dt) as min_data, max(date_time_dt) as max_data
                     from sito_completo")
View(head(date_sito,100))
# min_data        max_data
# 2016-05-19     2018-07-11


## Sezione Guidatv #################################################################################

sito_guidatv <- filter(sito_completo, "post_channel = 'Guidatv'")
View(head(sito_guidatv,100))
nrow(sito_guidatv)
# 85.346.738

# createOrReplaceTempView(sito_guidatv, "sito_guidatv")
# valdist_2_livello_guidatv <- sql("select secondo_livello_prop, count(post_visid_concatenated)
#                                  from sito_guidatv
#                                  group by secondo_livello_prop")
# View(head(valdist_2_livello_guidatv,100))

valdist_2_livello_guidatv_ <- summarize(groupBy(sito_guidatv, "secondo_livello_prop"), count = count(sito_completo$post_visid_concatenated))
View(head(valdist_2_livello_guidatv_,100))


## msite ##########################################################################################

msite <- filter(sito_completo, "page_name_post_evar LIKE '%msite%'")
View(head(msite,1000))
nrow(msite)
# 26.471

createOrReplaceTempView(msite, "msite")
date_msite <- sql("select min(date_time_dt) as min_data, max(date_time_dt) as max_data
                     from msite")
View(head(date_msite,100))
# min_data        max_data
# 2016-06-29      2018-07-11

date_msite_1 <- sql("select date_time_dt, count(post_visid_concatenated)
                    from msite
                    group by date_time_dt
                    order by date_time_dt")
View(head(date_msite_1,100))
date_msite_2 <- arrange(date_msite_1, desc(date_msite_1$date_time_dt))
View(head(date_msite_2,100))

sec_liv_msite <- summarize(groupBy(msite, "secondo_livello_prop"), count = count(msite$post_visid_concatenated))
View(head(sec_liv_msite,100))
# secondo_livello_prop     count
# NA                       1246
# offerta mobile           21754
# aol                      3455


################################################################################################################################################################
## Verifica delle sezioni ##
################################################################################################################################################################

pac_off <- filter(sito_completo, "page_url_pulita_post_prop LIKE '%pacchetti-offerte%'")
View(head(pac_off,100))
nrow(pac_off)
# 24.976.287

# pac_off_msite <- withColumn(pac_off, "msite_prova", substr(pac_off$page_url_pulita_post_prop, 1, 20))
# View(head(pac_off_msite,100))
# 
# valdist_msite <- summarize(groupBy(pac_off_msite, "msite_prova"), count = count(pac_off_msite$post_visid_concatenated))
# View(head(valdist_msite,100))


acquista <- filter(sito_completo, "page_url_pulita_post_prop LIKE '%sky.it/acquista%' or page_url_pulita_post_prop LIKE '%abbonamento.sky.it/aol%'")
View(head(acquista,10000))
nrow(acquista)
# 29.507.905

# valdist_sec_liv <- summarize(groupBy(acquista, "secondo_livello_prop"), count = count(acquista$post_visid_concatenated))
# View(head(valdist_sec_liv,100))
# valdist_url <- summarize(groupBy(acquista, "page_url_pulita_post_prop"), count = count(acquista$post_visid_concatenated))
# View(head(valdist_url,1000))


tecnologia <- filter(sito_completo, "page_url_pulita_post_prop LIKE '%sky.it/tecnologia%'")
View(head(tecnologia,100))
nrow(tecnologia)
# 22.120.335

# valdist_sec_liv <- summarize(groupBy(tecnologia, "secondo_livello_prop"), count = count(tecnologia$post_visid_concatenated))
# View(head(valdist_sec_liv,100))
# 
# prova <- filter(tecnologia, "secondo_livello_prop = 'd=v59'")
# View(head(prova,100))


sito_guidatv <- filter(sito_completo, "post_channel = 'Guidatv'")
View(head(sito_guidatv,100))
nrow(sito_guidatv)
# 85.346.738
guidatv <- summarize(groupBy(sito_guidatv, "secondo_livello_prop"), count = count(sito_guidatv$post_visid_concatenated))
View(head(guidatv,100))
nrow(guidatv)
# 48

# prova <- filter(sito_guidatv, "secondo_livello_prop = 'pagine di servizio'")
# View(head(prova,100))


extra <- filter(sito_completo, "page_url_pulita_post_prop LIKE '%sky.it/extra%'")
View(head(extra,100))
nrow(extra)
# 37.748.523

# valdist_sec_liv <- summarize(groupBy(extra, "secondo_livello_prop"), count = count(extra$post_visid_concatenated))
# View(head(valdist_sec_liv,100))


assistenza <- filter(sito_completo, "page_url_pulita_post_prop LIKE '%sky.it/assistenza%'")
View(head(assistenza, 100))
nrow(assistenza)
# 82.363.327

# valdist_sec_liv <- summarize(groupBy(assistenza, "secondo_livello_prop"), count = count(assistenza$post_visid_concatenated))
# View(head(valdist_sec_liv,100))

assistenza_conosci <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/conosci%'")
View(head(assistenza_conosci,100))
nrow(assistenza_conosci)
# 23.903.553
assistenza_gestisci <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/gestisci%'")
View(head(assistenza_gestisci,100))
nrow(assistenza_gestisci)
# 13.979.658
assistenza_risolvi <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/risolvi%'")
View(head(assistenza_risolvi,100))
nrow(assistenza_risolvi)
# 7.222.184
assistenza_skyq <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/skyq%'")
View(head(assistenza_skyq,100))
nrow(assistenza_skyq)
# 736.239
assistenza_contatta <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/contatta%'")
View(head(assistenza_contatta,100))
nrow(assistenza_contatta)
# 10.108.994
assistenza_benvenuto <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/benvenuto%'")
View(head(assistenza_benvenuto,100))
nrow(assistenza_benvenuto)
# 724.033

## AGGIUNGI SERVIZI SPACCATI
assistenza_servizi <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/index%'")
View(head(assistenza_servizi,100))
nrow(assistenza_servizi)
# 7.942.581
assistenza_sky_locator <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/spazio-sky/locator%'")
View(head(assistenza_sky_locator,100))
nrow(assistenza_sky_locator)
# 
assistenza_condomini <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/skyready%'")
View(head(assistenza_condomini,100))
nrow(assistenza_condomini)
# 372.999
assistenza_sky_expert <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/sky-expert%'")
View(head(assistenza_sky_expert,100))
nrow(assistenza_sky_expert)
# 263.225
assistenza_trova_sky_service <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/trova_skyservice%'")
View(head(assistenza_trova_sky_service,100))
nrow(assistenza_trova_sky_service)
# 2.711.178
assistenza_trasloca_sky <- filter(assistenza, "page_url_pulita_post_prop LIKE '%sky.it/assistenza/trasloca-sky%'")
View(head(assistenza_trasloca_sky,100))
nrow(assistenza_trasloca_sky)
# 693.040

valdist_ter_liv <- summarize(groupBy(assistenza_trasloca_sky, "terzo_livello_prop"), count = count(assistenza_trasloca_sky$post_visid_concatenated))
View(head(valdist_ter_liv,100))



##### SISTEMARE ASSISTENZA nelle 2 sottosezioni!! #####################




landing <- filter(sito_completo, "page_url_pulita_post_prop LIKE '%www.sky.it/landing%' and secondo_livello_prop = 'landing'")
View(head(landing,100))
nrow(landing)
# 58.074.826

# valdist_sec_liv <- summarize(groupBy(landing, "secondo_livello_prop"), count = count(landing$post_visid_concatenated))
# View(head(valdist_sec_liv,100))
# 
# prova <- filter(landing, "secondo_livello_prop = 'common-corp'")
# View(head(prova,100))



pag_servizio <- filter(sito_completo, "secondo_livello_prop = 'pagine di servizio'")
View(head(pag_servizio,1000))
nrow(pag_servizio)
# 75.316.001

# valdist_sec_liv <- summarize(groupBy(pag_servizio, "page_url_pulita_post_prop"), count = count(pag_servizio$post_visid_concatenated))
# View(head(valdist_sec_liv,100))



#chiudi sessione
sparkR.stop()
