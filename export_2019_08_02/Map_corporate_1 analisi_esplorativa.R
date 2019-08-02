
## Analisi esplorativa -> mappatura tracking code per: 
## SMS
## DEM 
## impression/click



#apri sessione
source("connection_R.R")
options(scipen = 1000)


#################################################################################################################################################################
#################################################################################################################################################################
## Report Suite = Skyitdev
#################################################################################################################################################################
#################################################################################################################################################################

skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
View(head(skyitdev,100))

skyit_1 <- select(skyitdev, "external_id_post_evar",
                  "post_visid_high",
                  "post_visid_low",
                  "post_visid_concatenated",
                  "visit_num",
                  "date_time",
                  #"post_evar17",
                  #"post_prop17",
                  "post_channel", 
                  #"page_url_post_evar", "page_url_post_prop", 
                  #"page_url_pulita_post_evar", 
                  "page_name_post_evar",
                  "page_url_pulita_post_prop",
                  "download_name_post_prop",
                  "secondo_livello_post_prop",
                  "secondo_livello_post_evar",
                  #"canale_corporate_post_prop", 
                  "terzo_livello_post_prop",
                  "terzo_livello_post_evar",
                  #"sottocanale_corporate_post_prop",
                  #"carrier",
                  #"paid_search",
                  "post_campaign", 
                  #"stack_tracking_code_post_evar",
                  "tracking_code_30_giorni_post_evar", 
                  #"aes_search_post_evar",
                  #"va_closer_detail",
                  "va_closer_id", # last touch
                  "va_finder_id", # first touch
                  # capire di quali refferal si parla
                  "first_hit_ref_domain",
                  "first_hit_ref_type",
                  "first_hit_referrer",
                  "visit_referrer",
                  #"visit_search_engine",
                  "chiave_ricerca_interna_corporate_post_evar",
                  "hit_source",
                  "exclude_hit"
)

skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))

skyit_4 <- withColumn(skyit_3, "secondo_livello_prop", lower(skyit_3$secondo_livello_post_prop))
skyit_5 <- withColumn(skyit_4, "terzo_livello_prop", lower(skyit_4$terzo_livello_post_prop))


## Verifica sul PRIMO LIVELLO ################################################################################
createOrReplaceTempView(skyit_5, "skyit_5")
valdist_primo_livello <- sql("select post_channel, count(post_visid_concatenated) as count_hit_primo_livello
                             from skyit_5
                             group by post_channel")
#View(head(valdist_primo_livello,100))

write.parquet(valdist_primo_livello, "/user/stefano.mazzucca/valdist_primo_livello.parquet")

valdist_primo_livello <- read.parquet("/user/stefano.mazzucca/valdist_primo_livello.parquet")
valdist_primo_livello_1 <- arrange(valdist_primo_livello, desc(valdist_primo_livello$count_hit_primo_livello))
View(valdist_primo_livello_1)
nrow(valdist_primo_livello_1)
# 1.029
###############################################################################################################


skyit_6 <- filter(skyit_5, "post_channel = 'corporate' or post_channel = 'Guidatv'")

View(head(skyit_6,1000))


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

View(head(skyit_6,1000))


## Verifiche parametri #########################################################################################################################################

ver_post_campaign <- distinct(select(skyit_6, "post_campaign", "stack_tracking_code_post_evar"))
ver_post_campaign_1 <- filter(ver_post_campaign, "post_campaign <> stack_tracking_code_post_evar")

write.parquet(ver_post_campaign_1, "/user/stefano.mazzucca/ver_post_campaign_corporate.parquet")

ver_post_campaign_1 <- read.parquet("/user/stefano.mazzucca/ver_post_campaign_corporate.parquet")
View(head(ver_post_campaign_1,100))
nrow(ver_post_campaign_1)
# 595.629
## Le differenze non sono significative. Utilizzeremo "post_campaign"


ver_secondo_livello <- summarize(groupBy(skyit_6, "secondo_livello_post_prop", "canale_corporate_post_prop"), count = count(skyit_6$secondo_livello_post_prop))
ver_secondo_livello_1 <- filter(ver_secondo_livello, "secondo_livello_post_prop <> canale_corporate_post_prop")

write.parquet(ver_secondo_livello_1, "/user/stefano.mazzucca/ver_secondo_livello_corporate.parquet")

ver_secondo_livello_1 <- read.parquet("/user/stefano.mazzucca/ver_secondo_livello_corporate.parquet")
View(head(ver_secondo_livello_1,100))
nrow(ver_secondo_livello_1)
# 6
## Utilizziamo secondo_livello_post_prop



ver_terzo_livello <- summarize(groupBy(skyit_6, "terzo_livello_post_prop", "sottocanale_corporate_post_prop"), count = count(skyit_6$terzo_livello_post_prop))
ver_terzo_livello_1 <- filter(ver_terzo_livello, "terzo_livello_post_prop <> sottocanale_corporate_post_prop")

write.parquet(ver_terzo_livello_1, "/user/stefano.mazzucca/ver_terzo_livello_corporate.parquet")

ver_terzo_livello_1 <- read.parquet("/user/stefano.mazzucca/ver_terzo_livello_corporate.parquet")
View(head(ver_terzo_livello_1,100))
nrow(ver_terzo_livello_1)
# 1
## utilizziamo terzo_livello_post_prop



ver_post_evar17 <- filter(skyit_6, "post_evar17 is NOT NULL")
ver_post_evar17_1 <- summarize(groupBy(ver_post_evar17, "post_evar17"), count = count(ver_post_evar17$post_evar17))

write.parquet(ver_post_evar17_1, "/user/stefano.mazzucca/ver_post_evar17_corporate.parquet")

ver_post_evar17_1 <- read.parquet("/user/stefano.mazzucca/ver_post_evar17_corporate.parquet")
View(head(ver_post_evar17_1,100))
nrow(ver_post_evar17_1)
# 3
## Inutile


#################################################################################################################################################################

## info su alcuni parametri #####################################################################################################################################

valdist_first_hit_ref_domain <- summarize(groupBy(skyit_3, "first_hit_ref_domain"), count = count(skyit_3$first_hit_ref_domain))
#valdist_first_hit_ref_domain_1 <- arrange(valdist_first_hit_ref_domain, desc(valdist_first_hit_ref_domain$count))
write.parquet(valdist_first_hit_ref_domain, "/user/stefano.mazzucca/valdist_first_hit_ref_domain.parquet")

valdist_first_hit_ref_domain <- read.parquet("/user/stefano.mazzucca/valdist_first_hit_ref_domain.parquet")
valdist_first_hit_ref_domain_1 <- arrange(valdist_first_hit_ref_domain, desc(valdist_first_hit_ref_domain$count))
View(head(valdist_first_hit_ref_domain_1,100))
nrow(valdist_first_hit_ref_domain_1)
# 33.426



valdist_post_campaign <- summarize(groupBy(skyit_3, "post_campaign"), count = count(skyit_3$post_campaign))
#valdist_post_campaign_1 <- arrange(valdist_post_campaign, desc(valdist_post_campaign$count))
write.parquet(valdist_post_campaign, "/user/stefano.mazzucca/valdist_post_campaign.parquet")

valdist_post_campaign <- read.parquet("/user/stefano.mazzucca/valdist_post_campaign.parquet")
valdist_post_campaign_1 <- arrange(valdist_post_campaign, desc(valdist_post_campaign$count))
View(head(valdist_post_campaign_1,100))
nrow(valdist_post_campaign_1)
# 128.033



valdist_chiave_ricerca_interna_corporate <- summarize(groupBy(skyit_3, "chiave_ricerca_interna_corporate_post_evar"), count = count(skyit_3$chiave_ricerca_interna_corporate_post_evar))
#valdist_chiave_ricerca_interna_corporate_1 <- arrange(valdist_chiave_ricerca_interna_corporate, desc(valdist_chiave_ricerca_interna_corporate$count))
write.parquet(valdist_chiave_ricerca_interna_corporate, "/user/stefano.mazzucca/valdist_chiave_ricerca_interna_corporate.parquet")

valdist_chiave_ricerca_interna_corporate <- read.parquet("/user/stefano.mazzucca/valdist_chiave_ricerca_interna_corporate.parquet")
valdist_chiave_ricerca_interna_corporate_1 <- arrange(valdist_chiave_ricerca_interna_corporate, desc(valdist_chiave_ricerca_interna_corporate$count))
View(head(valdist_chiave_ricerca_interna_corporate_1,100))
nrow(valdist_chiave_ricerca_interna_corporate_1)
# 602.566



valdist_tracking_code_30_giorni <- summarize(groupBy(skyit_3, "tracking_code_30_giorni_post_evar"), count = count(skyit_3$tracking_code_30_giorni_post_evar))
#valdist_tracking_code_30_giorni_1 <- arrange(valdist_tracking_code_30_giorni, desc(valdist_tracking_code_30_giorni$count))
write.parquet(valdist_tracking_code_30_giorni, "/user/stefano.mazzucca/valdist_tracking_code_30_giorni.parquet")

valdist_tracking_code_30_giorni <- read.parquet("/user/stefano.mazzucca/valdist_tracking_code_30_giorni.parquet")
valdist_tracking_code_30_giorni_1 <- arrange(valdist_tracking_code_30_giorni, desc(valdist_tracking_code_30_giorni$count))
View(head(valdist_tracking_code_30_giorni_1,100))
nrow(valdist_tracking_code_30_giorni_1)
# 127.340



valdist_aes_search_post_evar <- summarize(groupBy(skyit_3, "aes_search_post_evar"), count = count(skyit_3$aes_search_post_evar))
#valdist_aes_search_post_evar_1 <- arrange(valdist_aes_search_post_evar, desc(valdist_aes_search_post_evar$count))
write.parquet(valdist_aes_search_post_evar, "/user/stefano.mazzucca/valdist_aes_search_post_evar.parquet")

valdist_aes_search_post_evar <- read.parquet("/user/stefano.mazzucca/valdist_aes_search_post_evar.parquet")
valdist_aes_search_post_evar_1 <- arrange(valdist_aes_search_post_evar, desc(valdist_aes_search_post_evar$count))
View(head(valdist_aes_search_post_evar_1,100))
nrow(valdist_aes_search_post_evar_1)
# 742.292



terzo_livello <- distinct(select(skyit_6, "terzo_livello_post_prop"))

write.parquet(terzo_livello, "/user/stefano.mazzucca/valdist_terzo_livello.parquet")

valdist_terzo_livello <- read.parquet("/user/stefano.mazzucca/valdist_terzo_livello.parquet")
View(valdist_terzo_livello)
nrow(valdist_terzo_livello)



secondo_livello <- distinct(select(skyit_6, "secondo_livello_post_prop"))

write.parquet(secondo_livello, "/user/stefano.mazzucca/valdist_secondo_livello.parquet")

valdist_secondo_livello <- read.parquet("/user/stefano.mazzucca/valdist_secondo_livello.parquet")
View(valdist_secondo_livello)
nrow(valdist_secondo_livello)

#################################################################################################################################################################

## Analisi sulle sezioni ########################################################################################################################################

# ver_sez_pac_off <- distinct(select(filter(skyit_6, "page_name_post_evar LIKE '%pacchetti e offerte%'"), "page_name_post_evar"))
# 
# write.parquet(ver_sez_pac_off, "/user/stefano.mazzucca/ver_sez_pac_off.parquet")
# 
# ver_sez_pac_off <- read.parquet("/user/stefano.mazzucca/ver_sez_pac_off.parquet")
# View(ver_sez_pac_off)
# nrow(ver_sez_pac_off)
# # 61
# 
# ver_2_sez_pac_off <- distinct(select(filter(skyit_6, "page_name_post_evar LIKE '%acchetti%' or page_name_post_evar LIKE '%offerte%'"), "page_name_post_evar"))
# 
# write.parquet(ver_2_sez_pac_off, "/user/stefano.mazzucca/ver_2_sez_pac_off.parquet")
# 
# ver_2_sez_pac_off <- read.parquet("/user/stefano.mazzucca/ver_2_sez_pac_off.parquet")
# View(head(ver_2_sez_pac_off,1000))
# nrow(ver_2_sez_pac_off)
# # 270
# ## Ci sarebbe da fare pulizia e capire differenze tra "pacchetti e offerte" e "pacchettieofferte" con verifica delle date!!!


sez_pac_off <- filter(skyit_6, "page_name_post_evar LIKE '%pacchetti e offerte%'")
# nrow(sez_pac_off) 25.497.425
sez_pac_off_1 <- summarize(groupBy(sez_pac_off, "page_name_post_evar", "secondo_livello"), count = count(sez_pac_off$secondo_livello))

write.parquet(sez_pac_off_1, "/user/stefano.mazzucca/valdist_sez_pac_off.parquet")

sez_pac_off_1 <- read.parquet("/user/stefano.mazzucca/valdist_sez_pac_off.parquet")
View(head(sez_pac_off_1,1000))
nrow(sez_pac_off_1)
# 410
## utilizziamo page_name per la selezione delle pagine perche' e' un dato piu' "sicuro"



sez_entrainsky <- filter(skyit_6, "page_name_post_evar LIKE '%:acquista:%'")
sez_entrainsky_1 <- distinct(select(sez_entrainsky, "page_name_post_evar"))

write.parquet(sez_entrainsky_1, "/user/stefano.mazzucca/valdist_sez_enrtainsky.parquet")

sez_entrainsky_1 <- read.parquet("/user/stefano.mazzucca/valdist_sez_enrtainsky.parquet")
View(head(sez_entrainsky_1,100))
nrow(sez_entrainsky_1)
# 86



sez_tecnologia <- filter(skyit_6, "page_name_post_evar LIKE '%:tecnologia:%'")
sez_tecnologia_1 <- summarize(groupBy(sez_tecnologia, "page_name_post_evar"), count = count(sez_tecnologia$page_name_post_evar))

write.parquet(sez_tecnologia_1, "/user/stefano.mazzucca/valdist_sez_tecnologia.parquet")

sez_tecnologia_1 <- read.parquet("/user/stefano.mazzucca/valdist_sez_tecnologia.parquet")
View(head(sez_tecnologia_1,200))
nrow(sez_tecnologia_1)
# 126




# sez_guidatv <- filter(skyit_6, "page_url_pulita_post_prop LIKE '%guidatv.sky%'")
# sez_guidatv_2 <- filter(skyit_6, "page_url_pulita_post_prop LIKE '%guidatv%'")
# 
# write.parquet(sez_guidatv, "/user/stefano.mazzucca/ver_sez_guidatv.parquet")
# write.parquet(sez_guidatv_2, "/user/stefano.mazzucca/ver_sez_guidatv_2.parquet")
# 
# sez_guidatv <- read.parquet("/user/stefano.mazzucca/ver_sez_guidatv.parquet")
# View(head(sez_guidatv,10000))
# nrow(sez_guidatv)
# # 606.104
# sez_guidatv_2 <- read.parquet("/user/stefano.mazzucca/ver_sez_guidatv_2.parquet")
# View(head(sez_guidatv_2,10000))
# nrow(sez_guidatv_2)
# # 606.439
# 
# createOrReplaceTempView(sez_guidatv_2, "sez_guidatv_2")
# valdist_guidatv_secondo_livello <- sql("select secondo_livello, count(secondo_livello)
#                                        from sez_guidatv_2
#                                        group by secondo_livello")
# View(head(valdist_guidatv_secondo_livello,100))
# # secondo_livello          count(secondo_livello)
# # extra                      3
# # assistenza                 596
# # pacchetti-offerte          1
# # homepage sky               14
# # tecnologia                 2
# # pagine di servizio         4
# # 404                        349612
# # engagement                 3
# # common-corp                227025
# # acquista                   11
# # business                   84
# # info                       1


## Verifiche sul secono livello post_porp e post_evar ############################################################################################################

ver_2_secondo_livello <- summarize(groupBy(skyit_6, "secondo_livello_post_prop", "secondo_livello_post_evar"), count = count(skyit_6$secondo_livello_post_prop))
ver_2_secondo_livello_1 <- filter(ver_2_secondo_livello, "secondo_livello_post_prop <> secondo_livello_post_evar")

write.parquet(ver_2_secondo_livello_1, "/user/stefano.mazzucca/ver_2_secondo_livello_corporate.parquet")

ver_2_secondo_livello_1 <- read.parquet("/user/stefano.mazzucca/ver_2_secondo_livello_corporate.parquet")
View(head(ver_2_secondo_livello_1,100))
nrow(ver_2_secondo_livello_1)
# 62
## 

















#################################################################################################################################################################
#################################################################################################################################################################
## Report Suite = Skyappwsc
#################################################################################################################################################################
#################################################################################################################################################################

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod/table=hitdata/hitdata.parquet")




#chiudi sessione
sparkR.stop()
