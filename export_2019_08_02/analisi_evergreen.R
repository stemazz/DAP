
## Analisi EVER GREEN


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
                  "va_closer_id",
                  
                  "page_template_post_prop",
                  "contatore_giorni_post_prop",
                  "data_prima_pubblicazione_post_prop",
                  "page_url_pulita_post_prop"
)
skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))

# filtro temporale
skyit_4 <- filter(skyit_3, skyit_3$date_time_dt >= "2018-06-01") 


skyit_5 <- filter(skyit_4, skyit_4$post_channel == "tg24")

skyit_6 <- filter(skyit_5, "page_template_post_prop LIKE 'photogallery' OR page_template_post_prop LIKE 'liveblog' OR 
                            page_template_post_prop LIKE 'foglia articolo' OR  page_template_post_prop LIKE 'AMP%' ")

skyit_6$last_touch_label <- ifelse(skyit_6$va_closer_id == 1, "editorial_recommendation",
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





write.parquet(skyit_6, "/user/stefano.mazzucca/analisi_evergreen/scarico_with_last_touch_channel_2018_06.parquet")



scarico <- read.parquet("/user/stefano.mazzucca/analisi_evergreen/scarico_2017_04.parquet")
View(head(scarico,100))
nrow(scarico)
# 335.218.806

gb_template <- summarize(groupBy(scarico, scarico$page_template_post_prop), count_p = count(scarico$post_visid_concatenated))
View(head(gb_template,100))


scarico_1 <- withColumn(scarico, "data_prima_pubblicazione_dt", cast(cast(unix_timestamp(scarico$data_prima_pubblicazione_post_prop, "dd/MM/yyyy"), "timestamp"), "date"))
# scarico_2 <- withColumn(scarico_1, "settimana", to_utc_timestamp(scarico_1$date_time_ts, "CET"))
# scarico_2 <- withColumn(scarico_1, "gg_diff", datediff(scarico_1$date_time_dt, scarico_1$data_prima_pubblicazione_dt))

scarico_2 <- filter(scarico_1, scarico_1$data_prima_pubblicazione_dt >= "2018-06-01")
View(head(scarico_2,100))
nrow(scarico_2)
# 202.573.475

scarico_2 <- withColumn(scarico_2, "visita_unica", concat(scarico_2$post_visid_concatenated, scarico_2$visit_num))
scarico_2 <- withColumn(scarico_2, "contatore_giorni_post_prop", cast(scarico_2$contatore_giorni_post_prop, "integer"))

scarico_2 <- withColumn(scarico_2, "flg_AMP", ifelse(contains(scarico_2$page_template_post_prop, c("AMP")), 1, 0))
scarico_2 <- withColumn(scarico_2, "flg_liveblog", ifelse(scarico_2$page_template_post_prop == "liveblog", 1, 0))
scarico_2 <- withColumn(scarico_2, "flg_foglia", ifelse(scarico_2$page_template_post_prop == "foglia articolo", 1, 0))
scarico_2 <- withColumn(scarico_2, "flg_photogal", ifelse(scarico_2$page_template_post_prop == "photogallery", 1, 0))
View(head(scarico_2,100))

scarico_2 <- withColumn(scarico_2, "anno", year(scarico_2$date_time_dt))
scarico_2 <- withColumn(scarico_2, "mese", month(scarico_2$date_time_dt))
scarico_2$giu18 <- ifelse(scarico_2$mese == "6" & scarico_2$anno == "2018", 1, 0)
scarico_2$lug18 <- ifelse(scarico_2$mese == "7" & scarico_2$anno == "2018", 1, 0)
scarico_2$ago18 <- ifelse(scarico_2$mese == "8" & scarico_2$anno == "2018", 1, 0)
scarico_2$set18 <- ifelse(scarico_2$mese == "9" & scarico_2$anno == "2018", 1, 0)
scarico_2$ott18 <- ifelse(scarico_2$mese == "10" & scarico_2$anno == "2018", 1, 0)
scarico_2$nov18 <- ifelse(scarico_2$mese == "11" & scarico_2$anno == "2018", 1, 0)
scarico_2$dic18 <- ifelse(scarico_2$mese == "12" & scarico_2$anno == "2018", 1, 0)
scarico_2$gen19 <- ifelse(scarico_2$mese == "1" & scarico_2$anno == "2019", 1, 0)
scarico_2$feb19 <- ifelse(scarico_2$mese == "2" & scarico_2$anno == "2019", 1, 0)
scarico_2$mar19 <- ifelse(scarico_2$mese == "3" & scarico_2$anno == "2019", 1, 0)
scarico_2$apr19 <- ifelse(scarico_2$mese == "4" & scarico_2$anno == "2019", 1, 0)
scarico_2$mag19 <- ifelse(scarico_2$mese == "5" & scarico_2$anno == "2019", 1, 0)
scarico_2$giu19 <- ifelse(scarico_2$mese == "6" & scarico_2$anno == "2019", 1, 0)
scarico_2$lug19 <- ifelse(scarico_2$mese == "7" & scarico_2$anno == "2019", 1, 0)
View(head(scarico_2,100))


# prima gruppo per visite
gb_articoli <- summarize(groupBy(scarico_2, scarico_2$page_url_pulita_post_prop, scarico_2$visita_unica), 
                         data_prima_pubblicaz = min(scarico_2$data_prima_pubblicazione_dt),
                         # count_visits = countDistinct(concat(scarico_2$post_visid_concatenated, scarico_2$visit_num)),
                         data_ultima_hit = max(scarico_2$date_time_dt),
                         # contro_verifica_data = (max(scarico_2$date_time_dt) - min(scarico_2$data_prima_pubblicazione_dt)), 
                         max_contatore = max(scarico_2$contatore_giorni_post_prop),
                         mezzo_contatore = round(max(scarico_2$contatore_giorni_post_prop)/2),
                         count_AMP = max(scarico_2$flg_AMP), 
                         count_liveblog = max(scarico_2$flg_liveblog),
                         count_foglia = max(scarico_2$flg_foglia),
                         count_photogal = max(scarico_2$flg_photogal),
                         sezione = last(scarico_2$secondo_livello_post_prop),
                         count_giu18 = max(scarico_2$giu18), 
                         count_lug18 = max(scarico_2$lug18), 
                         count_ago18 = max(scarico_2$ago18), 
                         count_set18 = max(scarico_2$set18), 
                         count_ott18 = max(scarico_2$ott18), 
                         count_nov18 = max(scarico_2$nov18), 
                         count_dic18 = max(scarico_2$dic18), 
                         count_gen19 = max(scarico_2$gen19), 
                         count_feb19 = max(scarico_2$feb19), 
                         count_mar19 = max(scarico_2$mar19), 
                         count_apr19 = max(scarico_2$apr19), 
                         count_mag19 = max(scarico_2$mag19), 
                         count_giu19 = max(scarico_2$giu19), 
                         count_lug19 = max(scarico_2$lug19)
                         )
View(head(gb_articoli,100))

# poi gruppo per page name articolo
gb_articoli_2 <- summarize(groupBy(gb_articoli, gb_articoli$page_url_pulita_post_prop), 
                           count_visits = countDistinct(gb_articoli$visita_unica),
                           data_prima_pubblicaz = min(gb_articoli$data_prima_pubblicaz),
                           data_ultima_hit = max(gb_articoli$data_ultima_hit),
                           max_contatore = max(gb_articoli$max_contatore),
                           mezzo_contatore = max(gb_articoli$mezzo_contatore),
                           count_AMP = sum(gb_articoli$count_AMP), 
                           count_liveblog = sum(gb_articoli$count_liveblog),
                           count_foglia = sum(gb_articoli$count_foglia),
                           count_photogal = sum(gb_articoli$count_photogal),
                           sezione = last(gb_articoli$sezione),
                           count_giu18 = sum(gb_articoli$count_giu18), 
                           count_lug18 = sum(gb_articoli$count_lug18), 
                           count_ago18 = sum(gb_articoli$count_ago18), 
                           count_set18 = sum(gb_articoli$count_set18), 
                           count_ott18 = sum(gb_articoli$count_ott18), 
                           count_nov18 = sum(gb_articoli$count_nov18), 
                           count_dic18 = sum(gb_articoli$count_dic18), 
                           count_gen19 = sum(gb_articoli$count_gen19), 
                           count_feb19 = sum(gb_articoli$count_feb19), 
                           count_mar19 = sum(gb_articoli$count_mar19), 
                           count_apr19 = sum(gb_articoli$count_apr19), 
                           count_mag19 = sum(gb_articoli$count_mag19), 
                           count_giu19 = sum(gb_articoli$count_giu19), 
                           count_lug19 = sum(gb_articoli$count_lug19)
)

gb_articoli_2 <- arrange(gb_articoli_2, desc(gb_articoli_2$max_contatore))
gb_articoli_2 <- withColumn(gb_articoli_2, "gg", datediff(gb_articoli_2$data_ultima_hit, gb_articoli_2$data_prima_pubblicaz))

View(head(gb_articoli_2,1000))

write.parquet(gb_articoli_2, "/user/stefano.mazzucca/analisi_evergreen/articoli_info_2.parquet", mode = "overwrite")



evergreen <- read.parquet("/user/stefano.mazzucca/analisi_evergreen/articoli_info_2.parquet")
View(head(evergreen,100))
nrow(evergreen)
# 105.739
# 73.224

evergreen <- arrange(evergreen, desc(evergreen$count_visits))
View(head(evergreen,100))






#### PROVE ######################################################################################################################################################

prova <- limit(scarico_2, 10000)
View(head(prova,1000))


# prima gruppo per visite
gb_articoli_prova <- summarize(groupBy(prova, prova$page_name_post_evar, prova$visita_unica), 
                         data_prima_pubblicaz = min(prova$data_prima_pubblicazione_dt),
                         # count_visits = countDistinct(concat(prova$post_visid_concatenated, prova$visit_num)),
                         data_ultima_hit = max(prova$date_time_dt),
                         # contro_verifica_data = (max(prova$date_time_dt) - min(prova$data_prima_pubblicazione_dt)), 
                         max_contatore = max(prova$contatore_giorni_post_prop),
                         mezzo_contatore = round(max(prova$contatore_giorni_post_prop)/2),
                         count_AMP = max(prova$flg_AMP), 
                         count_liveblog = max(prova$flg_liveblog),
                         count_foglia = max(prova$flg_foglia),
                         count_photogal = max(prova$flg_photogal)
)
View(head(gb_articoli_prova,100))

# poi gruppo per page name articolo
gb_articoli_prova_2 <- summarize(groupBy(gb_articoli_prova, gb_articoli_prova$page_name_post_evar), 
                           count_visits = countDistinct(gb_articoli_prova$visita_unica),
                           data_prima_pubblicaz = min(gb_articoli_prova$data_prima_pubblicaz),
                           data_ultima_hit = max(gb_articoli_prova$data_ultima_hit),
                           max_contatore = max(gb_articoli_prova$max_contatore),
                           mezzo_contatore = max(gb_articoli_prova$mezzo_contatore),
                           count_AMP = sum(gb_articoli_prova$count_AMP), 
                           count_liveblog = sum(gb_articoli_prova$count_liveblog),
                           count_foglia = sum(gb_articoli_prova$count_foglia),
                           count_photogal = sum(gb_articoli_prova$count_photogal)
)

gb_articoli_prova_2 <- arrange(gb_articoli_prova_2, desc(gb_articoli_prova_2$max_contatore))
gb_articoli_prova_2 <- withColumn(gb_articoli_prova_2, "gg", datediff(gb_articoli_prova_2$data_ultima_hit, gb_articoli_prova_2$data_prima_pubblicaz))

View(head(gb_articoli_prova_2,1000))



#################################################################################################################################################################
#################################################################################################################################################################
## con last touch channel ##
#################################################################################################################################################################
#################################################################################################################################################################


scarico <- read.parquet("/user/stefano.mazzucca/analisi_evergreen/scarico_with_last_touch_channel_2018_06.parquet")
View(head(scarico,100))
nrow(scarico)
# 222.183.892

gb_template <- summarize(groupBy(scarico, scarico$page_template_post_prop), count_p = count(scarico$post_visid_concatenated))
View(head(gb_template,100))


scarico_1 <- withColumn(scarico, "data_prima_pubblicazione_dt", cast(cast(unix_timestamp(scarico$data_prima_pubblicazione_post_prop, "dd/MM/yyyy"), "timestamp"), "date"))
# scarico_2 <- withColumn(scarico_1, "settimana", to_utc_timestamp(scarico_1$date_time_ts, "CET"))
# scarico_2 <- withColumn(scarico_1, "gg_diff", datediff(scarico_1$date_time_dt, scarico_1$data_prima_pubblicazione_dt))

scarico_2 <- filter(scarico_1, scarico_1$data_prima_pubblicazione_dt >= "2018-06-01")
View(head(scarico_2,100))
nrow(scarico_2)
# 211.960.141

scarico_2 <- withColumn(scarico_2, "visita_unica", concat(scarico_2$post_visid_concatenated, scarico_2$visit_num))
scarico_2 <- withColumn(scarico_2, "contatore_giorni_post_prop", cast(scarico_2$contatore_giorni_post_prop, "integer"))

scarico_2 <- withColumn(scarico_2, "flg_AMP", ifelse(contains(scarico_2$page_template_post_prop, c("AMP")), 1, 0))
scarico_2 <- withColumn(scarico_2, "flg_liveblog", ifelse(scarico_2$page_template_post_prop == "liveblog", 1, 0))
scarico_2 <- withColumn(scarico_2, "flg_foglia", ifelse(scarico_2$page_template_post_prop == "foglia articolo", 1, 0))
scarico_2 <- withColumn(scarico_2, "flg_photogal", ifelse(scarico_2$page_template_post_prop == "photogallery", 1, 0))
View(head(scarico_2,100))

scarico_2 <- withColumn(scarico_2, "anno", year(scarico_2$date_time_dt))
scarico_2 <- withColumn(scarico_2, "mese", month(scarico_2$date_time_dt))
scarico_2$giu18 <- ifelse(scarico_2$mese == "6" & scarico_2$anno == "2018", 1, 0)
scarico_2$lug18 <- ifelse(scarico_2$mese == "7" & scarico_2$anno == "2018", 1, 0)
scarico_2$ago18 <- ifelse(scarico_2$mese == "8" & scarico_2$anno == "2018", 1, 0)
scarico_2$set18 <- ifelse(scarico_2$mese == "9" & scarico_2$anno == "2018", 1, 0)
scarico_2$ott18 <- ifelse(scarico_2$mese == "10" & scarico_2$anno == "2018", 1, 0)
scarico_2$nov18 <- ifelse(scarico_2$mese == "11" & scarico_2$anno == "2018", 1, 0)
scarico_2$dic18 <- ifelse(scarico_2$mese == "12" & scarico_2$anno == "2018", 1, 0)
scarico_2$gen19 <- ifelse(scarico_2$mese == "1" & scarico_2$anno == "2019", 1, 0)
scarico_2$feb19 <- ifelse(scarico_2$mese == "2" & scarico_2$anno == "2019", 1, 0)
scarico_2$mar19 <- ifelse(scarico_2$mese == "3" & scarico_2$anno == "2019", 1, 0)
scarico_2$apr19 <- ifelse(scarico_2$mese == "4" & scarico_2$anno == "2019", 1, 0)
scarico_2$mag19 <- ifelse(scarico_2$mese == "5" & scarico_2$anno == "2019", 1, 0)
scarico_2$giu19 <- ifelse(scarico_2$mese == "6" & scarico_2$anno == "2019", 1, 0)
scarico_2$lug19 <- ifelse(scarico_2$mese == "7" & scarico_2$anno == "2019", 1, 0)
View(head(scarico_2,100))


scarico_2$flg_seo <- ifelse(scarico_2$last_touch_label == "search_engine_organic", 1, 0)
scarico_2$flg_internal <- ifelse(scarico_2$last_touch_label == "internal_referrer_w", 1, 0)
scarico_2$flg_social <- ifelse(scarico_2$last_touch_label == "social_network", 1, 0)
scarico_2$flg_paid_social <- ifelse(scarico_2$last_touch_label == "paid_social", 1, 0)
scarico_2$flg_direct <- ifelse(scarico_2$last_touch_label == "direct_bookmark", 1, 0)
scarico_2$flg_refer_domain <- ifelse(scarico_2$last_touch_label == "referring_domains", 1, 0)
scarico_2$flg_amp_x <- ifelse(scarico_2$last_touch_label == "AMP_w", 1, 0)
View(head(scarico_2,100))


# prima gruppo per visite
gb_articoli <- summarize(groupBy(scarico_2, scarico_2$page_url_pulita_post_prop, scarico_2$visita_unica), 
                         data_prima_pubblicaz = min(scarico_2$data_prima_pubblicazione_dt),
                         # count_visits = countDistinct(concat(scarico_2$post_visid_concatenated, scarico_2$visit_num)),
                         data_ultima_hit = max(scarico_2$date_time_dt),
                         # contro_verifica_data = (max(scarico_2$date_time_dt) - min(scarico_2$data_prima_pubblicazione_dt)), 
                         max_contatore = max(scarico_2$contatore_giorni_post_prop),
                         mezzo_contatore = round(max(scarico_2$contatore_giorni_post_prop)/2),
                         count_AMP = max(scarico_2$flg_AMP), 
                         count_liveblog = max(scarico_2$flg_liveblog),
                         count_foglia = max(scarico_2$flg_foglia),
                         count_photogal = max(scarico_2$flg_photogal),
                         sezione = last(scarico_2$secondo_livello_post_prop),
                         count_giu18 = max(scarico_2$giu18), 
                         count_lug18 = max(scarico_2$lug18), 
                         count_ago18 = max(scarico_2$ago18), 
                         count_set18 = max(scarico_2$set18), 
                         count_ott18 = max(scarico_2$ott18), 
                         count_nov18 = max(scarico_2$nov18), 
                         count_dic18 = max(scarico_2$dic18), 
                         count_gen19 = max(scarico_2$gen19), 
                         count_feb19 = max(scarico_2$feb19), 
                         count_mar19 = max(scarico_2$mar19), 
                         count_apr19 = max(scarico_2$apr19), 
                         count_mag19 = max(scarico_2$mag19), 
                         count_giu19 = max(scarico_2$giu19), 
                         count_lug19 = max(scarico_2$lug19),
                         count_seo = max(scarico_2$flg_seo),
                         count_internal = max(scarico_2$flg_internal),
                         count_social = max(scarico_2$flg_social),
                         count_paid_social = max(scarico_2$flg_paid_social),
                         count_direct = max(scarico_2$flg_direct),
                         count_refer_domain = max(scarico_2$flg_refer_domain),
                         count_amp_x = max(scarico_2$flg_amp_x)
)
View(head(gb_articoli,100))

# poi gruppo per page name articolo
gb_articoli_2 <- summarize(groupBy(gb_articoli, gb_articoli$page_url_pulita_post_prop), 
                           count_visits = countDistinct(gb_articoli$visita_unica),
                           data_prima_pubblicaz = min(gb_articoli$data_prima_pubblicaz),
                           data_ultima_hit = max(gb_articoli$data_ultima_hit),
                           max_contatore = max(gb_articoli$max_contatore),
                           mezzo_contatore = max(gb_articoli$mezzo_contatore),
                           count_AMP = sum(gb_articoli$count_AMP), 
                           count_liveblog = sum(gb_articoli$count_liveblog),
                           count_foglia = sum(gb_articoli$count_foglia),
                           count_photogal = sum(gb_articoli$count_photogal),
                           sezione = last(gb_articoli$sezione),
                           count_giu18 = sum(gb_articoli$count_giu18), 
                           count_lug18 = sum(gb_articoli$count_lug18), 
                           count_ago18 = sum(gb_articoli$count_ago18), 
                           count_set18 = sum(gb_articoli$count_set18), 
                           count_ott18 = sum(gb_articoli$count_ott18), 
                           count_nov18 = sum(gb_articoli$count_nov18), 
                           count_dic18 = sum(gb_articoli$count_dic18), 
                           count_gen19 = sum(gb_articoli$count_gen19), 
                           count_feb19 = sum(gb_articoli$count_feb19), 
                           count_mar19 = sum(gb_articoli$count_mar19), 
                           count_apr19 = sum(gb_articoli$count_apr19), 
                           count_mag19 = sum(gb_articoli$count_mag19), 
                           count_giu19 = sum(gb_articoli$count_giu19), 
                           count_lug19 = sum(gb_articoli$count_lug19),
                           count_seo = sum(gb_articoli$count_seo),
                           count_internal = sum(gb_articoli$count_internal),
                           count_social = sum(gb_articoli$count_social),
                           count_paid_social = sum(gb_articoli$count_paid_social),
                           count_direct = sum(gb_articoli$count_direct),
                           count_refer_domain = sum(gb_articoli$count_refer_domain),
                           count_amp_x = sum(gb_articoli$count_amp_x)
)

gb_articoli_2 <- arrange(gb_articoli_2, desc(gb_articoli_2$max_contatore))
gb_articoli_2 <- withColumn(gb_articoli_2, "gg", datediff(gb_articoli_2$data_ultima_hit, gb_articoli_2$data_prima_pubblicaz))

View(head(gb_articoli_2,1000))

write.parquet(gb_articoli_2, "/user/stefano.mazzucca/analisi_evergreen/articoli_info_with_last_touch.parquet", mode = "overwrite")



evergreen <- read.parquet("/user/stefano.mazzucca/analisi_evergreen/articoli_info_with_last_touch.parquet")
View(head(evergreen,100))
nrow(evergreen)
# 76.079

evergreen <- arrange(evergreen, desc(evergreen$count_visits))
View(head(evergreen,100))

write.df(repartition(evergreen, 1), "/user/stefano.mazzucca/analisi_evergreen/articoli_info_with_last_touch.csv", source = "csv", header = "true", delimiter = ";")


