
## Lenti-veloci (5)
## Conversioni -- Utilizzo APP


#apri sessione
source("connection_R.R")
options(scipen = 1000)



lv_ibridi <- read.parquet("/user/stefano.mazzucca/lenti_veloci/lenti_veloci_ibridi_def.parquet")
View(head(lv_ibridi,100))
nrow(lv_ibridi)
# 60.386
summ_ibridi <- summarize(lv_ibridi, min = min(lv_ibridi$data_stipula_dt), max = max(lv_ibridi$data_stipula_dt))
View(head(summ_ibridi,100))
# min           max
# 2017-11-01    2018-06-30


lv_digital <- read.parquet("/user/stefano.mazzucca/lenti_veloci/lenti_veloci_digital_info.parquet")
View(head(lv_digital,100))
nrow(lv_digital)
# 25.889
summ_digital <- summarize(lv_digital, min = min(lv_digital$data_stipula_dt), max = max(lv_digital$data_stipula_dt))
View(head(summ_digital,100))
# min           max
# 2017-11-01    2018-06-30




#### APP sport (si ext-id)
skyappsport <- read.parquet("/STAGE/adobe/reportsuite=skyappsport.prod")
# names(skyappsport)
skyappsport <- select(skyappsport, "date_time", 
                      "domain", 
                      "monthly_visitor",
                      "page_name_post_evar", 
                      # "sistema_operativo_post_evar", 
                      "site_section_post_evar", 
                      "page_title_post_evar",
                      "external_id_v0_post_evar", 
                      "external_id_post_evar", 
                      "site_subsection_post_evar", 
                      "nome_video_post_evar", 
                      "nome_sport_post_evar",
                      "post_mobilehourofday", 
                      "post_mobiledevice", 
                      # "post_page_event", "post_page_event_var1", "post_page_event_var2",
                      # "post_page_url", "post_pagename", "page_title_post_prop", "external_id_post_prop", "ref_domain", 
                      # "post_persistent_cookie", 
                      "visit_num", 
                      "post_visid_concatenated", 
                      "date")
View(head(skyappsport,2000))
nrow(skyappsport)
# 1.225.593.415
createOrReplaceTempView(skyappsport, "skyappsport")
valdist_skyappsport <- sql("select external_id_post_evar,  
                                    max(date) as ultima_visita, count(post_visid_concatenated) as count_hit
                           from skyappsport
                           group by external_id_post_evar")
View(head(valdist_skyappsport,100))
nrow(valdist_skyappsport)
# 252.187
write.parquet(valdist_skyappsport, "/user/stefano.mazzucca/skyappsport_utenti_unici.parquet",  mode = "overwrite")



#### APP igt (no ext-id)
igt <- read.parquet("/STAGE/adobe/reportsuite=skyappigtprod")
# names(igt)
igt <- select (igt, "date_time", 
               "domain", 
               # "monthly_visitor",
               "page_name_post_evar", 
               "sistema_operativo_post_evar", 
               "site_section_post_evar", 
               # "page_title_post_evar", "external_id_v0_post_evar", "external_id_post_evar", "site_subsection_post_evar", "nome_video_post_evar", "nome_sport_post_evar",
               "post_mobilehourofday", 
               "post_mobiledevice", 
               "post_page_event", "post_page_event_var1", "post_page_event_var2",
               # "post_page_url", "post_pagename", "page_title_post_prop", "external_id_post_prop", "ref_domain", 
               "post_persistent_cookie", 
               "visit_num", 
               "post_visid_concatenated", 
               "date")
View(head(igt,2000))
nrow(igt)
# 2.431.905
createOrReplaceTempView(igt, "igt")
valdist_igt <- sql("select post_visid_concatenated, 
                            max(date) as ultima_visita, count(post_visid_concatenated) as count_hit
                    from igt
                    group by post_visid_concatenated")
View(head(valdist_igt,100))
nrow(valdist_igt)
# 75.085
write.parquet(valdist_igt, "/user/stefano.mazzucca/igtapp_utenti_unici.parquet",  mode = "overwrite")



#### APP appkids (si ext-id)
appkids <- read.parquet("/STAGE/adobe/reportsuite=skyappkids.prod")
# names(appkids)
appkids <- select(appkids, "date_time", 
                  "domain", 
                  # "monthly_visitor",
                  "exclude_hit", "browser", "first_hit_page_url", "first_hit_pagename", "first_hit_ref_domain", "first_hit_ref_type", "first_hit_referrer", "hit_source",
                  "page_name_post_evar", 
                  # "sistema_operativo_post_evar", 
                  "site_section_post_evar", 
                  "homepage", 
                  "post_channel", 
                  # "page_title_post_evar", 
                  # "external_id_v0_post_evar", 
                  "external_id_post_evar_post_evar",
                  "site_subsection_post_evar", 
                  "post_mobilehourofday", 
                  "post_mobiledevice", 
                  # "post_page_event", "post_page_event_var1", "post_page_event_var2",
                  # "post_page_url", "post_pagename", "page_title_post_prop", "external_id_post_prop", "ref_domain", 
                  "post_persistent_cookie", 
                  "canale_post_evar", 
                  "serie_post_evar", 
                  "tipologia_errore_post_evar", 
                  "visit_num", 
                  "post_visid_concatenated", 
                  "date")
appkids <- filter(appkids,"tipologia_errore_post_evar is NULL")
# appkids <- filter(appkids,"external_id_post_evar_post_evar is not NULL")
View(head(appkids,2000))
nrow(appkids)
# 79.850.675
createOrReplaceTempView(appkids, "appkids")
valdist_appkids <- sql("select external_id_post_evar_post_evar, 
                            max(date) as ultima_visita, count(post_visid_concatenated) as count_hit
                    from appkids
                    group by external_id_post_evar_post_evar")
View(head(valdist_appkids,100))
nrow(valdist_appkids)
# 96.466
write.parquet(valdist_appkids, "/user/stefano.mazzucca/appkids_utenti_unici.parquet",  mode = "overwrite")



#### APP masterchef (si ext-id)
masterchef <- read.parquet("/STAGE/adobe/reportsuite=skyappmasterchef")
# names(masterchef)
masterchef <- select(masterchef, "date_time", 
                     "domain", 
                     "first_hit_page_url", "first_hit_pagename", "first_hit_ref_domain", 
                     "homepage",
                     "post_campaign", 
                     "post_channel",
                     "tipo_dispositivo_post_evar",
                     #"page_name_post_evar", 
                     "page_name_post_prop", 
                     # "site_section_post_evar",
                     "site_section_post_prop",
                     "external_id_post_evar", 
                     # "external_id_post_prop",
                     # "social_share_post_evar",
                     "social_share_post_prop", 
                     # "sistema_operativo_post_evar", 
                     "sistema_operativo_post_prop",
                     "hour_of_day_post_evar", 
                     # "hour_of_day_post_prop", 
                     "post_page_event", "post_page_event_var1", "post_page_event_var2", 
                     "post_page_type", 
                     "post_page_url", 
                     "post_pagename", 
                     "post_videoadname", 
                     "post_videochannel", 
                     "post_videochapter",
                     "visit_num", 
                     "post_visid_concatenated", 
                     "date")
View(head(masterchef,2000))
nrow(masterchef)
# 5.386.498
createOrReplaceTempView(masterchef, "masterchef")
valdist_masterchef <- sql("select external_id_post_evar,  
                            max(date) as ultima_visita, count(post_visid_concatenated) as count_hit
                          from masterchef
                          group by external_id_post_evar")
View(head(valdist_masterchef,100))
nrow(valdist_masterchef)
# 2.702
write.parquet(valdist_masterchef, "/user/stefano.mazzucca/masterchef_utenti_unici.parquet",  mode = "overwrite")



#### APP skyarte (no ext-id)
skyarte <- read.parquet("/STAGE/adobe/reportsuite=skyappskyarte.prod")
# names(skyarte)
skyarte <- select(skyarte,"date_time", 
                  "domain",
                  "connection_type", 
                  "first_hit_page_url", "first_hit_pagename", "first_hit_ref_domain",
                  "post_page_event", "post_page_event_var1", "post_page_event_var2", 
                  "post_page_type", 
                  "post_page_url", 
                  "post_pagename", 
                  # "post_pagename_no_url",
                  "post_video", 
                  "post_videochannel", 
                  "post_videochapter", 
                  "visit_num", 
                  "visit_page_num", 
                  "post_visid_concatenated", 
                  "date")
View(head(skyarte,2000))
nrow(skyarte)
# 181.161
createOrReplaceTempView(skyarte, "skyarte")
valdist_skyarte <- sql("select post_visid_concatenated, 
                            max(date) as ultima_visita, count(post_visid_concatenated) as count_hit
                          from skyarte
                          group by post_visid_concatenated")
View(head(valdist_skyarte,100))
nrow(valdist_skyarte)
# 13.109
write.parquet(valdist_skyarte, "/user/stefano.mazzucca/skyarte_utenti_unici.parquet",  mode = "overwrite")



#### APP skytg24 (no ext-id)
skytg24 <- read.parquet("/STAGE/adobe/reportsuite=skyappskytg24.prod")
# names(skytg24)
skytg24 <- select(skytg24, "date_time", 
                  "domain", 
                  "connection_type", 
                  "first_hit_page_url", "first_hit_pagename", "first_hit_ref_domain", "first_hit_ref_type", "first_hit_referrer",
                  "post_channel", 
                  "post_mobiledayofweek", 
                  "post_mobiledevice", 
                  "post_mobilehourofday", 
                  "post_mobilelaunchnumber", 
                  "post_page_event", "post_page_event_var1", "post_page_event_var2", 
                  "post_page_type", 
                  "post_page_url", 
                  "post_pagename", 
                  "post_videoadname",
                  "post_videochannel", 
                  "post_videochapter", 
                  "post_videoname", 
                  "visit_num", 
                  "post_visid_concatenated", 
                  "date")
View(head(skytg24,2000))
nrow(skytg24)
# 44.499.550
createOrReplaceTempView(skytg24, "skytg24")
valdist_skytg24 <- sql("select post_visid_concatenated, 
                       max(date) as ultima_visita, count(post_visid_concatenated) as count_hit
                       from skytg24
                       group by post_visid_concatenated")
View(head(valdist_skytg24,100))
nrow(valdist_skytg24)
# 830.301
write.parquet(valdist_skytg24, "/user/stefano.mazzucca/skytg24_utenti_unici.parquet",  mode = "overwrite")



#### APP appguidatv (si ext-id)
appguidatv <- read.parquet("/STAGE/adobe/reportsuite=skyappguidatv.prod")
# names(appguidatv)
appguidatv <- select (appguidatv, "date_time", 
                      # "domain", 
                      "post_channel", 
                      "tipo_dispositivo_post_evar", 
                      "tipo_rete_post_evar", 
                      "page_name_post_evar",
                      # "page_name_post_prop",
                      "sistema_operativo_post_evar", 
                      "fascia_oraria_visita_post_evar", 
                      "tipo_visitatore_post_evar",
                      "canale_post_evar", 
                      "contenuto_post_evar", 
                      "contenuto_primafila_post_evar",
                      # "contenuto_primafila_post_prop",
                      "categoria_post_evar", 
                      # "categoria_post_prop",
                      "oggi_prossimi_giorni_post_evar", 
                      # "oggi_prossimi_giorni_post_prop",
                      "site_section_post_evar", 
                      # "site_section_post_prop",
                      "external_id_post_evar",
                      # "external_id_v0_post_evar", 
                      "social_share_post_evar",
                      # "social_share_post_prop",
                      "fascia_oraria_consulta_palinsesto_post_evar", 
                      # "fascia_oraria_consulta_palinsesto_post_prop",
                      "post_page_event", "post_page_event_var1", "post_page_event_var2", 
                      "post_page_type", 
                      "post_page_url", 
                      "post_pagename",
                      "post_visid_concatenated", 
                      "date")
View(head(appguidatv,2000))
nrow(appguidatv)
# 283.487.986
createOrReplaceTempView(appguidatv, "appguidatv")
valdist_appguidatv <- sql("select external_id_post_evar, 
                            max(date) as ultima_visita, count(post_visid_concatenated) as count_hit
                          from appguidatv
                          group by external_id_post_evar")
View(head(valdist_appguidatv,100))
nrow(valdist_appguidatv)
# 530.006
write.parquet(valdist_appguidatv, "/user/stefano.mazzucca/appguidatv_utenti_unici.parquet",  mode = "overwrite")



#### APP appxfactor (si ext-id)
appxfactor <- read.parquet("/STAGE/adobe/reportsuite=skyappxfactor.prod")
View(head(appxfactor,2000))
# names(appxfactor)
# appxfactor <- distinct(select (appxfactor,"external_id_post_evar","post_visid_concatenated","date"))
# View(head(appxfactor,1000))
# nrow(appxfactor)
# # 4.667.191
createOrReplaceTempView(appxfactor, "appxfactor")
valdist_appxfactor <- sql("select external_id_post_evar,  
                          max(date) as ultima_visita, count(post_visid_concatenated) as count_hit
                          from appxfactor
                          group by external_id_post_evar")
View(head(valdist_appxfactor,100))
nrow(valdist_appxfactor)
# 132.714
write.parquet(valdist_appxfactor, "/user/stefano.mazzucca/appxfactor_utenti_unici.parquet",  mode = "overwrite")



#### APP skycielotvdev (no ext-id)
app_skycielotvdev <- read.parquet("/STAGE/adobe/reportsuite=skycielotvdev")
View(head(app_skycielotvdev,2000))
# names(app_skycielotvdev)
# app_skycielotvdev <- distinct(select (app_skycielotvdev,"post_visid_concatenated","date"))
# View(head(app_skycielotvdev,1000))
# nrow(app_skycielotvdev)
# # 6.922.847
createOrReplaceTempView(app_skycielotvdev, "app_skycielotvdev")
valdist_app_skycielotvdev <- sql("select post_visid_concatenated, 
                                          max(date) as ultima_visita, count(post_visid_concatenated) as count_hit
                                  from app_skycielotvdev
                                  group by post_visid_concatenated")
View(head(valdist_app_skycielotvdev,100))
nrow(valdist_app_skycielotvdev)
# 4.137.014
write.parquet(valdist_app_skycielotvdev, "/user/stefano.mazzucca/app_skycielotvdev_utenti_unici.parquet",  mode = "overwrite")



#### APP skymtv8prod (no ext-id)
app_skymtv8prod <- read.parquet("/STAGE/adobe/reportsuite=skymtv8prod")
View(head(app_skymtv8prod,2000))
# names(app_skymtv8prod)
# app_skymtv8prod <- distinct(select (app_skymtv8prod,"post_visid_concatenated","date"))
# View(head(app_skymtv8prod,1000))
# nrow(app_skymtv8prod)
# # 17.826.195
createOrReplaceTempView(app_skymtv8prod, "app_skymtv8prod")
valdist_app_skymtv8prod <- sql("select post_visid_concatenated, 
                                          max(date) as ultima_visita, count(post_visid_concatenated) as count_hit
                                  from app_skymtv8prod
                                  group by post_visid_concatenated")
View(head(valdist_app_skymtv8prod,100))
nrow(valdist_app_skymtv8prod)
# 10.888.325
write.parquet(valdist_app_skymtv8prod, "/user/stefano.mazzucca/app_skymtv8prod_utenti_unici.parquet",  mode = "overwrite")



#################################################################################################################################################################
## Recupero i dizionari per agganciare ai cookie l'external_id #################################################################################################
#################################################################################################################################################################

diz_ibridi_cookie <- read.parquet("/user/stefano.mazzucca/lenti_veloci/lenti_veloci_estrazione_ibridi_CookiesADOBE.parquet")
diz_ibridi_cookie <- filter(diz_ibridi_cookie,"external_id_post_evar is not NULL")
View(head(diz_ibridi_cookie,100))


## Creo dizionario cookies Adobe per i lenti-veloci DIGITAL #####################################################################################################
base_ext_id <- distinct(select(lv_digital,"COD_CLIENTE_CIFRATO"))
nrow(base_ext_id) 
# 24.128
skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated"))
#join
createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
createOrReplaceTempView(base_ext_id,"base_ext_id")
base_ext_id_2 <- sql("select *
                     from base_ext_id  t1 left join skyitdev_df_3 t2
                     on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(base_ext_id_2,"/user/stefano.mazzucca/lenti_veloci_estrazione_digital_CookiesADOBE.parquet")
#################################################################################################################################################################

diz_digital_cookie <- read.parquet("/user/stefano.mazzucca/lenti_veloci_estrazione_digital_CookiesADOBE.parquet")
diz_digital_cookie <- filter(diz_digital_cookie,"external_id_post_evar is not NULL")
View(head(diz_digital_cookie,100))



#################################################################################################################################################################
#################################################################################################################################################################
## Riepilogo APP ##
#################################################################################################################################################################
#################################################################################################################################################################

## APP SPORT (si ext_id) ###############################################################################################################################
skyappsport_unici <- read.parquet("/user/stefano.mazzucca/lenti_veloci/skyappsport_utenti_unici.parquet")
View(head(skyappsport_unici,100))
nrow(skyappsport_unici)
# 252.187
# ext_id <- filter(skyappsport_unici, "external_id_post_evar is NOT NULL")
# ext_id <- distinct(select(ext_id, "external_id_post_evar"))
# nrow(ext_id)
# # 250.781

## join skyappsport ibridi
createOrReplaceTempView(skyappsport_unici, "skyappsport_unici")
createOrReplaceTempView(diz_ibridi_cookie, "diz_ibridi_cookie")
skyappsport_coo <- sql("select distinct t1.*
                       from skyappsport_unici t1
                       inner join diz_ibridi_cookie t2
                       on t1.external_id_post_evar = t2.external_id_post_evar ")
skyappsport_coo <- withColumn(skyappsport_coo, "flg_descrizione", lit("ibridi"))
View(head(skyappsport_coo,100))
nrow(skyappsport_coo)
# 3.787
## join skyappsport digital
createOrReplaceTempView(skyappsport_unici, "skyappsport_unici")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
skyappsport_coo_2 <- sql("select distinct t1.*
                       from skyappsport_unici t1
                       inner join diz_digital_cookie t2
                       on t1.external_id_post_evar = t2.external_id_post_evar ")
skyappsport_coo_2 <- withColumn(skyappsport_coo_2, "flg_descrizione", lit("digital"))
View(head(skyappsport_coo_2,100))
nrow(skyappsport_coo_2)
# 805
## union skyappsport
skyappsport_coo_tot <- rbind(skyappsport_coo, skyappsport_coo_2)
skyappsport_coo_tot <- arrange(skyappsport_coo_tot, desc(skyappsport_coo_tot$ultima_visita))
skyappsport_coo_tot <- withColumn(skyappsport_coo_tot, "flg_app", lit("skysport"))
createOrReplaceTempView(skyappsport_coo_tot, "skyappsport_coo_tot")
skyappsport_coo_tot <- sql("select distinct external_id_post_evar, flg_descrizione, flg_app,
                                          max(ultima_visita) as ultima_visita,
                                          sum(count_hit) as count_hit
                           from skyappsport_coo_tot
                           group by external_id_post_evar, flg_descrizione, flg_app")
View(head(skyappsport_coo_tot,1000))
nrow(skyappsport_coo_tot)
# 4.592


## APP IGT (no ext_id) ###############################################################################################################################
igt_unici <- read.parquet("/user/stefano.mazzucca/lenti_veloci/igtapp_utenti_unici.parquet")
View(head(igt_unici,100))
nrow(igt_unici)
# 75.085

## join igt ibridi
createOrReplaceTempView(igt_unici, "igt_unici")
createOrReplaceTempView(diz_ibridi_cookie, "diz_ibridi_cookie")
igt_coo <- sql("select distinct t2.external_id_post_evar, t1.*
                       from igt_unici t1
                       inner join diz_ibridi_cookie t2
                       on t1.post_visid_concatenated = t2.post_visid_concatenated ")
igt_coo <- withColumn(igt_coo, "flg_descrizione", lit("ibridi"))
View(head(igt_coo,100))
nrow(igt_coo)
# 0
## join igt digital
createOrReplaceTempView(igt_unici, "igt_unici")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
igt_coo_2 <- sql("select distinct t2.external_id_post_evar, t1.*
                       from igt_unici t1
                       inner join diz_digital_cookie t2
                       on t1.post_visid_concatenated = t2.post_visid_concatenated ")
igt_coo_2 <- withColumn(igt_coo_2, "flg_descrizione", lit("digital"))
View(head(igt_coo_2,100))
nrow(igt_coo_2)
# 0
## union igt
igt_coo_tot <- rbind(igt_coo, igt_coo_2)
igt_coo_tot <- arrange(igt_coo_tot, desc(igt_coo_tot$ultima_visita))
igt_coo_tot <- withColumn(igt_coo_tot, "flg_app", lit("igt"))
createOrReplaceTempView(igt_coo_tot, "igt_coo_tot")
igt_coo_tot <- sql("select distinct external_id_post_evar, flg_descrizione, flg_app,
                                          max(ultima_visita) as ultima_visita,
                                          sum(count_hit) as count_hit
                           from igt_coo_tot
                           group by external_id_post_evar, flg_descrizione, flg_app")
View(head(igt_coo_tot,100))
nrow(igt_coo_tot)
# 0


## APP KIDS (si ext_id) ###############################################################################################################################
appkids_unici <- read.parquet("/user/stefano.mazzucca/lenti_veloci/appkids_utenti_unici.parquet")
View(head(appkids_unici,100))
nrow(appkids_unici)
# 258.005

## join appkids ibridi
createOrReplaceTempView(appkids_unici, "appkids_unici")
createOrReplaceTempView(diz_ibridi_cookie, "diz_ibridi_cookie")
appkids_coo <- sql("select distinct t1.*
                       from appkids_unici t1
                       inner join diz_ibridi_cookie t2
                       on t1.external_id_post_evar_post_evar = t2.external_id_post_evar ")
appkids_coo <- withColumn(appkids_coo, "flg_descrizione", lit("ibridi"))
View(head(appkids_coo,100))
nrow(appkids_coo)
# 642
## join appkids digital
createOrReplaceTempView(appkids_unici, "appkids_unici")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
appkids_coo_2 <- sql("select distinct t1.*
                       from appkids_unici t1
                       inner join diz_digital_cookie t2
                       on t1.external_id_post_evar_post_evar = t2.external_id_post_evar ")
appkids_coo_2 <- withColumn(appkids_coo_2, "flg_descrizione", lit("digital"))
View(head(appkids_coo_2,100))
nrow(appkids_coo_2)
# 156
## union appkids
appkids_coo_tot <- rbind(appkids_coo, appkids_coo_2)
appkids_coo_tot <- arrange(appkids_coo_tot, desc(appkids_coo_tot$ultima_visita))
appkids_coo_tot <- withColumn(appkids_coo_tot, "flg_app", lit("kids"))
createOrReplaceTempView(appkids_coo_tot, "appkids_coo_tot")
appkids_coo_tot <- sql("select distinct external_id_post_evar_post_evar as external_id_post_evar, flg_descrizione, flg_app,
                                          max(ultima_visita) as ultima_visita,
                                          sum(count_hit) as count_hit
                           from appkids_coo_tot
                           group by external_id_post_evar_post_evar, flg_descrizione, flg_app")
View(head(appkids_coo_tot,100))
nrow(appkids_coo_tot)
# 798


## APP MASTERCHEF (si ext_id) ###############################################################################################################################
masterchef_unici <- read.parquet("/user/stefano.mazzucca/lenti_veloci/masterchef_utenti_unici.parquet")
View(head(masterchef_unici,100))
nrow(masterchef_unici)
# 86.933

## join masterchef ibridi
createOrReplaceTempView(masterchef_unici, "masterchef_unici")
createOrReplaceTempView(diz_ibridi_cookie, "diz_ibridi_cookie")
masterchef_coo <- sql("select distinct t1.*
                       from masterchef_unici t1
                       inner join diz_ibridi_cookie t2
                       on t1.external_id_post_evar = t2.external_id_post_evar ")
masterchef_coo <- withColumn(masterchef_coo, "flg_descrizione", lit("ibridi"))
View(head(masterchef_coo,100))
nrow(masterchef_coo)
# 4
## join masterchef digital
createOrReplaceTempView(masterchef_unici, "masterchef_unici")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
masterchef_coo_2 <- sql("select distinct t1.*
                       from masterchef_unici t1
                       inner join diz_digital_cookie t2
                       on t1.external_id_post_evar = t2.external_id_post_evar ")
masterchef_coo_2 <- withColumn(masterchef_coo_2, "flg_descrizione", lit("digital"))
View(head(masterchef_coo_2,100))
nrow(masterchef_coo_2)
# 2
## union masterchef
masterchef_coo_tot <- rbind(masterchef_coo, masterchef_coo_2)
masterchef_coo_tot <- arrange(masterchef_coo_tot, desc(masterchef_coo_tot$ultima_visita))
masterchef_coo_tot <- withColumn(masterchef_coo_tot, "flg_app", lit("masterchef"))
createOrReplaceTempView(masterchef_coo_tot, "masterchef_coo_tot")
masterchef_coo_tot <- sql("select distinct external_id_post_evar, flg_descrizione, flg_app,
                                          max(ultima_visita) as ultima_visita,
                                          sum(count_hit) as count_hit
                           from masterchef_coo_tot
                           group by external_id_post_evar, flg_descrizione, flg_app")
View(head(masterchef_coo_tot,100))
nrow(masterchef_coo_tot)
# 6


## APP SKYARTE (no ext_id) ###############################################################################################################################
skyarte_unici <- read.parquet("/user/stefano.mazzucca/lenti_veloci/skyarte_utenti_unici.parquet")
View(head(skyarte_unici,100))
nrow(skyarte_unici)
# 13.109

## join skyarte ibridi
createOrReplaceTempView(skyarte_unici, "skyarte_unici")
createOrReplaceTempView(diz_ibridi_cookie, "diz_ibridi_cookie")
skyarte_coo <- sql("select distinct t2.external_id_post_evar, t1.*
                       from skyarte_unici t1
                       inner join diz_ibridi_cookie t2
                       on t1.post_visid_concatenated = t2.post_visid_concatenated ")
skyarte_coo <- withColumn(skyarte_coo, "flg_descrizione", lit("ibridi"))
View(head(skyarte_coo,100))
nrow(skyarte_coo)
# 0
## join skyarte digital
createOrReplaceTempView(skyarte_unici, "skyarte_unici")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
skyarte_coo_2 <- sql("select distinct t2.external_id_post_evar, t1.*
                       from skyarte_unici t1
                       inner join diz_digital_cookie t2
                       on t1.post_visid_concatenated = t2.post_visid_concatenated ")
skyarte_coo_2 <- withColumn(skyarte_coo_2, "flg_descrizione", lit("digital"))
View(head(skyarte_coo_2,100))
nrow(skyarte_coo_2)
# 0
## union skyarte
skyarte_coo_tot <- rbind(skyarte_coo, skyarte_coo_2)
skyarte_coo_tot <- arrange(skyarte_coo_tot, desc(skyarte_coo_tot$ultima_visita))
skyarte_coo_tot <- withColumn(skyarte_coo_tot, "flg_app", lit("skyarte"))
createOrReplaceTempView(skyarte_coo_tot, "skyarte_coo_tot")
skyarte_coo_tot <- sql("select distinct external_id_post_evar, flg_descrizione, flg_app,
                                          max(ultima_visita) as ultima_visita,
                                          sum(count_hit) as count_hit
                           from skyarte_coo_tot
                           group by external_id_post_evar, flg_descrizione, flg_app")
View(head(skyarte_coo_tot,100))
nrow(skyarte_coo_tot)
# 0


## APP TG24 (no ext_id) ###############################################################################################################################
skytg24_unici <- read.parquet("/user/stefano.mazzucca/lenti_veloci/skytg24_utenti_unici.parquet")
View(head(skytg24_unici,100))
nrow(skytg24_unici)
# 830.301

## join skytg24 ibridi
createOrReplaceTempView(skytg24_unici, "skytg24_unici")
createOrReplaceTempView(diz_ibridi_cookie, "diz_ibridi_cookie")
skytg24_coo <- sql("select distinct t2.external_id_post_evar, t1.*
                       from skytg24_unici t1
                       inner join diz_ibridi_cookie t2
                       on t1.post_visid_concatenated = t2.post_visid_concatenated ")
skytg24_coo <- withColumn(skytg24_coo, "flg_descrizione", lit("ibridi"))
View(head(skytg24_coo,100))
nrow(skytg24_coo)
# 0
## join skytg24 digital
createOrReplaceTempView(skytg24_unici, "skytg24_unici")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
skytg24_coo_2 <- sql("select distinct t2.external_id_post_evar, t1.*
                       from skytg24_unici t1
                       inner join diz_digital_cookie t2
                       on t1.post_visid_concatenated = t2.post_visid_concatenated ")
skytg24_coo_2 <- withColumn(skytg24_coo_2, "flg_descrizione", lit("digital"))
View(head(skytg24_coo_2,100))
nrow(skytg24_coo_2)
# 0
## union skytg24
skytg24_coo_tot <- rbind(skytg24_coo, skytg24_coo_2)
skytg24_coo_tot <- arrange(skytg24_coo_tot, desc(skytg24_coo_tot$ultima_visita))
skytg24_coo_tot <- withColumn(skytg24_coo_tot, "flg_app", lit("skytg24"))
createOrReplaceTempView(skytg24_coo_tot, "skytg24_coo_tot")
skytg24_coo_tot <- sql("select distinct external_id_post_evar, flg_descrizione, flg_app,
                                          max(ultima_visita) as ultima_visita,
                                          sum(count_hit) as count_hit
                           from skytg24_coo_tot
                           group by external_id_post_evar, flg_descrizione, flg_app")
View(head(skytg24_coo_tot,100))
nrow(skytg24_coo_tot)
# 0


## APP GUIDATV (si ext_id) ###############################################################################################################################
appguidatv_unici <- read.parquet("/user/stefano.mazzucca/lenti_veloci/appguidatv_utenti_unici.parquet")
View(head(appguidatv_unici,100))
nrow(appguidatv_unici)
# 2.662.061

## join appguidatv ibridi
createOrReplaceTempView(appguidatv_unici, "appguidatv_unici")
createOrReplaceTempView(diz_ibridi_cookie, "diz_ibridi_cookie")
appguidatv_coo <- sql("select distinct t1.*
                       from appguidatv_unici t1
                       inner join diz_ibridi_cookie t2
                       on t1.external_id_post_evar = t2.external_id_post_evar ")
appguidatv_coo <- withColumn(appguidatv_coo, "flg_descrizione", lit("ibridi"))
View(head(appguidatv_coo,100))
nrow(appguidatv_coo)
# 9.488
## join appguidatv digital
createOrReplaceTempView(appguidatv_unici, "appguidatv_unici")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
appguidatv_coo_2 <- sql("select distinct t1.*
                       from appguidatv_unici t1
                       inner join diz_digital_cookie t2
                       on t1.external_id_post_evar = t2.external_id_post_evar ")
appguidatv_coo_2 <- withColumn(appguidatv_coo_2, "flg_descrizione", lit("digital"))
View(head(appguidatv_coo_2,100))
nrow(appguidatv_coo_2)
# 1.926
## union appguidatv
appguidatv_coo_tot <- rbind(appguidatv_coo, appguidatv_coo_2)
appguidatv_coo_tot <- arrange(appguidatv_coo_tot, desc(appguidatv_coo_tot$ultima_visita))
appguidatv_coo_tot <- withColumn(appguidatv_coo_tot, "flg_app", lit("guidatv"))
createOrReplaceTempView(appguidatv_coo_tot, "appguidatv_coo_tot")
appguidatv_coo_tot <- sql("select distinct external_id_post_evar, flg_descrizione, flg_app,
                                          max(ultima_visita) as ultima_visita,
                                          sum(count_hit) as count_hit
                           from appguidatv_coo_tot
                           group by external_id_post_evar, flg_descrizione, flg_app")
View(head(appguidatv_coo_tot,100))
nrow(appguidatv_coo_tot)
# 11.414


## APP XFACTOR (si ext_id) ###############################################################################################################################
appxfactor_unici <- read.parquet("/user/stefano.mazzucca/lenti_veloci/appxfactor_utenti_unici.parquet")
View(head(appxfactor_unici,100))
nrow(appxfactor_unici)
# 1.077.565

## join appxfactor ibridi
createOrReplaceTempView(appxfactor_unici, "appxfactor_unici")
createOrReplaceTempView(diz_ibridi_cookie, "diz_ibridi_cookie")
appxfactor_coo <- sql("select distinct t1.*
                       from appxfactor_unici t1
                       inner join diz_ibridi_cookie t2
                       on t1.external_id_post_evar = t2.external_id_post_evar ")
appxfactor_coo <- withColumn(appxfactor_coo, "flg_descrizione", lit("ibridi"))
View(head(appxfactor_coo,100))
nrow(appxfactor_coo)
# 278
## join appxfactor digital
createOrReplaceTempView(appxfactor_unici, "appxfactor_unici")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
appxfactor_coo_2 <- sql("select distinct t1.*
                       from appxfactor_unici t1
                       inner join diz_digital_cookie t2
                       on t1.external_id_post_evar = t2.external_id_post_evar ")
appxfactor_coo_2 <- withColumn(appxfactor_coo_2, "flg_descrizione", lit("digital"))
View(head(appxfactor_coo_2,100))
nrow(appxfactor_coo_2)
# 47
## union appxfactor
appxfactor_coo_tot <- rbind(appxfactor_coo, appxfactor_coo_2)
appxfactor_coo_tot <- arrange(appxfactor_coo_tot, desc(appxfactor_coo_tot$ultima_visita))
appxfactor_coo_tot <- withColumn(appxfactor_coo_tot, "flg_app", lit("xfactor"))
createOrReplaceTempView(appxfactor_coo_tot, "appxfactor_coo_tot")
appxfactor_coo_tot <- sql("select distinct external_id_post_evar, flg_descrizione, flg_app,
                                          max(ultima_visita) as ultima_visita,
                                          sum(count_hit) as count_hit
                           from appxfactor_coo_tot
                           group by external_id_post_evar, flg_descrizione, flg_app")
View(head(appxfactor_coo_tot,100))
nrow(appxfactor_coo_tot)
# 325


## APP CIELO (no ext_id) ###############################################################################################################################
app_skycielotvdev_unici <- read.parquet("/user/stefano.mazzucca/lenti_veloci/app_skycielotvdev_utenti_unici.parquet")
View(head(app_skycielotvdev_unici,100))
nrow(app_skycielotvdev_unici)
# 4.137.014

## join app_skycielotvdev ibridi
createOrReplaceTempView(app_skycielotvdev_unici, "app_skycielotvdev_unici")
createOrReplaceTempView(diz_ibridi_cookie, "diz_ibridi_cookie")
app_skycielotvdev_coo <- sql("select distinct t2.external_id_post_evar, t1.*
                       from app_skycielotvdev_unici t1
                       inner join diz_ibridi_cookie t2
                       on t1.post_visid_concatenated = t2.post_visid_concatenated ")
app_skycielotvdev_coo <- withColumn(app_skycielotvdev_coo, "flg_descrizione", lit("ibridi"))
View(head(app_skycielotvdev_coo,100))
nrow(app_skycielotvdev_coo)
# 1.668
## join app_skycielotvdev digital
createOrReplaceTempView(app_skycielotvdev_unici, "app_skycielotvdev_unici")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
app_skycielotvdev_coo_2 <- sql("select distinct t2.external_id_post_evar, t1.*
                       from app_skycielotvdev_unici t1
                       inner join diz_digital_cookie t2
                       on t1.post_visid_concatenated = t2.post_visid_concatenated ")
app_skycielotvdev_coo_2 <- withColumn(app_skycielotvdev_coo_2, "flg_descrizione", lit("digital"))
View(head(app_skycielotvdev_coo_2,100))
nrow(app_skycielotvdev_coo_2)
# 405
## union app_skycielotvdev
app_skycielotvdev_coo_tot <- rbind(app_skycielotvdev_coo, app_skycielotvdev_coo_2)
app_skycielotvdev_coo_tot <- arrange(app_skycielotvdev_coo_tot, desc(app_skycielotvdev_coo_tot$ultima_visita))
app_skycielotvdev_coo_tot <- withColumn(app_skycielotvdev_coo_tot, "flg_app", lit("skycielotv"))
createOrReplaceTempView(app_skycielotvdev_coo_tot, "app_skycielotvdev_coo_tot")
app_skycielotvdev_coo_tot <- sql("select distinct external_id_post_evar, flg_descrizione, flg_app,
                                          max(ultima_visita) as ultima_visita,
                                          sum(count_hit) as count_hit
                           from app_skycielotvdev_coo_tot
                           group by external_id_post_evar, flg_descrizione, flg_app")
View(head(app_skycielotvdev_coo_tot,100))
nrow(app_skycielotvdev_coo_tot)
# 1.936


## APP MTV8 (no ext_id) ###############################################################################################################################
app_skymtv8prod_unici <- read.parquet("/user/stefano.mazzucca/lenti_veloci/app_skymtv8prod_utenti_unici.parquet")
View(head(app_skymtv8prod_unici,100))
nrow(app_skymtv8prod_unici)
# 10.888.325

## join app_skymtv8prod ibridi
createOrReplaceTempView(app_skymtv8prod_unici, "app_skymtv8prod_unici")
createOrReplaceTempView(diz_ibridi_cookie, "diz_ibridi_cookie")
app_skymtv8prod_coo <- sql("select distinct t2.external_id_post_evar, t1.*
                       from app_skymtv8prod_unici t1
                       inner join diz_ibridi_cookie t2
                       on t1.post_visid_concatenated = t2.post_visid_concatenated ")
app_skymtv8prod_coo <- withColumn(app_skymtv8prod_coo, "flg_descrizione", lit("ibridi"))
View(head(app_skymtv8prod_coo,100))
nrow(app_skymtv8prod_coo)
# 6.728
## join app_skymtv8prod digital
createOrReplaceTempView(app_skymtv8prod_unici, "app_skymtv8prod_unici")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
app_skymtv8prod_coo_2 <- sql("select distinct t2.external_id_post_evar, t1.*
                       from app_skymtv8prod_unici t1
                       inner join diz_digital_cookie t2
                       on t1.post_visid_concatenated = t2.post_visid_concatenated ")
app_skymtv8prod_coo_2 <- withColumn(app_skymtv8prod_coo_2, "flg_descrizione", lit("digital"))
View(head(app_skymtv8prod_coo_2,100))
nrow(app_skymtv8prod_coo_2)
# 1.862
## union app_skymtv8prod
app_skymtv8prod_coo_tot <- rbind(app_skymtv8prod_coo, app_skymtv8prod_coo_2)
app_skymtv8prod_coo_tot <- arrange(app_skymtv8prod_coo_tot, desc(app_skymtv8prod_coo_tot$ultima_visita))
app_skymtv8prod_coo_tot <- withColumn(app_skymtv8prod_coo_tot, "flg_app", lit("skymtv8"))
createOrReplaceTempView(app_skymtv8prod_coo_tot, "app_skymtv8prod_coo_tot")
app_skymtv8prod_coo_tot <- sql("select distinct external_id_post_evar, flg_descrizione, flg_app,
                                          max(ultima_visita) as ultima_visita,
                                          sum(count_hit) as count_hit
                           from app_skymtv8prod_coo_tot
                           group by external_id_post_evar, flg_descrizione, flg_app")
View(head(app_skymtv8prod_coo_tot,100))
nrow(app_skymtv8prod_coo_tot)
# 7.617


## UNION APP ###################################################################################################################################################

igt_coo_tot <- select(igt_coo_tot, "external_id_post_evar", "ultima_visita", "count_hit", "flg_descrizione", "flg_app")
skyarte_coo_tot <- select(skyarte_coo_tot, "external_id_post_evar", "ultima_visita", "count_hit", "flg_descrizione", "flg_app")
skytg24_coo_tot <- select(skytg24_coo_tot, "external_id_post_evar", "ultima_visita", "count_hit", "flg_descrizione", "flg_app")
app_skycielotvdev_coo_tot <- select(app_skycielotvdev_coo_tot, "external_id_post_evar", "ultima_visita", "count_hit", "flg_descrizione", "flg_app")
app_skymtv8prod_coo_tot <- select(app_skymtv8prod_coo_tot, "external_id_post_evar", "ultima_visita", "count_hit", "flg_descrizione", "flg_app")

app_veloci_lenti <- rbind(skyappsport_coo_tot, igt_coo_tot, appkids_coo_tot, masterchef_coo_tot, skyarte_coo_tot, skytg24_coo_tot, 
                          appguidatv_coo_tot, appxfactor_coo_tot, app_skycielotvdev_coo_tot, app_skymtv8prod_coo_tot)
app_veloci_lenti <- distinct(select(app_veloci_lenti, "external_id_post_evar", "ultima_visita", "count_hit", "flg_descrizione", "flg_app"))
View(head(app_veloci_lenti,1000))
nrow(app_veloci_lenti)
# 26.688


write.parquet(app_veloci_lenti, "/user/stefano.mazzucca/lenti_veloci/veloci_lenti_utilizzo_app.parquet", mode = "overwrite")




###############################################################################################################################################################
###############################################################################################################################################################
## Aggangio info ##
###############################################################################################################################################################
###############################################################################################################################################################

app_veloci_lenti <- read.parquet("/user/stefano.mazzucca/lenti_veloci/veloci_lenti_utilizzo_app.parquet")
View(head(app_veloci_lenti,100))
nrow(app_veloci_lenti)
# 26.688

# prova <- arrange(app_veloci_lenti, asc(app_veloci_lenti$external_id_post_evar), desc(app_veloci_lenti$ultima_visita))
# View(head(prova,1000))
# #### VERIFICHE ####
# createOrReplaceTempView(app_veloci_lenti, "app_veloci_lenti")
# count_extid <- sql("select external_id_post_evar, flg_app, count(external_id_post_evar)
#                    from app_veloci_lenti
#                    group by external_id_post_evar, flg_app")
# count_extid <- arrange(count_extid, desc(count_extid$`count(external_id_post_evar)`))
# View(head(count_extid,247))
# nrow(count_extid)
# # 26.443
# count_extid_filt <- filter(count_extid, count_extid$`count(external_id_post_evar)` != 1)
# View(head(count_extid_filt,100))
# nrow(count_extid_filt)
# # 245 DOPPIONI !! <<<<---------------------------------------------------------------------------------------------------------------------------------------
# ver_1 <- filter(app_veloci_lenti, "post_visid_concatenated = '32569440865679147734611689350785332794'")
# View(head(ver_1))
# ver_2 <- filter(app_veloci_lenti, "post_visid_concatenated = '32515273661560009714611689343805832949'")
# View(head(ver_2))
# ver_3 <- filter(app_veloci_lenti, "post_visid_concatenated = '71372639161877308371415130812919695827'")
# View(head(ver_3))
# 
#
# createOrReplaceTempView(lv_ibridi, "lv_ibridi")
# createOrReplaceTempView(lv_digital, "lv_digital")
# ver_digital_ibridi <- sql("select distinct t1.COD_CLIENTE_CIFRATO, t2.COD_CLIENTE_CIFRATO
#                           from lv_ibridi t1
#                           inner join lv_digital t2
#                           on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO ")
# nrow(ver_digital_ibridi)
# # 625 !!ATTENZIONE!! Ci sono dei doppioni nelle due liste!! ATTENZIONE <<<<------------------------------------------------------------------------------------


valdist_extid <- distinct(select(app_veloci_lenti, "external_id_post_evar"))
nrow(valdist_extid)
# 21.300


# 
# ###############################################################################################################################################################
# ## Spaklyr per utilizzare la matrice sparsa ##
# ###############################################################################################################################################################
# 
# #USER
# SPARK_USER="stefano.mazzucca"
# 
# # connection #
# Sys.setenv(SPARK_HOME="/home/rstudio/spark/spark-2.1.0-bin-hadoop2.7/")
# Sys.setenv(SPARK_HOME_VERSION="2.1.0")
# Sys.setenv(SPARK_HDFS_CONFIG_URL="/usr/lib/rstudio-server/hadoop/")
# Sys.setenv(SPARK_USER=SPARK_USER)
# 
# # Conf obbligatorie
# conf <- list()
# conf$spark.mesos.executor.docker.image <- "mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.7"
# conf$spark.mesos.executor.home <- "/opt/spark/dist"
# 
# # Puoi aggiungere le opzioni aggiuntive ad es.:
# 
# conf$spark.executor.cores <-"2"
# conf$spark.executor.memory <- "15G"
# conf$spark.executor.heartbeat <- "60s"
# conf$spark.cores.max <- "10"
# conf$spark.driver.maxResultSize <- "40G"
# conf$spark.driver.memory <- "6G"
# conf$spark.driver.cores <- "2"
# 
# library(sparklyr)
# sc <- spark_connect(master = 'mesos://leader.mesos:5050', config = conf, app_name = 'sparklyr_SM')
# 
# #library("dplyr", lib.loc="/usr/local/lib/R/site-library")
# #library("dplyr", lib.loc="~/R/x86_64-pc-linux-gnu-library/3.3")
# library(dplyr)
# ######################################################################################################
# 
# app_veloci_lenti_lyr <- spark_read_parquet(sc, "app_veloci_lenti_lyr", "/user/stefano.mazzucca/lenti_veloci/veloci_lenti_utilizzo_app.parquet")
# View(head(app_veloci_lenti_lyr,100))
# nrow(app_veloci_lenti_lyr)
# # 26.688
# 
# app_veloci_lenti_lyr_2 <- ml_create_dummy_variables(app_veloci_lenti_lyr, "flg_app")
# View(head(app_veloci_lenti_lyr_2,100))
# nrow(app_veloci_lenti_lyr_2)
# # 26.688
# 
# app_veloci_lenti_lyr_2 %>% collect()
# 
# 
# spark_disconnect(sc)
# 
# ###############################################################################################################################################################
# ###############################################################################################################################################################
# 
# 
# app_veloci_lenti_2 <- read.parquet("")
# View(head(app_veloci_lenti,100))
# nrow(app_veloci_lenti)
# # 26.688












## Join tab inziiale con le info app 
createOrReplaceTempView(lv_digital, "lv_digital")
createOrReplaceTempView(app_veloci_lenti, "app_veloci_lenti")
lv_digital_infoapp <- sql("select distinct t1.*, t2.flg_app, t2.count_hit as count_hit_app, t2.ultima_visita as ultima_visita_app
                               from lv_digital t1
                               left join app_veloci_lenti t2
                               on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar ")
View(head(lv_digital_infoapp,100))
nrow(lv_digital_infoapp)
# 27.004

#25.889


createOrReplaceTempView(app_veloci_lenti_extid, "app_veloci_lenti_extid")
createOrReplaceTempView(diz_digital_cookie, "diz_digital_cookie")
app_veloci_lenti_extid_2 <- sql("select distinct t1.*, t2.external_id_post_evar as external_id_post_evar_digital
                               from app_veloci_lenti_extid t1
                               left join diz_digital_cookie t2
                               on t1.external_id_post_evar = t2.COD_CLIENTE_CIFRATO ")
View(head(app_veloci_lenti_extid_2,100))
nrow(app_veloci_lenti_extid_2)
# 32.694

app_veloci_lenti_extid_2$external_id_post_evar <- ifelse(isNull(app_veloci_lenti_extid_2$external_id_post_evar_ibridi), app_veloci_lenti_extid_2$external_id_post_evar_digital,
                                                         app_veloci_lenti_extid_2$external_id_post_evar_ibridi)
app_veloci_lenti_extid_2$external_id_post_evar_ibridi <- NULL
app_veloci_lenti_extid_2$external_id_post_evar_digital <- NULL
View(head(app_veloci_lenti_extid_2,100))
nrow(app_veloci_lenti_extid_2)
# 32.694

filt_app_veloci_lenti_extid_2 <- filter(app_veloci_lenti_extid_2, "external_id_post_evar is not null")
nrow(filt_app_veloci_lenti_extid_2)
# 10.671











# invece dalle impression di ADFORM possiamo vedere: "device-name","browser-name","os-name"
# che sono interessanti, cos?? vediamo quanti hanno Mac o IPhone







#chiudi sessione
sparkR.stop()

  