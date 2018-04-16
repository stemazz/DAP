
# CJ_PDISC_vers09
# navigazione utenti pdisc su sito pubblico, wsc, APP


#apri sessione
source("connection_R.R")
options(scipen = 1000)



######### Recupero il target PDISC da cui partire per analizzare le navigazioni degli utenti


base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
View(head(base_pdisc,100))
nrow(base_pdisc)
# 37.976

valdist_base <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_base)
# 37.753


base_pdisc_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc.parquet")
View(head(base_pdisc_coo,100))
nrow(base_pdisc_coo)
# 161.766
ver_clienti <- distinct(select(base_pdisc_coo, "COD_CLIENTE_CIFRATO"))
nrow(ver_clienti)
# 24.340 clienti pdisc on line (su device)



createOrReplaceTempView(base_pdisc, "base_pdisc")
createOrReplaceTempView(base_pdisc_coo, "base_pdisc_coo")

base_pdisc_coo_2 <- sql("select t1.*, t2.post_visid_concatenated
                        from base_pdisc t1
                        left join base_pdisc_coo t2
                          on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
View(head(base_pdisc_coo_2,100))
nrow(base_pdisc_coo_2)
# 176.511



#############################################################################################################################################################################
######### Recupero dati navigazione sito pubblico ##########################################################################################################################
#############################################################################################################################################################################


skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
# skyitdev_df_1 <- filter(skyitdev_df,"external_id_post_evar is not NULL")

skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated", "date_time", 
                        "visit_num", "page_url_post_evar", "page_url_post_prop", "download_name_post_prop", "secondo_livello_post_evar", "terzo_livello_post_evar")

skyitdev_df_3 <-withColumn(skyitdev_df_2, "date", cast(skyitdev_df_2$date_time, "date"))

skyitdev_df_4 <- withColumn(skyitdev_df_3, "ts", cast(skyitdev_df_3$date_time, "timestamp"))

skyitdev_df_5 <- filter(skyitdev_df_4,"ts >= '2017-03-01 00:00:00' and ts <= '2018-01-01 00:00:00' ")

skyitdev_df_6 <- filter(skyitdev_df_5,"
                   secondo_livello_post_evar like '%guidatv%' or
                   secondo_livello_post_evar like '%Guidatv%' or
                   secondo_livello_post_evar like '%pagine di servizio%' or
                   secondo_livello_post_evar like '%assistenza%' or
                   secondo_livello_post_evar like '%fai da te%' or
                   secondo_livello_post_evar like '%extra%' or
                   secondo_livello_post_evar like '%tecnologia%' or
                   secondo_livello_post_evar like '%pacchetti-offerte%' or
                   terzo_livello_post_evar like '%conosci%' or
                   terzo_livello_post_evar like '%gestisci%' or
                   terzo_livello_post_evar like '%contatta%' or
                   terzo_livello_post_evar like '%home%' or
                   terzo_livello_post_evar like '%ricerca%' or
                   terzo_livello_post_evar like '%risolvi%'
                   ")

createOrReplaceTempView(skyitdev_df_6,"skyitdev_df_6")
createOrReplaceTempView(base_pdisc_coo_2,"base_pdisc_coo_2")

join_base_sez_pubblica <- sql("select t1.*, 
                                t2.visit_num, t2.secondo_livello_post_evar, t2.terzo_livello_post_evar, t2.page_url_post_evar, t2.page_url_post_prop, 
                                t2.download_name_post_prop, t2.ts as data_vis_pag
                              from base_pdisc_coo_2 t1 
                              inner join skyitdev_df_6 t2
                                on t1.post_visid_concatenated = t2.post_visid_concatenated
                                and t2.date <= t1.data_ingr_pdisc_formdate") ############## CONTROLLA CAMPO DATA!!!!!!!!!!!!


#### Salvataggio ####

write.parquet(join_base_sez_pubblica,"/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_sito.parquet")



CJ_PDISC_scarico_pag_viste_sito <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_sito.parquet")
View(head(CJ_PDISC_scarico_pag_viste_sito,100))
nrow(CJ_PDISC_scarico_pag_viste_sito)
# 1.622.250



#############################################################################################################################################################################
######### Recupero dati navigazione da WSC ##########################################################################################################################
#############################################################################################################################################################################


skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")


#inizia selezione
skyit_wsc_v1 <- select(skyitdev_df, "external_id_post_evar", "post_visid_high", "post_visid_low", "post_visid_concatenated", "date_time", "visit_num",
                       "secondo_livello_post_evar", "terzo_livello_post_evar")

skyit_wsc_v2 <- withColumn(skyit_wsc_v1, "ts", cast(skyit_wsc_v1$date_time, "timestamp"))

skyit_wsc_v3 <- withColumn(skyit_wsc_v2, "date", cast(skyit_wsc_v2$date_time, "date"))

skyit_wsc_v3 <- filter(skyit_wsc_v3,"ts >= '2017-03-01 00:00:00' and ts <= '2018-01-01 00:00:00' and external_id_post_evar is not NULL")

skyit_wsc_v4 <- filter(skyit_wsc_v3,"
                       terzo_livello_post_evar like '%faidate gestisci dati servizi%' or
                       terzo_livello_post_evar like '%faidate arricchisci abbonamento%' or
                       terzo_livello_post_evar like '%faidate fatture pagamenti%' or
                       terzo_livello_post_evar like '%faidate webtracking%' or
                       terzo_livello_post_evar like '%faidate extra%'
                       ")


createOrReplaceTempView(skyit_wsc_v4,"skyit_wsc_v4")
createOrReplaceTempView(base_pdisc_coo_2,"base_pdisc_coo_2")

join_base_wsc <- sql("select t1.*,
                      t2.visit_num, t2.secondo_livello_post_evar, t2.terzo_livello_post_evar,
                      t2.ts as data_vis_pag
                     from base_pdisc_coo_2 t1 
                     inner join skyit_wsc_v4 t2
                      on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar
                      and t2.date <= t1.data_ingr_pdisc_formdate") ############## CONTROLLA CAMPO DATA!!!!!!


#### Salvataggio ####

write.parquet(join_base_wsc,"/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_wsc.parquet")




CJ_PDISC_scarico_pag_viste_wsc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_wsc.parquet")
View(head(CJ_PDISC_scarico_pag_viste_wsc,100))
nrow(CJ_PDISC_scarico_pag_viste_wsc)
# 5.599.758






#############################################################################################################################################################################
######### Recupero dati navigazione da APP ##########################################################################################################################
#############################################################################################################################################################################


skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")

skyappwsc_2 <- withColumn(skyappwsc, "ts", cast(skyappwsc$date_time, "timestamp"))

skyappwsc_3 <- withColumn(skyappwsc_2, "date", cast(skyappwsc$date_time, "date"))

skyappwsc_4 <- filter(skyappwsc_3,"ts >= '2017-03-01 00:00:00' and ts <= '2018-01-01 00:00:00' ")

skyappwsc_5 <- select(skyappwsc_4,
                      "external_id_post_evar",
                      "site_section",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "post_pagename",
                      "date_time",
                      "ts",
                      "date",
                      "visit_num")

skyappwsc_6 <- filter(skyappwsc_5,"
                     site_section like '%dati e fatture%' or
                     site_section like '%il mio abbonamento%' or
                     site_section like '%sky extra%' or
                     site_section like '%comunicazioni%' or
                     site_section like '%assistenza%' or
                     site_section like '%widget%' or
                     site_section like '%impostazioni%' or
                     site_section like '%i miei dati%' or
                     page_name_post_evar like '%assistenza:home%' or
                     page_name_post_evar like '%assistenza:contatta%' or
                     page_name_post_evar like '%assistenza:conosci%' or
                     page_name_post_evar like '%assistenza:ricerca%' or
                     page_name_post_evar like '%assistenza:gestisci%' or
                     page_name_post_evar like '%assistenza:risolvi%' or
                     page_name_post_evar like '%widget:dispositivi%' or
                     page_name_post_evar like '%widget:ultimafattura%' or
                     page_name_post_evar like '%widget:contatta sky%' or
                     page_name_post_evar like '%widget:gestisci%'
                     ")


createOrReplaceTempView(skyappwsc_6,"skyappwsc_6")
createOrReplaceTempView(base_pdisc,"base_pdisc") # UTILIZZO la base pdisc totale perch?? NON ci sono i cookies su APP (se si connettono sono loggati)

join_base_app <- sql("select t1.*,
                      t2.visit_num, t2.site_section,t2.page_name_post_evar,
                      t2.ts as data_visualizzaz_pagina
                     from base_pdisc t1 
                     inner join skyappwsc_6 t2
                      on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar
                      and t2.date <= t1.data_ingr_pdisc_formdate") ############## CONTROLLA CAMPO DATA!!!!!!!!!!!


#### Salvataggio ####

write.parquet(join_base_app,"/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_app.parquet")



CJ_PDISC_scarico_pag_viste_app <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_app.parquet")
View(head(CJ_PDISC_scarico_pag_viste_app,100))
nrow(CJ_PDISC_scarico_pag_viste_app)
# 209.416 # 282.583








#chiudi sessione
sparkR.stop()
