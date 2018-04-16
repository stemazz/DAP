
# CJ_PDISC_vers06

#apri sessione
source("connection_R.R")
options(scipen = 1000)


# 
# ## Recupero il target PDISC su cui fare le analisi
# 
# base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
# View(head(base_pdisc,100))
# nrow(base_pdisc)
# # 37.976
# 
# valdist_cod_contr <- distinct(select(base_pdisc, "COD_CONTRATTO"))
# nrow(valdist_cod_contr)
# # 37.976
# 
# valdist_base <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
# nrow(valdist_base)
# # 37.753
# 
# 
# ######### Recupero le chiavi
# 
# skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
# # skyitdev_df_1 <- filter(skyitdev_df,"external_id_post_evar is not NULL")
# skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low","post_visid_concatenated")
# skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low","post_visid_concatenated"))
# 
# 
# createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
# createOrReplaceTempView(valdist_base,"valdist_base")
# 
# recupera_cookies <- sql("select *
#                        from skyitdev_df_3 t1 
#                        inner join valdist_base t2
#                        on t2.COD_CLIENTE_CIFRATO = t1.external_id_post_evar")
# View(head(recupera_cookies,100))
# nrow(recupera_cookies)
# 
# 
# # Salvataggio delle chiavi
# 
# write.parquet(recupera_cookies,"/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc.parquet")
# 
# # prova <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc.parquet")
# # View(head(prova,100))
# # nrow(prova)
# # # 161.766
# 
# 
# 
# 
# ################## Di queste mi tengo solo quei record valorizzati da un cookies "rintracciabile" con ext_id 
# 
# ## Recupero le chiavi
# chiavi_id_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc.parquet")
# 
# createOrReplaceTempView(chiavi_id_coo,"chiavi_id_coo")
# 
# verifica_count <- sql("select 
#                         count(distinct external_id_post_evar) as num_external_id_post_evar,
#                         count(distinct post_visid_concatenated) as num_post_visid_concatenated,
#                         count(distinct COD_CLIENTE_CIFRATO) as num_COD_CLIENTE_CIFRATO
#                       from chiavi_id_coo")
# View(head(verifica_count))
# # 24.340
# # 161.471
# # 24.340
# 
# 
# ## join con il campione
# createOrReplaceTempView(base_pdisc,"base_pdisc")
# createOrReplaceTempView(chiavi_id_coo,"chiavi_id_coo")
# 
# base_coo <- sql("select t1.*, t2.post_visid_concatenated
#                     from base_pdisc t1
#                     left join chiavi_id_coo t2
#                       on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
# #View(head(base_coo,100))
# #nrow(base_coo)
# # 176.511
# 
# 
# 
# 
# ## Aggiungo alla base un paramtero: data_ingr_pdisc_formdate_4m, cio?? la data di ingresso in pdisc - 4 mesi (per le osservazioni successive)
# base_coo_2 <- withColumn(base_coo, "data_ingr_pdisc_formdate_4m", date_sub(base_coo$data_ingr_pdisc_formdate, 122))
# #View(head(base_coo_2,200))
# 
# 
# 
# # Salvataggio del campione con i relativi cookies
# 
# write.parquet(base_coo_2,"/user/stefano.mazzucca/CJ_PDISC_base_pdisc_coo.parquet")
# 
# base_pdisc_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_pdisc_coo.parquet")
# View(head(base_pdisc_coo,100))
# nrow(base_pdisc_coo)
# # 176.511
# 


### Recupero la base completa da analisi di Valentina:

base_pdisc_flg_visite <- read.parquet("/user/valentina/CJ_PDISC_base_flg_visite.parquet")

View(head(base_pdisc_flg_visite,100))
nrow(base_pdisc_flg_visite)

## ------------ Riprendi da qui e valuta che cambiare ------------------------------------------------------------------------------------------------------------------------




#################################################################################################################
#################################################################################################################
#### join base con le visite alla sezione pubblica del sito
#################################################################################################################
#################################################################################################################

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")


#inizia selezione
skyit_v1 <- select(skyitdev_df,
                   "external_id_post_evar", 
                   "post_visid_high", "post_visid_low","post_visid_concatenated","date_time","visit_num",
                   "secondo_livello_post_evar","terzo_livello_post_evar")

skyit_v2 <- withColumn(skyit_v1, "ts", cast(skyit_v1$date_time, "timestamp"))
skyit_v3 <- withColumn(skyit_v2, "date", cast(skyit_v2$date_time, "date"))
skyit_v3 <- filter(skyit_v3,"ts >= '2017-03-01 00:00:00' and ts <= '2018-01-01 00:00:00' ")
#View(head(skyit_v3,100))

skyit_v4 <- filter(skyit_v3,"
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

createOrReplaceTempView(skyit_v4,"skyit_v4")
createOrReplaceTempView(base_pdisc_coo,"base_pdisc_coo")

join_base_sez_pubblica <- sql("select t1.*, 
                                t2.visit_num, t2.secondo_livello_post_evar, t2.terzo_livello_post_evar, 
                                t2.ts as data_visualizzaz_pagina
                              from base_pdisc_coo t1 
                              inner join skyit_v4 t2
                                on t1.post_visid_concatenated = t2.post_visid_concatenated
                                and t2.date >= t1.data_ingr_pdisc_formdate_4m and t2.date <= t1.data_ingr_pdisc_formdate") ###################### CONTROLLA CAMPO DATA!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


#### Salvataggio ####

write.parquet(join_base_sez_pubblica,"/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_sito.parquet")



# leggi tabella
CJ_PDISC_scarico_pag_viste_sito <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_sito.parquet")

View(head(CJ_PDISC_scarico_pag_viste_sito,100))
nrow(CJ_PDISC_scarico_pag_viste_sito) 
# 902.181




#################################################################################################################
#################################################################################################################
#### join base con le visite al WSC
#################################################################################################################
#################################################################################################################

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")


#inizia selezione
skyit_wsc_v1 <- select(skyitdev_df,
                       "external_id_post_evar",
                       "post_visid_high", "post_visid_low","post_visid_concatenated","date_time","visit_num",
                       "secondo_livello_post_evar","terzo_livello_post_evar")

skyit_wsc_v2 <- withColumn(skyit_wsc_v1, "ts", cast(skyit_wsc_v1$date_time, "timestamp"))
skyit_wsc_v3 <- withColumn(skyit_wsc_v2, "date", cast(skyit_wsc_v2$date_time, "date"))
skyit_wsc_v3 <- filter(skyit_wsc_v3,"ts >= '2017-03-01 00:00:00' and ts <= '2018-01-01 00:00:00'
                       and external_id_post_evar is not NULL")

#skyit_wsc_v3bis <- filter(skyit_wsc_v3, "external_id_post_evar is not NULL")

skyit_wsc_v4 <- filter(skyit_wsc_v3,"
                       terzo_livello_post_evar like '%faidate gestisci dati servizi%' or
                       terzo_livello_post_evar like '%faidate arricchisci abbonamento%' or
                       terzo_livello_post_evar like '%faidate fatture pagamenti%' or
                       terzo_livello_post_evar like '%faidate webtracking%' or
                       terzo_livello_post_evar like '%faidate extra%'
                       ")


createOrReplaceTempView(skyit_wsc_v4,"skyit_wsc_v4")
createOrReplaceTempView(base_pdisc_coo,"base_pdisc_coo")

join_base_wsc <- sql("select t1.*,
                      t2.visit_num, t2.secondo_livello_post_evar, t2.terzo_livello_post_evar,
                      t2.ts as data_visualizzaz_pagina
                     from base_pdisc_coo t1 
                     inner join skyit_wsc_v4 t2
                      on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar
                      and t2.date >= t1.data_ingr_pdisc_formdate_4m and t2.date <= t1.data_ingr_pdisc_formdate") ###################### CONTROLLA CAMPO DATA!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


#### Salvataggio ####

write.parquet(join_base_wsc,"/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_wsc.parquet")



# leggi tabella
CJ_PDISC_scarico_pag_viste_wsc <- read.parquet("/userstefano.mazzucca/CJ_PDISC_scarico_pag_viste_wsc.parquet")

#View(head(CJ_PDISC_scarico_pag_viste_sito,100))
#nrow(CJ_PDISC_scarico_pag_viste_sito) 
# 







#################################################################################################################
#################################################################################################################
#### join base con le visite sulla APP
#################################################################################################################
#################################################################################################################

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")


skyappwsc_2 <- withColumn(skyappwsc, "ts", cast(skyappwsc$date_time, "timestamp"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date", cast(skyappwsc$date_time, "date"))
skyappwsc_3 <- filter(skyappwsc_3,"ts >= '2017-03-01 00:00:00' and ts <= '2018-01-01 00:00:00' ")

skyappwsc_4 <- select(skyappwsc_3,
                      "external_id_v0",
                      "site_section",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "post_pagename",
                      "date_time",
                      "ts",
                      "date",
                      "visit_num")

skyappwsc_5 <- filter(skyappwsc_4,"
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


createOrReplaceTempView(skyappwsc_5,"skyappwsc_5")
createOrReplaceTempView(base_pdisc_coo,"base_pdisc_coo")

join_base_app <- sql("select t1.*,
                      t2.visit_num, t2.site_section,t2.page_name_post_evar,
                      t2.ts as data_visualizzaz_pagina
                     from base_pdisc_coo t1 
                     inner join skyappwsc_5 t2
                      on t1.COD_CLIENTE_CIFRATO = t2.external_id_v0
                      and t2.date >= t1.data_ingr_pdisc_formdate_4m and t2.date <= t1.data_ingr_pdisc_formdate") ###################### CONTROLLA CAMPO DATA!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


#### Salvataggio ####

write.parquet(join_base_app,"/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_app.parquet")



# leggi tabella
CJ_PDISC_scarico_pag_viste_app <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_pag_viste_app.parquet")

#View(head(CJ_PDISC_scarico_pag_viste_app,100))
#nrow(CJ_PDISC_scarico_pag_viste_app)
# 









#chiudi sessione
sparkR.stop()

