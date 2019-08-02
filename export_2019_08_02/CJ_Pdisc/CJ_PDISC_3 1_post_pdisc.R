
# CJ_PDISC_post_pdisc 1 BIS
# scarico navigazione POST-PDISC (in uscita 31-01-2018)
# creazione delle SEQUENZE di navigazione


#apri sessione
source("connection_R.R")
options(scipen = 1000)



### recupera basi ###############################################################################################################################################################

base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
View(head(base_pdisc,100))
nrow(base_pdisc) 
# 37.976

valdist_base_clienti <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_base_clienti)
# 37.753


################################################################################################################################################################################
################################################################################################################################################################################
#### unisco le visite sulla APP
################################################################################################################################################################################
################################################################################################################################################################################

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod/table=hitdata/hitdata.parquet")
View(head(skyappwsc,100))


#seleziona campi
skyappwsc_1 <- select(skyappwsc,
                      "external_id_post_evar",
                      "site_section",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "post_pagename",
                      "date_time",
                      "visit_num", "post_visid_concatenated", "hit_source", "exclude_hit")

skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_ts", cast(skyappwsc_2$date_time, "timestamp"))

#filtro temporale
skyappwsc_4 <- filter(skyappwsc_3,"date_time_dt >= '2017-03-01'")
skyappwsc_5 <- filter(skyappwsc_4,"date_time_dt <= '2018-01-31'")

skyappwsc_6 <- filter(skyappwsc_5,"external_id_post_evar is not NULL")


#unisco alla base
createOrReplaceTempView(skyappwsc_6,"skyappwsc_6")
createOrReplaceTempView(base_pdisc,"base_pdisc")

join_base_app <- sql("SELECT t1.COD_CONTRATTO,t1.COD_CLIENTE_FRUITORE,t1.COD_CLIENTE_CIFRATO,t1.data_ingr_pdisc_formdate,
                             t2.site_section,t2.page_name_post_evar,t2.page_name_post_prop,t2.post_pagename,
                             t2.visit_num,t2.post_visid_concatenated,t2.hit_source,t2.exclude_hit,t2.date_time,t2.date_time_dt,t2.date_time_ts
                     FROM base_pdisc t1
                     INNER JOIN skyappwsc_6 t2
                     ON t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")


write.parquet(join_base_app,"/user/stefano.mazzucca/CJ_PDISC_scarico_app_post_pdisc1.parquet")




################################################################################################################################################################################
################################################################################################################################################################################
#### unisco le visite sul Sito Pubblico / WSC
################################################################################################################################################################################
################################################################################################################################################################################


skyitdev_df <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
View(head(skyitdev_df,100))

# prova1 <- distinct(select(skyitdev_df, "post_channel")) # post_channel == "corporate"
# 
# prova <- distinct(select(skyitdev_df, "canale_corporate_post_prop"))
# View(head(prova,10000))

# Creo le chiavi ext_id con cookies

skyitdev_df_1 <- distinct(select(skyitdev_df, "external_id_post_evar", "post_visid_concatenated"))
skyitdev_df_2 <- filter(skyitdev_df_1, "external_id_post_evar is NOT NULL")

ext_id <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(ext_id)
# 37.753

createOrReplaceTempView(ext_id, "ext_id")
createOrReplaceTempView(skyitdev_df_2, "skyitdev_df_2")

diz_ext_id_coo <- sql("select t2.*
                      from ext_id t1
                      left join skyitdev_df_2 t2
                      on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(diz_ext_id_coo,"/user/stefano.mazzucca/CJ_PDISC_chiavi_coo.parquet")


chiavi_coo_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_chiavi_coo.parquet")
View(head(chiavi_coo_pdisc,100))
nrow(chiavi_coo_pdisc)
# 196.271


createOrReplaceTempView(chiavi_coo_pdisc, "chiavi_coo_pdisc")
createOrReplaceTempView(base_pdisc, "base_pdisc")

base_pdisc_coo <- sql("select t1.*, t2.post_visid_concatenated
                      from base_pdisc t1
                      inner join chiavi_coo_pdisc t2
                      on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
View(head(base_pdisc_coo,100))
nrow(base_pdisc_coo)
# 184.878

write.parquet(base_pdisc_coo,"/user/stefano.mazzucca/CJ_PDISC_base_pdisc_coo.parquet")


base_pdisc_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_pdisc_coo.parquet")

ver_clienti <- distinct(select(base_pdisc_coo, "COD_CLIENTE_CIFRATO"))
nrow(ver_clienti)
# 24.935 clienti pdisc on line (su device)






# filtri 
skyit_v1 <- select(skyitdev_df, "external_id_post_evar",
                   "post_visid_high",
                   "post_visid_low",
                   "post_visid_concatenated",
                   "date_time",
                   "page_url_post_evar","page_url_post_prop","download_name_post_prop",
                   "secondo_livello_post_evar",
                   "terzo_livello_post_evar",
                   "visit_num",
                   "hit_source",
                   "exclude_hit", 
                   "post_channel")


skyit_v2 <- withColumn(skyit_v1, "date_time_dt", cast(skyit_v1$date_time, "date"))
skyit_v3 <- withColumn(skyit_v2, "date_time_ts", cast(skyit_v2$date_time, "timestamp"))


# filtro temporale
skyit_v4 <- filter(skyit_v3,"date_time_dt >= '2017-03-01'")
skyit_v5 <- filter(skyit_v4,"date_time_dt <= '2018-01-31'")


#unisco alla base
createOrReplaceTempView(skyit_v5,"skyit_v5")
createOrReplaceTempView(base_pdisc_coo,"base_pdisc_coo")

join_base_sez_pubblica <- sql("SELECT t1.COD_CONTRATTO, t1.COD_CLIENTE_FRUITORE,t1.COD_CLIENTE_CIFRATO,
                                      t1.data_ingr_pdisc_formdate, t1.post_visid_concatenated,
                                      t2.visit_num,t2.hit_source,t2.exclude_hit,
                                      t2.date_time, t2.date_time_dt, t2.date_time_ts,
                                      t2.page_url_post_evar,t2.page_url_post_prop,t2.download_name_post_prop,t2.secondo_livello_post_evar,t2.terzo_livello_post_evar,
                                      t2.post_channel
                              FROM base_pdisc_coo t1
                              INNER JOIN skyit_v5 t2
                              ON t1.post_visid_concatenated = t2.post_visid_concatenated")

#salvataggio
write.parquet(join_base_sez_pubblica,"/user/stefano.mazzucca/CJ_PDISC_scarico_skyitdev_post_pdisc1.parquet")



################################################################################################################################################################################
################################################################################################################################################################################

## Analisi sulle anbigazioni ##


CJ_PDISC_app_post_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_app_post_pdisc1.parquet")
View(head(CJ_PDISC_app_post_pdisc,100))
nrow(CJ_PDISC_app_post_pdisc)
# 1.089.218

CJ_PDISC_sito_pub_post_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_skyitdev_post_pdisc1.parquet")
## filtra su "post_channel" == "corporate"
CJ_PDISC_sito_pub_post_pdisc_1 <- filter(CJ_PDISC_sito_pub_post_pdisc, "post_channel like 'corporate'")
CJ_PDISC_sito_pub_post_pdisc_2 <- filter(CJ_PDISC_sito_pub_post_pdisc_1, "terzo_livello_post_evar NOT LIKE '%faidate%'")
View(head(CJ_PDISC_sito_pub_post_pdisc_2,100))
nrow(CJ_PDISC_sito_pub_post_pdisc_2)
# 5.672.439 -> (filtered: 1.780.685) -> (pubb: 1.027.345)

CJ_PDISC_sito_wsc_post_pdisc <- filter(CJ_PDISC_sito_pub_post_pdisc_1, "terzo_livello_post_evar LIKE '%faidate%'")
View(head(CJ_PDISC_sito_wsc_post_pdisc,100))
nrow(CJ_PDISC_sito_wsc_post_pdisc)
# 583.475


## Analisi sui clienti che navigano sulle pagine Sky ###########################################################################################################################
## filtro temporale tra 1 MARZO 2017 e 31 GENNAIO 2018 ##


valdist_pag_sito <- distinct(select(CJ_PDISC_sito_pub_post_pdisc_2, "COD_CLIENTE_CIFRATO"))
nrow(valdist_pag_sito)
# 21.973 clienti che navigano su sito pubblico

valdist_app <- distinct(select(CJ_PDISC_app_post_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_app)
# 11.409 clienti che naviagno su APP

valdist_sito_wsc <- distinct(select(CJ_PDISC_sito_wsc_post_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_sito_wsc)
# 18.941 clienti che navigano su sito wsc


sky_tot <- rbind(valdist_pag_sito, valdist_app, valdist_sito_wsc)

valdist_sky_tot <- distinct(select(sky_tot, "COD_CLIENTE_CIFRATO"))
nrow(valdist_sky_tot)
# 23.286 cleinti distinti che navigano tra MARZO 2017 e GENNAIO 2018 tra SITO SKY (Skyitdev) e APP




##############################################################################################################################################################################
## Analisi ###################################################################################################################################################################
##############################################################################################################################################################################



###

sky_sito_pub_1 <- withColumn(CJ_PDISC_sito_pub_post_pdisc_2, "appoggio_data", lit('2018-01-31'))
sky_sito_pub_2 <- withColumn(sky_sito_pub_1, "appoggio_data_dt", cast(sky_sito_pub_1$appoggio_data, "date"))
sky_sito_pub_3 <- withColumn(sky_sito_pub_2, "gg_in_pdisc", datediff(sky_sito_pub_2$appoggio_data_dt, sky_sito_pub_2$data_ingr_pdisc_formdate))

sky_sito_pub_3$flg_post_pdisc <- ifelse(sky_sito_pub_3$date_time_dt > sky_sito_pub_3$data_ingr_pdisc_formdate, 1, 0)
View(head(sky_sito_pub_3,1000))

sky_sito_pub_flg1 <- filter(sky_sito_pub_3, "flg_post_pdisc == 1")
nrow(sky_sito_pub_flg1)
# 229.466
valdist_flg1_pag_pub <- distinct(select(sky_sito_pub_flg1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_flg1_pag_pub)
# 12.208 clienti che navigano su sito pubblico Sky nel post-pdisc (tra l'INGRESSO in PDISC e il 31 GENNAIO 2018)




sky_wsc_1 <- withColumn(CJ_PDISC_sito_wsc_post_pdisc, "appoggio_data", lit('2018-01-31'))
sky_wsc_2 <- withColumn(sky_wsc_1, "appoggio_data_dt", cast(sky_wsc_1$appoggio_data, "date"))
sky_wsc_3 <- withColumn(sky_wsc_2, "gg_in_pdisc", datediff(sky_wsc_2$appoggio_data_dt, sky_wsc_2$data_ingr_pdisc_formdate))

sky_wsc_3$flg_post_pdisc <- ifelse(sky_wsc_3$date_time_dt > sky_wsc_3$data_ingr_pdisc_formdate, 1, 0)
View(head(sky_wsc_3,1000))

sky_wsc_flg1 <- filter(sky_wsc_3, "flg_post_pdisc == 1")
nrow(sky_wsc_flg1)
# 156.003
valdist_flg1_wsc <- distinct(select(sky_wsc_flg1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_flg1_wsc)
# 8.613 clienti che navigano su sito WSC Sky nel post-pdisc (tra l'INGRESSO in PDISC e il 31 GENNAIO 2018)




sky_app_1 <- withColumn(CJ_PDISC_app_post_pdisc, "appoggio_data", lit('2018-01-31'))
sky_app_2 <- withColumn(sky_app_1, "appoggio_data_dt", cast(sky_app_1$appoggio_data, "date"))
sky_app_3 <- withColumn(sky_app_2, "gg_in_pdisc", datediff(sky_app_2$appoggio_data_dt, sky_app_2$data_ingr_pdisc_formdate))

sky_app_3$flg_post_pdisc <- ifelse(sky_app_3$date_time_dt > sky_app_3$data_ingr_pdisc_formdate, 1, 0)
View(head(sky_wsc_3,1000))

sky_app_flg1 <- filter(sky_app_3, "flg_post_pdisc == 1")
nrow(sky_app_flg1)
# 283.417
valdist_flg1_app <- distinct(select(sky_app_flg1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_flg1_app)
# 6.096 clienti che navigano su APP Sky nel post-pdisc (tra l'INGRESSO in PDISC e il 31 GENNAIO 2018)




navigatori_post_pdisc <- rbind(valdist_flg1_pag_pub, valdist_flg1_wsc, valdist_flg1_app)

valdist_navigatori_post_pdisc <- distinct(select(navigatori_post_pdisc, "COD_CLIENTE_CIFRATO"))
View(head(valdist_navigatori_post_pdisc,100))
nrow(valdist_navigatori_post_pdisc)
# 14.715 clienti totali che navigano dopo l'invio del PDISC (dal giorno dopo e entro il 31 Gennaio 2018)


write.parquet(valdist_navigatori_post_pdisc, "/user/stefano.mazzucca/CJ_PDISC_navigatori_post_pdisc1.parquet")
write.df(repartition( valdist_navigatori_post_pdisc, 1), path = "/user/stefano.mazzucca/CJ_PDISC_navigatori_post_pdisc1.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


##############################################################################################################################################################################
## Creazione sequenze navigazione ############################################################################################################################################
##############################################################################################################################################################################

#filter_1 <- filter(sky_sito_pub_3, "page_url_post_evar like '%www.sky.it%'")
#filter_2 <- filter(filter_1, "flg_post_pdisc == 1")
filter_sito_pub <- filter(sky_sito_pub_flg1,
                          "secondo_livello_post_evar like '%homepage-sky%' or 
                          secondo_livello_post_evar like '%homepage sky%' or 
                          secondo_livello_post_evar like '%guidatv%' or
                          secondo_livello_post_evar like '%Guidatv%' or
                          secondo_livello_post_evar like '%pagine di servizio%' or
                          secondo_livello_post_evar like '%assistenza%' or
                          (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar like '%conosci%') or
                          (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar like '%gestisci%') or
                          (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar   like '%contatta%') or
                          (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar   like '%home%') or
                          (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar   like '%ricerca%' ) or
                          (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar   like '%risolvi%') or
                          secondo_livello_post_evar like '%fai da te%' or
                          secondo_livello_post_evar like '%extra%' or
                          secondo_livello_post_evar like '%tecnologia%' or
                          secondo_livello_post_evar like '%pacchetti-offerte%' ")
nrow(filter_sito_pub)
# 180.450

#filter_temp_wsc <- filter(sky_wsc_3, "flg_post_pdisc = 1")
filter_wsc <- filter(sky_wsc_flg1,"
                     terzo_livello_post_evar   like '%faidate home%' or
                     terzo_livello_post_evar   like '%faidate gestisci dati servizi%' or
                     terzo_livello_post_evar   like '%faidate arricchisci abbonamento%' or
                     terzo_livello_post_evar   like '%faidate fatture pagamenti%' or
                     terzo_livello_post_evar   like '%faidate webtracking%' or
                     terzo_livello_post_evar   like '%faidate extra%'") 
nrow(filter_wsc)
# 154.597

#filter_temp_app <- filter(sky_app_3, "flg_post_pdisc = 1")
filter_app <- filter(sky_app_flg1,"
                     site_section like '%homepage%' or
                     site_section like '%home%' or
                     site_section like '%intro%' or
                     site_section like '%login%' or
                     site_section like '%dati e fatture%' or
                     site_section like '%il mio abbonamento%' or
                     site_section like '%sky extra%' or
                     site_section like '%comunicazioni%' or
                     site_section like '%impostazioni%' or
                     site_section like '%i miei dati%' or
                     site_section like '%assistenza%' or
                     page_name_post_evar like '%assistenza:home%' or
                     page_name_post_evar like '%assistenza:contatta%' or
                     page_name_post_evar like '%assistenza:conosci%' or
                     page_name_post_evar like '%assistenza:ricerca%' or
                     page_name_post_evar like '%assistenza:gestisci%' or
                     page_name_post_evar like '%assistenza:risolvi%' or
                     site_section like '%widget%' or
                     page_name_post_evar like '%widget:dispositivi%' or
                     page_name_post_evar like '%widget:ultimafattura%' or
                     page_name_post_evar like '%widget:contatta sky%' or
                     page_name_post_evar like '%widget:gestisci%' or
                     page_name_post_evar = 'arricchisci abbonamento:home' ")
nrow(filter_app)
# 213.364



#### Pulizia sequenze di navigazione

## Sito pubblico

filter_sito_pub <- withColumn(filter_sito_pub,"concatena_chiave",
                              concat(filter_sito_pub$post_visid_concatenated, filter_sito_pub$visit_num))

createOrReplaceTempView(filter_sito_pub, "filter_sito_pub")
skypubblico <- sql("select COD_CLIENTE_CIFRATO, 
                   post_visid_concatenated, 
                   visit_num, 
                   concatena_chiave, 
                   date_time_dt, 
                   date_time_ts,
                   data_ingr_pdisc_formdate, 
                   hit_source, 
                   exclude_hit,
                   secondo_livello_post_evar, 
                   terzo_livello_post_evar,
                   download_name_post_prop,
                   page_url_post_evar
                   from filter_sito_pub")

skypubblico$sequenza <- ifelse(skypubblico$secondo_livello_post_evar == 'assistenza', 
                               concat_ws(sep="_", skypubblico$secondo_livello_post_evar, skypubblico$terzo_livello_post_evar), skypubblico$secondo_livello_post_evar)

#View(head(filter(skypubblico, "secondo_livello_post_evar == 'assistenza'"), 1000))
View(head(skypubblico,100))
nrow(skypubblico)
# 180.450 (ceck OK!)

createOrReplaceTempView(skypubblico,"skypubblico")

skypubblico <- sql("SELECT COD_CLIENTE_CIFRATO, 
                   post_visid_concatenated, 
                   visit_num, 
                   concatena_chiave, 
                   date_time_dt, 
                   date_time_ts, 
                   data_ingr_pdisc_formdate, 
                   hit_source, 
                   exclude_hit, 
                   sequenza,
                   download_name_post_prop
                   FROM skypubblico
                   ORDER BY COD_CLIENTE_CIFRATO")

skypubblico = withColumn(skypubblico, "canale", lit("sito_pubblico"))

View(head(skypubblico, 100))

## AGGIUNTA per "disdetta" su URL in "assistenza_conosci" #####################################################################################################################

# skypubblico$sequenza2 <- ifelse(contains(skypubblico$page_url_post_evar, 
#                                          c('assistenza/conosci/informazioni-sull-abbonamento/termini-e-condizioni/come-posso-dare-la-disdetta-del-mio-contratto.html')), 
#                                 'assistenza_conosci_disdetta', skypubblico$sequenza)
# # a <- filter(skypubblico, "sequenza2 LIKE '%assistenza_conosci_disdetta%'")
# # View(head(a,1000))
# # nrow(a)
# # # 8.063
# # View(head(skypubblico,1000))
# # nrow(skypubblico)
# # # 835.140 (ok!)
# skypubblico_1 <- select(skypubblico, "COD_CLIENTE_CIFRATO", 
#                         "post_visid_concatenated", 
#                         "visit_num", 
#                         "concatena_chiave", 
#                         "date_time_dt", 
#                         "date_time_ts", 
#                         "data_ingr_pdisc_formdate", 
#                         "hit_source", 
#                         "exclude_hit", 
#                         "sequenza2",
#                         "download_name_post_prop", 
#                         "canale")
# skypubblico_2 <- withColumnRenamed(skypubblico_1, "sequenza2", "sequenza")
# View(head(skypubblico_2,1000))
# nrow(skypubblico_2)
# # 835.140

###############################################################################################################################################################################



## Sito WSC

filter_wsc <- withColumn(filter_wsc,"concatena_chiave",
                         concat(filter_wsc$post_visid_concatenated, filter_wsc$visit_num))

createOrReplaceTempView(filter_wsc, "filter_wsc")
skywsc <- sql("select COD_CLIENTE_CIFRATO, 
              post_visid_concatenated, 
              visit_num, 
              concatena_chiave, 
              date_time_dt, 
              date_time_ts, 
              data_ingr_pdisc_formdate, 
              hit_source, 
              exclude_hit,
              terzo_livello_post_evar,
              download_name_post_prop
              from filter_wsc")

skywsc$sequenza <- skywsc$terzo_livello_post_evar
nrow(skywsc)
# 154.597 (ceck OK!)

createOrReplaceTempView(skywsc,"skywsc")

skywsc <- sql("SELECT COD_CLIENTE_CIFRATO, 
              post_visid_concatenated, 
              visit_num, 
              concatena_chiave, 
              date_time_dt, 
              date_time_ts, 
              data_ingr_pdisc_formdate, 
              hit_source, 
              exclude_hit,
              sequenza,
              download_name_post_prop
              FROM skywsc
              ORDER BY COD_CLIENTE_CIFRATO")

skywsc = withColumn(skywsc, "canale", lit("sito_wsc"))

View(head(skywsc, 100))



## APP

filter_app <- withColumn(filter_app,"concatena_chiave",
                         concat(filter_app$post_visid_concatenated, filter_app$visit_num))

createOrReplaceTempView(filter_app, "filter_app")
skyapp <- sql("SELECT COD_CLIENTE_CIFRATO, 
              post_visid_concatenated, 
              visit_num, 
              concatena_chiave, 
              date_time_dt, 
              date_time_ts,
              data_ingr_pdisc_formdate, 
              hit_source, 
              exclude_hit,
              site_section,
              page_name_post_evar
              FROM filter_app")

skyapp = withColumn(skyapp, "secondo_livello_post_evar", lit("app"))
skyapp$site_section_app <- concat_ws(sep=" ", skyapp$secondo_livello_post_evar, skyapp$site_section)
skyapp$page_name_app <- concat_ws(sep=" ", skyapp$secondo_livello_post_evar, skyapp$page_name_post_evar)

skyapp$sequenza <- ifelse(skyapp$site_section_app == 'app assistenza', skyapp$page_name_app, skyapp$site_section_app)
skyapp$sequenza <- ifelse(skyapp$site_section_app == 'app widget', skyapp$page_name_app, skyapp$site_section_app)

skyapp$sequenza <- ifelse(contains(skyapp$site_section_app, 'intro'), 'app home', 
                          ifelse(skyapp$site_section_app == 'app login', 'app home',
                                 ifelse(skyapp$site_section_app == 'app homepage', 'app home',
                                        skyapp$site_section_app)))

skyapp$sequenza = ifelse(contains(skyapp$page_name_app, c("conosci")), 'app assistenza conosci', 
                         ifelse(contains(skyapp$page_name_app, c("gestisci")), 'app assistenza gestisci', 
                                ifelse(contains(skyapp$page_name_app, c("assistenza:home")), 'app assistenza home',
                                       ifelse(contains(skyapp$page_name_app, c("contatta")), 'app assistenza contatta',
                                              ifelse(contains(skyapp$page_name_app, c("risolvi")), 'app assistenza risolvi',
                                                     ifelse(contains(skyapp$page_name_app, c("ricerca")), 'app assistenza ricerca',skyapp$sequenza))))))

skyapp$sequenza = ifelse(contains(skyapp$page_name_app, c("dispositivi")), 'app widget dispositivi', 
                         ifelse(contains(skyapp$page_name_app, c("ultimafattura")), 'app widget ultimafattura', 
                                ifelse(contains(skyapp$page_name_app, c("contatta sky")), 'app widget contatta sky',
                                       ifelse(contains(skyapp$page_name_app, c("widget:gestisci")), 'app widget gestisci', skyapp$sequenza))))

View(head(skyapp,100))
nrow(skyapp)
# 213.364 (ceck OK!)

createOrReplaceTempView(skyapp,"skyapp")
skyapp <- sql("SELECT COD_CLIENTE_CIFRATO, 
              post_visid_concatenated, 
              visit_num, 
              concatena_chiave, 
              date_time_dt, 
              date_time_ts,
              data_ingr_pdisc_formdate, 
              hit_source, 
              exclude_hit,
              sequenza
              FROM skyapp")

#skyapp = withColumn(skyapp, "download_name_post_prop", lit(""))
createOrReplaceTempView(skyapp,"skyapp")

skyapp_1 <- sql("select *, 
                NULL as download_name_post_prop
                from skyapp")

skyapp_1 = withColumn(skyapp_1, "canale", lit("app"))

View(head(skyapp_1,100))



## Unisco le 3 basi #############################################################################################################################################################

sky_pubbl_wsc_app <- rbind(skywsc, skypubblico, skyapp_1)

createOrReplaceTempView(sky_pubbl_wsc_app, "sky_pubbl_wsc_app")
sky_pubbl_wsc_app$sequenza <- regexp_replace(sky_pubbl_wsc_app$sequenza, " ", "_")
sky_pubbl_wsc_app$sequenza <- regexp_replace(sky_pubbl_wsc_app$sequenza, "-", "_")

createOrReplaceTempView(sky_pubbl_wsc_app,"sky_pubbl_wsc_app")
sky_pubbl_wsc_app_1 <- sql("select *
                           from sky_pubbl_wsc_app
                           order by COD_CLIENTE_CIFRATO, date_time_ts, concatena_chiave")

View(head(sky_pubbl_wsc_app_1, 1000))
str(sky_pubbl_wsc_app_1)

nrow(skypubblico) + nrow(skyapp) + nrow(skywsc)
# 548.411 
nrow(sky_pubbl_wsc_app_1)
# 548.411


write.parquet(sky_pubbl_wsc_app_1,"/user/stefano.mazzucca/CJ_PDISC_seq_nav_post_pdisc1.parquet")





## Leggo la tabella ##

CJ_seq_nav_post_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_seq_nav_post_pdisc1.parquet")
View(head(CJ_seq_nav_post_pdisc,100))
nrow(CJ_seq_nav_post_pdisc)
# 548.411

valdist_clienti <- SparkR::distinct(SparkR::select(CJ_seq_nav_post_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti)
# 14.024 navigatori post pdisc delle pagine selezionate


# ##############################################################################################################################################################################
# ## Converto dei parametri per l'elaborazione successiva
# ##############################################################################################################################################################################
# 
# CJ_seq_nav_post_pdisc_2 <- withColumn(CJ_seq_nav_post_pdisc, "appoggio", lit("A"))
# CJ_seq_nav_post_pdisc_3 <- withColumn(CJ_seq_nav_post_pdisc_2, "concatena_chiave_2", concat_ws("-", CJ_seq_nav_post_pdisc_2$appoggio, CJ_seq_nav_post_pdisc_2$concatena_chiave))
# CJ_seq_nav_post_pdisc_3$appoggio <- NULL
# View(head(CJ_seq_nav_post_pdisc_3,100))
# nrow(CJ_seq_nav_post_pdisc_3)
# # 514.706



##############################################################################################################################################################################
##############################################################################################################################################################################
## Analisi sulle metriche delle pagine visitate ##
##############################################################################################################################################################################
##############################################################################################################################################################################


createOrReplaceTempView(CJ_seq_nav_post_pdisc, "CJ_seq_nav_post_pdisc")
nav_canale_post_pdisc <- sql("select COD_CLIENTE_CIFRATO, 
                             last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                             canale,
                             count(canale) as n_visit
                             from CJ_seq_nav_post_pdisc
                             group by COD_CLIENTE_CIFRATO, canale
                             order by COD_CLIENTE_CIFRATO")
View(head(nav_canale_post_pdisc,100))
nrow(nav_canale_post_pdisc)
# 31.238 (16.331 clienti)



## Devo prima "trasporre" la matrice delle sequenze
## vai a CJ_PDISC_post_pdisc_2







################################################################################################################################################################################

## Verifico quanti clienti pdisc abbiano il consenso privacy (POST-PDISC)

flg_privacy <- read.df("/user/stefano.mazzucca/FLG_COM_TERZI.csv", source = "csv", header = "true", delimiter = ";")
View(head(flg_privacy,100))
nrow(flg_privacy)
# 37.976


# cl_post_pdisc_flg_privacy <- join(valdist_clienti, flg_privacy, valdist_clienti$COD_CLIENTE_CIFRATO == flg_privacy$COD_CLIENTE_CIFRATO, "left")
# View(head(cl, 100))
# nrow(cl_post_pdisc_flg_privacy)
# # 16.442

createOrReplaceTempView(valdist_clienti, "valdist_clienti")
createOrReplaceTempView(flg_privacy, "flg_privacy")

clienti_post_pdisc_flg_privacy <- sql("select t1.COD_CLIENTE_CIFRATO, t2.FLG_COMUNICAZIONE_TERZI
                                      from valdist_clienti t1
                                      left join flg_privacy t2
                                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(clienti_post_pdisc_flg_privacy,100))
nrow(clienti_post_pdisc_flg_privacy)
# 16.442

num_cl_flg_privacy_post_pdisc <- summarize(groupBy(clienti_post_pdisc_flg_privacy, "FLG_COMUNICAZIONE_TERZI"), count = n(clienti_post_pdisc_flg_privacy$FLG_COMUNICAZIONE_TERZI))
View(head(num_cl_flg_privacy_post_pdisc))
# FLG_COMUNICAZIONE_TERZI   count
#         0                 2.298
#         1                 14.144




## Verifico quanti clienti pdisc abbiano il consenso privacy (su TUTTA la base)

createOrReplaceTempView(valdist_base_clienti, "valdist_base_clienti")
createOrReplaceTempView(flg_privacy, "flg_privacy")

clienti_base_flg_privacy <- sql("select t1.COD_CLIENTE_CIFRATO, t2.FLG_COMUNICAZIONE_TERZI
                                       from valdist_base_clienti t1
                                       left join flg_privacy t2
                                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(clienti_base_flg_privacy,100))
nrow(clienti_base_flg_privacy)
# 37.976

num_cl_flg_privacy_base <- summarize(groupBy(clienti_base_flg_privacy, "FLG_COMUNICAZIONE_TERZI"), count = n(clienti_base_flg_privacy$FLG_COMUNICAZIONE_TERZI))
View(head(num_cl_flg_privacy_base))
# FLG_COMUNICAZIONE_TERZI   count
#         0                 6.093
#         1                 31.882









#chiudi sessione
sparkR.stop()
