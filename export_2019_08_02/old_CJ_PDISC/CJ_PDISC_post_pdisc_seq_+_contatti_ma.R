
# CJ_PDISC_post_ev
#scarico tabellone di navigazione POST-PDISC (per APP)

#apri sessione
source("connection_R.R")
options(scipen = 1000)



### recupera basi ###############################################################################################################################################################

base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
nrow(base_pdisc) # 37.976

################################################################################################################################################################################
################################################################################################################################################################################
#### unisco le visite sulla APP
################################################################################################################################################################################
################################################################################################################################################################################

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")


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
skyappwsc_5 <- filter(skyappwsc_4,"date_time_dt <= '2018-02-10'")

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


write.parquet(join_base_app,"/user/stefano.mazzucca/CJ_PDISC_scarico_app_post_pdisc.parquet")


################################################################################################################################################################################
################################################################################################################################################################################

# lo scaricone post pdisc del sito/wsc e' in: 
# write.parquet(join_base_sez_pubblica,"/user/valentina/CJ_PDISC_scarico_pag_sito_post_pdisc.parquet")
# 
# 
# eventi del CRM che hanno come unico filtro temporale data_evento <= data pdisc :
# write.parquet(base_all_eventi_pre_4,"/user/valentina/CJ_PDISC_all_eventi_pre_pdisc.parquet")
#
# Eventi CRM post pdisc: 
# ---?


CJ_PDISC_app_post_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_app_post_pdisc.parquet")
View(head(CJ_PDISC_app_post_pdisc,100))
nrow(CJ_PDISC_app_post_pdisc)
# 1.145.548

CJ_PDISC_sito_pub_post_pdisc <- read.parquet("/user/valentina/CJ_PDISC_scarico_pag_sito_post_pdisc.parquet")
View(head(CJ_PDISC_sito_pub_post_pdisc,100))
nrow(CJ_PDISC_sito_pub_post_pdisc)
# 5.016.097

## manca anche base contatti CRM post pdisc



## Analisi sui clienti che navigano sulle pagine Sky ###########################################################################################################################
## filtro temporale tra 1 MARZO 2017 e 10 FEBBRAIO 2018 ##

valdist_pag_sito <- distinct(select(CJ_PDISC_sito_pub_post_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_pag_sito)
# 22.300

valdist_app <- distinct(select(CJ_PDISC_app_post_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_app)
# 11.549 clienti che naviagno su APP

sky_tot <- union(valdist_pag_sito, valdist_app)

valdist_sky_tot <- distinct(select(sky_tot, "COD_CLIENTE_CIFRATO"))
nrow(valdist_sky_tot)
# 23.382 cleinti distinti che navigano tra MARZO e DICEMBRE 2017 tra SITO SKY (Skyitdev) e APP 




sky_sito_pub <- filter(CJ_PDISC_sito_pub_post_pdisc,"terzo_livello_post_evar NOT LIKE '%faidate%' ")

valdist_clienti_pag_pub <- distinct(select(sky_sito_pub, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pag_pub)
# 21.907 clienti che naviagno su sito pubblico Sky


sky_wsc <- filter(CJ_PDISC_sito_pub_post_pdisc,"terzo_livello_post_evar LIKE '%faidate%' ")

valdist_clienti_wsc <- distinct(select(sky_wsc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_wsc)
# 18.548 clienti che naviagno su sito WSC




##############################################################################################################################################################################
## Analisi ###################################################################################################################################################################
##############################################################################################################################################################################


sky_sito_pub_1 <- withColumn(sky_sito_pub, "appoggio_data", lit('2018-01-31'))
sky_sito_pub_2 <- withColumn(sky_sito_pub_1, "appoggio_data_dt", cast(sky_sito_pub_1$appoggio_data, "date"))
sky_sito_pub_3 <- withColumn(sky_sito_pub_2, "gg_in_pdisc", datediff(sky_sito_pub_2$appoggio_data_dt, sky_sito_pub_2$data_ingr_pdisc_formdate))

sky_sito_pub_3$flg_post_pdisc <- ifelse(sky_sito_pub_3$date_time_dt >= sky_sito_pub_3$data_ingr_pdisc_formdate & 
                                        sky_sito_pub_3$date_time_dt <= sky_sito_pub_3$appoggio_data_dt, 1, 0)
View(head(sky_sito_pub_3,1000))

sky_sito_pub_flg1 <- filter(sky_sito_pub_3, "flg_post_pdisc == 1")
valdist_flg1_pag_pub <- distinct(select(sky_sito_pub_flg1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_flg1_pag_pub)
# 11.914 clienti che navigano su sito pubblico Sky nel post-pdisc (tra l'INGRESSO in PDISC e il 31 GENNAIO 2018)




sky_wsc_1 <- withColumn(sky_wsc, "appoggio_data", lit('2018-01-31'))
sky_wsc_2 <- withColumn(sky_wsc_1, "appoggio_data_dt", cast(sky_wsc_1$appoggio_data, "date"))
sky_wsc_3 <- withColumn(sky_wsc_2, "gg_in_pdisc", datediff(sky_wsc_2$appoggio_data_dt, sky_wsc_2$data_ingr_pdisc_formdate))

sky_wsc_3$flg_post_pdisc <- ifelse(sky_wsc_3$date_time_dt >= sky_wsc_3$data_ingr_pdisc_formdate & 
                                     sky_wsc_3$date_time_dt <= sky_wsc_3$appoggio_data_dt, 1, 0)
View(head(sky_wsc_3,1000))

sky_wsc_flg1 <- filter(sky_wsc_3, "flg_post_pdisc == 1")
valdist_flg1_wsc <- distinct(select(sky_wsc_flg1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_flg1_wsc)
# 7.282 clienti che navigano su sito WSC Sky nel post-pdisc (tra l'INGRESSO in PDISC e il 31 GENNAIO 2018)





sky_app_1 <- withColumn(CJ_PDISC_app_post_pdisc, "appoggio_data", lit('2018-01-31'))
sky_app_2 <- withColumn(sky_app_1, "appoggio_data_dt", cast(sky_app_1$appoggio_data, "date"))
sky_app_3 <- withColumn(sky_app_2, "gg_in_pdisc", datediff(sky_app_2$appoggio_data_dt, sky_app_2$data_ingr_pdisc_formdate))

sky_app_3$flg_post_pdisc <- ifelse(sky_app_3$date_time_dt >= sky_app_3$data_ingr_pdisc_formdate & 
                                     sky_app_3$date_time_dt <= sky_app_3$appoggio_data_dt, 1, 0)
View(head(sky_wsc_3,1000))

sky_app_flg1 <- filter(sky_app_3, "flg_post_pdisc == 1")
valdist_flg1_app <- distinct(select(sky_app_flg1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_flg1_app)
# 6.215 clienti che navigano su APP Sky nel post-pdisc (tra l'INGRESSO in PDISC e il 31 GENNAIO 2018)




navigatori_post_pdisc <- rbind(valdist_flg1_pag_pub, valdist_flg1_wsc, valdist_flg1_app)

valdist_navigatori_post_pdisc <- distinct(select(navigatori_post_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_navigatori_post_pdisc)
# 14.596 clienti totali che navigano nell'ultimo mese prima del PDISC




##############################################################################################################################################################################
## Creazione sequenze navigazione ############################################################################################################################################
##############################################################################################################################################################################

filter_1 <- filter(sky_sito_pub_3, "page_url_post_evar like '%www.sky.it%'")
filter_2 <- filter(filter_1, "flg_post_pdisc = 1")
filter_sito_pub <- filter(filter_2,
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
# 148.090

filter_temp_wsc <- filter(sky_wsc_3, "flg_post_pdisc = 1")
filter_wsc <- filter(filter_temp_wsc,"
                     terzo_livello_post_evar   like '%faidate home%' or
                     terzo_livello_post_evar   like '%faidate gestisci dati servizi%' or
                     terzo_livello_post_evar   like '%faidate arricchisci abbonamento%' or
                     terzo_livello_post_evar   like '%faidate fatture pagamenti%' or
                     terzo_livello_post_evar   like '%faidate webtracking%' or
                     terzo_livello_post_evar   like '%faidate extra%' ") 
nrow(filter_wsc)
# 147.774

filter_temp_app <- filter(sky_app_3, "flg_post_pdisc = 1")
filter_app <- filter(filter_temp_app,"
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
# 218.842



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
                       download_name_post_prop
                   from filter_sito_pub")

skypubblico$sequenza <- ifelse(skypubblico$secondo_livello_post_evar == 'assistenza', 
                               concat_ws(sep="_", skypubblico$secondo_livello_post_evar, skypubblico$terzo_livello_post_evar), skypubblico$secondo_livello_post_evar)

#View(head(filter(skypubblico, "secondo_livello_post_evar == 'assistenza'"), 1000))
View(head(skypubblico,100))
nrow(skypubblico)
# 148.090

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
# 147.774

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
# 218.842

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




## Unisco le 3 basi #############################################################################################################################################################

sky_pubbl_wsc_app <- rbind(skywsc, skypubblico, skyapp_1)

createOrReplaceTempView(sky_pubbl_wsc_app, "sky_pubbl_wsc_app")
sky_pubbl_wsc_app$sequenza <- regexp_replace(sky_pubbl_wsc_app$sequenza, " ", "_")

createOrReplaceTempView(sky_pubbl_wsc_app,"sky_pubbl_wsc_app")
sky_pubbl_wsc_app_1 <- sql("select *
                           from sky_pubbl_wsc_app
                           order by COD_CLIENTE_CIFRATO, date_time_ts, concatena_chiave")

View(head(sky_pubbl_wsc_app_1, 1000))
str(sky_pubbl_wsc_app_1)

nrow(skypubblico) + nrow(skyapp) + nrow(skywsc)
# 514.706 
nrow(sky_pubbl_wsc_app_1)
# 514.706


write.parquet(sky_pubbl_wsc_app_1,"/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_post_pdisc.parquet")





## Leggo la tabella ##

CJ_seq_nav_post_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_post_pdisc.parquet")
View(head(CJ_seq_nav_post_pdisc,100))
nrow(CJ_seq_nav_post_pdisc)
# 514.706

valdist_clienti <- distinct(select(CJ_seq_nav_post_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti)
# 12.902 navigatori post pdisc delle pagine selezionate


##############################################################################################################################################################################
## Converto dei parametri per l'elaborazione successiva
##############################################################################################################################################################################

CJ_seq_nav_post_pdisc_2 <- withColumn(CJ_seq_nav_post_pdisc, "appoggio", lit("A"))
CJ_seq_nav_post_pdisc_3 <- withColumn(CJ_seq_nav_post_pdisc_2, "concatena_chiave_2", concat_ws("-", CJ_seq_nav_post_pdisc_2$appoggio, CJ_seq_nav_post_pdisc_2$concatena_chiave))
CJ_seq_nav_post_pdisc_3$appoggio <- NULL
View(head(CJ_seq_nav_post_pdisc_3,100))
nrow(CJ_seq_nav_post_pdisc_3)
# 514.706



# export 
write.df(repartition( CJ_seq_nav_post_pdisc_3, 1),path = "/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_post_pdisc_tot.csv", "csv", sep=";", mode = "overwrite", header=TRUE)








pdisc_path <- "/user/stefano.mazzucca/CJ_PDISC_seq_nav_post_pdisc_reshaped.csv"
reshaped <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ",")
View(head(reshaped,1000))
nrow(reshaped)
# 71.774


ver_clienti <- distinct(select(reshaped, "COD_CLIENTE_CIFRATO"))
nrow(ver_clienti)
# 12.902

reshaped_2 <- withColumn(reshaped, "dt", cast(reshaped$date_time_dt, "date"))
reshaped_3 <- withColumn(reshaped_2, "date_pdisc", cast(reshaped$data_ingr_pdisc, "date"))

View(head(reshaped_3, 100))




##############################################################################################################################################################################
##############################################################################################################################################################################
##############################################################################################################################################################################




##############################################################################################################################################################################
## Metto insieme eventi DIGITAL con quelli di MARKETING AUTOMATION
##############################################################################################################################################################################

########################################################################################################################################################################
########################################################################################################################################################################
# Sequenze di navigazione DIGITAL

seq_nav_post_pdisc <- read.df("/user/stefano.mazzucca/CJ_PDISC_seq_nav_post_pdisc_reshaped.csv", source = "csv", header = TRUE)
nrow(seq_nav_post_pdisc) 
# 71.774


## Sistemo i parametri

seq_nav_post_pdisc$dwnl <- ifelse(contains(seq_nav_post_pdisc$download_name_post_prop, c(".pdf")), "dwnl_modulo", NA)

seq_nav_post_pdisc_2 <- withColumn(seq_nav_post_pdisc, "LEV_1", lit("DIGITAL"))
seq_nav_post_pdisc_2$CANALE_1 <- ifelse(contains(seq_nav_post_pdisc_2$canale, c("sito")), "WEB", "APP")

seq_nav_post_pdisc_3 <- withColumn(seq_nav_post_pdisc_2, "MOTIVO_CNT", seq_nav_post_pdisc_2$sequenza)
seq_nav_post_pdisc_4 <- withColumn(seq_nav_post_pdisc_3, "LEV_1_CANALE_1", concat_ws("_", seq_nav_post_pdisc_3$LEV_1, seq_nav_post_pdisc_3$CANALE_1))
seq_nav_post_pdisc_5 <- withColumn(seq_nav_post_pdisc_4, "MOTIVO_CNT", concat_ws(", ", seq_nav_post_pdisc_4$MOTIVO_CNT, seq_nav_post_pdisc_4$dwnl))

seq_nav_post_pdisc_5 <- select(seq_nav_post_pdisc_4, "COD_CLIENTE_CIFRATO", "date_time_dt", "date_time_ts", "data_ingr_pdisc",
                    "LEV_1", "CANALE_1", "LEV_1_CANALE_1", "MOTIVO_CNT")

seq_nav_post_pdisc_6 <- withColumn(seq_nav_post_pdisc_5, "date_time_ok",substring_index(seq_nav_post_pdisc_5$date_time_ts, ',', 1) )

seq_nav_post_pdisc_6$date_time_ts <- NULL

seq_nav_post_pdisc_7 <- withColumn(seq_nav_post_pdisc_6, "data_evento_ts", cast(seq_nav_post_pdisc_6$date_time_ok,"timestamp"))

seq_nav_post_pdisc_7$date_time_ok <- NULL

seq_nav_post_pdisc_8 <- withColumnRenamed(seq_nav_post_pdisc_7, "date_time_dt", "data_evento_dt")

seq_nav_post_pdisc_9 <- withColumn(seq_nav_post_pdisc_8, "data_evento_dt", cast(seq_nav_post_pdisc_8$data_evento_dt,"date"))
seq_nav_post_pdisc_10 <- withColumn(seq_nav_post_pdisc_9, "data_ingr_pdisc_formdate", cast(seq_nav_post_pdisc_9$data_ingr_pdisc,"date"))


#ordino le variabili
seq_nav_post_pdisc_11 <- select(seq_nav_post_pdisc_10,
                     "COD_CLIENTE_CIFRATO","data_evento_dt","data_ingr_pdisc_formdate",
                     "LEV_1","CANALE_1","LEV_1_CANALE_1","MOTIVO_CNT")

View(head(seq_nav_post_pdisc_11,2000))
nrow(seq_nav_post_pdisc_11)
# 71.774


########################################################################################################################################################################
########################################################################################################################################################################
## Contatti di Marketing Automation

pdisc_contatti_ma <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_contatti_marketing_automation.parquet")

pdisc_contatti_ma_1 <- withColumnRenamed(pdisc_contatti_ma, "data", "data_evento_dt")
pdisc_contatti_ma_2 <- withColumnRenamed(pdisc_contatti_ma_1, "canale", "CANALE_1")
pdisc_contatti_ma_3 <- withColumn(pdisc_contatti_ma_2, "LEV_1", lit("MARKETING_AUTOMATION"))
pdisc_contatti_ma_4 <- withColumn(pdisc_contatti_ma_3, "LEV_1_CANALE_1", concat_ws("_", pdisc_contatti_ma_3$LEV_1, pdisc_contatti_ma_3$CANALE_1))
pdisc_contatti_ma_5 <- withColumn(pdisc_contatti_ma_4, "MOTIVO_CNT", lit("RETENTION"))

pdisc_contatti_ma_6 <- select(pdisc_contatti_ma_5, "COD_CLIENTE_CIFRATO", "data_evento_dt", "data_ingr_pdisc_formdate",
                              "LEV_1", "CANALE_1", "LEV_1_CANALE_1", "MOTIVO_CNT")

View(head(pdisc_contatti_ma_6,100))
nrow(pdisc_contatti_ma_6)
# 370.767


########################################################################################################################################################################
########################################################################################################################################################################
## Unione dei contatti

union_ma_nav <- union(pdisc_contatti_ma_6, seq_nav_post_pdisc_11)

#View(head(union_ma_nav,1000))

createOrReplaceTempView(union_ma_nav, "union_ma_nav")
union_ma_nav_2 <- sql("select *
                       from union_ma_nav
                       order by COD_CLIENTE_CIFRATO, data_evento_dt")
View(head(union_ma_nav_2,1000))
nrow(union_ma_nav_2) 
# 442.541


## Controllo sugli eventi digital
filtra_digital <- filter(union_ma_nav_2,"LEV_1='DIGITAL'")
filtra_digital <- arrange(filtra_digital, filtra_digital$COD_CLIENTE_CIFRATO, filtra_digital$data_evento_dt)
View(head(filtra_digital,2000))
nrow(filtra_digital)
# 71.774




write.df(repartition( union_ma_nav_2, 1),path = "/user/stefano.mazzucca/CJ_PDISC_contatti_post_pdisc.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


write.parquet(union_ma_nav_2,"/user/stefano.mazzucca/CJ_PDISC_contatti_post_pdisc.parquet")




## Leggo la tabella ##

CJ_PDISC_contatti_post_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_contatti_post_pdisc.parquet")
View(head(CJ_PDISC_contatti_post_pdisc,1000))
nrow(CJ_PDISC_contatti_post_pdisc)
# 442.541



## Verifiche ###################################################################################################################################################################

ver_clienti <- distinct(select(CJ_PDISC_contatti_post_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(ver_clienti)
# 30.875

ver_filter_ma <- distinct(select(filter(CJ_PDISC_contatti_post_pdisc, "LEV_1 like '%MARKETING%'"), "COD_CLIENTE_CIFRATO"))
nrow(ver_filter_ma)
# 27.541 (vs 27.541 dei clienti contattati da ma -> ceck OK!)

ver_filter_digital <- distinct(select(filter(CJ_PDISC_contatti_post_pdisc, "LEV_1 like '%DIGITAL%'"), "COD_CLIENTE_CIFRATO"))
nrow(ver_filter_digital)
# 12.902 (vs 12.902 dei clienti intercettati on line post-pdisc -> ceck OK!)


createOrReplaceTempView(ver_filter_ma, "ver_filter_ma")
createOrReplaceTempView(ver_filter_digital, "ver_filter_digital")

anti_join_clienti <- sql("select t1.COD_CLIENTE_CIFRATO
                          from ver_filter_digital t1
                          left join ver_filter_ma t2 
                            on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO where t2.COD_CLIENTE_CIFRATO is NULL")
nrow(anti_join_clienti)
# 3.334 clienti in DIGITAL ma non in MARKETING AUTOMATION.
# 9.568 clienti sia in DIGITAL, sia in MA.


ver_data <- filter(CJ_PDISC_contatti_post_pdisc, "data_evento_dt >= data_ingr_pdisc_formdate")
nrow(ver_data)
# 439.387 (vs 442.541 record totali...)

ver_data_2 <- filter(CJ_PDISC_contatti_post_pdisc, "data_evento_dt < data_ingr_pdisc_formdate")
nrow(ver_data_2)
# 3.154 (vs 442.541 record totali... -> sono tutti contatti di MARKETING AUTOMATION ?)
ver_data_3 <- filter(ver_data_2, "LEV_1 like '%DIGITAL%'")
nrow(ver_data_3)
# 0 (SI, sono tutti di MA!!)
ver_data_2_clienti <- distinct(select(ver_data_2, "COD_CLIENTE_CIFRATO"))
nrow(ver_data_2_clienti)
# 1.051


#chiudi sessione
sparkR.stop()
