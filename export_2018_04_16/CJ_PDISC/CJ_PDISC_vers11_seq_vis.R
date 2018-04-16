

# CJ_PDISC_vers11_sequenze_visite


#apri sessione
source("connection_R.R")
options(scipen = 1000)


######### Recupero il target PDISC da cui partire per analizzare le navigazioni degli utenti ####################################################################################

base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
nrow(base_pdisc)
# 37.976

valdist_base <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_base)
# 37.753


base_pdisc_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc.parquet")
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
nrow(base_pdisc_coo_2)
# 176.511

#################################################################################################################################################################################


#################################################################################################################################################################################
#################################################################################################################################################################################
## IMPO: i dati di navigazione sono raccolti tra MARZO 2017 e DICEMBRE 2017 ###################################################################################################
#################################################################################################################################################################################
#################################################################################################################################################################################


# ## Base sulle Properties Sky: skyitdev #########################################################################################################################################
# 
# skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
# 
# skyitdev_df_1 <- select(skyitdev_df,
#                         "external_id_post_evar",    
#                         "post_visid_high", "post_visid_low","post_visid_concatenated","date_time","visit_num",
#                         "page_url_post_evar", "page_url_post_prop", "download_name_post_prop", 
#                         "secondo_livello_post_evar","terzo_livello_post_evar",
#                         "hit_source","exclude_hit")
# 
# skyitdev_df_2 <- withColumn(skyitdev_df_1, "ts", cast(skyitdev_df_1$date_time, "timestamp"))
# skyitdev_df_3 <- filter(skyitdev_df_2,"ts>='2017-03-01 00:00:00'")
# skyitdev_df_4 <- filter(skyitdev_df_3,"ts<='2018-01-01 00:00:00'")
# # filtro successivo sulla base pdisc in join: t1.ts <= t2.data_ingr_pdisc_formdate


######### Recupero i dati di navigazione degli utenti per il sito pubblico ######################################################################################################

CJ_PDISC_tab_sito_pub <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_tab_sito_pub.parquet")
View(head(CJ_PDISC_tab_sito_pub,100))
nrow(CJ_PDISC_tab_sito_pub)
# 1.926.447

ver_valdist_utenti_tot <- distinct(select(CJ_PDISC_tab_sito_pub, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_utenti_tot)
# 21.110


filter_sito_pub <- filter(CJ_PDISC_tab_sito_pub,
                          "secondo_livello_post_evar like '%guidatv%' or
                           secondo_livello_post_evar like '%Guidatv%' or
                           secondo_livello_post_evar like '%pagine di servizio%' or
                           secondo_livello_post_evar like '%assistenza%' or
                           terzo_livello_post_evar   like '%conosci%' or
                           terzo_livello_post_evar   like '%gestisci%' or
                           terzo_livello_post_evar   like '%contatta%' or
                           terzo_livello_post_evar   like '%home%' or
                           terzo_livello_post_evar   like '%ricerca%' or
                           terzo_livello_post_evar   like '%risolvi%' or
                           secondo_livello_post_evar like '%fai da te%' or
                           secondo_livello_post_evar like '%extra%' or
                           secondo_livello_post_evar like '%tecnologia%' or
                           secondo_livello_post_evar like '%pacchetti-offerte%' ")
# or page_url_post_evar like '%/pacchetti-offerte/%' 

View(head(filter_sito_pub,100))
nrow(filter_sito_pub)
# 983.955

ver_valdist_utenti <- distinct(select(filter_sito_pub, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_utenti)
# 20.547 utenti che navigano sul sito pubblico (vs 37.753 dei pdisc totali) 

#################################################################################################################################################################################


######### Recupero i dati di navigazione degli utenti per il WSC ################################################################################################################

CJ_PDISC_tab_wsc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_tab_wsc.parquet")
View(head(CJ_PDISC_tab_wsc,100))
nrow(CJ_PDISC_tab_wsc)
# 582.479

ver_valdist_utenti_wsc_tot <- distinct(select(CJ_PDISC_tab_wsc, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_utenti_wsc_tot)
# 17.153


filter_wsc <- filter(CJ_PDISC_tab_wsc,"
                    terzo_livello_post_evar   like '%faidate gestisci dati servizi%' or
                    terzo_livello_post_evar   like '%faidate arricchisci abbonamento%' or
                    terzo_livello_post_evar   like '%faidate fatture pagamenti%' or
                    terzo_livello_post_evar   like '%faidate webtracking%' or
                    terzo_livello_post_evar   like '%faidate extra%' ") 
# or page_url_post_evar like '%abbonamento.sky.it/skyportal/faces/faiDaTe/subscription%' 

View(head(filter_wsc,100))
nrow(filter_wsc)
# 424.839

ver_valdist_utenti_wsc <- distinct(select(filter_wsc, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_utenti_wsc)
# 16.633 utenti che navigano sul WSC (vs 37.753 dei pdisc totali)

#################################################################################################################################################################################


######### Recupero i dati di navigazione degli utenti per le APP ################################################################################################################

CJ_PDISC_tab_app <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_tab_app.parquet")
View(head(CJ_PDISC_tab_app,100))
nrow(CJ_PDISC_tab_app)
# 7.434.135

ver_valdist_utenti_app_tot <- distinct(select(CJ_PDISC_tab_app, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_utenti_app_tot)
# 9.498 utenti che utlizzano l'APP di Sky


filter_app <- filter(CJ_PDISC_tab_app,"
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
                      page_name_post_evar = 'arricchisci abbonamento:home' ") # <- controlla!

View(head(filter_app,100))
nrow(filter_app)
# 2.844.660

ver_valdist_utenti_app <- distinct(select(filter_app, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_utenti_app)
# 8.985 utenti che navigano su APP (vs 37.753 dei pdisc totali) 

#################################################################################################################################################################################

# ## VERIFICHE
# ## I clienti intercettati, fanno tutti parte dei 24k con cookies (base_pdisc_coo) ?
# 
# # base_pdisc_coo # 24.340
# # ver_valdist_utenti_tot # 21.110
# # ver_valdist_utenti_wsc_tot # 17.153
# # ver_valdist_utenti_app_tot # 9.498
# 
# createOrReplaceTempView(ver_valdist_utenti_tot, "ver_valdist_utenti_tot")
# createOrReplaceTempView(base_pdisc_coo, "base_pdisc_coo")
# 
# ver_1 <- sql("select distinct t1.COD_CLIENTE_CIFRATO
#              from ver_valdist_utenti_tot t1
#              inner join base_pdisc_coo t2
#               on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
# nrow(ver_1)
# # 21.110 ceck OK!
# 
# 
# createOrReplaceTempView(ver_valdist_utenti_wsc_tot, "ver_valdist_utenti_wsc_tot")
# createOrReplaceTempView(base_pdisc_coo, "base_pdisc_coo")
# 
# ver_2 <- sql("select distinct t1.COD_CLIENTE_CIFRATO
#              from ver_valdist_utenti_wsc_tot t1
#              inner join base_pdisc_coo t2
#               on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
# nrow(ver_2)
# # 17.153 ceck OK!
# 
# 
# createOrReplaceTempView(ver_valdist_utenti_app_tot, "ver_valdist_utenti_app_tot")
# createOrReplaceTempView(base_pdisc_coo, "base_pdisc_coo")
# 
# ver_3 <- sql("select distinct t1.COD_CLIENTE_CIFRATO
#              from ver_valdist_utenti_app_tot t1
#              inner join base_pdisc_coo t2
#               on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
# nrow(ver_3)
# # 9.073 DA VEIRIFICAREEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE
# # 9.498 - 9.073 = 425 utenti "ballerini".......................................................................................................................................
# 
# 
# createOrReplaceTempView(ver_valdist_utenti_tot, "ver_valdist_utenti_tot")
# createOrReplaceTempView(ver_valdist_utenti_wsc_tot, "ver_valdist_utenti_wsc_tot")
# 
# ver_4 <- sql("select distinct t1.COD_CLIENTE_CIFRATO
#              from ver_valdist_utenti_tot t1
#              inner join ver_valdist_utenti_wsc_tot t2
#               on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(ver_4)
# # 17.020 vs 17.153 degli utenti in WSC mancano:
# # 17.153 - 17.020 = 133 utenti in WSC ma non in SITO PUBBLICO
# 
# 
# createOrReplaceTempView(ver_valdist_utenti_tot, "ver_valdist_utenti_tot")
# createOrReplaceTempView(ver_valdist_utenti_app_tot, "ver_valdist_utenti_app_tot")
# 
# ver_5 <- sql("select distinct t1.COD_CLIENTE_CIFRATO
#              from ver_valdist_utenti_tot t1
#              inner join ver_valdist_utenti_app_tot t2
#               on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(ver_5)
# # 8.486 vs 9.498 degli utenti APP mancano:
# # 9.498 - 8.486 = 1.012 utenti che navigano in APP ma non su SITO PUBBLICO
# 
# 
# createOrReplaceTempView(ver_valdist_utenti_wsc_tot, "ver_valdist_utenti_wsc_tot")
# createOrReplaceTempView(ver_valdist_utenti_app_tot, "ver_valdist_utenti_app_tot")
# 
# ver_6 <- sql("select distinct t1.COD_CLIENTE_CIFRATO
#              from ver_valdist_utenti_wsc_tot t1
#              inner join ver_valdist_utenti_app_tot t2
#               on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(ver_6)
# # 6.805 vs 9.498 degli utenti APP mancano:
# # 9.498 - 6.805 = 2.693 utenti che navigano in APP ma non su WSC
# 
# 
# # JOIN fra tutte e 3 le basi per capire quanti utenti distinti "intercettiamo" on line su siti di Propetries Sky:
# 
# createOrReplaceTempView(ver_valdist_utenti_tot, "ver_valdist_utenti_tot")
# createOrReplaceTempView(ver_valdist_utenti_wsc_tot, "ver_valdist_utenti_wsc_tot")
# createOrReplaceTempView(ver_valdist_utenti_app_tot, "ver_valdist_utenti_app_tot")
# createOrReplaceTempView(base_pdisc_coo, "base_pdisc_coo")
# 
# ver_left <- sql("select distinct t1.COD_CLIENTE_CIFRATO as base, t2.COD_CLIENTE_CIFRATO as sito_pub, t3.COD_CLIENTE_CIFRATO as wsc, t4.COD_CLIENTE_CIFRATO as app
#              from base_pdisc_coo t1
#              left join ver_valdist_utenti_tot t2
#               on t1.external_id_post_evar = t2.COD_CLIENTE_CIFRATO
#              left join ver_valdist_utenti_wsc_tot t3
#               on t1.external_id_post_evar = t3.COD_CLIENTE_CIFRATO
#              left join ver_valdist_utenti_app_tot t4
#               on t1.external_id_post_evar = t4.COD_CLIENTE_CIFRATO")
# View(head(ver_left,100))
# nrow(ver_left)
# # 24.340
# 
# ver_left_1 <- filter(ver_left, "sito_pub is NULL and wsc is NULL and app is NULL")
# nrow(ver_left_1)
# # 2.546
# # 24.340 - 2.546 = 21.794 utenti intercettati nel filtro temporale impostato!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#  

#################################################################################################################################################################################








#chiudi sessione
sparkR.stop()
