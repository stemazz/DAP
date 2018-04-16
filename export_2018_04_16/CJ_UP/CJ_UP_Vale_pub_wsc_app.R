#apri sessione
source("connection_R.r")
options(scipen = 1000)


#################################################################################################################
#################################################################################################################
#recupera basi

#base all
base_all_dett <- read.parquet("/user/valentina/CJ_UP_base_filtrata_4.parquet")
nrow(base_all_dett) #3694583

#campione + target
base_campionata <- read.parquet("/user/stefano.mazzucca/CJ_UP_campione.parquet") #93374

# createOrReplaceTempView(base_campionata,"base_campionata")
# verifica <- sql("select flg_up_3m, count(distinct COD_CONTRATTO) as num_COD_CONTRATTO, count(distinct SKY_ID) as num_SKY_ID
# from base_campionata group by flg_up_3m")
# View(head(verifica,100))
# 
# verifica_2 <- sql("select digitalizzazione_bin, count(distinct COD_CONTRATTO) as num_COD_CONTRATTO, count(distinct SKY_ID) as num_SKY_ID
# from base_campionata where flg_up_3m=0 group by digitalizzazione_bin ")
# View(head(verifica_2,100))


#base con i cookies
recupera_cookies_all <- read.parquet("/user/stefano.mazzucca/CJ_UP_chiavi_cookies_campione.parquet")


#aggiungi info
base_campionata_2 <- distinct(select(base_campionata,
                                     "SKY_ID","flg_up_3m","Data_ordine_3m","canale_ordine_3m","DATA_t0_ts",
                                     "DATA_t0_4m_ts","digitalizzazione_bin","fascia_digitalizzazione"
))
nrow(base_campionata_2) #92499

createOrReplaceTempView(recupera_cookies_all,"recupera_cookies_all")
createOrReplaceTempView(base_campionata_2,"base_campionata_2")

recupera_cookies_all_2 <- sql("select t2.*,
                              t1.external_id_post_evar,t1.post_visid_high,t1.post_visid_low,t1.post_visid_concatenated
                              from recupera_cookies_all t1 inner join base_campionata_2 t2
                              on t1.SKY_ID=t2.SKY_ID")


#################################################################################################################
#################################################################################################################
#### unisco le visite alla sezione pubblica del sito
#################################################################################################################
#################################################################################################################

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")


#inizia selezione
skyit_v1 <- select(skyitdev_df,
                   "external_id_post_evar", 
                   "post_visid_high", "post_visid_low","post_visid_concatenated","date_time",
                   "secondo_livello_post_evar","terzo_livello_post_evar")

skyit_v2 <- withColumn(skyit_v1, "ts", cast(skyit_v1$date_time, "timestamp"))
skyit_v3 <- filter(skyit_v2,"ts>='2017-03-01 00:00:00' and ts<='2017-10-01 00:00:00' ")

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
createOrReplaceTempView(recupera_cookies_all_2,"recupera_cookies_all_2")

join_base_sez_pubblica <- sql("select t1.*, 
                              t2.secondo_livello_post_evar,t2.terzo_livello_post_evar,
                              t2.ts as data_visualizzaz_pagina
                              from recupera_cookies_all_2 t1 inner join skyit_v4 t2
                              on t1.post_visid_concatenated=t2.post_visid_concatenated
                              and t2.ts>=t1.DATA_t0_4m_ts and t2.ts<=t1.DATA_t0_ts")

write.parquet(join_base_sez_pubblica,"/user/valentina/CJ_UP_scarico_pag_viste_sito.parquet")

#leggi tabella
CJ_UP_scarico_pag_viste_sito <- read.parquet("/user/valentina/CJ_UP_scarico_pag_viste_sito.parquet")
nrow(CJ_UP_scarico_pag_viste_sito) #3.080.553
View(head(CJ_UP_scarico_pag_viste_sito,100))




#################################################################################################################
#################################################################################################################
#### unisco le visite al WSC
#################################################################################################################
#################################################################################################################

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")

#inizia selezione
skyit_wsc_v1 <- select(skyitdev_df,
                       "external_id_post_evar",
                       "post_visid_high", "post_visid_low","post_visid_concatenated","date_time",
                       "secondo_livello_post_evar","terzo_livello_post_evar")

skyit_wsc_v2 <- withColumn(skyit_wsc_v1, "ts", cast(skyit_wsc_v1$date_time, "timestamp"))
skyit_wsc_v3 <- filter(skyit_wsc_v2,"ts>='2017-03-01 00:00:00' and ts<='2017-10-01 00:00:00'
                       and external_id_post_evar is not NULL")

skyit_wsc_v4 <- filter(skyit_wsc_v3,"
                       terzo_livello_post_evar like '%faidate gestisci dati servizi%' or
                       terzo_livello_post_evar like '%faidate arricchisci abbonamento%' or
                       terzo_livello_post_evar like '%faidate fatture pagamenti%' or
                       terzo_livello_post_evar like '%faidate webtracking%' or
                       terzo_livello_post_evar like '%faidate extra%'
                       ")


createOrReplaceTempView(skyit_wsc_v4,"skyit_wsc_v4")
createOrReplaceTempView(recupera_cookies_all_2,"recupera_cookies_all_2")

join_base_wsc <- sql("select t1.*,
                     t2.secondo_livello_post_evar,t2.terzo_livello_post_evar,
                     t2.ts as data_visualizzaz_pagina
                     from recupera_cookies_all_2 t1 inner join skyit_wsc_v4 t2
                     on t1.external_id_post_evar=t2.external_id_post_evar
                     and t2.ts>=t1.DATA_t0_4m_ts and t2.ts<=t1.DATA_t0_ts")

write.parquet(join_base_wsc,"/user/valentina/CJ_UP_scarico_pag_viste_wsc.parquet")
join_base_wsc <- read.parquet("/user/valentina/CJ_UP_scarico_pag_viste_wsc.parquet")




#################################################################################################################
#################################################################################################################
#### unisco le visite sulla APP
#################################################################################################################
#################################################################################################################

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")

skyappwsc_2 <- withColumn(skyappwsc, "ts", cast(skyappwsc$date_time, "timestamp"))

skyappwsc_3 <- filter(skyappwsc_2,"ts>='2017-03-01 00:00:00' and ts<='2017-09-30 00:00:00' ")

#seleziona campi
skyappwsc_4 <- select(skyappwsc_3,
                      "external_id_v0",
                      "site_section",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "post_pagename",
                      "date_time",
                      "ts")

skyappwsc_4 <- filter(skyappwsc_4,"
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


createOrReplaceTempView(skyappwsc_4,"skyappwsc_4")
createOrReplaceTempView(recupera_cookies_all_2,"recupera_cookies_all_2")

join_base_app <- sql("select t1.*,
                     t2.site_section,t2.page_name_post_evar,
                     t2.ts as data_visualizzaz_pagina
                     from recupera_cookies_all_2 t1 inner join skyappwsc_4 t2
                     on t1.external_id_post_evar=t2.external_id_v0
                     and t2.ts>=t1.DATA_t0_4m_ts and t2.ts<=t1.DATA_t0_ts")

write.parquet(join_base_app,"/user/valentina/CJ_UP_scarico_pag_viste_app.parquet")

leggi <- read.parquet("/user/valentina/CJ_UP_scarico_pag_viste_app.parquet")
View(head(leggi,200))



#chiudi sessione
sparkR.stop()
