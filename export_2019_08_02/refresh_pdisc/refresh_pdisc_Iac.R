
## Refresh analisi pdisc nel epriodo post-disdetta
## codice di Iac


source("connection_R.R")
options(scipen = 10000)

require(dplyr)


#### CREAZIONE LISTA UTENTI PDISC "UNICI" #####

movimenti <- read.df("/user/stefano.mazzucca/cj_pdisc/20180927_richiesta_estrazioni_per_analisi_clienti_in_Pdisc_Estr_2.csv", source = "csv", header = "true", delimiter = "$")
View(head(movimenti,100))
nrow(movimenti)
# 3.836.545
pdisc_1 <- read.df("/user/valentina/20180927_richiesta_estrazioni_per_analisi_clienti_in_Pdisc.csv", source = "csv", header = "true", delimiter = ";")
View(head(pdisc_1,100))
nrow(pdisc_1)
# 517.700

pdisc_1 <- withColumn(pdisc_1,"dat_prima_attivazione", cast(unix_timestamp(pdisc_1$dat_prima_attivazione, 'dd/MM/yyyy'),'timestamp'))
pdisc_1 <- withColumn(pdisc_1,"data_ingresso_pdisc", cast(unix_timestamp(pdisc_1$data_ingresso_pdisc, 'dd/MM/yyyy'),'timestamp'))

createOrReplaceTempView(pdisc_1, "pdisc_1")
query <- SparkR::sql("SELECT cod_cliente_cifrato, COUNT(cod_cliente_cifrato) as count, min(data_ingresso_pdisc) as MIN_data_ingresso_pdisc 
             FROM pdisc_1 
             GROUP BY 1")
createOrReplaceTempView(query, "query")
test <- SparkR::sql("SELECT a.*, q.count, q.MIN_data_ingresso_pdisc 
            FROM pdisc_1 a 
            LEFT JOIN query q 
            on a.cod_cliente_cifrato == q.cod_cliente_cifrato")
test <- filter(test, test$data_ingresso_pdisc == test$MIN_data_ingresso_pdisc)
test <- dropDuplicates(test, c("cod_cliente_cifrato"))

createOrReplaceTempView(test, "test")
test_count <- SparkR::sql("SELECT COUNT(DISTINCT(cod_cliente_cifrato)) 
                  FROM test")

write.parquet(test, "/user/jacopoa/pdisc_distinti.parquet")
test <- read.parquet("/user/jacopoa/pdisc_distinti.parquet")
View(head(test,100))
nrow(test)
# 480.549


#### RECUPERA NAVIGAZIONE SKYITDEV #####

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low","post_visid_concatenated")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low","post_visid_concatenated"))

createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
createOrReplaceTempView(test,"test")
recupera_cookies <- SparkR::sql("select  t1.external_id_post_evar, t1.post_visid_high, t1.post_visid_low, t1.post_visid_concatenated, 
                                  t2.dat_prima_attivazione, data_ingresso_pdisc, causale_cessazione
                        from skyitdev_df_3 t1
                        inner join test t2
                        on t2.cod_cliente_cifrato = t1.external_id_post_evar")

write.parquet(recupera_cookies, "/user/jacopoa/recupera_cookies_pdisc.parquet")
recupera_cookies <- read.parquet("/user/jacopoa/recupera_cookies_pdisc.parquet")
View(head(recupera_cookies,100))
nrow(recupera_cookies)
# 2.516.680


navigazione_pdisc <- read.parquet("/user/valentina/20180928_AnalisiPdisc_ScaricoNavigaz_SkyItDev_noFilter.parquet")

createOrReplaceTempView(navigazione_pdisc,"navigazione_pdisc")
createOrReplaceTempView(recupera_cookies,"recupera_cookies")
recupera_cookies_navigati <- SparkR::sql("select  t1.*, t2.external_id_post_evar AS external_id, t2.dat_prima_attivazione, t2.data_ingresso_pdisc, t2.causale_cessazione
                                 from navigazione_pdisc t1
                                 inner join recupera_cookies t2
                                 on t1.post_visid_concatenated = t2.post_visid_concatenated")

write.parquet(recupera_cookies_navigati, "/user/jacopoa/recupera_cookies_navigati_pdisc.parquet")
recupera_cookies_navigati <- read.parquet("/user/jacopoa/recupera_cookies_navigati_pdisc.parquet")
View(head(recupera_cookies_navigati,100))
nrow(recupera_cookies_navigati)
# 11.285.1167


movimenti <- filter(movimenti, movimenti$des_stato_cntr_da=="Active Pdisc")
createOrReplaceTempView(movimenti,"movimenti")
query_2 <- SparkR::sql("SELECT cod_cliente_cifrato, COUNT(cod_cliente_cifrato) as count, min(datas) as MIN_datas 
               FROM movimenti 
               GROUP BY 1")
createOrReplaceTempView(query_2, "query_2")
test_2 <- SparkR::sql("SELECT a.*, q.count, q.MIN_datas 
              FROM movimenti a 
              LEFT JOIN query_2 q 
              on a.cod_cliente_cifrato == q.cod_cliente_cifrato")
test_2 <- filter(test_2, test_2$datas == test_2$MIN_datas)
test_2 <- dropDuplicates(test_2, c("cod_cliente_cifrato"))

createOrReplaceTempView(test_2, "test_2")
createOrReplaceTempView(recupera_cookies_navigati,"recupera_cookies_navigati")
recupera_cookies_navigati_pdisc <- SparkR::sql("select  t1.*, t2.des_stato_cntr_da, t2.des_stato_cntr_a, t2.datas
                                       from recupera_cookies_navigati t1
                                       inner join test_2 t2
                                       on t1.external_id = t2.cod_cliente_cifrato")

recupera_cookies_navigati_pdisc <- filter(recupera_cookies_navigati_pdisc, 
                                          recupera_cookies_navigati_pdisc$date_time_dt >= recupera_cookies_navigati_pdisc$data_ingresso_pdisc)
recupera_cookies_navigati_pdisc <- withColumn(recupera_cookies_navigati_pdisc, "visit_uniq", 
                                              concat_ws('-', recupera_cookies_navigati_pdisc$post_visid_concatenated, recupera_cookies_navigati_pdisc$visit_num))
# createOrReplaceTempView(recupera_cookies_navigati_pdisc,"recupera_cookies_navigati_pdisc")
recupera_cookies_navigati_pdisc <- withColumn(recupera_cookies_navigati_pdisc, "DATA_7DAYS", date_add(recupera_cookies_navigati_pdisc$data_ingresso_pdisc, 7))
recupera_cookies_navigati_pdisc <- withColumn(recupera_cookies_navigati_pdisc, "PERIODO", 
                                              ifelse((recupera_cookies_navigati_pdisc$date_time_dt <= recupera_cookies_navigati_pdisc$DATA_7DAYS & recupera_cookies_navigati_pdisc$date_time_dt > recupera_cookies_navigati_pdisc$data_ingresso_pdisc), '7_DAYS', 
                                                          ifelse((recupera_cookies_navigati_pdisc$date_time_dt > recupera_cookies_navigati_pdisc$DATA_7DAYS & recupera_cookies_navigati_pdisc$date_time_dt <= recupera_cookies_navigati_pdisc$datas), 'PRIMO_MOV',
                                                                 ifelse((recupera_cookies_navigati_pdisc$date_time_dt < recupera_cookies_navigati_pdisc$data_ingresso_pdisc), 'PRE-PDISC', 'NIENTE'))))


df <- recupera_cookies_navigati_pdisc
createOrReplaceTempView(df, "df")
df <- SparkR::sql("SELECT *,
         CASE WHEN (terzo_livello_post_prop LIKE 'faidate fatture pagamenti') THEN visit_uniq ELSE Null END AS wsc_fatture_pagamenti,
         CASE WHEN (terzo_livello_post_prop LIKE 'faidate extra') THEN visit_uniq ELSE Null END AS wsc_extra,
         CASE WHEN (terzo_livello_post_prop LIKE 'faidate arricchisci abbonamento') THEN visit_uniq ELSE Null END AS wsc_arricchischi_abbonamento,
         CASE WHEN (terzo_livello_post_prop LIKE 'faidate gestisci dati servizi') THEN visit_uniq ELSE Null END AS wsc_gestisci_dati_servizi,
         CASE WHEN (terzo_livello_post_prop LIKE 'faidate webtracking') THEN visit_uniq ELSE Null END AS wsc_webtracking,
         CASE WHEN terzo_livello_post_prop LIKE 'contatta' THEN visit_uniq ELSE Null END AS assistenza_contatta,
         CASE WHEN terzo_livello_post_prop LIKE 'conosci' THEN visit_uniq ELSE Null END AS assistenza_conosci,
         CASE WHEN terzo_livello_post_prop LIKE 'home' THEN visit_uniq ELSE Null END AS assistenza_home,
         CASE WHEN terzo_livello_post_prop LIKE 'gestisci' THEN visit_uniq ELSE Null END AS assistenza_gestisci,
         CASE WHEN terzo_livello_post_prop LIKE 'ricerca' THEN visit_uniq ELSE Null END AS assistenza_ricerca,
         CASE WHEN terzo_livello_post_prop LIKE 'risolvi' THEN visit_uniq ELSE Null END AS assistenza_risolvi,
         CASE WHEN secondo_livello_post_prop LIKE 'faidate' THEN visit_uniq ELSE Null END AS fai_da_te,
         CASE WHEN (secondo_livello_post_prop LIKE '%guidatv%' OR secondo_livello_post_prop LIKE '%Guidatv%') THEN visit_uniq ELSE Null END AS guida_tv,
         CASE WHEN secondo_livello_post_prop LIKE 'pagine di servizio' THEN visit_uniq ELSE Null END AS pagine_di_servizio,
         CASE WHEN secondo_livello_post_prop LIKE 'extra' THEN visit_uniq ELSE Null END AS extra,
         CASE WHEN secondo_livello_post_prop LIKE 'tecnologia' THEN visit_uniq ELSE Null END AS tecnologia,
         CASE WHEN secondo_livello_post_prop LIKE 'pacchetti-offerte' THEN visit_uniq ELSE Null END AS pacchetti_offerte,
         CASE WHEN secondo_livello_post_prop LIKE 'landing' THEN visit_uniq ELSE Null END AS landing,
         CASE WHEN (page_url_post_evar LIKE '%www.sky.it/landing/clienti/cmn-pdsc%' or 
                    page_url_post_evar LIKE '%www.sky.it/landing/clienti/cmn-ammva%') THEN visit_uniq ELSE Null END AS landing_pdisc
         FROM df"
)
 
write.parquet(df, "/user/stefano.mazzucca/refresh_pdisc/db_pdisc_desktop.parquet", mode = "overwrite")
db_pdisc_desktop <- read.parquet("/user/stefano.mazzucca/refresh_pdisc/db_pdisc_desktop.parquet")
View(head(db_pdisc_desktop,100))
nrow(db_pdisc_desktop)
# 59.127.761

db_pdisc_desktop <- withColumn(db_pdisc_desktop, "PERIODO", 
                               ifelse((db_pdisc_desktop$date_time_dt <= db_pdisc_desktop$DATA_7DAYS & db_pdisc_desktop$date_time_dt > db_pdisc_desktop$data_ingresso_pdisc), '7_DAYS', 
                                          ifelse((db_pdisc_desktop$date_time_dt > db_pdisc_desktop$DATA_7DAYS & db_pdisc_desktop$date_time_dt <= db_pdisc_desktop$datas), 'PRIMO_MOV',
                                                      ifelse((db_pdisc_desktop$date_time_dt < db_pdisc_desktop$data_ingresso_pdisc), 'PRE-PDISC', 'NIENTE'))))
db_pdisc_desktop <- dropDuplicates(db_pdisc_desktop, c("date_time_ts", "post_visid_concatenated", "visit_num"))


####### RECUPERA NAVIGAZIONE SKY APP WSC ##########

skyappwscdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyappwsc.prod")
skyappwscdev_df_2 <- select(skyappwscdev_df,"external_id_post_evar","post_visid_high", "post_visid_low","post_visid_concatenated")
skyappwscdev_df_3 <- distinct(select(skyappwscdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low","post_visid_concatenated"))

createOrReplaceTempView(skyappwscdev_df_3,"skyappwscdev_df_3")
recupera_cookies_appwsc <- SparkR::sql("select  t1.external_id_post_evar,t1.post_visid_high,t1.post_visid_low,t1.post_visid_concatenated, t2.dat_prima_attivazione,data_ingresso_pdisc, causale_cessazione
                        from skyappwscdev_df_3 t1
                        inner join test t2
                        on t2.cod_cliente_cifrato = t1.external_id_post_evar")

write.parquet(recupera_cookies_appwsc, "/user/jacopoa/recupera_cookies_appwsc_pdisc.parquet")
recupera_cookies_appwsc <- read.parquet("/user/jacopoa/recupera_cookies_appwsc_pdisc.parquet")
View(head(recupera_cookies_appwsc,100))
nrow(recupera_cookies_appwsc)
# 385.666

skyappwsc <- select(skyappwscdev_df, "external_id_post_evar",
                    "post_channel",
                    "post_pagename",
                    "post_visid_high",
                    "post_visid_low",
                    "post_visid_concatenated",
                    "date_time",
                    "page_name_post_prop",
                    "site_section",
                    "visit_num",
                    "hit_source",
                    "exclude_hit",
                    "post_campaign",
                    "va_closer_id") 
skyappwsc <- filter(skyappwsc, skyappwsc$date_time >  cast(lit("2018-01-01 00:00:00"), "timestamp")) 

createOrReplaceTempView(skyappwsc,"skyappwsc")
createOrReplaceTempView(recupera_cookies_appwsc,"recupera_cookies_appwsc")
recupera_cookies_appwsc_navigati <- SparkR::sql("select  t1.*, t2.external_id_post_evar AS external_id, t2.dat_prima_attivazione, 
                                                  t2.data_ingresso_pdisc, t2.causale_cessazione
                                         from skyappwsc t1
                                         inner join recupera_cookies_appwsc t2
                                         on t1.post_visid_concatenated = t2.post_visid_concatenated")

write.parquet(recupera_cookies_appwsc_navigati, "/user/jacopoa/recupera_cookies_appwsc_navigati.parquet", mode='overwrite')
recupera_cookies_appwsc_navigati <- read.parquet("/user/jacopoa/recupera_cookies_appwsc_navigati.parquet")
View(head(recupera_cookies_appwsc_navigati,100))
nrow(recupera_cookies_appwsc_navigati)
# 21.131.831

movimenti <- filter(movimenti, movimenti$des_stato_cntr_da=="Active Pdisc")
createOrReplaceTempView(movimenti,"movimenti")
query_2 <- SparkR::sql("SELECT cod_cliente_cifrato, COUNT(cod_cliente_cifrato) as count, min(datas) as MIN_datas 
               FROM movimenti 
               GROUP BY 1")
createOrReplaceTempView(query_2, "query_2")
test_2 <- SparkR::sql("SELECT a.*, q.count, q.MIN_datas 
              FROM movimenti a 
              LEFT JOIN query_2 q 
              on a.cod_cliente_cifrato == q.cod_cliente_cifrato")
test_2 <- filter(test_2, test_2$datas == test_2$MIN_datas)
test_2 <- dropDuplicates(test_2, c("cod_cliente_cifrato"))

createOrReplaceTempView(test_2, "test_2")
createOrReplaceTempView(recupera_cookies_appwsc_navigati,"recupera_cookies_appwsc_navigati")
recupera_cookies_appwsc_navigati_pdisc <- SparkR::sql("select  t1.*, t2.des_stato_cntr_da, t2.des_stato_cntr_a, t2.datas
                                 from recupera_cookies_appwsc_navigati t1
                                 inner join test_2 t2
                                 on t1.external_id = t2.cod_cliente_cifrato")

recupera_cookies_appwsc_navigati_pdisc <- withColumn(recupera_cookies_appwsc_navigati_pdisc, "date_time_dt", 
                                                     cast(recupera_cookies_appwsc_navigati_pdisc$date_time, "date"))
recupera_cookies_appwsc_navigati_pdisc <- filter(recupera_cookies_appwsc_navigati_pdisc, 
                                                 recupera_cookies_appwsc_navigati_pdisc$date_time_dt >= recupera_cookies_appwsc_navigati_pdisc$data_ingresso_pdisc)
recupera_cookies_appwsc_navigati_pdisc <- withColumn(recupera_cookies_appwsc_navigati_pdisc, "visit_uniq", 
                                                     concat_ws('-', recupera_cookies_appwsc_navigati_pdisc$post_visid_concatenated, recupera_cookies_appwsc_navigati_pdisc$visit_num))
# createOrReplaceTempView(recupera_cookies_appwsc_navigati_pdisc,"recupera_cookies_appwsc_navigati_pdisc")
recupera_cookies_appwsc_navigati_pdisc <- withColumn(recupera_cookies_appwsc_navigati_pdisc, "DATA_7DAYS", 
                                                     date_add(recupera_cookies_appwsc_navigati_pdisc$data_ingresso_pdisc, 7))
recupera_cookies_appwsc_navigati_pdisc <- withColumn(recupera_cookies_appwsc_navigati_pdisc, "PERIODO", 
                                                     ifelse((recupera_cookies_appwsc_navigati_pdisc$date_time_dt <= recupera_cookies_appwsc_navigati_pdisc$DATA_7DAYS & recupera_cookies_appwsc_navigati_pdisc$date_time_dt > recupera_cookies_appwsc_navigati_pdisc$data_ingresso_pdisc), '7_DAYS', 
                                                              ifelse((recupera_cookies_appwsc_navigati_pdisc$date_time_dt > recupera_cookies_appwsc_navigati_pdisc$DATA_7DAYS & recupera_cookies_appwsc_navigati_pdisc$date_time_dt <= recupera_cookies_appwsc_navigati_pdisc$datas), 'FINO_AL_PRIMO_MOV', 
                                                                        ifelse((recupera_cookies_appwsc_navigati_pdisc$date_time_dt < recupera_cookies_appwsc_navigati_pdisc$data_ingresso_pdisc), 'PRE_PDISC', 'NIENTE'))))

df_appwsc <- recupera_cookies_appwsc_navigati_pdisc
createOrReplaceTempView(df_appwsc, "df_appwsc")
df_appwsc <- SparkR::sql("SELECT *,
          CASE WHEN (site_section LIKE 'fatture' OR site_section LIKE 'dati e fatture') THEN visit_num ELSE Null END AS app_dati_fatture,
          CASE WHEN site_section LIKE 'widget' THEN visit_num ELSE Null END AS app_widget,
          CASE WHEN page_name_post_prop LIKE 'widget:dispositivi%' THEN visit_num ELSE Null END AS app_widget_dispositivi,
          CASE WHEN page_name_post_prop LIKE 'widget:ultimafattura%' THEN visit_num ELSE Null END AS app_widget_ultima_fattura,
          CASE WHEN page_name_post_prop LIKE 'widget:contatta sky%' THEN visit_num ELSE Null END AS app_widget_contatta,
          CASE WHEN page_name_post_prop LIKE 'widget:gestisci%' THEN visit_num ELSE Null END AS app_widget_gestisci,
          CASE WHEN site_section LIKE '%extra%' THEN visit_num ELSE Null END AS app_extra,
          CASE WHEN site_section LIKE 'arricchisci abbonamento' THEN visit_num ELSE Null END AS app_arricchisci_abbonamento,
          CASE WHEN (site_section LIKE 'stato abbonamento' OR site_section LIKE 'gestione servizi' OR site_section LIKE 'i miei dati') THEN visit_num ELSE Null END AS app_stato_abbonamento,
          CASE WHEN site_section LIKE 'il mio abbonamento' THEN visit_num ELSE Null END AS app_il_mio_abbonamento,
          CASE WHEN site_section LIKE 'contatta sky' THEN visit_num ELSE Null END AS app_contatta_sky,
          CASE WHEN site_section LIKE 'impostazioni' THEN visit_num ELSE Null END AS app_impostazioni,
          CASE WHEN site_section LIKE '%assistenza%' THEN visit_num ELSE Null END AS app_assistenza,
          CASE WHEN page_name_post_prop LIKE 'assistenza:home%' THEN visit_num ELSE Null END AS app_assistenza_home,
          CASE WHEN page_name_post_prop LIKE 'assistenza:contatta%' THEN visit_num ELSE Null END AS app_assistenza_contatta,
          CASE WHEN page_name_post_prop LIKE 'assistenza:conosci%' THEN visit_num ELSE Null END AS app_assistenza_conosci,
          CASE WHEN page_name_post_prop LIKE 'assistenza:ricerca%' THEN visit_num ELSE Null END AS app_assistenza_ricerca,
          CASE WHEN page_name_post_prop LIKE 'assistenza:gestisci%' THEN visit_num ELSE Null END AS app_assistenza_gestisci,
          CASE WHEN page_name_post_prop LIKE 'assistenza:risolvi%' THEN visit_num ELSE Null END AS app_assistenza_risolvi,
          CASE WHEN site_section LIKE 'comunicazioni' THEN visit_num ELSE Null END AS app_comunicazioni,
          CASE WHEN site_section LIKE 'gestisci servizi' THEN visit_num ELSE Null END AS app_gestisci_servizi,
          CASE WHEN site_section LIKE 'info' THEN visit_num ELSE Null END AS app_info
          FROM df_appwsc"
)

write.parquet(df_appwsc, "/user/jacopoa/db_pdisc_appwsc.parquet")
db_pdisc_appwsc <- read.parquet("/user/jacopoa/db_pdisc_appwsc.parquet")
View(head(db_pdisc_appwsc,100))
nrow(db_pdisc_appwsc)
# 12.671.284

db_pdisc_appwsc <- withColumn(db_pdisc_appwsc, "PERIODO", 
                              ifelse((db_pdisc_appwsc$date_time_dt <= db_pdisc_appwsc$DATA_7DAYS & db_pdisc_appwsc$date_time_dt > db_pdisc_appwsc$data_ingresso_pdisc), '7_DAYS', 
                                      ifelse((db_pdisc_appwsc$date_time_dt > db_pdisc_appwsc$DATA_7DAYS & db_pdisc_appwsc$date_time_dt <= db_pdisc_appwsc$datas), 'FINO_AL_PRIMO_MOV', 
                                              ifelse((db_pdisc_appwsc$date_time_dt < db_pdisc_appwsc$data_ingresso_pdisc), 'PRE_PDISC', 'NIENTE'))))
db_pdisc_appwsc <- dropDuplicates(db_pdisc_appwsc, c("date_time", "post_visid_concatenated", "visit_num"))


##################### AGGREGAZIONI E JOIN WEB + APP ###########################

df_kpi <- agg(
  groupBy(db_pdisc_desktop, "external_id", "PERIODO"),
  # visite
  visite_totali = countDistinct(db_pdisc_desktop$visit_uniq), 
  wsc_fatture_pagamenti = countDistinct(db_pdisc_desktop$wsc_fatture_pagamenti),
  wsc_arricchischi_abbonamento = countDistinct(db_pdisc_desktop$wsc_arricchischi_abbonamento),
  wsc_extra = countDistinct(db_pdisc_desktop$wsc_extra),
  wsc_gestisci_dati_servizi = countDistinct(db_pdisc_desktop$wsc_gestisci_dati_servizi),
  wsc_webtracking = countDistinct(db_pdisc_desktop$wsc_webtracking),
  assistenza_contatta = countDistinct(db_pdisc_desktop$assistenza_contatta),
  assistenza_conosci = countDistinct(db_pdisc_desktop$assistenza_conosci),
  assistenza_home = countDistinct(db_pdisc_desktop$assistenza_home),
  assistenza_gestisci = countDistinct(db_pdisc_desktop$assistenza_gestisci),
  assistenza_ricerca = countDistinct(db_pdisc_desktop$assistenza_ricerca),
  assistenza_risolvi = countDistinct(db_pdisc_desktop$assistenza_risolvi),
  fai_da_te = countDistinct(db_pdisc_desktop$fai_da_te),
  guida_tv = countDistinct(db_pdisc_desktop$guida_tv),
  pagine_di_servizio = countDistinct(db_pdisc_desktop$pagine_di_servizio),
  tecnologia = countDistinct(db_pdisc_desktop$tecnologia),
  extra = countDistinct(db_pdisc_desktop$extra),
  pacchetti_offerte = countDistinct(db_pdisc_desktop$pacchetti_offerte),
  landing = countDistinct(db_pdisc_desktop$landing),
  landing_pdisc = countDistinct(db_pdisc_desktop$landing_pdisc))


df_kpi_appwsc <- agg(
  groupBy(db_pdisc_appwsc, "external_id", "PERIODO"),
  # visite
  visite_totali_app = countDistinct(db_pdisc_appwsc$visit_uniq), 
  app_dati_fatture = countDistinct(db_pdisc_appwsc$app_dati_fatture),
  app_widget = countDistinct(db_pdisc_appwsc$app_widget),
  app_widget_dispositivi = countDistinct(db_pdisc_appwsc$app_widget_dispositivi),
  app_widget_ultima_fattura = countDistinct(db_pdisc_appwsc$app_widget_ultima_fattura),
  app_widget_contatta = countDistinct(db_pdisc_appwsc$app_widget_contatta),
  app_widget_gestisci = countDistinct(db_pdisc_appwsc$app_widget_gestisci),
  app_extra = countDistinct(db_pdisc_appwsc$app_extra),
  app_arricchisci_abbonamento = countDistinct(db_pdisc_appwsc$app_arricchisci_abbonamento),
  app_stato_abbonamento = countDistinct(db_pdisc_appwsc$app_stato_abbonamento),
  app_il_mio_abbonamento = countDistinct(db_pdisc_appwsc$app_il_mio_abbonamento),
  app_contatta_sky = countDistinct(db_pdisc_appwsc$app_contatta_sky),
  app_impostazioni = countDistinct(db_pdisc_appwsc$app_impostazioni),
  app_assistenza = countDistinct(db_pdisc_appwsc$app_assistenza),
  app_assistenza_home = countDistinct(db_pdisc_appwsc$app_assistenza_home),
  app_assistenza_contatta = countDistinct(db_pdisc_appwsc$app_assistenza_contatta),
  app_assistenza_conosci = countDistinct(db_pdisc_appwsc$app_assistenza_conosci),
  app_assistenza_ricerca = countDistinct(db_pdisc_appwsc$app_assistenza_ricerca),
  app_assistenza_risolvi = countDistinct(db_pdisc_appwsc$app_assistenza_risolvi),
  app_assistenza_gestisci = countDistinct(db_pdisc_appwsc$app_assistenza_gestisci),
  app_comunicazioni = countDistinct(db_pdisc_appwsc$app_comunicazioni),
  app_gestisci_servizi = countDistinct(db_pdisc_appwsc$app_gestisci_servizi),
  app_info = countDistinct(db_pdisc_appwsc$app_info))

visitor_rollup_pdisc <- NULL
visitor_rollup_pdisc <- list(df_kpi, df_kpi_appwsc) %>%
  Reduce(function(...) merge(..., all=TRUE, by=c("external_id", "PERIODO")), .)

visitor_rollup_pdisc <- fillna(visitor_rollup_pdisc, "99", cols= "external_id_x")
visitor_rollup_pdisc <- fillna(visitor_rollup_pdisc, "99", cols= "PERIODO_x")

visitor_rollup_pdisc <- withColumn(visitor_rollup_pdisc, "external_id_x", ifelse(visitor_rollup_pdisc$external_id_x=="99", visitor_rollup_pdisc$external_id_y, visitor_rollup_pdisc$external_id_x))
visitor_rollup_pdisc <- withColumn(visitor_rollup_pdisc, "PERIODO_x", ifelse(visitor_rollup_pdisc$PERIODO_x=="99", visitor_rollup_pdisc$PERIODO_y, visitor_rollup_pdisc$PERIODO_x))

visitor_rollup_pdisc$external_id_y <- NULL

visitor_rollup_pdisc <- withColumnRenamed(visitor_rollup_pdisc, "PERIODO_x", "PERIODO")
visitor_rollup_pdisc <- withColumnRenamed(visitor_rollup_pdisc, "external_id_x", "external_id")

write.parquet(visitor_rollup_pdisc, "/user/stefano.mazzucca/refresh_pdisc/visitor_rollup_pdisc.parquet")

#### CONTEGGI GENERALI ####

visitor_rollup_pdisc <- read.parquet("/user/stefano.mazzucca/refresh_pdisc/visitor_rollup_pdisc.parquet")
View(visitor_rollup_pdisc,100)
nrow(visitor_rollup_pdisc)
# 441.276

visitor_rollup_pdisc$PERIODO_y <- NULL
visitor_rollup_pdisc <- fillna(visitor_rollup_pdisc, 0)
visitor_rollup_pdisc <- filter(visitor_rollup_pdisc, visitor_rollup_pdisc$PERIODO!="NIENTE")

createOrReplaceTempView(visitor_rollup_pdisc, "visitor_rollup_pdisc")
count_generico <- SparkR::sql("SELECT COUNT(DISTINCT(external_id)) as count 
                      FROM visitor_rollup_pdisc") 
# 250.933
count_7days <- SparkR::sql("SELECT COUNT(DISTINCT(external_id)) as count 
                   FROM visitor_rollup_pdisc 
                   WHERE PERIODO=='7_DAYS'") 
# 114.395
# count_prepdisc <- sql("SELECT COUNT(DISTINCT(external_id)) as count
#                       FROM visitor_rollup_pdisc
#                       WHERE PERIODO=='PRE_PDISC'")
# 114.490

visitor_rollup_pdisc_df <- as.data.frame(visitor_rollup_pdisc)

visitor_rollup_pdisc <- withColumn(visitor_rollup_pdisc, "WSC_Total", visitor_rollup_pdisc$wsc_fatture_pagamenti 
                                   + visitor_rollup_pdisc$wsc_arricchischi_abbonamento 
                                   + visitor_rollup_pdisc$wsc_extra 
                                   + visitor_rollup_pdisc$wsc_gestisci_dati_servizi 
                                   + visitor_rollup_pdisc$wsc_webtracking)
visitor_rollup_pdisc <- withColumn(visitor_rollup_pdisc, "APP_WSC_Total", visitor_rollup_pdisc$app_dati_fatture
                                   + visitor_rollup_pdisc$app_widget
                                   + visitor_rollup_pdisc$app_extra
                                   + visitor_rollup_pdisc$app_arricchisci_abbonamento
                                   + visitor_rollup_pdisc$app_stato_abbonamento
                                   + visitor_rollup_pdisc$app_il_mio_abbonamento
                                   + visitor_rollup_pdisc$app_contatta_sky
                                   + visitor_rollup_pdisc$app_impostazioni
                                   + visitor_rollup_pdisc$app_assistenza
                                   + visitor_rollup_pdisc$app_comunicazioni
                                   + visitor_rollup_pdisc$app_gestisci_servizi
                                   + visitor_rollup_pdisc$app_info
                                   + visitor_rollup_pdisc$app_widget_dispositivi
                                   + visitor_rollup_pdisc$app_widget_ultima_fattura
                                   + visitor_rollup_pdisc$app_widget_contatta
                                   + visitor_rollup_pdisc$app_widget_gestisci
                                   + visitor_rollup_pdisc$app_assistenza_home
                                   + visitor_rollup_pdisc$app_assistenza_contatta
                                   + visitor_rollup_pdisc$app_assistenza_conosci
                                   + visitor_rollup_pdisc$app_assistenza_ricerca
                                   + visitor_rollup_pdisc$app_assistenza_risolvi
                                   + visitor_rollup_pdisc$app_assistenza_gestisci)

visitor_rollup_pdisc <- withColumn(visitor_rollup_pdisc, "Skyit_Total", visitor_rollup_pdisc$assistenza_contatta
                                   + visitor_rollup_pdisc$assistenza_conosci
                                   + visitor_rollup_pdisc$assistenza_home
                                   + visitor_rollup_pdisc$assistenza_gestisci
                                   + visitor_rollup_pdisc$assistenza_ricerca
                                   + visitor_rollup_pdisc$assistenza_risolvi
                                   + visitor_rollup_pdisc$fai_da_te
                                   + visitor_rollup_pdisc$guida_tv
                                   + visitor_rollup_pdisc$pagine_di_servizio
                                   + visitor_rollup_pdisc$tecnologia
                                   + visitor_rollup_pdisc$extra
                                   + visitor_rollup_pdisc$pacchetti_offerte
                                   + visitor_rollup_pdisc$landing
                                   + visitor_rollup_pdisc$landing_pdisc)

createOrReplaceTempView(visitor_rollup_pdisc, "visitor_rollup_pdisc")

##### CONTEGGIO ISTOGRAMMI #####

agg <- SparkR::sql("SELECT DISTINCT external_id, sum(visite_totali) as Skyit_Total, sum(WSC_Total) as WSC_Total, 
                    sum(visite_totali_app) as APP_WSC_Total 
           FROM visitor_rollup_pdisc 
           GROUP BY 1")
createOrReplaceTempView(agg, "agg")
skytot <- SparkR::sql("SELECT COUNT(DISTINCT(external_id)) 
              FROM agg 
              WHERE Skyit_Total > 0") 
# 135.238 - 53.201 = 82.037
wsctot <- SparkR::sql("SELECT COUNT(DISTINCT(external_id)) 
              FROM agg 
              WHERE WSC_Total > 0") 
# 53.201
appwsctot <- SparkR::sql("SELECT COUNT(DISTINCT(external_id)) 
                 FROM agg 
                 WHERE APP_WSC_Total > 0") 
# 56.319
agg_7days <- SparkR::sql("SELECT DISTINCT external_id, sum(visite_totali) as Skyit_Total, sum(WSC_Total) as WSC_Total, 
                        sum(visite_totali_app) as APP_WSC_Total 
                 FROM visitor_rollup_pdisc 
                 WHERE PERIODO=='7_DAYS' 
                 GROUP BY 1")
createOrReplaceTempView(agg_7days, "agg_7days")
skytot_7days <- SparkR::sql("SELECT COUNT(DISTINCT(external_id)) 
                    FROM agg_7days 
                    WHERE Skyit_Total > 0") 
# 94.433 - 27.117 = 67.316
wsctot_7days <- SparkR::sql("SELECT COUNT(DISTINCT(external_id)) 
                    FROM agg_7days 
                    WHERE WSC_Total > 0") 
# 27.117
appwsctot_7days <- SparkR::sql("SELECT COUNT(DISTINCT(external_id)) 
                       FROM agg_7days 
                       WHERE APP_WSC_Total > 0") 
# 35.270

### CONTEGGI PUNTUALI PER GRAFICI BARRE ####

agg_allcolumns <- SparkR::sql("SELECT DISTINCT external_id, 
                      sum(wsc_fatture_pagamenti) as wsc_fatture_pagamenti, 
                      sum(wsc_arricchischi_abbonamento) as wsc_arricchischi_abbonamento, 
                      sum(wsc_extra) as wsc_extra, 
                      sum(wsc_gestisci_dati_servizi) as wsc_gestisci_dati_servizi, 
                      sum(wsc_webtracking) as wsc_webtracking,
                      sum(assistenza_contatta) as assistenza_contatta, 
                      sum(assistenza_conosci) as assistenza_conosci, 
                      sum(assistenza_home) as assistenza_home,
                      sum(assistenza_gestisci) as assistenza_gestisci, 
                      sum(assistenza_ricerca) as assistenza_ricerca, 
                      sum(assistenza_risolvi) as assistenza_risolvi,
                      sum(fai_da_te) as fai_da_te, 
                      sum(guida_tv) as guida_tv, 
                      sum(pagine_di_servizio) as pagine_di_servizio,
                      sum(tecnologia) as tecnologia, 
                      sum(extra) as extra, 
                      sum(pacchetti_offerte) as pacchetti_offerte,
                      sum(landing) as landing,
                      sum(landing_pdisc) as landing_pdisc,
                      sum(app_dati_fatture) as app_dati_fatture, 
                      sum(app_widget_dispositivi) as app_widget_dispositivi, 
                      sum(app_widget_ultima_fattura) as app_widget_ultima_fattura,
                      sum(app_widget_contatta) as app_widget_contatta, 
                      sum(app_widget_gestisci) as app_widget_gestisci, 
                      sum(app_extra) as app_extra,
                      sum(app_arricchisci_abbonamento) as app_arricchisci_abbonamento, 
                      sum(app_stato_abbonamento) as app_stato_abbonamento, 
                      sum(app_il_mio_abbonamento) as app_il_mio_abbonamento,
                      sum(app_contatta_sky) as app_contatta_sky, 
                      sum(app_impostazioni) as app_impostazioni, 
                      sum(app_assistenza_contatta) as app_assistenza_contatta,
                      sum(app_assistenza_conosci) as app_assistenza_conosci, 
                      sum(app_assistenza_ricerca) as app_assistenza_ricerca, 
                      sum(app_assistenza_risolvi) as app_assistenza_risolvi,
                      sum(app_assistenza_gestisci) as app_assistenza_gestisci, 
                      sum(app_comunicazioni) as app_comunicazioni, 
                      sum(app_gestisci_servizi) as app_gestisci_servizi,
                      sum(app_info) as app_info,
                      sum(Skyit_Total) as Skyit_Total, 
                      sum(WSC_Total) as WSC_Total, 
                      sum(APP_WSC_Total) as APP_WSC_Total
                      FROM visitor_rollup_pdisc GROUP BY 1")

write.parquet(agg_allcolumns, "/user/stefano.mazzucca/refresh_pdisc/agg_allcolumns_pdisc.parquet")
agg_allcolumns <- read.parquet("/user/stefano.mazzucca/refresh_pdisc/agg_allcolumns_pdisc.parquet")
View(head(agg_allcolumns,100))
nrow(agg_allcolumns)
# 250.933

agg_allcolumns <- withColumn(agg_allcolumns, "NAVIGANTI", lit("NAVIGANTI"))

#### AGGIUNTA SOCIODEMO #####

sociodemo <- read.df("/user/stefano.mazzucca/cj_pdisc/20180928_analisi_clienti_in_Pdisc_da_Gen2018_a_Lug2018.csv", source = "csv", header = "true", delimiter = ";")
View(head(sociodemo,100))
nrow(sociodemo)
# 518.537

sociodemo <- dropDuplicates(sociodemo, c("cod_cliente_cifrato"))
createOrReplaceTempView(sociodemo, "sociodemo")
createOrReplaceTempView(agg_allcolumns, "agg_allcolumns")

demo <- SparkR::sql("SELECT t.*, a.external_id as ext_id, a.naviganti, s.cod_cliente_cifrato as bsag, s.area_nielsen_pre_ingr_pdisc as area_nielsen, 
                      s.eta_anagr_clie_pre_ingr_pdisc as eta, s.nome_prov_pre_ingr_pdisc as prov, 
                      s.tenure_pre_ingr_pdisc as tenure, s.pack_pre_ingr_pdisc as pacchetto, s.tipo_pag_pre_ingr_pdisc, s.flg_sky_q_plus_pre_ingr_pdisc,
                      s.flg_skyq_black_pre_ingr_pdisc, s.flg_multi_pre_ingr_pdisc, s.flg_vod_pre_ingr_pdisc, s.flg_hdmysky_pre_ingr_pdisc,
                      s.flg_skygo_pre_ingr_pdisc, s.flg_skygo_plus_pre_ingr_pdisc
            FROM test_2 t
            LEFT JOIN agg_allcolumns a
            ON t.cod_cliente_cifrato = a.external_id
            LEFT JOIN sociodemo s
            ON t.cod_cliente_cifrato = s.cod_cliente_cifrato")

demo <- fillna(demo, "99", cols= "NAVIGANTI")
demo <- withColumn(demo, "NAVIGANTI", ifelse(demo$NAVIGANTI=="99", lit("NON_NAVIGANTI"), demo$NAVIGANTI))
demo <- withColumn(demo, "eta", cast(demo$eta, "integer"))
demo <- withColumn(demo, "eta", ifelse(demo$eta > 18 & demo$eta <= 25, '18-25', 
                                       ifelse(demo$eta > 25 & demo$eta <= 35, "26-35",
                                              ifelse(demo$eta > 35  & demo$eta <= 45, "36-45",
                                                     ifelse(demo$eta > 45 & demo$eta <= 55, "46-55",
                                                            ifelse(demo$eta > 55 & demo$eta <= 65, "55-65", "over_65"))))))
demo <- withColumn(demo, "flg_skyq_black", ifelse(demo$flg_skyq_black_pre_ingr_pdisc == 'Y', 1, 0))
demo <- fillna(demo, '0')
demo <- withColumn(demo, "flg_skygo", ifelse(demo$flg_skygo_pre_ingr_pdisc + demo$flg_skygo_plus_pre_ingr_pdisc >= 1, 1, 0))
demo <- withColumn(demo, "flg_sky_q", ifelse(demo$flg_sky_q_plus_pre_ingr_pdisc == '1' | demo$flg_skyq_black == '1', 1, 0))
View(head(demo,100))



df_demo <- agg(
  groupBy(demo, "NAVIGANTI", "area_nielsen", "tenure", "eta", "flg_skygo", "flg_sky_q", "flg_multi_pre_ingr_pdisc", 
                "flg_vod_pre_ingr_pdisc", "flg_hdmysky_pre_ingr_pdisc"),
  # visite
  id = countDistinct(demo$cod_cliente_cifrato))
View(head(df_demo,100))

# df_demo <- as.data.frame(df_demo)
write.df(repartition( df_demo, 1), path = "/user/stefano.mazzucca/refresh_pdisc/sociodemo2.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


visitor_rollup_pdisc_df <- as.data.frame(agg_allcolumns)
visitor_rollup_pdisc_df[,2:40][visitor_rollup_pdisc_df[,2:40]!=0] <- 1
View(head(visitor_rollup_pdisc_df,100))
nrow(visitor_rollup_pdisc_df)
# 250.933
visitor_rollup_pdisc_2 <- createDataFrame(visitor_rollup_pdisc_df)

write.df(repartition( visitor_rollup_pdisc_2, 1), path = "/user/stefano.mazzucca/refresh_pdisc/visitor_rollup_pdisc_df.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




#chiudi sessione
sparkR.stop()
