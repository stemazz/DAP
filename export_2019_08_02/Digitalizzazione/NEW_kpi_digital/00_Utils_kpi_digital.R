
## Utils Digitalizzazione ## 

source("connection_R.R")
options(scipen = 10000)




data_inizio <- as.Date("2018-11-01") # data inizio perimetro
data_fine <- as.Date("2019-04-30") # data fine perimetro


path_skyitdev <- '/STAGE/adobe/reportsuite=skyitdev' 
path_appwsc <- '/STAGE/adobe/reportsuite=skyappwsc.prod'
path_appgtv <- "/STAGE/adobe/reportsuite=skyappguidatv.prod"
path_appsport <- "/STAGE/adobe/reportsuite=skyappsport.prod"
path_appxfactor <- "/STAGE/adobe/reportsuite=skyappxfactor.prod"

path_scarico_skyitdev_senza_diz <- '/user/emma.cambieri/Digitalizzazione/Apr2019/scarico_skyitdev_senza_diz_nov_apr.parquet' ## di EMMA!!
path_scarico_skyitdev <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/scarico_skyitdev_con_diz_nov_apr.parquet'
path_scarico_skyappwsc <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/scarico_skyappwsc_nov_apr.parquett'
path_scarico_skyappgtv <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/scarico_skyappgtv_nov_apr.parquet'
path_scarico_skyappsport <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/scarico_skyappsport_nov_apr.parquet'
path_scarico_skyappxfactor <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/scarico_skyappxfactor_nov_apr.parquet'

path_scarico_corporate <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/scarico_corporate_gtv_nov_apr.parquet' 
path_scarico_tg24 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/scarico_tg24_nov_apr.parquet'
path_scarico_sport <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/scarico_sport_nov_apr.parquet'

path_diz_skyitdev <- '/user/emma.cambieri/Digitalizzazione/Apr2019/diz_skyitdev_nov_apr.parquet' ## di EMMA!!
path_diz_skyitdev_2 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/diz_skyitdev_nov_apr_def.parquet'
path_diz_appgtv <- "/user/stefano.mazzucca/Digitalizzazione/Apr2019/diz_appgtv_nov_apr.parquet"
path_diz_appsport <- "/user/stefano.mazzucca/Digitalizzazione/Apr2019/diz_appsport_nov_apr.parquet"
path_diz_appxfactor <- "/user/stefano.mazzucca/Digitalizzazione/Apr2019/diz_appxfactor_nov_apr.parquet"


path_ext_id_distinti_senza_NA <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/external_id_distinti_na.parquet'

path_df_variabile_1 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_3_df_variabile_1.parquet'
path_df_variabile_2 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_4_df_variabile_2.parquet'
path_df_variabile_3 <- "/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_5_df_variabile_3.parquet"
path_df_variabile_4 <- "/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_6_df_variabile_4.parquet"

path_mobile <- "/user/stefano.mazzucca/Digitalizzazione/Apr2019/mobile_desktop.parquet"

path_df_variabile_5 <- "/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_7_df_variabile_5.parquet"

path_social_network <- "/user/stefano.mazzucca/Digitalizzazione/Apr2019/social.parquet"

path_df_variabile_6 <- "/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_8_df_variabile_6.parquet"

path_secondi_corporate <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/secondi_corporate.parquet'
path_secondi_tg24 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/secondi_tg24.parquet'
path_secondi_sport <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/secondi_sport.parquet'
path_secondi_appwsc <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/secondi_appwsc.parquet'
path_secondi_appgtv <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/secondi_appgtv.parquet'
path_secondi_appsport <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/secondi_appsport.parquet'
path_secondi_appxfactor <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/secondi_xfactor.parquet'

path_df_variabile_7 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_9_df_variabile_7.parquet'

path_df_variabile_8 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_10_df_variabile_8.parquet'
path_df_variabile_9 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_11_df_variabile_9.parquet'
path_df_variabile_10 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_12_df_variabile_10.parquet'
path_df_variabile_11 <- "/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_13_df_variabile_11.parquet"
path_df_variabile_12 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_14_df_variabile_12.parquet'
path_df_variabile_13 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_15_df_variabile_13.parquet'
path_df_variabile_14 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_16_df_variabile_14.parquet'
path_df_variabile_15 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_17_df_variabile_15.parquet'
path_df_variabile_16 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_18_df_variabile_16.parquet'
path_df_variabile_17 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_19_df_variabile_17.parquet'
path_df_variabile_18 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_20_df_variabile_18.parquet'
path_df_variabile_19 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_21_df_variabile_19.parquet'
path_df_variabile_20 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_22_df_variabile_20.parquet'
path_df_variabile_21 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_23_df_variabile_21.parquet'
path_df_variabile_22 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_24_df_variabile_22.parquet'
path_df_variabile_23 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/step_25_df_variabile_23.parquet'

path_df_variabile_24 <- '/user/stefano.mazzucca/Digitalizzazione/Apr2019/df_pulito.parquet'


path_impression= '/STAGE/adform/table=Impression'
path_tp <- "/STAGE/adform/table=Trackingpoint/Trackingpoint.parquet"

path_scarico_impression = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/scarico_impression.parquet"
path_dizionario_adform = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/dizionario_adform.parquet"

path_impression_con_diz = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/impression_con_diz.parquet"

path_impression_variabile0 = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/impression_variabili0.parquet"
path_impression_variabili1 = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/impression_variabili1.parquet"
path_impression_variabili2 = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/impression_variabili2.parquet"
path_impression_variabili3 = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/impression_variabili3.parquet"
path_impression_variabili4 = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/impression_variabili4.parquet"

path_impression_social = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/impression_social.parquet"
path_impression_join_finale = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/impression_join_finale.parquet"

path_impression_finale_pulito = "/user/stefano.mazzucca/Digitalizzazione/Apr2019/impression_finale_pulito.parquet"





# funzione per il calcolo del tempo in pagina

remove_too_short <- function(df0, duration){
  ## ORDINO PER SKYID E TIME DELLA VISITA
  df0 <- orderBy(df0, "COD_CLIENTE_CIFRATO", "date_time_ts")
  
  ## AGGIUNGO TIMESTAMP DELLA VISITA ALLA PAGINA SUCCESSIVA
  mywindow = orderBy(windowPartitionBy(df0$COD_CLIENTE_CIFRATO), desc(df0$date_time_ts))
  df0 <- withColumn(df0, "nextvisit", over(lag(df0$date_time_ts), mywindow))
  df0 <- orderBy(df0, asc(df0$COD_CLIENTE_CIFRATO), asc(df0$date_time_ts))
  
  ## CALCOLO LA DURATA DI VIEW DI UNA PAGINA
  createOrReplaceTempView(df0, "df0")
  df0 <- sql("SELECT *, CAST(nextvisit AS LONG) - CAST(date_time_ts AS LONG) as sec_on_page from df0")
  df0 <- filter(df0, isNull(df0$sec_on_page) | df0$sec_on_page >= duration)
  df0$sec_on_page <- ifelse(df0$sec_on_page >= 3600, NA, df0$sec_on_page)
  # df0 <- withColumn(df0,  "min_on_page", round(df0$sec_on_page / 60))
  df0 <- fillna(df0, 0)
  return(df0)
}


