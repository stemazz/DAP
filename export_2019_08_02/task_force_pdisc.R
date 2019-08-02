
## Task Force PDISC ##

source("connection_R.R")
options(scipen = 10000)


path_full <- "/user/stefano.mazzucca/task_force_pdisc/cb_aprile_19.txt"

data_fine <- as.Date("2019-05-07")
data_inizio <- as.Date("2019-02-01")
diff_gg <- data_fine - data_inizio

path_mappatura_cookie_skyid <- "/user/stefano.mazzucca/task_force_pdisc/dizionario.parquet"
path_navigazione_sito <- "/user/stefano.mazzucca/task_force_pdisc/nav_sito.parquet"
path_navigazione_app <- "/user/stefano.mazzucca/task_force_pdisc/nav_app.parquet"

path_kpi_sito <- "/user/stefano.mazzucca/task_force_pdisc/kpi_sito.parquet"
path_kpi_app <- "/user/stefano.mazzucca/task_force_pdisc/kpi_app.parquet"


path_kpi_finale <- "/user/stefano.mazzucca/task_force_pdisc/kpi_finale.parquet"



path_diz_adform <- "/user/stefano.mazzucca/task_force_pdisc/dizionario_adform.parquet"
path_click_adform <- "/user/stefano.mazzucca/task_force_pdisc/click.parquet"




full <- read.df(path_full, source = "csv", header = T, delimiter = "\t")
View(head(full,100))
nrow(full)
# 4.606.862
valdist_extid <- distinct(select(full, full$COD_CLIENTE_CIFRATO))
nrow(valdist_extid)
# 4.469.599


## Navigazioni sito
skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
skyit_1 <- select(skyitdev, "external_id_post_evar",
                  "post_visid_high",
                  "post_visid_low",
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
                  "post_page_event_value"
)
skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))
skyit_4 <- filter(skyit_3, "post_channel = 'corporate' or post_channel = 'Guidatv'")

# filtro temporale
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt >= data_inizio) 
skyit_6 <- filter(skyit_5, skyit_5$date_time_dt <= data_fine)

## Creo le chiavi ext_id con cookies (dizionario)
diz_1 <- distinct(select(skyitdev, "external_id_post_evar", "post_visid_concatenated", "date_time"))
diz_2 <- filter(diz_1, "external_id_post_evar is NOT NULL")
diz_3 <- filter(diz_2, "post_visid_concatenated is NOT NULL")
diz_4 <- withColumn(diz_3, "date_time_dt", cast(diz_3$date_time, "date"))
diz_5 <- filter(diz_4, diz_4$date_time_dt >= "2018-05-07")
diz_6 <- select(diz_5, "external_id_post_evar", "post_visid_concatenated")
clienti_unici <- distinct(select(full, "COD_CLIENTE_CIFRATO"))
diz_7 <- merge(diz_6, clienti_unici, by.x = "external_id_post_evar", by.y = "COD_CLIENTE_CIFRATO")

write.parquet(diz_7, path_mappatura_cookie_skyid, mode = "overwrite")
diz <- read.parquet(path_mappatura_cookie_skyid)
View(head(diz,100))
nrow(diz)
# 760.698.524

diz_f <- summarize(groupBy(diz, diz$external_id_post_evar, diz$post_visid_concatenated), count = count(diz$COD_CLIENTE_CIFRATO))

diz <- select(diz_f, diz_f$external_id_post_evar, diz_f$post_visid_concatenated)
View(head(diz,100))
nrow(diz)
# 11.059.235


## join tra full_CB e chiavi_cookie
createOrReplaceTempView(clienti_unici, "clienti_unici")
createOrReplaceTempView(diz, "diz")

full_coo <- sql("select distinct t1.*, t2.post_visid_concatenated
                from clienti_unici t1
                left join diz t2
                on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

## join tra full e navigazioni (skyit_6)
createOrReplaceTempView(full_coo,"full_coo")
createOrReplaceTempView(skyit_6,"skyit_6")

nav_sito <- sql("SELECT DISTINCT t1.*, 
                t2.visit_num, t2.post_channel,
                t2.date_time, t2.date_time_dt, t2.date_time_ts,
                t2.page_name_post_evar, t2.page_url_post_evar, t2.secondo_livello_post_prop, t2.terzo_livello_post_prop,
                t2.hit_source, t2.exclude_hit
                FROM full_coo t1
                LEFT JOIN skyit_6 t2
                ON t1.post_visid_concatenated = t2.post_visid_concatenated")

write.parquet(nav_sito, path_navigazione_sito, mode = "overwrite")
nav_sito <- read.parquet(path_navigazione_sito)
View(head(nav_sito,100))
nrow(nav_sito)
# 43.996.343

valdist_exts <- distinct(select(nav_sito, nav_sito$COD_CLIENTE_CIFRATO))
nrow(valdist_exts)
# 4.469.599


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
  df0 <- withColumn(df0,  "min_on_page", round(df0$sec_on_page / 60))
}

add_kpi_sito <- function(df) {
  # Creo la concatenazione cookie-visit_num
  df <- withColumn(df, "visit_uniq", concat_ws('-', df$post_visid_concatenated, df$visit_num))
  
  # Aggiugno colonne necessarie ai nuovi kpi
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,
            
            -- Visite per categoria
            CASE 
             -- WHEN secondo_livello_post_prop LIKE 'fai da te' THEN 'wsc_tot'
             WHEN (terzo_livello_post_prop LIKE 'faidate fatture pagamenti') THEN 'wsc_fatture_pagamenti'
             WHEN (terzo_livello_post_prop LIKE 'faidate extra') THEN 'wsc_extra'
             WHEN (terzo_livello_post_prop LIKE 'faidate arricchisci abbonamento') THEN 'wsc_arricchischi_abbonamento'
             WHEN (terzo_livello_post_prop LIKE 'faidate gestisci dati servizi') THEN 'wsc_gestisci_dati_servizi'
             WHEN (terzo_livello_post_prop LIKE 'faidate webtracking') THEN 'wsc_webtracking_comunicazione'
            
             -- WHEN secondo_livello_post_prop LIKE 'assistenza' THEN 'assistenza_tot'
             WHEN terzo_livello_post_prop LIKE 'contatta' THEN 'assistenza_contatta'
             WHEN terzo_livello_post_prop LIKE 'conosci' THEN 'assistenza_conosci'
             WHEN terzo_livello_post_prop LIKE 'home' THEN 'assistenza_home'
             WHEN terzo_livello_post_prop LIKE 'gestisci' THEN 'assistenza_gestisci'
             WHEN terzo_livello_post_prop LIKE 'ricerca' THEN 'assistenza_ricerca'
             WHEN terzo_livello_post_prop LIKE 'risolvi' THEN 'assistenza_risolvi'

             WHEN terzo_livello_post_prop LIKE 'trasloca-sky' THEN 'Trasloco'
             WHEN terzo_livello_post_prop LIKE 'trasparenza-tariffaria' THEN 'TrasparenzaTariffaria'
             WHEN terzo_livello_post_prop LIKE 'info disdetta' THEN 'InfoDisdetta'
             WHEN terzo_livello_post_prop LIKE 'trova_skyservice' THEN 'TrovaSkyService'
             WHEN (page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%') THEN 'SearchDisdetta'
             WHEN terzo_livello_post_prop LIKE 'sky-expert' THEN 'SkyExpert'

             WHEN (secondo_livello_post_prop LIKE '%guidatv%' OR secondo_livello_post_prop LIKE '%Guidatv%') THEN 'guida_tv'
             WHEN secondo_livello_post_prop LIKE 'pagine di servizio' THEN 'pagine_di_servizio'
             WHEN secondo_livello_post_prop LIKE 'extra' THEN 'extra'
             WHEN secondo_livello_post_prop LIKE 'tecnologia' OR secondo_livello_post_prop LIKE 'come-vedere' THEN 'tecnologia'
             WHEN secondo_livello_post_prop LIKE 'pacchetti-offerte' THEN 'pacchetti_offerte'
            ELSE Null END AS touchpoint
            
            FROM df"
  )
  
  return(df)
}

nav_sito_1 <- remove_too_short(nav_sito, 5)
df_kpi_sito <- add_kpi_sito(nav_sito_1)

write.parquet(df_kpi_sito, path_kpi_sito, mode = "overwrite")
kpi_sito <- read.parquet(path_kpi_sito)
View(head(kpi_sito,100))
nrow(kpi_sito)
# 42.023.596


## Naviagazioni APP
skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod/table=hitdata/hitdata.parquet")
skyappwsc_1 <- select(skyappwsc, "external_id_post_evar",
                      "post_visid_high",
                      "post_visid_low",
                      "post_visid_concatenated",
                      "visit_num",
                      "date_time",
                      "post_channel", 
                      "page_name_post_evar",
                      "page_name_post_prop", 
                      "page_url_post_prop",
                      "site_section",
                      "hit_source",
                      "exclude_hit",
                      "post_page_event_value"
)
skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_ts", cast(skyappwsc_2$date_time, "timestamp"))
skyappwsc_4 <- filter(skyappwsc_3, "external_id_post_evar is not NULL")

# filtro temporale
skyappwsc_5 <- filter(skyappwsc_4, skyappwsc_4$date_time_dt >= data_inizio) 
skyappwsc_6 <- filter(skyappwsc_5, skyappwsc_5$date_time_dt <= data_fine)

## join tra full e navigazioni (skyappwsc_6)
clienti_unici <- distinct(select(full, "COD_CLIENTE_CIFRATO"))
createOrReplaceTempView(clienti_unici, "clienti_unici")
createOrReplaceTempView(skyappwsc_6, "skyappwsc_6")

nav_app_wsc <- sql("SELECT DISTINCT t1.*, t2.post_visid_concatenated,
                   t2.visit_num, t2.post_channel,
                   t2.date_time, t2.date_time_dt, t2.date_time_ts,
                   t2.page_name_post_evar, t2.page_name_post_prop, t2.page_url_post_prop, t2.site_section,
                   t2.hit_source, t2.exclude_hit
                   FROM clienti_unici t1
                   LEFT JOIN skyappwsc_6 t2
                   ON t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(nav_app_wsc, path_navigazione_app, mode = "overwrite")
nav_app <- read.parquet(path_navigazione_app)
View(head(nav_app,100))
nrow(nav_app)
# 29.512.083

valdist_ext <- distinct(select(nav_app, nav_app$COD_CLIENTE_CIFRATO))
nrow(valdist_ext)
# 4.469.599


# sisitemare clienti con data del touchpoint

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
  df0 <- withColumn(df0,  "min_on_page", round(df0$sec_on_page / 60))
}

add_kpi_app <- function(df) {
  # Aggiugno colonne necessarie ai nuovi kpi
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,
            
            -- Visite per categoria
            CASE WHEN site_section LIKE '%fattur%' THEN 'app_dati_fatture'
             WHEN page_name_post_prop LIKE 'widget:dispositivi%' THEN 'app_widget_dispositivi'
             WHEN page_name_post_prop LIKE 'widget:ultimafattura%' THEN 'app_widget_ultima_fattura'
             WHEN page_name_post_prop LIKE 'widget:contatta sky%' THEN 'app_widget_contatta'
             WHEN page_name_post_prop LIKE 'widget:gestisci%' THEN 'app_widget_gestisci'
             
             WHEN site_section LIKE '%assistenza%' THEN 'app_assistenza'
             -- WHEN page_name_post_prop LIKE 'assistenza:home%' THEN 'app_assistenza_home'
             -- WHEN page_name_post_prop LIKE 'assistenza:contatta%' THEN 'app_assistenza_contatta'
             -- WHEN page_name_post_prop LIKE 'assistenza:conosci%' THEN 'app_assistenza_conosci'
             -- WHEN page_name_post_prop LIKE 'assistenza:ricerca%' THEN 'app_assistenza_ricerca'
             -- WHEN page_name_post_prop LIKE 'assistenza:gestisci%' THEN 'app_assistenza_gestisci'
             -- WHEN page_name_post_prop LIKE 'assistenza:risolvi%' THEN 'app_assistenza_risolvi'
             WHEN page_name_post_prop LIKE '%extra%' OR site_section LIKE '%extra%' THEN 'app_extra'
             WHEN site_section LIKE 'arricchisci abbonamento' THEN 'app_arricchisci_abbonamento'
             WHEN site_section LIKE 'il mio abbonamento' THEN 'app_stato_abbonamento'

             WHEN site_section LIKE 'impostazioni' THEN 'app_impostazioni'
             WHEN site_section LIKE '%home%' OR page_name_post_prop LIKE 'home' THEN 'app_home'

             WHEN page_name_post_evar LIKE 'arricchisci abbonamento:lista voucher' THEN 'visit_Info_DAZN'
            ELSE Null END AS touchpoint
            
            FROM df"
  )
  return(df)
}

nav_app_1 <- remove_too_short(nav_app, 5)
df_kpi_app <- add_kpi_app(nav_app_1)

write.parquet(df_kpi_app, path_kpi_app, mode = "overwrite")
kpi_app <- read.parquet(path_kpi_app)
View(head(kpi_app,100))
nrow(kpi_app)
# 24.687.104


########################################################################################################################################################
## migliorare la mappatura (con if else) !! ############################################################################################################
########################################################################################################################################################


## Adform ##

adform_tp_1 <- read.parquet('hdfs:///STAGE/adform/table=Trackingpoint/Trackingpoint.parquet')
View(head(adform_tp_1,1000))
date = summarize(adform_tp_1, max_data = max(adform_tp_1$yyyymmdd), min_data = min(adform_tp_1$yyyymmdd))
View(head(date,100))
# max_data    min_data
# 20190514    20170331

adform_tp_2 <- withColumn(adform_tp_1, "date_time", cast(adform_tp_1$yyyymmdd, "string"))
adform_tp_3 <- withColumn(adform_tp_2, "date_time_dt", cast(cast(unix_timestamp(adform_tp_2$date_time, "yyyyMMdd"), 'timestamp'), 'date'))
date_test <- filter(adform_tp_3, adform_tp_3$date_time_dt >= '2019-04-01' & adform_tp_3$date_time_dt <= '2019-05-15')
date_test_1 <- summarize(groupBy(date_test, date_test$date_time_dt), count = count(date_test$CookieID))
View(head(date_test_1,100))
# date_time_dt    count
# 2019-04-18      8.596
# 2019-05-07      6.340.908
# 2019-05-08      6.735.036
# 2019-05-09      9.787.248
# 2019-05-10      7.003.535
# 2019-05-11      12.299.409
# 2019-05-12      8.237.871
# 2019-05-13      14.506.497
# 2019-05-14      7.256.299
# 2019-05-15      6.390.999


## diz con gli altri campi identificativi
createOrReplaceTempView(adform_tp_1, "adform_tp_1")
adform_tp_2 <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                            CookieID,
                            `device-name`,
                            `os-name`, 
                            `browser-name`
                    from adform_tp_1
                    having (skyid <> '' and length(skyid)= 48)
                    order by skyid")

diz_adform <- filter(adform_tp_2, "CookieID <> 0")

write.parquet(diz_adform, path_diz_adform)
diz_adform <- read.parquet(path_diz_adform)
View(head(diz_adform,1000))
nrow(diz_adform)
# 83.874.432



click <- read.parquet("/STAGE/adform/table=Click")
View(head(click,100))

# createOrReplaceTempView(click, "click")
# 
# click_1 <- sql("select distinct CookieID, PublisherDomain, PublisherURL, DestinationURL
#                       -- regexp_extract(crossdevicedata.externalid, 'externalid = ',2)  AS skyid -- DA RIVEDERE!!!!!!
#                 from click
#                 -- having (skyid <> '' and length(skyid)= 48)
#                ")
# write.parquet(click_1, "/user/stefano.mazzucca/task_force_pdisc/test_click.parquet")
# prova <- read.parquet("/user/stefano.mazzucca/task_force_pdisc/test_click.parquet")
# View(head(prova,100))

click_1 <- withColumn(click, "date_time", cast(click$yyyymmdd, "string"))
click_1 <- withColumn(click_1, "date_time_dt", cast(cast(unix_timestamp(click_1$date_time, "yyyyMMdd"), "timestamp"), "date"))
click_2 <- filter(click_1, click_1$date_time_dt >= data_inizio & click_1$date_time_dt <= data_fine)

createOrReplaceTempView(click_2, "click_2")
createOrReplaceTempView(diz_adform, "diz_adform")
click_3 <- sql("select distinct t2.skyid, t1.CookieID, t1.`device-name`, t1.`os-name`, t1.`browser-name`, 
                          t1.date_time_dt, t1.PublisherDomain, t1.PublisherURL, t1.DestinationURL, t1.Timestamp
               from click_2 t1
               inner join diz_adform t2
               on t1.CookieID = t2.CookieID and t1.`device-name` = t2.`device-name` 
                  and t1.`os-name` = t2.`os-name` and t1.`browser-name` = t2.`browser-name` ")

write.parquet(click_3, path_click_adform, mode = "overwrite")
click_nav <- read.parquet(path_click_adform)
View(head(click_nav,100))
nrow(click_nav)
# 16.435.283
# 18.194.449


click_nav_with_domain <- filter(click_nav, isNotNull(click_nav$PublisherDomain))
View(head(click_nav_with_domain,100))
nrow(click_nav_with_domain)
# 12.410.173
# 13.427.594

gb_domain <- summarize(groupBy(click_nav_with_domain, click_nav_with_domain$PublisherDomain), count = count(click_nav_with_domain$skyid))
gb_domain <- arrange(gb_domain, desc(gb_domain$count))
View(head(gb_domain,100))


## cookie positivi ##
click_nav_coo_positivi <- filter(click_nav, "CookieID NOT LIKE '-%'")
View(head(click_nav_coo_positivi,100))
nrow(click_nav_coo_positivi)
# 1.517.298

click_nav_coo_with_domain <- filter(click_nav_coo_positivi, isNotNull(click_nav_coo_positivi$PublisherDomain))
View(head(click_nav_coo_with_domain,100))
nrow(click_nav_coo_with_domain)
# 948.742

gb_domain_coo <- summarize(groupBy(click_nav_coo_with_domain, click_nav_coo_with_domain$PublisherDomain), count = count(click_nav_coo_with_domain$skyid))
gb_domain_coo <- arrange(gb_domain_coo, desc(gb_domain_coo$count))
View(head(gb_domain_coo,100))





valdist_skyid <- summarize(groupBy(click_nav, click_nav$skyid), count_coo = countDistinct(click_nav$CookieID))
valdist_skyid_2 <- arrange(valdist_skyid, desc(valdist_skyid$count_coo))
View(head(valdist_skyid_2,1000))
nrow(valdist_skyid)
# 1.624.295

valdist_skyid_pos <- summarize(groupBy(click_nav_coo_positivi, click_nav_coo_positivi$skyid), count_coo = countDistinct(click_nav_coo_positivi$CookieID))
valdist_skyid_pos_2 <- arrange(valdist_skyid_pos, desc(valdist_skyid_pos$count_coo))
View(head(valdist_skyid_pos_2,1000))
nrow(valdist_skyid_pos)
# 581.864


valdist_cookie <- summarize(groupBy(click_nav, click_nav$CookieID), count_skyid = countDistinct(click_nav$skyid))
valdist_cookie_2 <- arrange(valdist_cookie, desc(valdist_cookie$count_skyid))
View(head(valdist_cookie_2,1000))
nrow(valdist_cookie)
# 1.405.155

valdist_cookie_pos <- summarize(groupBy(click_nav_coo_positivi, click_nav_coo_positivi$CookieID), count_skyid = countDistinct(click_nav_coo_positivi$skyid))
valdist_cookie_pos_2 <- arrange(valdist_cookie_pos, desc(valdist_cookie_pos$count_skyid))
View(head(valdist_cookie_pos_2,1000))
nrow(valdist_cookie_pos)
# 668.339





#chiudi sessione
sparkR.stop()
