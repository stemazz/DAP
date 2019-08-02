
## ISTRUZIONI: 
## Impostare i path di lettura e scrittura e le date sul file "01_CD_START.R"

## Aggiornare path_full solo in caso di una nuova estrazione della CB da BI (in cima)
## Per aggiornare la tabella FULL: aggiornare path_delta per estrazione dalla tabella DMP (in fondo)




## Se abbiamo un nuovo file "full" da BI lo dobbiamo importare con questo script, altrimenti salta: 
if(path_full_new == ""){
  
  full <- read.df(path_full_csv, source = "csv" ,header = "true", delimiter = ",")
  full <- filter(full, isNotNull(full$COD_CLIENTE_CIFRATO))
  full_1 <- withColumn(full, "DAT_PRIMA_ATTIV_CNTR_dt", cast(cast(unix_timestamp(full$DAT_PRIMA_ATTIV_CNTR, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))
  
  write.parquet(full_1, path_full, mode = "overwrite")

}

full <- read.parquet(path_full)


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
                  "exclude_hit"
)
skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))
skyit_4 <- filter(skyit_3, "post_channel = 'corporate' or post_channel = 'Guidatv'")

# filtro temporale
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt >= (data_modello - 90)) 
skyit_6 <- filter(skyit_5, skyit_5$date_time_dt <= data_modello )

## Creo le chiavi ext_id con cookies (dizionario)
diz_1 <- distinct(select(skyitdev, "external_id_post_evar", "post_visid_concatenated"))
diz_2 <- filter(diz_1, "external_id_post_evar is NOT NULL")
diz_3 <- filter(diz_2, "post_visid_concatenated is NOT NULL")
clienti_unici <- distinct(select(full, "COD_CLIENTE_CIFRATO"))
diz_4 <- merge(diz_3, clienti_unici, by.x = "external_id_post_evar", by.y = "COD_CLIENTE_CIFRATO")

write.parquet(diz_4, path_mappatura_cookie_skyid)
diz_4 <- read.parquet(path_mappatura_cookie_skyid)


## join tra full_CB e chiavi_cookie
createOrReplaceTempView(full, "full")
createOrReplaceTempView(diz_4, "diz_4")

full_coo <- sql("select distinct t1.*, t2.post_visid_concatenated
                           from full t1
                           inner join diz_4 t2
                           on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

## join tra full e navigazioni (skyit_6)
createOrReplaceTempView(full_coo,"full_coo")
createOrReplaceTempView(skyit_6,"skyit_6")

nav_sito <- sql("SELECT t1.*, 
                        t2.visit_num, t2.post_channel,
                        t2.date_time, t2.date_time_dt, t2.date_time_ts,
                        t2.page_name_post_evar, t2.page_url_post_evar, t2.secondo_livello_post_prop, t2.terzo_livello_post_prop,
                        t2.hit_source, t2.exclude_hit
                 FROM full_coo t1
                 INNER JOIN skyit_6 t2
                  ON t1.post_visid_concatenated = t2.post_visid_concatenated")

write.parquet(nav_sito, path_navigazione_sito)


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
                      "page_url_post_prop",
                      "site_section",
                      "hit_source",
                      "exclude_hit"
)
skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_ts", cast(skyappwsc_2$date_time, "timestamp"))
skyappwsc_4 <- filter(skyappwsc_3, "external_id_post_evar is not NULL")

# filtro temporale
skyappwsc_5 <- filter(skyappwsc_4, skyappwsc_4$date_time_dt >= (data_modello - 90)) 
skyappwsc_6 <- filter(skyappwsc_5, skyappwsc_5$date_time_dt <= data_modello )

## join tra full e navigazioni (skyappwsc_6)
createOrReplaceTempView(full, "full")
createOrReplaceTempView(skyappwsc_6, "skyappwsc_6")

nav_app_wsc <- sql("SELECT t1.*, 
                           t2.visit_num, t2.post_channel,
                           t2.date_time, t2.date_time_dt, t2.date_time_ts,
                           t2.page_name_post_evar, t2.page_url_post_prop, t2.site_section,
                           t2.hit_source, t2.exclude_hit
                   FROM full t1
                   INNER JOIN skyappwsc_6 t2
                    ON t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(nav_app_wsc, path_navigazione_app)


## SOLO per quando si deve aggiornare la tabella FULL ##########################################################################################################

if(path_delta != ""){

  ## Aggiornamento dati dalla DMP
  delta_view <- read.parquet("/STAGE/DMP/cb_week.parquet")
  # View(head(delta_view,100))
  # nrow(delta_view)
  # 12.679.256
  delta_view_1 <- withColumn(delta_view, "data_dt", cast(cast(unix_timestamp(delta_view$ID_DAY_WEEK, 'dd/MM/yyyy'), 'timestamp'), 'date'))
  
  # summ_delta_view <- summarize(delta_view_1, min = min(delta_view_1$data_dt), max = max(delta_view_1$data_dt))
  # View(head(summ_delta_view,100))
  # # min           max
  # # 2017-10-04    2018-07-23
  
  # trova_ultima_data_update <- select(delta_view_1, "COD_CONTRATTO_CRIP", "COD_CLIENTE_FRU_CRIP", "ID_DAY_WEEK", "data_dt")
  # View(head(trova_ultima_data_update,100))
  # trova_ultima_data_update_1 <- arrange(distinct(select(trova_ultima_data_update, "data_dt")), desc(trova_ultima_data_update$data_dt))
  # View(head(trova_ultima_data_update_1,100))
  
  
  delta_view_2 <- filter(delta_view_1, delta_view_1$data_dt >= data_precedente_estrazione & delta_view_1$data_dt <= data_modello) ## ATTENZIONE ALLE DATE !!
  # nrow(delta_view_2)
  # # 140.281
  
  write.parquet(delta_view_2, path_delta, mode = "overwrite")

}

