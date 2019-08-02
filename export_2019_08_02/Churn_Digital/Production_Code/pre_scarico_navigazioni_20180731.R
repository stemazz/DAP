
## ISTRUZIONI: 
## Impostare i path di lettura e scrittura sul file "01_CD_START.R"
## Modifica le date di estrazione delle navigazioni SETTIMANA PER SETTIMANA!



## Se abbiamo un nuovo file "full", procedi nella lettura, altrimenti salta: 

# full <- read.df("/user/stefano.mazzucca/20180718_richiesta_estrazione_cb_attiva.txt", source = "csv" ,header = "true", delimiter = "|")
# full_1 <- withColumn(full, "DAT_PRIMA_ATTIVAZIONE_dt", cast(cast(unix_timestamp(full$DAT_PRIMA_ATTIVAZIONE, 'dd/MM/yyyy'), 'timestamp'), 'date'))
# 
# write.parquet(full_1, path_full)


## Navigazioni aggiornate per Churn Digital Model
full <- read.parquet(path_full)

path_navigazione_sito <- "/user/stefano.mazzucca/churn_digital/nav_sito_20180731.parquet"
path_navigazione_app <- "/user/stefano.mazzucca/churn_digital/nav_app_20180731.parquet"
path_mappatura_cookie_skyid <- "/user/stefano.mazzucca/churn_digital/mappatura_skyid_coo_20180731.parquet"

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
skyit_5 <- filter(skyit_4,"date_time_dt >= '2018-04-01'") ## MODIFICA LE DATE SETTIMANA PER SETTIMANA! #########################################################
skyit_6 <- filter(skyit_5,"date_time_dt <= '2018-07-31'")

## Creo le chiavi ext_id con cookies (dizionario)
diz_1 <- distinct(select(skyitdev, "external_id_post_evar", "post_visid_concatenated"))
diz_2 <- filter(diz_1, "external_id_post_evar is NOT NULL")
diz_3 <- filter(diz_2, "post_visid_concatenated is NOT NULL")
clienti_unici <- distinct(select(full, "COD_CLIENTE_CIFRATO"))
diz_4 <- merge(diz_3, clienti_unici, by.x = "external_id_post_evar", by.y = "COD_CLIENTE_CIFRATO")

write.parquet(diz_4, path_mappatura_cookie_skyid)


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

# nav_sito <- read.parquet(path_navigazione_sito)
# View(head(nav_sito,100))
# 
# nav_pre_sito <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_navigazSkyITDev.parquet")
# View(head(nav_pre_sito,100))


## naviagazioni app
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
skyappwsc_5 <- filter(skyappwsc_4,"date_time_dt >= '2018-04-01'") ## MODIFICA LE DATE SETTIMANA PER SETTIMANA! #################################################
skyappwsc_6 <- filter(skyappwsc_5,"date_time_dt <= '2018-07-31'")

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

