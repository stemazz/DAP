
## Elaborazioni post_scarico sito corporate e app wsc ##

source("connection_R.R")
options(scipen = 10000)


source("01_mappatura_sito_app.R")



# path lettura - scrittura
path_full <- "/user/stefano.mazzucca/churn_digital/cb_attiva_agosto_2018.parquet"


path_mappatura_cookie_skyid <- "/user/stefano.mazzucca/scarico_navigazioni/dizionario.parquet"
path_navigazione_sito <- "/user/stefano.mazzucca/scarico_navigazioni/nav_sito.parquet"
path_navigazione_app <- "/user/stefano.mazzucca/scarico_navigazioni/nav_app.parquet"

path_kpi_sito <- "/user/stefano.mazzucca/scarico_navigazioni/kpi_sito_2.parquet"
path_kpi_app <- "/user/stefano.mazzucca/scarico_navigazioni/kpi_app_2.parquet"
path_kpi_finale <- "/user/stefano.mazzucca/scarico_navigazioni/kpi_finale_2.parquet"

data <- as.Date("2018-10-25")
gg_analizzati <- as.integer(90)




# Requisito minimo del file "full" e' avere la variabile "COD_CLIENTE_CIFRATO", "data_prima_attiv_dt"
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
                  "exclude_hit",
                  "post_page_event_value"
)
skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))
skyit_4 <- filter(skyit_3, "post_channel = 'corporate' or post_channel = 'Guidatv'")

# filtro temporale
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt >= (data - gg_analizzati)) 
skyit_6 <- filter(skyit_5, skyit_5$date_time_dt <= data)

## Creo le chiavi ext_id con cookies (dizionario)
diz_1 <- distinct(select(skyitdev, "external_id_post_evar", "post_visid_concatenated"))
diz_2 <- filter(diz_1, "external_id_post_evar is NOT NULL")
diz_3 <- filter(diz_2, "post_visid_concatenated is NOT NULL")
clienti_unici <- distinct(select(full, "COD_CLIENTE_CIFRATO"))
diz_4 <- merge(diz_3, clienti_unici, by.x = "external_id_post_evar", by.y = "COD_CLIENTE_CIFRATO")

write.parquet(diz_4, path_mappatura_cookie_skyid)
diz <- read.parquet(path_mappatura_cookie_skyid)


## join tra full_CB e chiavi_cookie
createOrReplaceTempView(full, "full")
createOrReplaceTempView(diz, "diz")

full_coo <- sql("select distinct t1.*, t2.post_visid_concatenated
                from full t1
                inner join diz t2
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
skyappwsc_5 <- filter(skyappwsc_4, skyappwsc_4$date_time_dt >= (data - gg_analizzati)) 
skyappwsc_6 <- filter(skyappwsc_5, skyappwsc_5$date_time_dt <= data)

## join tra full e navigazioni (skyappwsc_6)
createOrReplaceTempView(full, "full")
createOrReplaceTempView(skyappwsc_6, "skyappwsc_6")

nav_app_wsc <- sql("SELECT DISTINCT t1.*, 
                           t2.visit_num, t2.post_channel,
                           t2.date_time, t2.date_time_dt, t2.date_time_ts,
                           t2.page_name_post_evar, t2.page_name_post_prop, t2.page_url_post_prop, t2.site_section,
                           t2.hit_source, t2.exclude_hit
                   FROM full t1
                   INNER JOIN skyappwsc_6 t2
                    ON t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(nav_app_wsc, path_navigazione_app)



################################################################################################################################################################
################################################################################################################################################################


clienti <- read.parquet(path_full)

# SELEZIONO COLONNE --------------------------------------------------------
clienti <- withColumn(clienti, "data", lit(data))

## NEL CASO DI ANALISI SU DATE ROLLING, INSERIRE QUI IL CAMPO DELLA DATA DA FILTRARE! ##################################################################
clienti <- select(clienti, c("COD_CLIENTE_CIFRATO",
                             "data_prima_attiv_dt",
                             "data")
) ## "data_scelta_di_filtro"

# group by sul COD_CLIENTE_CIFRATO per evitare doppioni
createOrReplaceTempView(clienti, "clienti")
query <- ("SELECT COD_CLIENTE_CIFRATO,
                 FIRST(data_prima_attiv_dt) AS data_prima_attiv_dt,
                 FIRST(data) AS data
          FROM clienti 
          GROUP BY COD_CLIENTE_CIFRATO")
clienti <- sql(query)


# IMPORTO MAPPATURA SKYID COOKIE ------------------------------------------
id_clienti <- read.parquet(path_mappatura_cookie_skyid)
id_clienti <- distinct(select(id_clienti, c("external_id_post_evar", "post_visid_concatenated")))


# IMPORTO NAVIGAZIONE SITO ------------------------------------------------
cookie_sito <- read.parquet(path_navigazione_sito)
cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt >= (data - gg_analizzati))
cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt <= data)
#cookie_sito <- filter(cookie_sito, isNotNull(cookie_sito$canale_corporate_post_prop))
cookie_sito <- filter(cookie_sito, "post_channel = 'corporate' or post_channel = 'Guidatv'")
cache(cookie_sito)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_sito <- join_COD_CLIENTE_CIFRATO(cookie_sito, clienti) 

## NEL CASO DI ANALISI SU DATE ROLLING, INSERIRE QUI IL FILTRO SULLE DATE! ###########################################################################
## cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt <= cookie_sito$data_scelta_di_filtro)

# ID UNIVOCI -------------------------------------------------------------
unique_id <- select(id_clienti, "external_id_post_evar")
unique_id <- distinct(unique_id)
# attenzione al nome del parametro
unique_id <- withColumnRenamed(unique_id, "external_id_post_evar", "COD_CLIENTE_CIFRATO")
#

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
cookie_sito <- remove_too_short(cookie_sito, 10)

# KPI SITO ----------------------------------------------------------------
df_kpi_sito <- start_kpi_sito(cookie_sito, unique_id)
# cache(df_kpi_sito)

write.parquet(df_kpi_sito, path_kpi_sito, mode = "overwrite")



# IMPORTO COOKIE APP ------------------------------------------------------
cookie_app <- read.parquet(path_navigazione_app)
cookie_app <- join_COD_CLIENTE_CIFRATO(unique_id, cookie_app)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_app <- join_COD_CLIENTE_CIFRATO(cookie_app, clienti)

## NEL CASO DI ANALISI SU DATE ROLLING, INSERIRE QUI IL FILTRO SULLE DATE! ###########################################################################
## cookie_app <- filter(cookie_app, cookie_app$date_time_dt <= cookie_app$data_scelta_di_filtro)

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
cookie_app <- remove_too_short(cookie_app, 4)

# KPI APP -----------------------------------------------------------------
df_kpi_app <- start_kpi_app(cookie_app, unique_id)
# cache(df_kpi_app)

write.parquet(df_kpi_app, path_kpi_app, mode = "overwrite")


# MERGE KPI ---------------------------------------------------------------
df_kpi_sito <- read.parquet(path_kpi_sito)
df_kpi_app <- read.parquet(path_kpi_app)

df_kpi <- join_COD_CLIENTE_CIFRATO(df_kpi_sito, df_kpi_app) 
df_kpi <- melt_kpi(df_kpi)

vars <- names(df_kpi)
vars <- vars[str_detect(vars, pattern = "_sito_1w")]
vars <- str_replace(vars, "_sito_1w", "")

df_kpi <- differential_kpi(df_kpi, vars) 


createOrReplaceTempView(clienti, "clienti")
createOrReplaceTempView(df_kpi, "df_kpi")
df_kpi_2 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid, t1.data_prima_attiv_dt, t1.data_analisi, t2.*
                from clienti t1 
                left join df_kpi t2 
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

write.parquet(df_kpi_2, path_kpi_finale, mode = "overwrite")


df_kpi <- read.parquet(path_kpi_finale)
View(head(df_kpi,100))
nrow(df_kpi)




#chiudi sessione
sparkR.stop()
