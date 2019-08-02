
## Elaborazioni post_scarico sito corporate e app wsc CON data di filtro ROLLING ##

source("connection_R.R")
options(scipen = 10000)


source("01_mappatura_sito_app_rolling_filter.R")



## path lettura e scrittura
path_utenti <- "/user/stefano.mazzucca/anti_downgrade/cod_cliente_cifr_ult_dwg.csv"

path_mappatura_cookie_skyid <- "/user/stefano.mazzucca/scarico_navigazioni_rolling/dizionario.parquet"
path_navigazione_sito <- "/user/stefano.mazzucca/scarico_navigazioni_rolling/nav_sito.parquet"
path_navigazione_app <- "/user/stefano.mazzucca/scarico_navigazioni_rolling/nav_app.parquet"

path_kpi_sito <- "/user/stefano.mazzucca/scarico_navigazioni_rolling/kpi_sito.parquet"
path_kpi_app <- "/user/stefano.mazzucca/scarico_navigazioni_rolling/kpi_app.parquet"
path_kpi_finale <- "/user/stefano.mazzucca/scarico_navigazioni_rolling/kpi_finale.parquet"

gg_analizzati <- as.integer(90)




# Requisito minimo del file "base" e' avere la variabile "COD_CLIENTE_CIFRATO", "data di filtro rolling"
base <- read.df(path_utenti, source = "csv",  header = "true", delimiter = ",")

base_1 <- withColumn(base, "ultimo_dwg_dt", cast(cast(unix_timestamp(base$ULT_DAT_RICH_DWG_VERO, 'ddMMMyyyy:HH:mm:ss'), "timestamp"),"date"))
View(head(base_1,100))
# printSchema(base_1)


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


## Creo le chiavi ext_id con cookies (dizionario)
diz_1 <- distinct(select(skyitdev, "external_id_post_evar", "post_visid_concatenated"))
diz_2 <- filter(diz_1, isNotNull(diz_1$external_id_post_evar))
diz_3 <- filter(diz_2,  isNotNull(diz_2$post_visid_concatenated))

base_unici <- distinct(select(base_1, "COD_CLIENTE_CIFRATO"))

createOrReplaceTempView(base_unici, "base_unici")
createOrReplaceTempView(diz_3, "diz_3")
diz_4 <- sql("select distinct t1.*, t2.post_visid_concatenated
             from base_unici t1
             left join diz_3 t2
             on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar ")
# diz_4 <- merge(diz_3, base_unici, by.x = "external_id_post_evar", by.y = "COD_CLIENTE_CIFRATO")

write.parquet(diz_4, path_mappatura_cookie_skyid, mode = "overwrite")
diz <- read.parquet(path_mappatura_cookie_skyid)


## join tra utenti target e chiavi_cookie
createOrReplaceTempView(base_1, "base_1")
createOrReplaceTempView(diz, "diz")

base_coo <- sql("select distinct t1.*, t2.post_visid_concatenated
                from base_1 t1
                left join diz t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")


## join tra base e navigazioni (skyit_4)
createOrReplaceTempView(base_coo,"base_coo")
createOrReplaceTempView(skyit_4,"skyit_4")

nav_sito <- sql("SELECT t1.*, 
                t2.visit_num, t2.post_channel,
                t2.date_time, t2.date_time_dt, t2.date_time_ts,
                t2.page_name_post_evar, t2.page_url_post_evar, t2.secondo_livello_post_prop, t2.terzo_livello_post_prop,
                t2.hit_source, t2.exclude_hit, t2.post_page_event_value
                FROM base_coo t1
                INNER JOIN skyit_4 t2
                ON t1.post_visid_concatenated = t2.post_visid_concatenated")

write.parquet(nav_sito, path_navigazione_sito, mode = "overwrite")



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
skyappwsc_4 <- filter(skyappwsc_3, isNotNull(skyappwsc_3$external_id_post_evar))


## join tra base e navigazioni (skyappwsc_4)
createOrReplaceTempView(base_1, "base_1")
createOrReplaceTempView(skyappwsc_4, "skyappwsc_4")

nav_app_wsc <- sql("SELECT t1.*, 
                   t2.visit_num, t2.post_channel,
                   t2.date_time, t2.date_time_dt, t2.date_time_ts,
                   t2.page_name_post_evar, t2.page_name_post_prop, t2.page_url_post_prop, t2.site_section,
                   t2.hit_source, t2.exclude_hit, t2.post_page_event_value
                   FROM base_1 t1
                   INNER JOIN skyappwsc_4 t2
                   ON t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(nav_app_wsc, path_navigazione_app, mode = "overwrite")


# nav_sito <- read.parquet(path_navigazione_sito)
# View(head(nav_sito,100))
# nrow(nav_sito)
# 
# 
# nav_app_wsc <- read.parquet(path_navigazione_app)
# View(head(nav_app_wsc,100))
# nrow(nav_app_wsc)
# 


################################################################################################################################################################
################################################################################################################################################################


clienti <- read.df(path_utenti, source = "csv",  header = "true", delimiter = ",")

clienti <- withColumn(clienti, "data_di_filtro", cast(cast(unix_timestamp(clienti$ULT_DAT_RICH_DWG_VERO, 'ddMMMyyyy:HH:mm:ss'), "timestamp"),"date"))

# SELEZIONO COLONNE --------------------------------------------------------
clienti <- select(clienti, c("COD_CLIENTE_CIFRATO",
                             "data_di_filtro")
) 

# group by sul COD_CLIENTE_CIFRATO per evitare doppioni
createOrReplaceTempView(clienti, "clienti")
query <- ("SELECT COD_CLIENTE_CIFRATO,
          FIRST(data_di_filtro) AS data_di_filtro
          FROM clienti 
          GROUP BY COD_CLIENTE_CIFRATO")
clienti <- sql(query)


# IMPORTO MAPPATURA SKYID COOKIE ------------------------------------------
id_clienti <- read.parquet(path_mappatura_cookie_skyid)
id_clienti <- distinct(select(id_clienti, c("COD_CLIENTE_CIFRATO", "post_visid_concatenated")))


# IMPORTO NAVIGAZIONE SITO ------------------------------------------------
cookie_sito <- read.parquet(path_navigazione_sito)
cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt <= cookie_sito$ultimo_dwg_dt)
cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt >= (date_sub(cookie_sito$ultimo_dwg_dt, gg_analizzati))) 
#cookie_sito <- filter(cookie_sito, isNotNull(cookie_sito$canale_corporate_post_prop))
cookie_sito <- filter(cookie_sito, "post_channel = 'corporate' or post_channel = 'Guidatv'")
cache(cookie_sito)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_sito <- join_COD_CLIENTE_CIFRATO(cookie_sito, clienti) 

# ID UNIVOCI -------------------------------------------------------------
unique_id <- select(id_clienti, "COD_CLIENTE_CIFRATO")
unique_id <- distinct(unique_id)

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

cookie_app <- filter(cookie_app, cookie_app$date_time_dt <= cookie_app$ultimo_dwg_dt)
cookie_app <- filter(cookie_app, cookie_app$date_time_dt >= (date_sub(cookie_app$ultimo_dwg_dt, gg_analizzati)))

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
df_kpi_2 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid, t1.data_prima_attiv_dt, t1.data_di_filtro, t2.*
                from clienti t1 
                left join df_kpi t2 
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

write.parquet(df_kpi_2, path_kpi_finale, mode = "overwrite")


df_kpi <- read.parquet(path_kpi_finale)
View(head(df_kpi,100))
nrow(df_kpi)







#chiudi sessione
sparkR.stop()
