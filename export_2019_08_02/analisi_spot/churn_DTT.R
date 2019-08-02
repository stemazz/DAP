
## Ananlisi churn DTT


source("connection_R.R")
options(scipen = 10000)


path_tier2 <- "/user/stefano.mazzucca/churn_dtt/14NOV18_basegeo.csv"
path_dtt <- "/user/stefano.mazzucca/churn_dtt/14NOV18_BASEDTT.csv"


base_tier2 <- read.df(path_tier2, source = "csv", header = "true", delimiter = ";")
base_tier2 <- withColumn(base_tier2, "dat_cessazione_promo_dt", cast(cast(unix_timestamp(base_tier2$DAT_CESSAZIONE_PROMO, 'dd/MM/yyyy HH:mm'), 'timestamp'),'date'))
View(head(base_tier2,100))
nrow(base_tier2)
# 19.814

# filt_active <- filter(base_tier2, base_tier2$DES_STATO_BUSINESS == 'Active')
# nrow(filt_active)
# # 10.067



base_dtt <- read.df(path_dtt, source = "csv", header = "true", delimiter = ";")
base_dtt <- filter(base_dtt, isNotNull(base_dtt$COD_CLIENTE_CIFRATO))
# base_dtt <- filter(base_dtt, base_dtt$DES_STATO_BUSINESS != 'Disconnected')
View(head(base_dtt,100))
nrow(base_dtt)
# 453.367  # 430.533



################################################################################################################################################################
################################################################################################################################################################
## DTT ##
################################################################################################################################################################
################################################################################################################################################################

## Mappatura sito #################################################################################################################################################

source("01_mappatura_sito_app.R")

path_mappatura_cookie_skyid <- "/user/stefano.mazzucca/churn_dtt/dizionario_dtt.parquet"
path_navigazione_sito <- "/user/stefano.mazzucca/churn_dtt/nav_sito_dtt.parquet"
path_navigazione_app <- "/user/stefano.mazzucca/churn_dtt/nav_app_dtt.parquet"

path_kpi_sito <- "/user/stefano.mazzucca/churn_dtt/kpi_sito_dtt.parquet"
path_kpi_app <- "/user/stefano.mazzucca/churn_dtt/kpi_app_dtt.parquet"
path_kpi_finale <- "/user/stefano.mazzucca/churn_dtt/kpi_finale_dtt.parquet"

data <- as.Date("2018-11-13")
gg_analizzati <- as.integer(90)




full <- base_dtt

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

write.parquet(diz_4, path_mappatura_cookie_skyid, mode = "overwrite")
diz <- read.parquet(path_mappatura_cookie_skyid)


## join tra full_CB e chiavi_cookie
createOrReplaceTempView(full, "full")
createOrReplaceTempView(diz, "diz")

full_coo <- sql("select distinct t1.*, t2.post_visid_concatenated
                from full t1
                inner join diz t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

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

write.parquet(nav_app_wsc, path_navigazione_app, mode = "overwrite")



################################################################################################################################################################
################################################################################################################################################################


clienti <- full

# SELEZIONO COLONNE --------------------------------------------------------
clienti <- withColumn(clienti, "data", lit(data))
clienti <- withColumn(clienti, "data_prima_attiv_dt", cast(cast(unix_timestamp(clienti$DAT_PRIMA_ATTIV_CNTR, 'dd/MM/yyyy HH:mm'), 'timestamp'), 'date'))

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
id_clienti <- distinct(select(id_clienti, c("COD_CLIENTE_CIFRATO", "post_visid_concatenated")))


# IMPORTO NAVIGAZIONE SITO ------------------------------------------------
cookie_sito <- read.parquet(path_navigazione_sito)
cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt >= (data - gg_analizzati))
cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt <= data)
#cookie_sito <- filter(cookie_sito, isNotNull(cookie_sito$canale_corporate_post_prop))
cookie_sito <- filter(cookie_sito, "post_channel = 'corporate' or post_channel = 'Guidatv'")
cache(cookie_sito)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_sito <- join_COD_CLIENTE_CIFRATO(cookie_sito, clienti) 


# ID UNIVOCI -------------------------------------------------------------
unique_id <- select(id_clienti, "COD_CLIENTE_CIFRATO")
unique_id <- distinct(unique_id)
# attenzione al nome del parametro
# unique_id <- withColumnRenamed(unique_id, "external_id_post_evar", "COD_CLIENTE_CIFRATO")
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
nrow(df_kpi)
# 163.533

createOrReplaceTempView(clienti, "clienti")
createOrReplaceTempView(df_kpi, "df_kpi")
df_kpi_2 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid, t1.data_prima_attiv_dt, t1.data, t2.*
                from clienti t1 
                left join df_kpi t2 
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

write.parquet(df_kpi_2, path_kpi_finale, mode = "overwrite")


df_kpi_dtt <- read.parquet(path_kpi_finale)
View(head(df_kpi_dtt,100))
nrow(df_kpi_dtt)
# 444.446

ver_cod_client <- filter(df_kpi_dtt, "cod_cliente_cifrato is NULL")
nrow(ver_cod_client)
# 280.913




################################################################################################################################################################
################################################################################################################################################################
## Tier2 ##
################################################################################################################################################################
################################################################################################################################################################

## Mappatura sito #################################################################################################################################################

source("01_mappatura_sito_app_rolling_filter.R")

path_mappatura_cookie_skyid <- "/user/stefano.mazzucca/churn_dtt/dizionario_tier2.parquet"
path_navigazione_sito <- "/user/stefano.mazzucca/churn_dtt/nav_sito_tier2.parquet"
path_navigazione_app <- "/user/stefano.mazzucca/churn_dtt/nav_app_tier2.parquet"

path_kpi_sito <- "/user/stefano.mazzucca/churn_dtt/kpi_sito_tier2.parquet"
path_kpi_app <- "/user/stefano.mazzucca/churn_dtt/kpi_app_tier2.parquet"
path_kpi_finale <- "/user/stefano.mazzucca/churn_dtt/kpi_finale_tier2.parquet"

gg_analizzati <- as.integer(90)





full <- base_tier2

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

base_unici <- distinct(select(full, "COD_CLIENTE_CIFRATO"))

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
createOrReplaceTempView(full, "full")
createOrReplaceTempView(diz, "diz")

base_coo <- sql("select distinct t1.*, t2.post_visid_concatenated
                from full t1
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
createOrReplaceTempView(full, "full")
createOrReplaceTempView(skyappwsc_4, "skyappwsc_4")

nav_app_wsc <- sql("SELECT t1.*, 
                   t2.visit_num, t2.post_channel,
                   t2.date_time, t2.date_time_dt, t2.date_time_ts,
                   t2.page_name_post_evar, t2.page_name_post_prop, t2.page_url_post_prop, t2.site_section,
                   t2.hit_source, t2.exclude_hit, t2.post_page_event_value
                   FROM full t1
                   INNER JOIN skyappwsc_4 t2
                   ON t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(nav_app_wsc, path_navigazione_app, mode = "overwrite")



################################################################################################################################################################
################################################################################################################################################################


clienti <- full

# SELEZIONO COLONNE --------------------------------------------------------
clienti <- select(clienti, c("COD_CLIENTE_CIFRATO",
                             "dat_cessazione_promo_dt")
) 

# group by sul COD_CLIENTE_CIFRATO per evitare doppioni
createOrReplaceTempView(clienti, "clienti")
query <- ("SELECT COD_CLIENTE_CIFRATO,
          FIRST(dat_cessazione_promo_dt) AS dat_cessazione_promo_dt
          FROM clienti 
          GROUP BY COD_CLIENTE_CIFRATO")
clienti <- sql(query)


# IMPORTO MAPPATURA SKYID COOKIE ------------------------------------------
id_clienti <- read.parquet(path_mappatura_cookie_skyid)
id_clienti <- distinct(select(id_clienti, c("COD_CLIENTE_CIFRATO", "post_visid_concatenated")))


# IMPORTO NAVIGAZIONE SITO ------------------------------------------------
cookie_sito <- read.parquet(path_navigazione_sito)
cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt <= cookie_sito$dat_cessazione_promo_dt)
cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt >= (date_sub(cookie_sito$dat_cessazione_promo_dt, gg_analizzati))) 
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
# cookie_app <- join_COD_CLIENTE_CIFRATO(unique_id, cookie_app)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_app <- join_COD_CLIENTE_CIFRATO(cookie_app, clienti)

cookie_app <- filter(cookie_app, cookie_app$date_time_dt <= cookie_app$dat_cessazione_promo_dt_x)
cookie_app <- filter(cookie_app, cookie_app$date_time_dt >= (date_sub(cookie_app$dat_cessazione_promo_dt_x, gg_analizzati)))

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
nrow(df_kpi)
# 19.788


createOrReplaceTempView(clienti, "clienti")
createOrReplaceTempView(df_kpi, "df_kpi")
df_kpi_2 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid, t1.dat_cessazione_promo_dt, t2.*
                from clienti t1 
                left join df_kpi t2 
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

write.parquet(df_kpi_2, path_kpi_finale, mode = "overwrite")


df_kpi_tier2 <- read.parquet(path_kpi_finale)
View(head(df_kpi_tier2,100))
nrow(df_kpi_tier2)
# 19.788

ver_cod_client <- filter(df_kpi_tier2, "cod_cliente_cifrato is NULL")
nrow(ver_cod_client)




##################################################################################################################################################################
##################################################################################################################################################################
## Elaborazioni
##################################################################################################################################################################
##################################################################################################################################################################

dtt <- read.parquet("/user/stefano.mazzucca/churn_dtt/kpi_finale_dtt.parquet")
View(head(dtt,100))
nrow(dtt)
# 444.446

dtt$flg_navigante <- ifelse(isNull(dtt$COD_CLIENTE_CIFRATO), 0, 1)

dtt <- select(dtt, "skyid", 
              "visite_totali_sito_2m", 
              "visite_FaiDaTe_sito_2m", "visite_Fatture_sito_2m", "visite_ArricchisciAbbonamento_sito_2m", "visite_FaiDaTeExtra_sito_2m", 
                                        "visite_Gestione_sito_2m", "visite_Comunicazione_sito_2m",
              "visite_Assistenza_sito_2m", 
              "visite_GuidaTv_sito_2m", 
              "visite_Extra_sito_2m", 
              "visite_Tecnologia_sito_2m", 
              "visite_PacchettiOfferte_sito_2m", 
              "visite_InfoDisdetta_sito_2m",
              "visite_SearchDisdetta_sito_2m", "visite_Trigger_sito_2m", 
              "visite_totali_app_2m", "visite_fatture_app_2m", "visite_assistenza_e_supporto_app_2m", "visite_extra_app_2m", "visite_arricchisci_abbonamento_app_2m", 
                                      "visite_contatta_sky_app_2m", "visite_info_app_2m", "visite_stato_abbonamento_app_2m", "visite_widget_app_2m",
              "visite_totali_2m",
              "flg_navigante")
View(head(dtt,100))




# dtt_aggreg <- agg(
#   groupBy(dtt, "visite_totali_sito_1w", "visite_FaiDaTe_sito_1w", "visite_Fatture_sito_1w", "visite_ArricchisciAbbonamento_sito_1w", "visite_Assistenza_sito_1w", 
#           "visite_GuidaTv_sito_1w", "visite_Extra_sito_1w", "visite_Tecnologia_sito_1w", "visite_PacchettiOfferte_sito_1w", "visite_InfoDisdetta_sito_1w",
#           "visite_SearchDisdetta_sito_1w", "visite_Trigger_sito_1w", 
#           "visite_totali_app_1w", "visite_fatture_app_1w", "visite_assistenza_e_supporto_app_1w", "visite_extra_app_1w", "visite_arricchisci_abbonamento_app_1w", 
#           "visite_contatta_sky_app_1w", "visite_info_app_1w", 
#           "visite_totali_1w",
#           
#           "visite_totali_sito_2w", "visite_FaiDaTe_sito_2w", "visite_Fatture_sito_2w", "visite_ArricchisciAbbonamento_sito_2w", "visite_Assistenza_sito_2w", 
#           "visite_GuidaTv_sito_2w", "visite_Extra_sito_2w", "visite_Tecnologia_sito_2w", "visite_PacchettiOfferte_sito_2w", "visite_InfoDisdetta_sito_2w",
#           "visite_SearchDisdetta_sito_2w", "visite_Trigger_sito_2w", 
#           "visite_totali_app_2w", "visite_fatture_app_2w", "visite_assistenza_e_supporto_app_2w", "visite_extra_app_2w", "visite_arricchisci_abbonamento_app_2w", 
#           "visite_contatta_sky_app_2w", "visite_info_app_2w", 
#           "visite_totali_2w",
#           
#           "visite_totali_sito_1m", "visite_FaiDaTe_sito_1m", "visite_Fatture_sito_1m", "visite_ArricchisciAbbonamento_sito_1m", "visite_Assistenza_sito_1m", 
#           "visite_GuidaTv_sito_1m", "visite_Extra_sito_1m", "visite_Tecnologia_sito_1m", "visite_PacchettiOfferte_sito_1m", "visite_InfoDisdetta_sito_1m",
#           "visite_SearchDisdetta_sito_1m", "visite_Trigger_sito_1m", 
#           "visite_totali_app_1m", "visite_fatture_app_1m", "visite_assistenza_e_supporto_app_1m", "visite_extra_app_1m", "visite_arricchisci_abbonamento_app_1m", 
#           "visite_contatta_sky_app_1m", "visite_info_app_1m", 
#           "visite_totali_1m",
#           
#           "visite_totali_sito_2m", "visite_FaiDaTe_sito_2m", "visite_Fatture_sito_2m", "visite_ArricchisciAbbonamento_sito_2m", "visite_Assistenza_sito_2m", 
#           "visite_GuidaTv_sito_2m", "visite_Extra_sito_2m", "visite_Tecnologia_sito_2m", "visite_PacchettiOfferte_sito_2m", "visite_InfoDisdetta_sito_2m",
#           "visite_SearchDisdetta_sito_2m", "visite_Trigger_sito_2m", 
#           "visite_totali_app_2m", "visite_fatture_app_2m", "visite_assistenza_e_supporto_app_2m", "visite_extra_app_2m", "visite_arricchisci_abbonamento_app_2m", 
#           "visite_contatta_sky_app_2m", "visite_info_app_2m", 
#           "visite_totali_2m"
#           ),
#   id = countDistinct(dtt$skyid))
# View(head(dtt_aggreg,100))

write.df(repartition( dtt, 1), path = "/user/stefano.mazzucca/churn_dtt/dtt_kpi_selected.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



tier2 <- read.parquet("/user/stefano.mazzucca/churn_dtt/kpi_finale_tier2.parquet")
View(head(tier2,100))
nrow(tier2)
# 19.788

# tier2$flg_navigante <- ifelse(isNull(tier2$COD_CLIENTE_CIFRATO), 0, 1)

tier2 <- select(tier2, "skyid", 
              "visite_totali_sito_2m", 
              "visite_FaiDaTe_sito_2m", "visite_Fatture_sito_2m", "visite_ArricchisciAbbonamento_sito_2m", "visite_FaiDaTeExtra_sito_2m", 
              "visite_Gestione_sito_2m", "visite_Comunicazione_sito_2m",
              "visite_Assistenza_sito_2m", 
              "visite_GuidaTv_sito_2m", 
              "visite_Extra_sito_2m", 
              "visite_Tecnologia_sito_2m", 
              "visite_PacchettiOfferte_sito_2m", 
              "visite_InfoDisdetta_sito_2m",
              "visite_SearchDisdetta_sito_2m", "visite_Trigger_sito_2m", 
              "visite_totali_app_2m", "visite_fatture_app_2m", "visite_assistenza_e_supporto_app_2m", "visite_extra_app_2m", "visite_arricchisci_abbonamento_app_2m", 
              "visite_contatta_sky_app_2m", "visite_info_app_2m", "visite_stato_abbonamento_app_2m", "visite_widget_app_2m",
              "visite_totali_2m" # , flg_navigante
              )
View(head(tier2,100))
nrow(tier2)

# tier2_aggreg <- agg(
#   groupBy(tier2, "visite_totali_sito_1w", "visite_FaiDaTe_sito_1w", "visite_Fatture_sito_1w", "visite_ArricchisciAbbonamento_sito_1w", "visite_Assistenza_sito_1w", 
#           "visite_GuidaTv_sito_1w", "visite_Extra_sito_1w", "visite_Tecnologia_sito_1w", "visite_PacchettiOfferte_sito_1w", "visite_InfoDisdetta_sito_1w",
#           "visite_SearchDisdetta_sito_1w", "visite_Trigger_sito_1w", 
#           "visite_totali_app_1w", "visite_fatture_app_1w", "visite_assistenza_e_supporto_app_1w", "visite_extra_app_1w", "visite_arricchisci_abbonamento_app_1w", 
#           "visite_contatta_sky_app_1w", "visite_info_app_1w", 
#           "visite_totali_1w",
#           
#           "visite_totali_sito_2w", "visite_FaiDaTe_sito_2w", "visite_Fatture_sito_2w", "visite_ArricchisciAbbonamento_sito_2w", "visite_Assistenza_sito_2w", 
#           "visite_GuidaTv_sito_2w", "visite_Extra_sito_2w", "visite_Tecnologia_sito_2w", "visite_PacchettiOfferte_sito_2w", "visite_InfoDisdetta_sito_2w",
#           "visite_SearchDisdetta_sito_2w", "visite_Trigger_sito_2w", 
#           "visite_totali_app_2w", "visite_fatture_app_2w", "visite_assistenza_e_supporto_app_2w", "visite_extra_app_2w", "visite_arricchisci_abbonamento_app_2w", 
#           "visite_contatta_sky_app_2w", "visite_info_app_2w", 
#           "visite_totali_2w",
#           
#           "visite_totali_sito_1m", "visite_FaiDaTe_sito_1m", "visite_Fatture_sito_1m", "visite_ArricchisciAbbonamento_sito_1m", "visite_Assistenza_sito_1m", 
#           "visite_GuidaTv_sito_1m", "visite_Extra_sito_1m", "visite_Tecnologia_sito_1m", "visite_PacchettiOfferte_sito_1m", "visite_InfoDisdetta_sito_1m",
#           "visite_SearchDisdetta_sito_1m", "visite_Trigger_sito_1m", 
#           "visite_totali_app_1m", "visite_fatture_app_1m", "visite_assistenza_e_supporto_app_1m", "visite_extra_app_1m", "visite_arricchisci_abbonamento_app_1m", 
#           "visite_contatta_sky_app_1m", "visite_info_app_1m", 
#           "visite_totali_1m",
#           
#           "visite_totali_sito_2m", "visite_FaiDaTe_sito_2m", "visite_Fatture_sito_2m", "visite_ArricchisciAbbonamento_sito_2m", "visite_Assistenza_sito_2m", 
#           "visite_GuidaTv_sito_2m", "visite_Extra_sito_2m", "visite_Tecnologia_sito_2m", "visite_PacchettiOfferte_sito_2m", "visite_InfoDisdetta_sito_2m",
#           "visite_SearchDisdetta_sito_2m", "visite_Trigger_sito_2m", 
#           "visite_totali_app_2m", "visite_fatture_app_2m", "visite_assistenza_e_supporto_app_2m", "visite_extra_app_2m", "visite_arricchisci_abbonamento_app_2m", 
#           "visite_contatta_sky_app_2m", "visite_info_app_2m", 
#           "visite_totali_2m"
#   ),
#   id = countDistinct(tier2$skyid))
# View(head(tier2_aggreg,100))

write.df(repartition( tier2, 1), path = "/user/stefano.mazzucca/churn_dtt/tier2_kpi_selected.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



#chiudi sessione
sparkR.stop()
