# IMPORT CLIENTI + INFO BASE ----------------------------------------------
clienti <- read.parquet(path_full)

# SELEZIONO COLONNE --------------------------------------------------------
clienti <- withColumn(clienti, "INGR_PDISC_dt", lit(NA))

clienti <- withColumnRenamed(clienti, "FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M1")
clienti <- withColumnRenamed(clienti, "FASCIA_SDM_PDISC_M4", "meno1M_FASCIA_SDM_PDISC_M4")
clienti <- withColumnRenamed(clienti, "DAT_PRIMA_ATTIV_CNTR_dt", "DAT_PRIMA_ATTIVAZIONE_dt")
clienti <- select(clienti, c("INGR_PDISC_dt",
                             "DAT_PRIMA_ATTIVAZIONE_dt",
                             "COD_CLIENTE_CIFRATO",
                             "meno1M_FASCIA_SDM_PDISC_M1",
                             "meno1M_FASCIA_SDM_PDISC_M4")
                  )

# INVECE DI RIMUOVERE TUTTI FACCIO GROUP E TENGO QUELLO A RISCHIO MAGGIORE M4 (QUINDI VALORE MINORE...)
# RIMUOVO CLIENTI MULTICONTRATTO ------------------------------------------
clienti$meno1M_FASCIA_SDM_PDISC_M1 <- cast(clienti$meno1M_FASCIA_SDM_PDISC_M1, "integer")
clienti$meno1M_FASCIA_SDM_PDISC_M4 <- cast(clienti$meno1M_FASCIA_SDM_PDISC_M4, "integer")

clienti <- dropna(clienti, how = "any", cols = c("meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4"))
# clienti <- fillna(clienti, 99, cols = c("meno1M_FASCIA_SDM_PDISC_M1"))
# clienti <- fillna(clienti, 99, cols = c("meno1M_FASCIA_SDM_PDISC_M4"))
clienti <- orderBy(clienti, "COD_CLIENTE_CIFRATO", "meno1M_FASCIA_SDM_PDISC_M4")
createOrReplaceTempView(clienti, "clienti")
query <- "SELECT COD_CLIENTE_CIFRATO,
                 FIRST(INGR_PDISC_dt) AS INGR_PDISC_dt,
                 FIRST(DAT_PRIMA_ATTIVAZIONE_dt) AS DAT_PRIMA_ATTIVAZIONE_dt, 
                 FIRST(meno1M_FASCIA_SDM_PDISC_M1) AS meno1M_FASCIA_SDM_PDISC_M1,
                 FIRST(meno1M_FASCIA_SDM_PDISC_M4) AS meno1M_FASCIA_SDM_PDISC_M4
          FROM clienti 
          GROUP BY 1"
clienti <- sql(query)


# IMPORTO MAPPATURA SKYID COOKIE ------------------------------------------
id_clienti <- read.parquet(path_mappatura_cookie_skyid)
id_clienti <- select(id_clienti, c("COD_CLIENTE_CIFRATO", "post_visid_concatenated"))


# IMPORTO NAVIGAZIONE SITO ------------------------------------------------
cookie_sito <- read.parquet(path_navigazione_sito)
cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt >= data_modello - 80)
cookie_sito <- filter(cookie_sito, cookie_sito$date_time_dt <= data_modello)
#cookie_sito <- filter(cookie_sito, isNotNull(cookie_sito$canale_corporate_post_prop))
cookie_sito <- filter(cookie_sito, "post_channel = 'corporate' or post_channel = 'Guidatv'")
cache(cookie_sito)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_sito <- join_COD_CLIENTE_CIFRATO(cookie_sito, clienti)
cookie_sito <- withColumnRenamed(cookie_sito, "INGR_PDISC_dt", "TARGET_dt")

# ID UNIVOCI -------------------------------------------------------------
unique_id <- select(id_clienti, "COD_CLIENTE_CIFRATO")
unique_id <- distinct(unique_id)

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
cookie_sito <- remove_too_short(cookie_sito, 10)

# KPI SITO ----------------------------------------------------------------
cookie_sito <- drop(cookie_sito, "INGR_PDISC_dt")
cookie_sito <- withColumn(cookie_sito, "TARGET_dt", lit(data_modello))
df_kpi_sito <- start_kpi_sito(cookie_sito, unique_id)
cache(df_kpi_sito)

write.parquet(df_kpi_sito, path_kpi_sito, mode = "overwrite")


# IMPORTO COOKIE APP ------------------------------------------------------
cookie_app <- read.parquet(path_navigazione_app)
cookie_app <- join_COD_CLIENTE_CIFRATO(unique_id, cookie_app)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_app <- join_COD_CLIENTE_CIFRATO(cookie_app, clienti)

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
#cookie_app <-  withColumnRenamed(cookie_app, "date_time", "date_time_ts")
cookie_app <- remove_too_short(cookie_app, 4)

# KPI APP -----------------------------------------------------------------
cookie_app <- withColumn(cookie_app, "TARGET_dt", lit(data_modello))
cookie_app <- drop(cookie_app, "INGR_PDISC_dt")
df_kpi_app <- start_kpi_app(cookie_app, unique_id)
cache(df_kpi_app)

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

# RIPRENDIAMO LE COLONNE CHURN E ALTRO ------------------------------------
df_kpi <- join_COD_CLIENTE_CIFRATO(clienti, df_kpi)
df_kpi <- drop(df_kpi, "INGR_PDISC_dt")
cache(df_kpi)

write.parquet(df_kpi, path_kpi_finale, mode = "overwrite")

