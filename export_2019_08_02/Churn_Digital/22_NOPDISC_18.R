# IMPORT CLIENTI + INFO BASE ----------------------------------------------
clienti_nopdisc_18 <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin.parquet")

# SELEZIONO COLONNE --------------------------------------------------------
clienti_nopdisc_18 <- withColumn(clienti_nopdisc_18, "INGR_PDISC_dt", lit(NA))

clienti_nopdisc_18 <- withColumnRenamed(clienti_nopdisc_18, "FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M1")
clienti_nopdisc_18 <- withColumnRenamed(clienti_nopdisc_18, "FASCIA_SDM_PDISC_M4", "meno1M_FASCIA_SDM_PDISC_M4")
clienti_nopdisc_18 <- select(clienti_nopdisc_18, c("INGR_PDISC_dt",
                                               "DAT_PRIMA_ATTIVAZIONE_dt",
                                               "COD_CLIENTE_CIFRATO",
                                               "meno1M_FASCIA_SDM_PDISC_M1",
                                               "meno1M_FASCIA_SDM_PDISC_M4")
)

# INVECE DI RIMUOVERE TUTTI FACCIO GROUP E TENGO QUELLO A RISCHIO MAGGIORE M4
# RIMUOVO CLIENTI MULTICONTRATTO ------------------------------------------
createOrReplaceTempView(clienti_nopdisc_18, "clienti_nopdisc")
query <- "SELECT COD_CLIENTE_CIFRATO, COUNT(*) FROM clienti_nopdisc GROUP BY 1"
id_multicontratto_nopdisc_18 <- sql(query)
id_multicontratto_nopdisc_18$`count(1)` <- cast(id_multicontratto_nopdisc_18$`count(1)`,"integer")
clienti_nopdisc_18 <- filter_on_count(clienti_nopdisc_18, id_multicontratto_nopdisc_18, 1)
clienti_nopdisc_18 <- drop(clienti_nopdisc_18, "count(1)")

# VA FATTA A MONTE!!!!!
# IMPORTO MAPPATURA ID ----------------------------------------------------
id_clienti_nopdisc_18 <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_CookiesADOBE.parquet")
id_clienti_nopdisc_18 <- select(id_clienti_nopdisc_18, c("COD_CLIENTE_CIFRATO", "post_visid_concatenated"))

# VA FATTA A MONTE!!!!!
# IMPORTO NAVIGAZIONE SITO -----------------------------------------------------
cookie_sito_nopdisc_18 <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_navigazSkyITDev_ok.parquet") ## CAMBIA QUI !! ###################
cookie_sito_nopdisc_18 <- filter(cookie_sito_nopdisc_18,"date_time_dt >= '2017-08-01'")
cookie_sito_nopdisc_18 <- filter(cookie_sito_nopdisc_18,"date_time_dt <= '2018-04-30'")

cookie_sito_nopdisc_18 <- filter(cookie_sito_nopdisc_18, "post_channel = 'corporate' or post_channel = 'Guidatv'")
cache(cookie_sito_nopdisc_18)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
#cookie_sito_nopdisc_18 <- drop(cookie_sito_nopdisc_18, c("INGR_nopdisc_dt", "DAT_PRIMA_ATTIVAZIONE_dt"))
cookie_sito_nopdisc_18 <- join_COD_CLIENTE_CIFRATO(cookie_sito_nopdisc_18, clienti_nopdisc_18)
cookie_sito_nopdisc_18 <- withColumnRenamed(cookie_sito_nopdisc_18, "INGR_nopdisc_dt", "TARGET_dt")

# ID UNIVOCI -------------------------------------------------------------
unique_id_nopdisc_18 <- select(id_clienti_nopdisc_18, "COD_CLIENTE_CIFRATO")
unique_id_nopdisc_18 <- distinct(unique_id_nopdisc_18)

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
cookie_sito_nopdisc_18 <- remove_too_short(cookie_sito_nopdisc_18, 10)

# KPI SITO ----------------------------------------------------------------
cookie_sito_nopdisc_18 <- drop(cookie_sito_nopdisc_18, "INGR_PDISC_dt")
cookie_sito_nopdisc_18 <- withColumn(cookie_sito_nopdisc_18, "TARGET_dt", lit("2018-04-30"))
df_kpi_sito_nopdisc_18 <- start_kpi_sito(cookie_sito_nopdisc_18, unique_id_nopdisc_18)
cache(df_kpi_sito_nopdisc_18)

write.parquet(df_kpi_sito_nopdisc_18, "/user/stefano.mazzucca/churn_digital/CD_df_kpi_sito_nopdisc_18.parquet", mode = "overwrite")


# IMPORTO COOKIE APP ------------------------------------------------------
cookie_app_nopdisc_18 <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_navigazAPP.parquet") ## CAMBIA QUI ############################
cookie_app_nopdisc_18 <- join_COD_CLIENTE_CIFRATO(unique_id_nopdisc_18, cookie_app_nopdisc_18)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_app_nopdisc_18 <- join_COD_CLIENTE_CIFRATO(cookie_app_nopdisc_18, clienti_nopdisc_18)

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
cookie_app_nopdisc_18 <-  withColumnRenamed(cookie_app_nopdisc_18, "date_time", "date_time_ts")
cookie_app_nopdisc_18 <- remove_too_short(cookie_app_nopdisc_18, 5)

# KPI APP -----------------------------------------------------------------
cookie_app_nopdisc_18 <- withColumn(cookie_app_nopdisc_18, "TARGET_dt", lit("2018-04-30"))
df_kpi_app_nopdisc_18 <- start_kpi_app(cookie_app_nopdisc_18, unique_id_nopdisc_18)
cache(df_kpi_app_nopdisc_18)

write.parquet(df_kpi_app_nopdisc_18, "/user/stefano.mazzucca/churn_digital/CD_df_kpi_app_nopdisc_18.parquet", mode = "overwrite")


# MERGE KPI ---------------------------------------------------------------
df_kpi_sito_nopdisc_18 <- read.parquet("/user/stefano.mazzucca/churn_digital/CD_df_kpi_sito_nopdisc_18.parquet")
df_kpi_app_nopdisc_18 <- read.parquet("/user/stefano.mazzucca/churn_digital/CD_df_kpi_app_nopdisc_18.parquet")

df_kpi_nopdisc_18 <- join_COD_CLIENTE_CIFRATO(df_kpi_sito_nopdisc_18, df_kpi_app_nopdisc_18)
df_kpi_nopdisc_18 <- melt_kpi(df_kpi_nopdisc_18)

vars <- names(df_kpi_nopdisc_18)
vars <- vars[str_detect(vars, pattern = "_sito_1w")]
vars <- str_replace(vars, "_sito_1w", "")

df_kpi_nopdisc_18 <- differential_kpi(df_kpi_nopdisc_18, vars)
#df_kpi_t <- binner(df_kpi)

# RIPRENDIAMO LE COLONNE CHURN E ALTRO ------------------------------------

df_kpi_nopdisc_18 <- join_COD_CLIENTE_CIFRATO(clienti_nopdisc_18, df_kpi_nopdisc_18)
df_kpi_nopdisc_18$INGR_PDISC_dt <- NULL
cache(df_kpi_nopdisc_18)

write.parquet(df_kpi_nopdisc_18, "/user/stefano.mazzucca/churn_digital/CD_df_kpi_nopdisc_18.parquet", mode = "overwrite")

#df_kpi_nopdisc_18 <- read.parquet("/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_nopdisc_18.parquet")
