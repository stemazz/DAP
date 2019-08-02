# IMPORT CLIENTI + INFO BASE ----------------------------------------------
clienti_pdisc_18 <- read.parquet("/user/valentina/churnOnline_scaricoBI_xtest_Pdisc.parquet")

# SELEZIONO COLONNE --------------------------------------------------------
clienti_pdisc_18 <- select(clienti_pdisc_18, c("INGR_PDISC_dt",
                                               "DAT_PRIMA_ATTIVAZIONE_dt",
                                               "COD_CLIENTE_CIFRATO",
                                               "meno1M_FASCIA_SDM_PDISC_M1",
                                               "meno1M_FASCIA_SDM_PDISC_M4")
                           )

# RIMUOVO CLIENTI MULTICONTRATTO ------------------------------------------
createOrReplaceTempView(clienti_pdisc_18, "clienti_pdisc")
query <- "SELECT COD_CLIENTE_CIFRATO, COUNT(*) FROM clienti_pdisc GROUP BY 1"
id_multicontratto_pdisc_18 <- sql(query)
id_multicontratto_pdisc_18$`count(1)` <- cast(id_multicontratto_pdisc_18$`count(1)`,"integer")
clienti_pdisc_18 <- filter_on_count(clienti_pdisc_18, id_multicontratto_pdisc_18, 1)
clienti_pdisc_18 <- drop(clienti_pdisc_18, "count(1)")

# IMPORTO MAPPATURA ID ----------------------------------------------------
id_clienti_pdisc_18 <- read.parquet("/user/valentina/churnOnline_scaricoBI_xtest_CookiesADOBE.parquet")
id_clienti_pdisc_18 <- select(id_clienti_pdisc_18, c("COD_CLIENTE_CIFRATO", "post_visid_concatenated"))

# IMPORTO COOKIE SITO -----------------------------------------------------
cookie_sito_pdisc_18 <- read.parquet("/user/valentina/churnOnline_scaricoBI_xtest_navigazSkyITDev_OK.parquet") ## CAMBIA QUI! ######################################

cookie_sito_pdisc_18 <- filter(cookie_sito_pdisc_18,"date_time_dt >= '2017-08-01'")
cookie_sito_pdisc_18 <- filter(cookie_sito_pdisc_18,"date_time_dt <= '2018-04-30'")
cookie_sito_pdisc_18 <- filter(cookie_sito_pdisc_18, "post_channel = 'corporate' or post_channel = 'Guidatv'")

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_sito_pdisc_18 <- drop(cookie_sito_pdisc_18, c("INGR_PDISC_dt", "DAT_PRIMA_ATTIVAZIONE_dt"))
cookie_sito_pdisc_18 <- join_COD_CLIENTE_CIFRATO(cookie_sito_pdisc_18, clienti_pdisc_18)
cookie_sito_pdisc_18 <- withColumnRenamed(cookie_sito_pdisc_18, "INGR_PDISC_dt", "TARGET_dt")

# ID UNIVOCI --------------------------------------------------------------
unique_id_pdisc_18 <- select(id_clienti_pdisc_18, "COD_CLIENTE_CIFRATO")
unique_id_pdisc_18 <- distinct(unique_id_pdisc_18)

cache(cookie_sito_pdisc_18)

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
cookie_sito_pdisc_18 <- remove_too_short(cookie_sito_pdisc_18, 10)

# KPI SITO ----------------------------------------------------------------
df_kpi_sito_pdisc_18 <- start_kpi_sito(cookie_sito_pdisc_18, unique_id_pdisc_18)
cache(df_kpi_sito_pdisc_18)

write.parquet(df_kpi_sito_pdisc_18, "/user/stefano.mazzucca/churn_digital/CD_df_kpi_sito_pdisc_18.parquet")

# IMPORTO COOKIE APP ------------------------------------------------------
cookie_app_pdisc_18 <- read.parquet("/user/valentina/churnOnline_scaricoBI_xtest_navigazAPP.parquet") ## CAMBIA QUI ##############################################
cookie_app_pdisc_18 <- join_COD_CLIENTE_CIFRATO(unique_id_pdisc_18, cookie_app_pdisc_18)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_app_pdisc_18 <- drop(cookie_app_pdisc_18, c("INGR_PDISC_dt", "DAT_PRIMA_ATTIVAZIONE_dt"))
cookie_app_pdisc_18 <- join_COD_CLIENTE_CIFRATO(cookie_app_pdisc_18, clienti_pdisc_18)
cookie_app_pdisc_18 <- withColumnRenamed(cookie_app_pdisc_18, "INGR_PDISC_dt", "TARGET_dt")

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
cookie_app_pdisc_18 <-  withColumnRenamed(cookie_app_pdisc_18, "date_time", "date_time_ts")
cookie_app_pdisc_18 <- remove_too_short(cookie_app_pdisc_18, 5)

# KPI APP -----------------------------------------------------------------
df_kpi_app_pdisc_18 <- start_kpi_app(cookie_app_pdisc_18, unique_id_pdisc_18)
cache(df_kpi_app_pdisc_18)

write.parquet(df_kpi_app_pdisc_18, "/user/stefano.mazzucca/churn_digital/CD_df_kpi_app_pdisc_18.parquet")


# MERGE KPI ---------------------------------------------------------------

df_kpi_pdisc_18 <- join_COD_CLIENTE_CIFRATO(df_kpi_sito_pdisc_18, df_kpi_app_pdisc_18)
df_kpi_pdisc_18 <- melt_kpi(df_kpi_pdisc_18)

vars <- names(df_kpi_pdisc_18)
vars <- vars[str_detect(vars, pattern = "_sito_1w")]
vars <- str_replace(vars, "_sito_1w", "")

df_kpi_pdisc_18 <- differential_kpi(df_kpi_pdisc_18, vars)
#df_kpi_t <- binner(df_kpi)

# RIPRENDIAMO LE COLONNE CHURN E ALTRO ------------------------------------

df_kpi_pdisc_18 <- join_COD_CLIENTE_CIFRATO(clienti_pdisc_18, df_kpi_pdisc_18)

write.parquet(df_kpi_pdisc_18, "/user/stefano.mazzucca/churn_digital/CD_df_kpi_pdisc_18.parquet")
