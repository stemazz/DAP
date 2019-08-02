## Setting NO PDISC per validazione modello
source("Churn_Digital/00_Utils.R")


# IMPORT CLIENTI + INFO BASE ----------------------------------------------
clienti_nopdisc_17 <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin.parquet")


# RINOMINO COLONNE --------------------------------------------------------
clienti_nopdisc_17 %>%
  withColumn("DAT_PRIMA_ATTIVAZIONE_dt", unix_timestamp(.$DAT_PRIMA_ATTIVAZIONE_dt, "dd/MM/yyyy") %>%
               cast("timestamp") %>%
               cast("date")) -> clienti_nopdisc_17

clienti_nopdisc_17 <- withColumnRenamed(clienti_nopdisc_17, "SKY_ID", "COD_CLIENTE_CIFRATO")
#clienti_nopdisc_17 <- withColumnRenamed(clienti_nopdisc_17, "DAT_PRIMA_ATTIV_CNTR", "DAT_PRIMA_ATTIVAZIONE_dt")
clienti_nopdisc_17 <- withColumnRenamed(clienti_nopdisc_17, "FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M1")
clienti_nopdisc_17 <- withColumnRenamed(clienti_nopdisc_17, "FASCIA_SDM_PDISC_M4", "meno1M_FASCIA_SDM_PDISC_M4")
clienti_nopdisc_17 <- withColumn(clienti_nopdisc_17, "INGR_PDISC_dt", lit(NA))

clienti_nopdisc_17 <- select(clienti_nopdisc_17, c("INGR_PDISC_dt",
                                                   "DAT_PRIMA_ATTIVAZIONE_dt",
                                                   "COD_CLIENTE_CIFRATO",
                                                   "meno1M_FASCIA_SDM_PDISC_M1",
                                                   "meno1M_FASCIA_SDM_PDISC_M4")
)

# RIMUOVO CLIENTI MULTICONTRATTO ------------------------------------------
createOrReplaceTempView(clienti_nopdisc_17, "clienti_nopdisc")
query <- "SELECT COD_CLIENTE_CIFRATO, COUNT(*) FROM clienti_nopdisc GROUP BY 1"
id_multicontratto_nopdisc_17 <- sql(query)
id_multicontratto_nopdisc_17$`count(1)` <- cast(id_multicontratto_nopdisc_17$`count(1)`,"integer")
clienti_nopdisc_17 <- filter_on_count(clienti_nopdisc_17, id_multicontratto_nopdisc_17, 1)
clienti_nopdisc_17 <- drop(clienti_nopdisc_17, "count(1)")

# IMPORTO MAPPATURA ID ----------------------------------------------------
id_clienti_nopdisc_17 <- read.parquet("/user/valentina/ModChurnOnLine_base_NO_Pdisc_cookies_adobe.parquet")
id_clienti_nopdisc_17 <- withColumnRenamed(id_clienti_nopdisc_17, "SKY_ID", "COD_CLIENTE_CIFRATO")
id_clienti_nopdisc_17 <- select(id_clienti_nopdisc_17, c("COD_CLIENTE_CIFRATO", "post_visid_concatenated"))


# IMPORTO COOKIE SITO -----------------------------------------------------

cookie_sito_nopdisc_17 <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_navigazSkyITDev_ok.parquet") ###### CAMBIO QUI! #####################################


cookie_sito_nopdisc_17 <- filter(cookie_sito_nopdisc_17,"date_time_dt >= '2017-03-01'")
cookie_sito_nopdisc_17 <- filter(cookie_sito_nopdisc_17,"date_time_dt <= '2017-09-30'")

#cookie_sito_nopdisc_17 <- filter(cookie_sito_nopdisc_17, isNotNull(cookie_sito_nopdisc_17$canale_corporate_post_prop)) 
cookie_sito_nopdisc_17 <- filter(cookie_sito_nopdisc_17, "post_channel = 'corporate' or post_channel = 'Guidatv'")

id_clienti_nopdisc_17 <- withColumnRenamed(id_clienti_nopdisc_17, "post_visid_concatenated", "post_visid_concatenated_y")
id_clienti_nopdisc_17 <- withColumnRenamed(id_clienti_nopdisc_17, "COD_CLIENTE_CIFRATO", "COD_CLIENTE_CIFRATO_y")
cookie_sito_nopdisc_17 <- join(id_clienti_nopdisc_17, cookie_sito_nopdisc_17, cookie_sito_nopdisc_17$post_visid_concatenated == id_clienti_nopdisc_17$post_visid_concatenated_y, "left_outer")
cookie_sito_nopdisc_17 <- drop(cookie_sito_nopdisc_17, "post_visid_concatenated_y")
cookie_sito_nopdisc_17 <- drop(cookie_sito_nopdisc_17, "COD_CLIENTE_CIFRATO_y")

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_sito_nopdisc_17 <- join_COD_CLIENTE_CIFRATO(cookie_sito_nopdisc_17, clienti_nopdisc_17)
cookie_sito_nopdisc_17 <- withColumnRenamed(cookie_sito_nopdisc_17, "INGR_nopdisc_dt", "TARGET_dt")

# ID UNIVOCI --------------------------------------------------------------
id_clienti_nopdisc_17 <- withColumnRenamed(id_clienti_nopdisc_17, "COD_CLIENTE_CIFRATO_y", "COD_CLIENTE_CIFRATO")
unique_id_nopdisc_17 <- select(id_clienti_nopdisc_17, "COD_CLIENTE_CIFRATO")
unique_id_nopdisc_17 <- distinct(unique_id_nopdisc_17)

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
cookie_sito_nopdisc_17 <- remove_too_short(cookie_sito_nopdisc_17, 10)

# KPI SITO ----------------------------------------------------------------
cookie_sito_nopdisc_17 <- withColumn(cookie_sito_nopdisc_17, "TARGET_dt", lit("2017-06-30"))
df_kpi_sito_nopdisc_17 <- start_kpi_sito(cookie_sito_nopdisc_17, unique_id_nopdisc_17)
#cache(df_kpi_sito_nopdisc_17)

write.parquet(df_kpi_sito_nopdisc_17, "/user/stefano.mazzucca/new_CJ_PDISC_df_kpi_sito_nopdisc_18.parquet")


# IMPORTO COOKIE APP ------------------------------------------------------
cookie_app_nopdisc_17 <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_navigazAPP.parquet") ## CAMBIO QUI!! ########################################
cookie_app_nopdisc_17 <- withColumnRenamed(cookie_app_nopdisc_17, "SKY_ID", "COD_CLIENTE_CIFRATO")
cookie_app_nopdisc_17 <- join_COD_CLIENTE_CIFRATO(unique_id_nopdisc_17, cookie_app_nopdisc_17)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_app_nopdisc_17 <- join_COD_CLIENTE_CIFRATO(cookie_app_nopdisc_17, clienti_nopdisc_17)

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
cookie_app_nopdisc_17 <-  withColumnRenamed(cookie_app_nopdisc_17, "date_time", "date_time_ts")
cookie_app_nopdisc_17 <- remove_too_short(cookie_app_nopdisc_17, 5)

# KPI APP -----------------------------------------------------------------
cookie_app_nopdisc_17 <- withColumn(cookie_app_nopdisc_17, "TARGET_dt", lit("2017-06-30"))
df_kpi_app_nopdisc_17 <- start_kpi_app(cookie_app_nopdisc_17, unique_id_nopdisc_17)
cache(df_kpi_app_nopdisc_17)


write.parquet(df_kpi_app_nopdisc_17, "/user/stefano.mazzucca/new_CJ_PDISC_df_kpi_app_nopdisc_18.parquet")


# MERGE KPI ---------------------------------------------------------------

df_kpi_sito_nopdisc_17 <- read.parquet("/user/stefano.mazzucca/new_CJ_PDISC_df_kpi_sito_nopdisc_18.parquet") ## CAMBIO DI PATH !!
df_kpi_app_nopdisc_17 <- read.parquet("/user/stefano.mazzucca/new_CJ_PDISC_df_kpi_app_nopdisc_18.parquet") ## CAMBIO DI PATH !!


df_kpi_nopdisc_17 <- join_COD_CLIENTE_CIFRATO(df_kpi_sito_nopdisc_17, df_kpi_app_nopdisc_17)
df_kpi_nopdisc_17 <- melt_kpi(df_kpi_nopdisc_17)

vars <- names(df_kpi_nopdisc_17)
vars <- vars[str_detect(vars, pattern = "_sito_1w")]
vars <- str_replace(vars, "_sito_1w", "")

df_kpi_nopdisc_17 <- differential_kpi(df_kpi_nopdisc_17, vars)
#df_kpi_t <- binner(df_kpi)



# RIPRENDIAMO LE COLONNE CHURN E ALTRO ------------------------------------

df_kpi_nopdisc_17 <- join_COD_CLIENTE_CIFRATO(clienti_nopdisc_17, df_kpi_nopdisc_17)
df_kpi_nopdisc_17 <- drop(df_kpi_nopdisc_17, "INGR_PDISC_dt")


write.parquet(df_kpi_nopdisc_17, "/user/stefano.mazzucca/new_CJ_PDISC_df_kpi_nopdisc_18.parquet")
