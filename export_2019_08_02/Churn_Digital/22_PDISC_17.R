# IMPORT CLIENTI + INFO BASE ----------------------------------------------
clienti_pdisc_17 <- read.parquet("/user/valentina/ModChurnOnLine_base_Pdisc.parquet")

# RINOMINO COLONNE --------------------------------------------------------
clienti_pdisc_17 %>%
  withColumn("DAT_PRIMA_ATTIV_CNTR", unix_timestamp(.$DAT_PRIMA_ATTIV_CNTR, "dd/MM/yyyy") %>%
               cast("timestamp") %>%
               cast("date")) -> clienti_pdisc_17

clienti_pdisc_17 <- withColumnRenamed(clienti_pdisc_17, "SKY_ID", "COD_CLIENTE_CIFRATO")
clienti_pdisc_17 <- withColumnRenamed(clienti_pdisc_17, "DATA_INGRESSO_PDISC", "INGR_PDISC_dt")
clienti_pdisc_17 <- withColumnRenamed(clienti_pdisc_17, "DAT_PRIMA_ATTIV_CNTR", "DAT_PRIMA_ATTIVAZIONE_dt")
clienti_pdisc_17 <- withColumnRenamed(clienti_pdisc_17, "FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M1")
clienti_pdisc_17 <- withColumnRenamed(clienti_pdisc_17, "FASCIA_SDM_PDISC_M4", "meno1M_FASCIA_SDM_PDISC_M4")


clienti_pdisc_17 <- select(clienti_pdisc_17, c("INGR_PDISC_dt",
                                               "DAT_PRIMA_ATTIVAZIONE_dt",
                                               "COD_CLIENTE_CIFRATO",
                                               "meno1M_FASCIA_SDM_PDISC_M1",
                                               "meno1M_FASCIA_SDM_PDISC_M4")
)


# RIMUOVO CLIENTI MULTICONTRATTO ------------------------------------------
createOrReplaceTempView(clienti_pdisc_17, "clienti_pdisc")
query <- "SELECT COD_CLIENTE_CIFRATO, COUNT(*) FROM clienti_pdisc GROUP BY 1"
id_multicontratto_pdisc_17 <- sql(query)
id_multicontratto_pdisc_17$`count(1)` <- cast(id_multicontratto_pdisc_17$`count(1)`,"integer")
clienti_pdisc_17 <- filter_on_count(clienti_pdisc_17, id_multicontratto_pdisc_17, 1)
clienti_pdisc_17 <- drop(clienti_pdisc_17, "count(1)")

# IMPORTO MAPPATURA ID ----------------------------------------------------
id_clienti_pdisc_17 <- read.parquet("/user/valentina/ModChurnOnLine_base_Pdisc_cookies_adobe.parquet")
id_clienti_pdisc_17 <- withColumnRenamed(id_clienti_pdisc_17, "SKY_ID", "COD_CLIENTE_CIFRATO")
id_clienti_pdisc_17 <- select(id_clienti_pdisc_17, c("COD_CLIENTE_CIFRATO", "post_visid_concatenated"))


# IMPORTO COOKIE SITO -----------------------------------------------------
#cookie_sito_pdisc_17 <- read.parquet("/user/riccardo.motta/churn_digital/skyitdev.parquet") #### CAMBIO QUI !!! #####################################
cookie_sito_pdisc_17 <- read.parquet("/user/valentina/churnOnline_rifaiMod_scarico_navigaz_SiPdisc_trainingModello.parquet") 

cookie_sito_pdisc_17 <- filter(cookie_sito_pdisc_17,"date_time_dt >= '2017-03-01'")
cookie_sito_pdisc_17 <- filter(cookie_sito_pdisc_17,"date_time_dt <= '2017-09-30'")

#cookie_sito_pdisc_17 <- filter(cookie_sito_pdisc_17, isNotNull(cookie_sito_pdisc_17$canale_corporate_post_prop))
cookie_sito_pdisc_17 <- filter(cookie_sito_pdisc_17, "post_channel = 'corporate' or post_channel = 'Guidatv'")

id_clienti_pdisc_17 <- withColumnRenamed(id_clienti_pdisc_17, "post_visid_concatenated", "post_visid_concatenated_y")
cookie_sito_pdisc_17 <- join(id_clienti_pdisc_17, cookie_sito_pdisc_17, id_clienti_pdisc_17$post_visid_concatenated_y == cookie_sito_pdisc_17$post_visid_concatenated, "left_outer")
cookie_sito_pdisc_17 <- drop(cookie_sito_pdisc_17, "post_visid_concatenated_y")


# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_sito_pdisc_17 <- join_COD_CLIENTE_CIFRATO(cookie_sito_pdisc_17, clienti_pdisc_17)
cookie_sito_pdisc_17 <- withColumnRenamed(cookie_sito_pdisc_17, "INGR_PDISC_dt", "TARGET_dt")

# ID UNIVOCI --------------------------------------------------------------
unique_id_pdisc_17 <- select(id_clienti_pdisc_17, "COD_CLIENTE_CIFRATO")
unique_id_pdisc_17 <- distinct(unique_id_pdisc_17)

cache(cookie_sito_pdisc_17)

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
cookie_sito_pdisc_17 <- remove_too_short(cookie_sito_pdisc_17, 10)

# KPI SITO ----------------------------------------------------------------
df_kpi_sito_pdisc_17 <- start_kpi_sito(cookie_sito_pdisc_17, unique_id_pdisc_17)
cache(df_kpi_sito_pdisc_17)

#write.parquet(df_kpi_sito_pdisc_17, "/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_sito_pdisc_172.parquet") #### SALVO IN ALTRO PATH !!!!
#write.parquet(df_kpi_sito_pdisc_17, "/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_sito_pdisc_172_bis.parquet")
write.parquet(df_kpi_sito_pdisc_17, "/user/stefano.mazzucca/churn_digital/df_kpi_sito_pdisc_17.parquet", mode = "overwrite")


# IMPORTO COOKIE APP ------------------------------------------------------
cookie_app_pdisc_17 <- read.parquet("/user/riccardo.motta/churn_digital/skyappwsc_pdisc.parquet")
cookie_app_pdisc_17 <- withColumnRenamed(cookie_app_pdisc_17, "SKY_ID", "COD_CLIENTE_CIFRATO")
cookie_app_pdisc_17 <- join_COD_CLIENTE_CIFRATO(unique_id_pdisc_17, cookie_app_pdisc_17)

# INSERISCO VARIABILI DI PARTENZA -----------------------------------------
cookie_app_pdisc_17 <- join_COD_CLIENTE_CIFRATO(cookie_app_pdisc_17, clienti_pdisc_17)
cookie_app_pdisc_17 <- withColumnRenamed(cookie_app_pdisc_17, "INGR_PDISC_dt", "TARGET_dt")

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
#cookie_app_pdisc_17 <-  withColumnRenamed(cookie_app_pdisc_17, "date_time", "date_time_ts")
cookie_app_pdisc_17 <- remove_too_short(cookie_app_pdisc_17, 5)

# KPI APP -----------------------------------------------------------------
df_kpi_app_pdisc_17 <- start_kpi_app(cookie_app_pdisc_17, unique_id_pdisc_17)
cache(df_kpi_app_pdisc_17)

#write.parquet(df_kpi_app_pdisc_17, "/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_app_pdisc_172.parquet")
#write.parquet(df_kpi_app_pdisc_17, "/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_app_pdisc_172_bis.parquet")
write.parquet(df_kpi_app_pdisc_17, "/user/stefano.mazzucca/churn_digital/df_kpi_app_pdisc_17.parquet", mode = "overwrite")

# MERGE KPI ---------------------------------------------------------------

#df_kpi_sito_pdisc_17 <- read.parquet("/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_sito_pdisc_172_bis.parquet")
df_kpi_sito_pdisc_17 <- read.parquet("/user/stefano.mazzucca/churn_digital/df_kpi_sito_pdisc_17.parquet")
#df_kpi_app_pdisc_17 <- read.parquet("/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_app_pdisc_172_bis.parquet")
df_kpi_app_pdisc_17 <- read.parquet("/user/stefano.mazzucca/churn_digital/df_kpi_app_pdisc_17.parquet")


df_kpi_pdisc_17 <- join_COD_CLIENTE_CIFRATO(df_kpi_sito_pdisc_17, df_kpi_app_pdisc_17)
df_kpi_pdisc_17 <- melt_kpi(df_kpi_pdisc_17)

vars <- names(df_kpi_pdisc_17)
vars <- vars[str_detect(vars, pattern = "_sito_1w")]
vars <- str_replace(vars, "_sito_1w", "")

df_kpi_pdisc_17 <- differential_kpi(df_kpi_pdisc_17, vars)
#df_kpi_t <- binner(df_kpi)


# RIPRENDIAMO LE COLONNE CHURN E ALTRO ------------------------------------

df_kpi_pdisc_17 <- join_COD_CLIENTE_CIFRATO(clienti_pdisc_17, df_kpi_pdisc_17)

#write.parquet(df_kpi_pdisc_17, "/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_pdisc_172.parquet") ## CAMBIO PER NON SOVRASCRIVERE !!!!
#write.parquet(df_kpi_pdisc_17, "/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_pdisc_172_bis.parquet")
write.parquet(df_kpi_pdisc_17, "/user/stefano.mazzucca/churn_digital/df_kpi_pdisc_17.parquet", mode = "overwrite")

