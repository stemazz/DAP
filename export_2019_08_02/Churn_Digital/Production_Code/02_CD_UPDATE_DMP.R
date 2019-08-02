
# IMPORT FILE -------------------------------------------------------------
DMP_1 <- read.parquet(path_delta)

# PARSO SKY_ID ------------------------------------------------------------
DMP_1 %>%
  withColumn("EDIT_DATE_DT", unix_timestamp(.$ID_DAY_WEEK, "dd/MM/yyyy") %>%
               cast("timestamp") %>%
               cast("date")) -> dmp
dmp %>%
  withColumn("DATA_PRIMA_ATTIVAZIONE_DT", unix_timestamp(.$DAT_PRIMA_ATTIVAZIONE, "yyyyMMdd") %>%
               cast("timestamp") %>%
               cast("date")) -> dmp

# ORDINO PER SKY_ID -------------------------------------------------------
dmp <- orderBy(dmp, "COD_CONTRATTO_CRIP", "EDIT_DATE_DT")


# SELEZIONO COLONNE UTILI -------------------------------------------------
dmp <- select(dmp, c("COD_CLIENTE_FRU_CRIP",
                     "FLG_STATO_CNTR_ATTIVO",
                     "FLG_STATO_CNTR_CESSATO",
                     "FLG_PDISC",
                     "FLG_SUSPENDED",
                     "DATA_PRIMA_ATTIVAZIONE_DT",
                     "EDIT_DATE_DT"
                     )
              )

# SELEZIONO UTENTI NON ATTIVI ---------------------------------------------
dmp_non_attivi <- filter(dmp, "FLG_STATO_CNTR_ATTIVO == 0 OR FLG_STATO_CNTR_CESSATO == 1 OR FLG_PDISC == 1 OR FLG_SUSPENDED == 1")
dmp_non_attivi <- select(dmp_non_attivi, "COD_CLIENTE_FRU_CRIP")
dmp_non_attivi <- unique(dmp_non_attivi)
cache(dmp_non_attivi)

# FILTRO ------------------------------------------------------------------
base_clienti <- read.parquet(path_full) 
createOrReplaceTempView(base_clienti, "base_clienti")
createOrReplaceTempView(dmp_non_attivi, "dmp_non_attivi")
base_clienti <- sql("select t1.*
                    from base_clienti t1
                    left join dmp_non_attivi t2
                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_FRU_CRIP where t2.COD_CLIENTE_FRU_CRIP is NULL")
cache(base_clienti)

# SALVO -------------------------------------------------------------------
write.parquet(base_clienti, path_full_new, mode = "overwrite")

