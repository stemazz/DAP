
## Aggiornamento "settimanale" della tabella dei CLIENTI ATTIVI (utilizzando la tabella sulla dap della DMP)


source("connection_R.R")
options(scipen = 10000)


## Tabella di appoggio (NON necessario modificare il path)
path_delta <- "/user/stefano.mazzucca/cb_delta_attivi.parquet"

## Tabella di estrazione cb pre e post aggiornamento (di volta in volta MODIFICARE i path!!)
path_full <- "/user/stefano.mazzucca/tabella_attivi_20180828.parquet"
path_full_new <- "/user/stefano.mazzucca/tabella_attivi_2018xxxx.parquet"

## Date di aggiornamento (di volta in volta MODIFICARE le date!!)
data_ultimo_aggiornamento <- '2018-08-28'
data_oggi <- '2018-xx-xx'



# Aggiornamento dati dalla DMP
delta_view <- read.parquet("/STAGE/DMP/cb_week.parquet")
# View(head(delta_view,100))
# nrow(delta_view)
# 13.186.243

delta_view_1 <- withColumn(delta_view, "data_dt", cast(cast(unix_timestamp(delta_view$ID_DAY_WEEK, 'dd/MM/yyyy'), 'timestamp'), 'date'))
delta_view_2 <- withColumn(delta_view_1, "data_prima_attivazione_dt", cast(cast(unix_timestamp(delta_view_1$DAT_PRIMA_ATTIVAZIONE, 'yyyyMMdd'), 'timestamp'), 'date'))

delta_view_3 <- filter(delta_view_2, delta_view_2$data_dt >= data_ultimo_aggiornamento & delta_view_2$data_dt <= data_oggi) 
# View(head(delta_view_3,100))
# nrow(delta_view_3)
# # 645.819


# summ_delta_view <- summarize(delta_view, min = min(delta_view$data_dt), max = max(delta_view$data_dt))
# View(head(summ_delta_view,100))
# # min           max
# # 2017-10-04    2018-08-08

# trova_ultima_data_update <- select(delta_view_1, "COD_CONTRATTO_CRIP", "COD_CLIENTE_FRU_CRIP", "ID_DAY_WEEK", "data_dt")
# View(head(trova_ultima_data_update,100))
# trova_ultima_data_update_1 <- arrange(distinct(select(trova_ultima_data_update, "data_dt")), desc(trova_ultima_data_update$data_dt))
# View(head(trova_ultima_data_update_1,100))


# ORDINO PER SKY_ID -------------------------------------------------------
delta_view_4 <- orderBy(delta_view_3, "COD_CLIENTE_FRU_CRIP", "data_dt")

# SELEZIONO COLONNE UTILI -------------------------------------------------
dmp <- distinct(select(delta_view_4, c("COD_CLIENTE_FRU_CRIP",
                              "FLG_STATO_CNTR_ATTIVO",
                              "FLG_STATO_CNTR_CESSATO",
                              "FLG_PDISC",
                              "FLG_SUSPENDED",
                              "data_prima_attivazione_dt",
                              "data_dt")))


write.parquet(dmp, path_delta, mode = "overwrite")



# IMPORT FILE DELTA_DMP -------------------------------------------------------------
dmp <- read.parquet(path_delta)

# SELEZIONO UTENTI NON ATTIVI ---------------------------------------------
dmp_non_attivi <- filter(dmp, "FLG_STATO_CNTR_ATTIVO == 0 OR FLG_STATO_CNTR_CESSATO == 1 OR FLG_PDISC == 1 OR FLG_SUSPENDED == 1")
# View(head(dmp_non_attivi,100))
# nrow(dmp_non_attivi)
# # 175.843
# valdist_dmp_non_attivi <- distinct(select(dmp_non_attivi, "COD_CLIENTE_FRU_CRIP"))
# nrow(valdist_dmp_non_attivi)
# # 157.381
cache(dmp_non_attivi)

# SELEZIONO UTENTI ATTIVI ---------------------------------------------
dmp_attivi <- filter(dmp, "FLG_STATO_CNTR_ATTIVO == 1 AND FLG_STATO_CNTR_CESSATO == 0 AND FLG_PDISC == 0 AND FLG_SUSPENDED == 0")
# View(head(dmp_attivi,100))
# nrow(dmp_attivi)
# # 466.631
# valdist_dmp_attivi <- distinct(select(dmp_attivi, "COD_CLIENTE_FRU_CRIP"))
# nrow(valdist_dmp_attivi)
# # 428.390
cache(dmp_attivi)

# FILTRO ------------------------------------------------------------------
base_clienti <- read.parquet(path_full) 
# View(head(base_clienti,100))
# nrow(base_clienti)
# # 4.169.306

## anti-join per eliminare i disconnessi
createOrReplaceTempView(base_clienti, "base_clienti")
createOrReplaceTempView(dmp_non_attivi, "dmp_non_attivi")
base_clienti_1 <- sql("select distinct t1.*
                    from base_clienti t1
                    left join dmp_non_attivi t2
                      on (t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_FRU_CRIP and 
                        t1.DAT_PRIMA_ATTIVAZIONE_dt = t2.data_prima_attivazione_dt) where t2.COD_CLIENTE_FRU_CRIP is NULL")
cache(base_clienti_1)
# nrow(base_clienti_1)
# # 4.083.502

## join per aggiungere i nuovi attivi
# createOrReplaceTempView(base_clienti_1, "base_clienti_1")
# createOrReplaceTempView(dmp_attivi, "dmp_attivi")
# base_clienti_2 <- sql("select t1.*
#                     from base_clienti_1 t1
#                     full outer join dmp_attivi t2 
#                       on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_FRU_CRIP")
# cache(base_clienti_2)

base_clienti_2 <- distinct(select(base_clienti_1,"COD_CLIENTE_CIFRATO", "DAT_PRIMA_ATTIVAZIONE_dt"))
# nrow(base_clienti_2)
# # 4.083.502

dmp_attivi_2 <- distinct(select(dmp_attivi, "COD_CLIENTE_FRU_CRIP", "data_prima_attivazione_dt"))
# nrow(dmp_attivi_2)
# # 430.895
dmp_attivi_3 <- withColumnRenamed(dmp_attivi_2, "COD_CLIENTE_FRU_CRIP", "COD_CLIENTE_CIFRATO")
dmp_attivi_4 <- withColumnRenamed(dmp_attivi_2, "data_prima_attivazione_dt", "DAT_PRIMA_ATTIVAZIONE_dt")

base_finale <- rbind(base_clienti_2, dmp_attivi_4)
base_finale_1 <- arrange(base_finale, desc(base_finale$DAT_PRIMA_ATTIVAZIONE_dt))
base_finale_2 <- distinct(base_finale_1)
# View(head(base_finale_2,1000))
# nrow(base_finale_2)
# # 4.223.784

# SALVO -------------------------------------------------------------------
write.parquet(base_finale_2, path_full_new, mode = "overwrite")



verifica <- read.parquet(path_full_new)
View(head(verifica,100))
nrow(verifica)
# 4.223.784



#chiudi sessione
sparkR.stop()
