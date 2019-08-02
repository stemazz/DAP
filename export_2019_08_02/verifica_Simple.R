
## Verifiche su DATI Simple

source("connection_R.R")
options(scipen = 10000)


## CB_WEEK

cb_update_week <- read.parquet("/STAGE/DMP/cb_week.parquet")
printSchema(cb_update_week)
View(head(cb_update_week,100))

cb_update_week <- withColumn(cb_update_week, "DAT_PRIMA_ATTIVAZIONE_dt", cast(cast(unix_timestamp(cb_update_week$DAT_PRIMA_ATTIVAZIONE, "yyyyMMdd"), "timestamp"), "date"))
cb_update_week <- withColumn(cb_update_week, "ID_DAY_WEEK_dt", cast(cast(unix_timestamp(cb_update_week$ID_DAY_WEEK, "dd/MM/yyyy"), "timestamp"), "date"))

ver_cb <- summarize(groupBy(cb_update_week, "ID_DAY_WEEK"), count = count(cb_update_week$COD_CLIENTE_FRU_CRIP))
View(head(ver_cb,200))

ver_cb_2 <- summarize(groupBy(cb_update_week, "COD_CLIENTE_FRU_CRIP"), count_week = count(cb_update_week$ID_DAY_WEEK_dt), 
                      DAT_PRIMA_ATTIVAZIONE_dt = min(cb_update_week$DAT_PRIMA_ATTIVAZIONE_dt),
                      first_week = min(cb_update_week$ID_DAY_WEEK_dt),
                      last_week = max(cb_update_week$ID_DAY_WEEK_dt),
                      flg_active = last(cb_update_week$FLG_STATO_CNTR_ATTIVO), 
                      fgl_last_cessato = last(cb_update_week$FLG_STATO_CNTR_CESSATO),
                      flg_max_cessato = max(cb_update_week$FLG_STATO_CNTR_CESSATO))
View(head(ver_cb_2,1000))
# es: 
# COD_CLIENTE_FRU_CRIP                            count_week    DAT_PRIMA_ATTIVAZIONE_dt    first_week    last_week   flg_active    fgl_last_cessato    flg_max_cessato
# 000d4e5158beb4f03872ed57b86565777a011f56e6550916    7             2009-10-19              2017-10-04    2018-06-03    1                   0                   1
#### NON MI POSSO FIDARE DEL LAST!! perche' (last != max)
nrow(ver_cb_2)
# 5.141.630


ver_cb_3 <- summarize(groupBy(cb_update_week, "COD_CLIENTE_FRU_CRIP"), count_week = count(cb_update_week$ID_DAY_WEEK_dt), 
                      DAT_PRIMA_ATTIVAZIONE_dt = min(cb_update_week$DAT_PRIMA_ATTIVAZIONE_dt),
                      first_week = min(cb_update_week$ID_DAY_WEEK_dt),
                      last_week = max(cb_update_week$ID_DAY_WEEK_dt),
                      flg_last_active = last(cb_update_week$FLG_STATO_CNTR_ATTIVO), 
                      flg_min_active = min(cb_update_week$FLG_STATO_CNTR_ATTIVO),
                      fgl_last_cessato = last(cb_update_week$FLG_STATO_CNTR_CESSATO),
                      flg_max_cessato = max(cb_update_week$FLG_STATO_CNTR_CESSATO),
                      flg_max_pdisc = max(cb_update_week$FLG_PDISC),
                      count_flg_pdisc = sum(cb_update_week$FLG_PDISC))
View(head(ver_cb_3,1000))


cb_attiva <- filter(ver_cb_3, ver_cb_3$flg_min_active == 1)
View(head(cb_attiva,100))
nrow(cb_attiva)
# 4.329.218


# verifico se esattamente l'ultima varizaione di stato_business ha flg_attivo = 1
ver_ver_cb_3 <- summarize(groupBy(cb_update_week, "COD_CONTRATTO_CRIP"), max_week = max(cb_update_week$ID_DAY_WEEK_dt))
nrow(ver_ver_cb_3)
# 5.141.630
createOrReplaceTempView(ver_ver_cb_3, "ver_ver_cb_3")
createOrReplaceTempView(cb_update_week, "cb_update_week")
join_ver <- sql("select distinct t2.*, t1.max_week
                from ver_ver_cb_3 t1
                inner join cb_update_week t2
                on t1.COD_CONTRATTO_CRIP = t2.COD_CONTRATTO_CRIP and 
                    t1.max_week = t2.ID_DAY_WEEK_dt")
View(head(join_ver,100))
nrow(join_ver)
# 5.175.832 su chiave cliente
# 5.392.925 sul contratto

cb_attiva_ver <- filter(join_ver, join_ver$FLG_STATO_CNTR_ATTIVO == 1)
View(head(cb_attiva_ver,100))
nrow(cb_attiva_ver)
# 4.514.123 su chiave cliente
# 4.658.182 sul contratto



## INTERESSI 

at5 <- read.parquet("/STAGE/DMP/simple/rawdata_interest_attributes_5.parquet")
# at5 <- read.parquet("/STAGE/DMP/simple/status/table=rawdata_interest_attributes_5") # NO!
View(head(at5,100))

ver_at5 <- filter(at5, "data_condivisione LIKE '2019%'")
View(head(ver_at5,100))


at2 <- read.parquet("/STAGE/DMP/simple/rawdata_interest_attributes_2.parquet")
View(head(at2,100))




# e' solo una tabella di date di aggiornamento (NON serve)
prova <- read.parquet("/STAGE/DMP/status/preprocess.parquet/table=cb_week")
View(head(prova,100))


# non piu' aggiornato
trigger_disdetta <- read.parquet("/STAGE/DMP/simple/esposti_disdetta.parquet")
View(head(trigger_disdetta,200))






#chiudi sessione
sparkR.stop()
