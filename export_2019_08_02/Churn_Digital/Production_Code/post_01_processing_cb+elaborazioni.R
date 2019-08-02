
## Base CB attivi per mese
## con valori dello score SDM


source("connection_R.R")
options(scipen = 10000)


# path in lettura
path_cb_aprile <- "/user/stefano.mazzucca/churn_digital/20181017_1_apr_2018.csv"
path_cb_maggio <- "/user/stefano.mazzucca/churn_digital/20181017_1_mag_2018.csv"
path_cb_giugno <- "/user/stefano.mazzucca/churn_digital/20181018_1_giu_2018.txt"
path_cb_luglio <- "/user/stefano.mazzucca/churn_digital/20181018_1_lug_2018.txt"
path_cb_agosto <- "/user/stefano.mazzucca/churn_digital/20181018_1_ago_2018.txt"
path_cb_settembre <- "/user/stefano.mazzucca/churn_digital/20181018_1_sett_2018.txt"

path_cdm_aprile <- "/user/stefano.mazzucca/churn_digital/2018_04_01/output_modello_CD_20180401.csv"
path_cdm_maggio <- "/user/stefano.mazzucca/churn_digital/2018_05_01/output_modello_CD_20180501.csv"
path_cdm_giugno <- "/user/stefano.mazzucca/churn_digital/2018_06_01/output_modello_CD_20180601.csv"
path_cdm_luglio <- "/user/stefano.mazzucca/churn_digital/2018_07_01/output_modello_CD_20180701.csv"
path_cdm_agosto <- "/user/stefano.mazzucca/churn_digital/2018_08_01/output_modello_CD_20180801.csv"
path_cdm_settembre <- "/user/stefano.mazzucca/churn_digital/2018_09_01/output_modello_CD_20180901.csv"

# path in scrttura
# path_cb_aprile_wr <- "/user/stefano.mazzucca/churn_digital/cb_attiva_aprile_2018.parquet"
path_cb_maggio_wr <- "/user/stefano.mazzucca/churn_digital/cb_attiva_maggio_2018.parquet"
path_cb_giugno_wr <- "/user/stefano.mazzucca/churn_digital/cb_attiva_giugno_2018.parquet"
path_cb_luglio_wr <- "/user/stefano.mazzucca/churn_digital/cb_attiva_luglio_2018.parquet"
path_cb_agosto_wr <- "/user/stefano.mazzucca/churn_digital/cb_attiva_agosto_2018.parquet"
# path_cb_settembre_wr <- "/user/stefano.mazzucca/churn_digital/cb_attiva_settembre_2018.parquet"

path_lista_maggio <- "/user/stefano.mazzucca/churn_digital/lista_CDMvsSDM_maggio_2018.parquet"
path_lista_giugno <- "/user/stefano.mazzucca/churn_digital/lista_CDMvsSDM_giugno_2018.parquet"
path_lista_luglio <- "/user/stefano.mazzucca/churn_digital/lista_CDMvsSDM_luglio_2018.parquet"
path_lista_agosto <- "/user/stefano.mazzucca/churn_digital/lista_CDMvsSDM_agosto_2018.parquet"





cb_attiv_2018_04 <- read.df(path_cb_aprile, source = "csv", header = "true", delimiter = "|")
View(head(cb_attiv_2018_04,100))
nrow(cb_attiv_2018_04)
# 4.325.992
cb_attiva_04 <- filter(cb_attiv_2018_04, "DES_STATO_CNTR like '%Active%'")
cb_attiva_04 <- filter(cb_attiva_04, cb_attiva_04$DES_CLS_UTENZA_SALES == 'RESIDENTIAL')
cb_attiva_04 <- withColumn(cb_attiva_04, "data_ingr_pdisc_dt", cast(cast(unix_timestamp(cb_attiva_04$DAT_VAL_RICH_CSZ_CNTR, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_04 <- withColumn(cb_attiva_04, "data_ingr_pdisc_dt", ifelse(cb_attiva_04$data_ingr_pdisc_dt == '9999-12-31', NULL, cb_attiva_04$data_ingr_pdisc_dt))
cb_attiva_04 <- withColumn(cb_attiva_04, "data_prima_attiv_dt", cast(cast(unix_timestamp(cb_attiva_04$DAT_PRIMA_ATTIVAZIONE, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_04 <- withColumn(cb_attiva_04, "mese_rif", lit('Aprile'))
cb_attiva_04 <- distinct(select(cb_attiva_04, "COD_CLIENTE_CIFRATO", "DES_STATO_CNTR", "DES_CLS_UTENZA_SALES", "data_prima_attiv_dt", "data_ingr_pdisc_dt", "FASCIA_SDM_PDISC_M1", "FASCIA_SDM_PDISC_M4", "mese_rif"))
View(head(cb_attiva_04,100))
nrow(cb_attiva_04)
# 4.287.452


cb_attiv_2018_05 <- read.df(path_cb_maggio, source = "csv", header = "true", delimiter = "|")
View(head(cb_attiv_2018_05,100))
nrow(cb_attiv_2018_05)
# 4.312.443
cb_attiva_05 <- filter(cb_attiv_2018_05, "DES_STATO_CNTR like '%Active%'")
cb_attiva_05 <- filter(cb_attiva_05, cb_attiva_05$DES_CLS_UTENZA_SALES == 'RESIDENTIAL')
cb_attiva_05 <- withColumn(cb_attiva_05, "data_ingr_pdisc_dt", cast(cast(unix_timestamp(cb_attiva_05$DAT_VAL_RICH_CSZ_CNTR, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_05 <- withColumn(cb_attiva_05, "data_ingr_pdisc_dt", ifelse(cb_attiva_05$data_ingr_pdisc_dt == '9999-12-31', NULL, cb_attiva_05$data_ingr_pdisc_dt))
cb_attiva_05 <- withColumn(cb_attiva_05, "data_prima_attiv_dt", cast(cast(unix_timestamp(cb_attiva_05$DAT_PRIMA_ATTIVAZIONE, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_05 <- withColumn(cb_attiva_05, "mese_rif", lit('Maggio'))
cb_attiva_05 <- distinct(select(cb_attiva_05, "COD_CLIENTE_CIFRATO", "DES_STATO_CNTR", "DES_CLS_UTENZA_SALES", "data_prima_attiv_dt", "data_ingr_pdisc_dt", "FASCIA_SDM_PDISC_M1", "FASCIA_SDM_PDISC_M4", "mese_rif"))
View(head(cb_attiva_05,100))
nrow(cb_attiva_05)
# 4.273.703


cb_attiv_2018_06 <- read.df(path_cb_giugno, source = "csv", header = "true", delimiter = "|")
View(head(cb_attiv_2018_06,100))
nrow(cb_attiv_2018_06)
# 4.305.051
cb_attiva_06 <- filter(cb_attiv_2018_06, "DES_STATO_CNTR like '%Active%'")
cb_attiva_06 <- filter(cb_attiva_06, cb_attiva_06$DES_CLS_UTENZA_SALES == 'RESIDENTIAL')
cb_attiva_06 <- withColumn(cb_attiva_06, "data_ingr_pdisc_dt", cast(cast(unix_timestamp(cb_attiva_06$DAT_VAL_RICH_CSZ_CNTR, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_06 <- withColumn(cb_attiva_06, "data_ingr_pdisc_dt", ifelse(cb_attiva_06$data_ingr_pdisc_dt == '9999-12-31', NULL, cb_attiva_06$data_ingr_pdisc_dt))
cb_attiva_06 <- withColumn(cb_attiva_06, "data_prima_attiv_dt", cast(cast(unix_timestamp(cb_attiva_06$DAT_PRIMA_ATTIVAZIONE, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_06 <- withColumn(cb_attiva_06, "mese_rif", lit('Giugno'))
cb_attiva_06 <- distinct(select(cb_attiva_06, "COD_CLIENTE_CIFRATO", "DES_STATO_CNTR", "DES_CLS_UTENZA_SALES", "data_prima_attiv_dt", "data_ingr_pdisc_dt", "FASCIA_SDM_PDISC_M1", "FASCIA_SDM_PDISC_M4", "mese_rif"))
View(head(cb_attiva_06,100))
nrow(cb_attiva_06)
# 4.265.854


cb_attiv_2018_07 <- read.df(path_cb_luglio, source = "csv", header = "true", delimiter = "|")
View(head(cb_attiv_2018_07,100))
nrow(cb_attiv_2018_07)
# 4.321.759
cb_attiva_07 <- filter(cb_attiv_2018_07, "DES_STATO_CNTR like '%Active%'")
cb_attiva_07 <- filter(cb_attiva_07, cb_attiva_07$DES_CLS_UTENZA_SALES == 'RESIDENTIAL')
cb_attiva_07 <- withColumn(cb_attiva_07, "data_ingr_pdisc_dt", cast(cast(unix_timestamp(cb_attiva_07$DAT_VAL_RICH_CSZ_CNTR, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_07 <- withColumn(cb_attiva_07, "data_ingr_pdisc_dt", ifelse(cb_attiva_07$data_ingr_pdisc_dt == '9999-12-31', NULL, cb_attiva_07$data_ingr_pdisc_dt))
cb_attiva_07 <- withColumn(cb_attiva_07, "data_prima_attiv_dt", cast(cast(unix_timestamp(cb_attiva_07$DAT_PRIMA_ATTIVAZIONE, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_07 <- withColumn(cb_attiva_07, "mese_rif", lit('Luglio'))
cb_attiva_07 <- distinct(select(cb_attiva_07, "COD_CLIENTE_CIFRATO", "DES_STATO_CNTR", "DES_CLS_UTENZA_SALES", "data_prima_attiv_dt", "data_ingr_pdisc_dt", "FASCIA_SDM_PDISC_M1", "FASCIA_SDM_PDISC_M4", "mese_rif"))
View(head(cb_attiva_07,100))
nrow(cb_attiva_07)
# 4.282.890


cb_attiv_2018_08 <- read.df(path_cb_agosto, source = "csv", header = "true", delimiter = "|")
View(head(cb_attiv_2018_08,100))
nrow(cb_attiv_2018_08)
# 4.390.083
cb_attiva_08 <- filter(cb_attiv_2018_08, "DES_STATO_CNTR like '%Active%'")
cb_attiva_08 <- filter(cb_attiva_08, cb_attiva_08$DES_CLS_UTENZA_SALES == 'RESIDENTIAL')
cb_attiva_08 <- withColumn(cb_attiva_08, "data_ingr_pdisc_dt", cast(cast(unix_timestamp(cb_attiva_08$DAT_VAL_RICH_CSZ_CNTR, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_08 <- withColumn(cb_attiva_08, "data_ingr_pdisc_dt", ifelse(cb_attiva_08$data_ingr_pdisc_dt == '9999-12-31', NULL, cb_attiva_08$data_ingr_pdisc_dt))
cb_attiva_08 <- withColumn(cb_attiva_08, "data_prima_attiv_dt", cast(cast(unix_timestamp(cb_attiva_08$DAT_PRIMA_ATTIVAZIONE, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_08 <- withColumn(cb_attiva_08, "mese_rif", lit('Agosto'))
cb_attiva_08 <- distinct(select(cb_attiva_08, "COD_CLIENTE_CIFRATO", "DES_STATO_CNTR", "DES_CLS_UTENZA_SALES", "data_prima_attiv_dt", "data_ingr_pdisc_dt", "FASCIA_SDM_PDISC_M1", "FASCIA_SDM_PDISC_M4", "mese_rif"))
View(head(cb_attiva_08,100))
nrow(cb_attiva_08)
# 4.349.036


cb_attiv_2018_09 <- read.df(path_cb_settembre, source = "csv", header = "true", delimiter = "|")
View(head(cb_attiv_2018_09,100))
nrow(cb_attiv_2018_09)
# 4.598.311
cb_attiva_09 <- filter(cb_attiv_2018_09, "DES_STATO_CNTR like '%Active%'")
cb_attiva_09 <- filter(cb_attiva_09, cb_attiva_09$DES_CLS_UTENZA_SALES == 'RESIDENTIAL')
cb_attiva_09 <- withColumn(cb_attiva_09, "data_ingr_pdisc_dt", cast(cast(unix_timestamp(cb_attiva_09$DAT_VAL_RICH_CSZ_CNTR, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_09 <- withColumn(cb_attiva_09, "data_ingr_pdisc_dt", ifelse(cb_attiva_09$data_ingr_pdisc_dt == '9999-12-31', NULL, cb_attiva_09$data_ingr_pdisc_dt))
cb_attiva_09 <- withColumn(cb_attiva_09, "data_prima_attiv_dt", cast(cast(unix_timestamp(cb_attiva_09$DAT_PRIMA_ATTIVAZIONE, 'dd/MM/yyyy'),'timestamp'),'date'))
cb_attiva_09 <- withColumn(cb_attiva_09, "mese_rif", lit('Settembre'))
cb_attiva_09 <- distinct(select(cb_attiva_09, "COD_CLIENTE_CIFRATO", "DES_STATO_CNTR", "DES_CLS_UTENZA_SALES", "data_prima_attiv_dt", "data_ingr_pdisc_dt", "FASCIA_SDM_PDISC_M1", "FASCIA_SDM_PDISC_M4", "mese_rif"))
View(head(cb_attiva_09,100))
nrow(cb_attiva_09)
# 4.549.212


## metto insieme i dati: 
createOrReplaceTempView(cb_attiva_04, "cb_attiva_04")
createOrReplaceTempView(cb_attiva_05, "cb_attiva_05")
createOrReplaceTempView(cb_attiva_06, "cb_attiva_06")
createOrReplaceTempView(cb_attiva_07, "cb_attiva_07")
createOrReplaceTempView(cb_attiva_08, "cb_attiva_08")
createOrReplaceTempView(cb_attiva_09, "cb_attiva_09")


cb_05 <- sql("select distinct t1.*, t2.data_ingr_pdisc_dt as data_ingr_pdisc_05_dt
                     from cb_attiva_05 t1
                    left join cb_attiva_06 t2
                    on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
createOrReplaceTempView(cb_05, "cb_05")
cb_05 <- sql("select distinct t1.*, t2.FASCIA_SDM_PDISC_M1 as fascia_sdm_05
                     from cb_05 t1
                    left join cb_attiva_04 t2
                    on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

write.parquet(cb_05, path_cb_maggio_wr, mode = "overwrite")


cb_06 <- sql("select distinct t1.*, t2.data_ingr_pdisc_dt as data_ingr_pdisc_06_dt
                     from cb_attiva_06 t1
                        left join cb_attiva_07 t2
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
createOrReplaceTempView(cb_06, "cb_06")
cb_06 <- sql("select distinct t1.*, t2.FASCIA_SDM_PDISC_M1 as fascia_sdm_06
                        from cb_06 t1
                        left join cb_attiva_05 t2
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

write.parquet(cb_06, path_cb_giugno_wr, mode = "overwrite")


cb_07 <- sql("select distinct t1.*, t2.data_ingr_pdisc_dt as data_ingr_pdisc_07_dt
                     from cb_attiva_07 t1
                        left join cb_attiva_08 t2
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
createOrReplaceTempView(cb_07, "cb_07")
cb_07 <- sql("select distinct t1.*, t2.FASCIA_SDM_PDISC_M1 as fascia_sdm_07
                        from cb_07 t1
                        left join cb_attiva_06 t2
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

write.parquet(cb_07, path_cb_luglio_wr, mode = "overwrite")


cb_08 <- sql("select distinct t1.*, t2.data_ingr_pdisc_dt as data_ingr_pdisc_08_dt
                     from cb_attiva_08 t1
                        left join cb_attiva_09 t2
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
createOrReplaceTempView(cb_08, "cb_08")
cb_08 <- sql("select distinct t1.*, t2.FASCIA_SDM_PDISC_M1 as fascia_sdm_08
                        from cb_08 t1
                        left join cb_attiva_07 t2
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

write.parquet(cb_08, path_cb_agosto_wr, mode = "overwrite")



## Read cb #####################################################################################################################################################

cb_05 <- read.parquet(path_cb_maggio_wr)
View(head(cb_05,100))
nrow(cb_05)
# 4.562.650
# prova <- filter(cb_05, isNotNull(cb_05$fascia_sdm_05) & cb_05$data_ingr_pdisc_05_dt >= '2018-05-01' & cb_05$data_ingr_pdisc_05_dt < '2018-06-01')
# View(head(prova,100))
# nrow(prova)
# # 61.080


cb_06 <- read.parquet(path_cb_giugno_wr)
View(head(cb_06,100))
nrow(cb_06)
# 4.535.829
prova <- filter(cb_06, isNotNull(cb_06$fascia_sdm_06) & cb_06$data_ingr_pdisc_06_dt >= '2018-06-01' & cb_06$data_ingr_pdisc_06_dt < '2018-07-01')
# View(head(prova,100))
# nrow(prova)
# # 47.993


cb_07 <- read.parquet(path_cb_luglio_wr)
View(head(cb_07,100))
nrow(cb_07)
# 4.587.661
# prova <- filter(cb_07, isNotNull(cb_07$fascia_sdm_07) & cb_07$data_ingr_pdisc_07_dt >= '2018-07-01' & cb_07$data_ingr_pdisc_07_dt < '2018-08-01')
# View(head(prova,100))
# nrow(prova)
# # 131.378


cb_08 <- read.parquet(path_cb_agosto_wr)
View(head(cb_08,100))
nrow(cb_08)
# 4.645.688
# prova <- filter(cb_08, isNotNull(cb_08$fascia_sdm_08) & cb_08$data_ingr_pdisc_08_dt >= '2018-08-01' & cb_08$data_ingr_pdisc_08_dt < '2018-09-01')
# View(head(prova,100))
# nrow(prova)
# # 66.939


###################################################################################################################################################################
###################################################################################################################################################################
## overlap ta i 2 modelli ##
###################################################################################################################################################################
###################################################################################################################################################################

## Maggio ########################################################################################################################################################

cdm_05 <- read.df("/user/stefano.mazzucca/churn_digital/2018_05_01/output_modello_CD_20180501.csv", source = "csv", header = "true", delimiter = ";")
View(head(cdm_05,100))
nrow(cdm_05)
# 1.505.072
lista_cd_1 <- withColumn(cdm_05, "decile", cast(cdm_05$decile, "double"))
lista_cd_1 $flg_pred <- ifelse(lista_cd_1$flg_pred == "TRUE", 1, 0)
lista_cd_2 <- withColumn(lista_cd_1, "data_test_cdm", lit(as.Date('2018-05-01')))
View(head(lista_cd_2,100))
nrow(lista_cd_2)
# 1.505.072


sdm_05 <- read.parquet(path_cb_maggio_wr)
View(head(sdm_05,100))
nrow(sdm_05)
# 4.562.650
lista_sdm_1 <- fillna(sdm_05, '99')
lista_sdm_2 <- withColumn(lista_sdm_1, "fascia_sdm", ifelse(lista_sdm_1$FASCIA_SDM_PDISC_M1 <= lista_sdm_1$fascia_sdm_05,
                                                       lista_sdm_1$FASCIA_SDM_PDISC_M1, lista_sdm_1$fascia_sdm_05))
lista_sdm_3 <- filter(lista_sdm_2, lista_sdm_2$fascia_sdm != '99')
lista_sdm_3$churn <- ifelse(isNotNull(lista_sdm_3$data_ingr_pdisc_05_dt) | isNotNull(lista_sdm_3$data_ingr_pdisc_dt), 1, 0)
lista_sdm_4 <- withColumn(lista_sdm_3, "data_ingr_pdisc_def", ifelse(isNotNull(lista_sdm_3$data_ingr_pdisc_05_dt), 
                                                                  lista_sdm_3$data_ingr_pdisc_05_dt, lista_sdm_3$data_ingr_pdisc_dt))
lista_sdm_5 <- filter(lista_sdm_4, lista_sdm_4$data_ingr_pdisc_def >= as.Date('2018-05-01') | isNull(lista_sdm_4$data_ingr_pdisc_def))
lista_sdm_6 <- distinct(select(lista_sdm_5, "COD_CLIENTE_CIFRATO", "DES_STATO_CNTR", "DES_CLS_UTENZA_SALES", "data_prima_attiv_dt", "mese_rif", 
                               "fascia_sdm", "data_ingr_pdisc_def", "churn"))
View(head(lista_sdm_6,100))
nrow(lista_sdm_6)
# 4.291.248
# prova <- filter(lista_sdm_6, "churn = 1")
# View(head(prova,100))
# nrow(prova)
# # 65.031


createOrReplaceTempView(lista_cd_2, "lista_cd_2")
createOrReplaceTempView(lista_sdm_6, "lista_sdm_6")

lista_05 <- sql("select distinct t1.*, t2.flg_pred, t2.decile, t2.data_test_cdm
                from lista_sdm_6 t1
                left join lista_cd_2 t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

lista_05 <- withColumn(lista_05, "gg_dalla_pred_a_effettivo_pdisc", datediff(lista_05$data_ingr_pdisc_def, lista_05$data_test_cdm))
# lista_05 <- filter(lista_05, lista_05$gg_dalla_pred_a_effettivo_pdisc >= 0 | isNull(lista_05$gg_dalla_pred_a_effettivo_pdisc))
View(head(lista_05,100))
nrow(lista_05)
# 4.291.248


write.parquet(lista_05, path_lista_maggio, mode = "overwrite")


## Osservazioni sul CD Model #############################################

lista_05 <- read.parquet(path_lista_maggio)
View(head(lista_05,100))
nrow(lista_05)
# 4.291.248

createOrReplaceTempView(lista_05, "lista_05")
cd_gb <- sql("select decile, churn, count(COD_CLIENTE_CIFRATO)
                   from lista_05 
                   group by decile, churn")
View(head(cd_gb,100))

lista_tot_cd <- filter(lista_05, isNotNull(lista_05$decile))
nrow(lista_tot_cd)
# 1.604.656
lista_tot_cd_pdisc <- filter(lista_tot_cd, lista_tot_cd$churn == 1)
nrow(lista_tot_cd_pdisc)
# 8.557 (troppo pochi)
pdisc_rate_tot_cd <- nrow(lista_tot_cd_pdisc)/nrow(lista_tot_cd)
cat(pdisc_rate_tot_cd)
# 0.005332607


## Osservazioni sul SDM Model #############################################

createOrReplaceTempView(lista_05, "lista_05")
sdm_gb <- sql("select fascia_sdm, churn, count(COD_CLIENTE_CIFRATO)
               from lista_05 
               group by fascia_sdm, churn")
View(head(sdm_gb,100))

lista_sdm_pdisc <- filter(lista_05, lista_05$churn == 1)
nrow(lista_sdm_pdisc)
# 65.031
pdisc_rate_tot_sdm <- nrow(lista_sdm_pdisc)/nrow(lista_05)
cat(pdisc_rate_tot_sdm)
# 0.01515433


## Osservazioni totali ####################################################

createOrReplaceTempView(lista_05, "lista_05")
gb_tot <- sql("select decile, fascia_sdm, 
                    count(COD_CLIENTE_CIFRATO) as count_tot, 
                    sum(churn) as count_pdisc
           from lista_05
           group by decile, fascia_sdm")
View(head(gb_tot,1000))

lista_tot_pdisc <- filter(lista_05, lista_05$churn == 1)
nrow(lista_tot_pdisc)
# 65.031
createOrReplaceTempView(lista_tot_pdisc, "lista_tot_pdisc")
gb_pdisc <- sql("select decile, fascia_sdm, 
                        count(COD_CLIENTE_CIFRATO) as count
               from lista_tot_pdisc
               group by decile, fascia_sdm")
View(head(gb_pdisc,1000))



## Giugno ########################################################################################################################################################

cdm_06 <- read.df("/user/stefano.mazzucca/churn_digital/2018_06_01/output_modello_CD_20180601.csv", source = "csv", header = "true", delimiter = ";")
View(head(cdm_06,100))
nrow(cdm_06)
# 1.400.650
lista_cd_1 <- withColumn(cdm_06, "decile", cast(cdm_06$decile, "double"))
lista_cd_1 $flg_pred <- ifelse(lista_cd_1$flg_pred == "TRUE", 1, 0)
lista_cd_2 <- withColumn(lista_cd_1, "data_test_cdm", lit(as.Date('2018-05-01')))
View(head(lista_cd_2,100))
nrow(lista_cd_2)
# 1.400.650


sdm_06 <- read.parquet(path_cb_giugno_wr)
View(head(sdm_06,100))
nrow(sdm_06)
# 4.535.829
lista_sdm_1 <- fillna(sdm_06, '99')
lista_sdm_2 <- withColumn(lista_sdm_1, "fascia_sdm", ifelse(lista_sdm_1$FASCIA_SDM_PDISC_M1 <= lista_sdm_1$fascia_sdm_06,
                                                            lista_sdm_1$FASCIA_SDM_PDISC_M1, lista_sdm_1$fascia_sdm_06))
lista_sdm_3 <- filter(lista_sdm_2, lista_sdm_2$fascia_sdm != '99')
lista_sdm_3$churn <- ifelse(isNotNull(lista_sdm_3$data_ingr_pdisc_06_dt) | isNotNull(lista_sdm_3$data_ingr_pdisc_dt), 1, 0)
lista_sdm_4 <- withColumn(lista_sdm_3, "data_ingr_pdisc_def", ifelse(isNotNull(lista_sdm_3$data_ingr_pdisc_06_dt), 
                                                                     lista_sdm_3$data_ingr_pdisc_06_dt, lista_sdm_3$data_ingr_pdisc_dt))
lista_sdm_5 <- filter(lista_sdm_4, lista_sdm_4$data_ingr_pdisc_def >= as.Date('2018-06-01') | isNull(lista_sdm_4$data_ingr_pdisc_def))
lista_sdm_6 <- distinct(select(lista_sdm_5, "COD_CLIENTE_CIFRATO", "DES_STATO_CNTR", "DES_CLS_UTENZA_SALES", "data_prima_attiv_dt", "mese_rif", 
                               "fascia_sdm", "data_ingr_pdisc_def", "churn"))
View(head(lista_sdm_6,100))
nrow(lista_sdm_6)
# 4.194.143
# prova <- filter(lista_sdm_6, "churn = 1")
# View(head(prova,100))
# nrow(prova)
# # 49.940


createOrReplaceTempView(lista_cd_2, "lista_cd_2")
createOrReplaceTempView(lista_sdm_6, "lista_sdm_6")

lista_06 <- sql("select distinct t1.*, t2.flg_pred, t2.decile, t2.data_test_cdm
                from lista_sdm_6 t1
                left join lista_cd_2 t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

lista_06 <- withColumn(lista_06, "gg_dalla_pred_a_effettivo_pdisc", datediff(lista_06$data_ingr_pdisc_def, lista_06$data_test_cdm))
# lista_06 <- filter(lista_06, lista_06$gg_dalla_pred_a_effettivo_pdisc >= 0 | isNull(lista_06$gg_dalla_pred_a_effettivo_pdisc))
View(head(lista_06,100))
nrow(lista_06)
# 4.194.143


write.parquet(lista_06, path_lista_giugno, mode = "overwrite")


## Osservazioni sul CD Model #############################################

lista_06 <- read.parquet(path_lista_giugno)
View(head(lista_06,100))
nrow(lista_06)
# 4.194.143

createOrReplaceTempView(lista_06, "lista_06")
cd_gb <- sql("select decile, churn, count(COD_CLIENTE_CIFRATO)
             from lista_06 
             group by decile, churn")
View(head(cd_gb,100))

lista_tot_cd <- filter(lista_06, isNotNull(lista_06$decile))
nrow(lista_tot_cd)
# 1.488.838
lista_tot_cd_pdisc <- filter(lista_tot_cd, lista_tot_cd$churn == 1)
nrow(lista_tot_cd_pdisc)
# 7.431 
pdisc_rate_tot_cd <- nrow(lista_tot_cd_pdisc)/nrow(lista_tot_cd)
cat(pdisc_rate_tot_cd)
# 0.004991141


## Osservazioni sul SDM Model #############################################

createOrReplaceTempView(lista_06, "lista_06")
sdm_gb <- sql("select fascia_sdm, churn, count(COD_CLIENTE_CIFRATO)
               from lista_06 
               group by fascia_sdm, churn")
View(head(sdm_gb,100))

lista_sdm_pdisc <- filter(lista_06, lista_06$churn == 1)
nrow(lista_sdm_pdisc)
# 49.940
pdisc_rate_tot_sdm <- nrow(lista_sdm_pdisc)/nrow(lista_06)
cat(pdisc_rate_tot_sdm)
# 0.01190708


## Osservazioni totali ####################################################

createOrReplaceTempView(lista_06, "lista_06")
gb_tot <- sql("select decile, fascia_sdm, 
                    count(COD_CLIENTE_CIFRATO) as count_tot, 
                    sum(churn) as count_pdisc
           from lista_06
           group by decile, fascia_sdm")
View(head(gb_tot,1000))

lista_tot_pdisc <- filter(lista_06, lista_06$churn == 1)
nrow(lista_tot_pdisc)
# 49.940
createOrReplaceTempView(lista_tot_pdisc, "lista_tot_pdisc")
gb_pdisc <- sql("select decile, fascia_sdm, 
                        count(COD_CLIENTE_CIFRATO) as count
               from lista_tot_pdisc
               group by decile, fascia_sdm")
View(head(gb_pdisc,1000))



## Luglio ########################################################################################################################################################

cdm_07 <- read.df("/user/stefano.mazzucca/churn_digital/2018_07_01/output_modello_CD_20180701.csv", source = "csv", header = "true", delimiter = ";")
View(head(cdm_07,100))
nrow(cdm_07)
# 1.270.184
lista_cd_1 <- withColumn(cdm_07, "decile", cast(cdm_07$decile, "double"))
lista_cd_1 $flg_pred <- ifelse(lista_cd_1$flg_pred == "TRUE", 1, 0)
lista_cd_2 <- withColumn(lista_cd_1, "data_test_cdm", lit(as.Date('2018-05-01')))
View(head(lista_cd_2,100))
nrow(lista_cd_2)
# 1.270.184


sdm_07 <- read.parquet(path_cb_luglio_wr)
View(head(sdm_07,100))
nrow(sdm_07)
# 4.587.661
lista_sdm_1 <- fillna(sdm_07, '99')
lista_sdm_2 <- withColumn(lista_sdm_1, "fascia_sdm", ifelse(lista_sdm_1$FASCIA_SDM_PDISC_M1 <= lista_sdm_1$fascia_sdm_07,
                                                            lista_sdm_1$FASCIA_SDM_PDISC_M1, lista_sdm_1$fascia_sdm_07))
lista_sdm_3 <- filter(lista_sdm_2, lista_sdm_2$fascia_sdm != '99')
lista_sdm_3$churn <- ifelse(isNotNull(lista_sdm_3$data_ingr_pdisc_07_dt) | isNotNull(lista_sdm_3$data_ingr_pdisc_dt), 1, 0)
lista_sdm_4 <- withColumn(lista_sdm_3, "data_ingr_pdisc_def", ifelse(isNotNull(lista_sdm_3$data_ingr_pdisc_07_dt), 
                                                                     lista_sdm_3$data_ingr_pdisc_07_dt, lista_sdm_3$data_ingr_pdisc_dt))
lista_sdm_5 <- filter(lista_sdm_4, lista_sdm_4$data_ingr_pdisc_def >= as.Date('2018-07-01') | isNull(lista_sdm_4$data_ingr_pdisc_def))
lista_sdm_6 <- distinct(select(lista_sdm_5, "COD_CLIENTE_CIFRATO", "DES_STATO_CNTR", "DES_CLS_UTENZA_SALES", "data_prima_attiv_dt", "mese_rif", 
                               "fascia_sdm", "data_ingr_pdisc_def", "churn"))
View(head(lista_sdm_6,100))
nrow(lista_sdm_6)
# 4.202.861
# prova <- filter(lista_sdm_6, "churn = 1")
# View(head(prova,100))
# nrow(prova)
# # 131.383


createOrReplaceTempView(lista_cd_2, "lista_cd_2")
createOrReplaceTempView(lista_sdm_6, "lista_sdm_6")

lista_07 <- sql("select distinct t1.*, t2.flg_pred, t2.decile, t2.data_test_cdm
                from lista_sdm_6 t1
                left join lista_cd_2 t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

lista_07 <- withColumn(lista_07, "gg_dalla_pred_a_effettivo_pdisc", datediff(lista_07$data_ingr_pdisc_def, lista_07$data_test_cdm))
# lista_07 <- filter(lista_07, lista_07$gg_dalla_pred_a_effettivo_pdisc >= 0 | isNull(lista_07$gg_dalla_pred_a_effettivo_pdisc))
View(head(lista_07,100))
nrow(lista_07)
# 4.202.861


write.parquet(lista_07, path_lista_luglio, mode = "overwrite")


## Osservazioni sul CD Model #############################################

lista_07 <- read.parquet(path_lista_luglio)
View(head(lista_07,100))
nrow(lista_07)
# 4.202.861

createOrReplaceTempView(lista_07, "lista_07")
cd_gb <- sql("select decile, churn, count(COD_CLIENTE_CIFRATO)
             from lista_07 
             group by decile, churn")
View(head(cd_gb,100))

lista_tot_cd <- filter(lista_07, isNotNull(lista_07$decile))
nrow(lista_tot_cd)
# 1.361.036
lista_tot_cd_pdisc <- filter(lista_tot_cd, lista_tot_cd$churn == 1)
nrow(lista_tot_cd_pdisc)
# 19.604 
pdisc_rate_tot_cd <- nrow(lista_tot_cd_pdisc)/nrow(lista_tot_cd)
cat(pdisc_rate_tot_cd)
# 0.01440373


## Osservazioni sul SDM Model #############################################

createOrReplaceTempView(lista_07, "lista_07")
sdm_gb <- sql("select fascia_sdm, churn, count(COD_CLIENTE_CIFRATO)
               from lista_07 
               group by fascia_sdm, churn")
View(head(sdm_gb,100))

lista_sdm_pdisc <- filter(lista_07, lista_07$churn == 1)
nrow(lista_sdm_pdisc)
# 131.383
pdisc_rate_tot_sdm <- nrow(lista_sdm_pdisc)/nrow(lista_07)
cat(pdisc_rate_tot_sdm)
# 0.03126037


## Osservazioni totali ####################################################

createOrReplaceTempView(lista_07, "lista_07")
gb_tot <- sql("select decile, fascia_sdm, 
                    count(COD_CLIENTE_CIFRATO) as count_tot, 
                    sum(churn) as count_pdisc
           from lista_07
           group by decile, fascia_sdm")
View(head(gb_tot,1000))

lista_tot_pdisc <- filter(lista_07, lista_07$churn == 1)
nrow(lista_tot_pdisc)
# 131.383
createOrReplaceTempView(lista_tot_pdisc, "lista_tot_pdisc")
gb_pdisc <- sql("select decile, fascia_sdm, 
                        count(COD_CLIENTE_CIFRATO) as count
               from lista_tot_pdisc
               group by decile, fascia_sdm")
View(head(gb_pdisc,1000))



## Agosto ########################################################################################################################################################

cdm_08 <- read.df("/user/stefano.mazzucca/churn_digital/2018_08_01/output_modello_CD_20180801.csv", source = "csv", header = "true", delimiter = ";")
View(head(cdm_08,100))
nrow(cdm_08)
# 1.260.477
lista_cd_1 <- withColumn(cdm_08, "decile", cast(cdm_08$decile, "double"))
lista_cd_1 $flg_pred <- ifelse(lista_cd_1$flg_pred == "TRUE", 1, 0)
lista_cd_2 <- withColumn(lista_cd_1, "data_test_cdm", lit(as.Date('2018-05-01')))
View(head(lista_cd_2,100))
nrow(lista_cd_2)
# 1.260.477


sdm_08 <- read.parquet(path_cb_agosto_wr)
View(head(sdm_08,100))
nrow(sdm_08)
# 4.645.688
lista_sdm_1 <- fillna(sdm_08, '99')
lista_sdm_2 <- withColumn(lista_sdm_1, "fascia_sdm", ifelse(lista_sdm_1$FASCIA_SDM_PDISC_M1 <= lista_sdm_1$fascia_sdm_08,
                                                            lista_sdm_1$FASCIA_SDM_PDISC_M1, lista_sdm_1$fascia_sdm_08))
lista_sdm_3 <- filter(lista_sdm_2, lista_sdm_2$fascia_sdm != '99')
lista_sdm_3$churn <- ifelse(isNotNull(lista_sdm_3$data_ingr_pdisc_08_dt) | isNotNull(lista_sdm_3$data_ingr_pdisc_dt), 1, 0)
lista_sdm_4 <- withColumn(lista_sdm_3, "data_ingr_pdisc_def", ifelse(isNotNull(lista_sdm_3$data_ingr_pdisc_08_dt), 
                                                                     lista_sdm_3$data_ingr_pdisc_08_dt, lista_sdm_3$data_ingr_pdisc_dt))
lista_sdm_5 <- filter(lista_sdm_4, lista_sdm_4$data_ingr_pdisc_def >= as.Date('2018-08-01') | isNull(lista_sdm_4$data_ingr_pdisc_def))
lista_sdm_6 <- distinct(select(lista_sdm_5, "COD_CLIENTE_CIFRATO", "DES_STATO_CNTR", "DES_CLS_UTENZA_SALES", "data_prima_attiv_dt", "mese_rif", 
                               "fascia_sdm", "data_ingr_pdisc_def", "churn"))
View(head(lista_sdm_6,100))
nrow(lista_sdm_6)
# 4.196.130
prova <- filter(lista_sdm_6, "churn = 1")
View(head(prova,100))
nrow(prova)
# 73.838


createOrReplaceTempView(lista_cd_2, "lista_cd_2")
createOrReplaceTempView(lista_sdm_6, "lista_sdm_6")

lista_08 <- sql("select distinct t1.*, t2.flg_pred, t2.decile, t2.data_test_cdm
                from lista_sdm_6 t1
                left join lista_cd_2 t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

lista_08 <- withColumn(lista_08, "gg_dalla_pred_a_effettivo_pdisc", datediff(lista_08$data_ingr_pdisc_def, lista_08$data_test_cdm))
# lista_08 <- filter(lista_08, lista_08$gg_dalla_pred_a_effettivo_pdisc >= 0 | isNull(lista_08$gg_dalla_pred_a_effettivo_pdisc))
View(head(lista_08,100))
nrow(lista_08)
# 4.196.130


write.parquet(lista_08, path_lista_agosto, mode = "overwrite")


## Osservazioni sul CD Model #############################################

lista_08 <- read.parquet(path_lista_agosto)
View(head(lista_08,100))
nrow(lista_08)
# 4.196.130

createOrReplaceTempView(lista_08, "lista_08")
cd_gb <- sql("select decile, churn, count(COD_CLIENTE_CIFRATO)
             from lista_08 
             group by decile, churn")
View(head(cd_gb,100))

lista_tot_cd <- filter(lista_08, isNotNull(lista_08$decile))
nrow(lista_tot_cd)
# 1.324.172
lista_tot_cd_pdisc <- filter(lista_tot_cd, lista_tot_cd$churn == 1)
nrow(lista_tot_cd_pdisc)
# 8.177 
pdisc_rate_tot_cd <- nrow(lista_tot_cd_pdisc)/nrow(lista_tot_cd)
cat(pdisc_rate_tot_cd)
# 0.00617518


## Osservazioni sul SDM Model #############################################

createOrReplaceTempView(lista_08, "lista_08")
sdm_gb <- sql("select fascia_sdm, churn, count(COD_CLIENTE_CIFRATO)
               from lista_08 
               group by fascia_sdm, churn")
View(head(sdm_gb,100))

lista_sdm_pdisc <- filter(lista_08, lista_08$churn == 1)
nrow(lista_sdm_pdisc)
# 73.838
pdisc_rate_tot_sdm <- nrow(lista_sdm_pdisc)/nrow(lista_08)
cat(pdisc_rate_tot_sdm)
# 0.01759669


## Osservazioni totali ####################################################

createOrReplaceTempView(lista_08, "lista_08")
gb_tot <- sql("select decile, fascia_sdm, 
                    count(COD_CLIENTE_CIFRATO) as count_tot, 
                    sum(churn) as count_pdisc
           from lista_08
           group by decile, fascia_sdm")
View(head(gb_tot,1000))

lista_tot_pdisc <- filter(lista_08, lista_08$churn == 1)
nrow(lista_tot_pdisc)
# 73.838
createOrReplaceTempView(lista_tot_pdisc, "lista_tot_pdisc")
gb_pdisc <- sql("select decile, fascia_sdm, 
                        count(COD_CLIENTE_CIFRATO) as count
               from lista_tot_pdisc
               group by decile, fascia_sdm")
View(head(gb_pdisc,1000))




#################################################################################################################################################################
#################################################################################################################################################################
## overlap tra liste CDM
#################################################################################################################################################################
#################################################################################################################################################################

## join

cdm_aprile <- read.df(path_cdm_aprile, source = "csv", header = "true", delimiter = ";")
cdm_maggio <- read.df(path_cdm_maggio, source = "csv", header = "true", delimiter = ";")
cdm_giugno <- read.df(path_cdm_giugno, source = "csv", header = "true", delimiter = ";")
cdm_luglio <- read.df(path_cdm_luglio, source = "csv", header = "true", delimiter = ";")
cdm_agosto <- read.df(path_cdm_agosto, source = "csv", header = "true", delimiter = ";")
cdm_settembre <- read.df(path_cdm_settembre, source = "csv", header = "true", delimiter = ";")
# View(head(cdm_settembre,100))

## overlap aprile-maggio
createOrReplaceTempView(cdm_aprile, "cdm_aprile")
createOrReplaceTempView(cdm_maggio, "cdm_maggio")
l1 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid_1, t2.COD_CLIENTE_CIFRATO as skyid_2, t1.decile as decile_1, t2.decile as decile_2
          from cdm_aprile t1
          full outer join cdm_maggio t2
          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(l1,100))
nrow(l1)
# 1.785.099

createOrReplaceTempView(l1, "l1")
gb1 <- sql("select decile_1, decile_2, count(*) as count
           from l1
           group by decile_1, decile_2")
View(head(gb1,1000))


## overlap maggio-giugno
createOrReplaceTempView(cdm_giugno, "cdm_giugno")
createOrReplaceTempView(cdm_maggio, "cdm_maggio")
l2 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid_1, t2.COD_CLIENTE_CIFRATO as skyid_2, t1.decile as decile_1, t2.decile as decile_2
          from cdm_giugno t1
          full outer join cdm_maggio t2
          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(l2,100))
nrow(l2)
# 1.746.101

createOrReplaceTempView(l2, "l2")
gb2 <- sql("select decile_1, decile_2, count(*) as count
           from l2
           group by decile_1, decile_2")
View(head(gb2,1000))


## overlap giugno-luglio
createOrReplaceTempView(cdm_giugno, "cdm_giugno")
createOrReplaceTempView(cdm_luglio, "cdm_luglio")
l3 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid_1, t2.COD_CLIENTE_CIFRATO as skyid_2, t1.decile as decile_1, t2.decile as decile_2
          from cdm_giugno t1
          full outer join cdm_luglio t2
          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(l3,100))
nrow(l3)
# 1.646.473

createOrReplaceTempView(l3, "l3")
gb3 <- sql("select decile_1, decile_2, count(*) as count
           from l3
           group by decile_1, decile_2")
View(head(gb3,1000))


## overlap luglio-agosto
createOrReplaceTempView(cdm_agosto, "cdm_agosto")
createOrReplaceTempView(cdm_luglio, "cdm_luglio")
l4 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid_1, t2.COD_CLIENTE_CIFRATO as skyid_2, t1.decile as decile_1, t2.decile as decile_2
          from cdm_agosto t1
          full outer join cdm_luglio t2
          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(l4,100))
nrow(l4)
# 1.605.355

createOrReplaceTempView(l4, "l4")
gb4 <- sql("select decile_1, decile_2, count(*) as count
           from l4
           group by decile_1, decile_2")
View(head(gb4,1000))


## overlap agosto-settembre
createOrReplaceTempView(cdm_agosto, "cdm_agosto")
createOrReplaceTempView(cdm_settembre, "cdm_settembre")
l5 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid_1, t2.COD_CLIENTE_CIFRATO as skyid_2, t1.decile as decile_1, t2.decile as decile_2
          from cdm_agosto t1
          full outer join cdm_settembre t2
          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(l5,100))
nrow(l5)
# 1.802.365

createOrReplaceTempView(l5, "l5")
gb5 <- sql("select decile_1, decile_2, count(*) as count
           from l5
           group by decile_1, decile_2")
View(head(gb5,1000))



## overlap aprile-...
createOrReplaceTempView(cdm_aprile, "cdm_aprile")
createOrReplaceTempView(cdm_settembre, "cdm_settembre")
l6 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid_1, t2.COD_CLIENTE_CIFRATO as skyid_2, t1.decile as decile_1, t2.decile as decile_2
          from cdm_aprile t1
          full outer join cdm_settembre t2
          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(l6,100))
nrow(l6)
# ...

createOrReplaceTempView(l6, "l6")
gb6 <- sql("select decile_1, decile_2, count(*) as count
           from l6
           group by decile_1, decile_2")
View(head(gb6,1000))


## overlap maggio-...
createOrReplaceTempView(cdm_maggio, "cdm_maggio")
createOrReplaceTempView(cdm_luglio, "cdm_luglio")
l6 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid_1, t2.COD_CLIENTE_CIFRATO as skyid_2, t1.decile as decile_1, t2.decile as decile_2
          from cdm_maggio t1
          full outer join cdm_luglio t2
          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(l6,100))
nrow(l6)
# ...

createOrReplaceTempView(l6, "l6")
gb6 <- sql("select decile_1, decile_2, count(*) as count
           from l6
           group by decile_1, decile_2")
View(head(gb6,1000))


## overlap giugno-...
createOrReplaceTempView(cdm_giugno, "cdm_giugno")
createOrReplaceTempView(cdm_agosto, "cdm_agosto")
l3 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid_1, t2.COD_CLIENTE_CIFRATO as skyid_2, t1.decile as decile_1, t2.decile as decile_2
          from cdm_giugno t1
          full outer join cdm_agosto t2
          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(l3,100))
nrow(l3)
# ...

createOrReplaceTempView(l3, "l3")
gb3 <- sql("select decile_1, decile_2, count(*) as count
           from l3
           group by decile_1, decile_2")
View(head(gb3,1000))


## overlap luglio-...
createOrReplaceTempView(cdm_luglio, "cdm_luglio")
createOrReplaceTempView(cdm_settembre, "cdm_settembre")
l3 <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid_1, t2.COD_CLIENTE_CIFRATO as skyid_2, t1.decile as decile_1, t2.decile as decile_2
          from cdm_luglio t1
          full outer join cdm_settembre t2
          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(l3,100))
nrow(l3)
# ...

createOrReplaceTempView(l3, "l3")
gb3 <- sql("select decile_1, decile_2, count(*) as count
           from l3
           group by decile_1, decile_2")
View(head(gb3,1000))


# ## Altre elaborazioni ######################################################################################################################################
# 
# extid_04 <-distinct(select(filter(cdm_aprile, cdm_aprile$decile == 1), "COD_CLIENTE_CIFRATO"))
# nrow(extid_04)
# # 148.920
# extid_05 <-distinct(select(filter(cdm_maggio, cdm_maggio$decile == 1), "COD_CLIENTE_CIFRATO"))
# nrow(extid_05)
# # 150.508
# extid_06 <-distinct(select(filter(cdm_giugno, cdm_giugno$decile == 1), "COD_CLIENTE_CIFRATO"))
# nrow(extid_06)
# # 140.065
# extid_07 <-distinct(select(filter(cdm_luglio, cdm_luglio$decile == 1), "COD_CLIENTE_CIFRATO"))
# nrow(extid_07)
# # 127.019
# extid_08 <-distinct(select(filter(cdm_agosto, cdm_agosto$decile == 1), "COD_CLIENTE_CIFRATO"))
# nrow(extid_08)
# # 126.048
# extid_09 <-distinct(select(filter(cdm_settembre, cdm_settembre$decile == 1), "COD_CLIENTE_CIFRATO"))
# nrow(extid_09)
# # 157.032
# 
# 
# ## overlap da aprile ##
# 
# createOrReplaceTempView(cdm_aprile, "cdm_aprile")
# createOrReplaceTempView(cdm_maggio, "cdm_maggio")
# overlap_04_05 <- sql("select t1.COD_CLIENTE_CIFRATO
#                       from cdm_aprile t1
#                       inner join cdm_maggio t2
#                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
#                       where t1.decile = 1 and t2.decile = 1")
# nrow(overlap_04_05)
# # 61.750
# overlap_rate_04_05 <- nrow(overlap_04_05)/nrow(extid_04)
# cat(overlap_rate_04_05)
# # 0.4146522
# 
# createOrReplaceTempView(overlap_04_05, "overlap_04_05")
# createOrReplaceTempView(cdm_giugno, "cdm_giugno")
# overlap_04_05_06 <- sql("select t1.COD_CLIENTE_CIFRATO
#                         from overlap_04_05 t1
#                         inner join (select COD_CLIENTE_CIFRATO
#                                     from cdm_giugno 
#                                     where decile = 1) t2
#                         on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(overlap_04_05_06)
# # 24.947
# extid_04_05 <- distinct(select(rbind(extid_04, extid_05), "COD_CLIENTE_CIFRATO"))
# nrow(extid_04_05)
# # 237.678
# overlap_rate_04_05_06 <- nrow(overlap_04_05_06)/nrow(extid_04_05)
# cat(overlap_rate_04_05_06)
# # 0.1049613
# 
# createOrReplaceTempView(overlap_04_05_06, "overlap_04_05_06")
# createOrReplaceTempView(cdm_luglio, "cdm_luglio")
# overlap_04_05_06_07 <- sql("select t1.COD_CLIENTE_CIFRATO
#                         from overlap_04_05_06 t1
#                         inner join (select COD_CLIENTE_CIFRATO
#                                     from cdm_luglio 
#                                     where decile = 1) t2
#                         on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(overlap_04_05_06_07)
# # 11.593
# extid_04_05_06 <- distinct(select(rbind(extid_04_05, extid_06), "COD_CLIENTE_CIFRATO"))
# nrow(extid_04_05_06)
# # 303.243
# overlap_rate_04_05_06_07 <- nrow(overlap_04_05_06_07)/nrow(extid_04_05_06)
# cat(overlap_rate_04_05_06_07)
# # 0.03823007
# 
# createOrReplaceTempView(overlap_04_05_06_07, "overlap_04_05_06_07")
# createOrReplaceTempView(cdm_agosto, "cdm_agosto")
# overlap_04_05_06_07_08 <- sql("select t1.COD_CLIENTE_CIFRATO
#                         from overlap_04_05_06_07 t1
#                         inner join (select COD_CLIENTE_CIFRATO
#                                     from cdm_agosto 
#                                     where decile = 1) t2
#                         on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(overlap_04_05_06_07_08)
# # 5.101
# extid_04_05_06_07 <- distinct(select(rbind(extid_04_05_06, extid_07), "COD_CLIENTE_CIFRATO"))
# nrow(extid_04_05_06_07)
# # 353.621
# overlap_rate_04_05_06_07_08 <- nrow(overlap_04_05_06_07_08)/nrow(extid_04_05_06_07)
# cat(overlap_rate_04_05_06_07_08)
# # 0.01442505
# 
# createOrReplaceTempView(overlap_04_05_06_07_08, "overlap_04_05_06_07_08")
# createOrReplaceTempView(cdm_settembre, "cdm_settembre")
# overlap_04_05_06_07_08_09 <- sql("select t1.COD_CLIENTE_CIFRATO
#                         from overlap_04_05_06_07_08 t1
#                         inner join (select COD_CLIENTE_CIFRATO
#                                     from cdm_settembre 
#                                     where decile = 1) t2
#                         on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(overlap_04_05_06_07_08_09)
# # 2.458
# extid_04_05_06_07_08 <- distinct(select(rbind(extid_04_05_06_07, extid_08), "COD_CLIENTE_CIFRATO"))
# nrow(extid_04_05_06_07_08)
# # 410.877
# overlap_rate_04_05_06_07_08_09 <- nrow(overlap_04_05_06_07_08_09)/nrow(extid_04_05_06_07_08)
# cat(overlap_rate_04_05_06_07_08_09)
# # 0.005982326
# 
# 
# 
# ## overlap da maggio ##
# 
# createOrReplaceTempView(cdm_giugno, "cdm_giugno")
# createOrReplaceTempView(cdm_maggio, "cdm_maggio")
# overlap_05_06 <- sql("select t1.COD_CLIENTE_CIFRATO
#                      from cdm_maggio t1
#                      inner join cdm_giugno t2
#                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
#                      where t1.decile = 1 and t2.decile = 1")
# nrow(overlap_05_06)
# # 61.484
# overlap_rate_05_06 <- nrow(overlap_05_06)/nrow(extid_05)
# cat(overlap_rate_05_06)
# # 0.4085098
# 
# createOrReplaceTempView(overlap_05_06, "overlap_05_06")
# createOrReplaceTempView(cdm_luglio, "cdm_luglio")
# overlap_05_06_07 <- sql("select t1.COD_CLIENTE_CIFRATO
#                         from overlap_05_06 t1
#                         inner join (select COD_CLIENTE_CIFRATO
#                         from cdm_luglio 
#                         where decile = 1) t2
#                         on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(overlap_05_06_07)
# # 23.541
# extid_05_06 <- distinct(select(rbind(extid_05, extid_06), "COD_CLIENTE_CIFRATO"))
# nrow(extid_05_06)
# # 229.089
# overlap_rate_05_06_07 <- nrow(overlap_05_06_07)/nrow(extid_05_06)
# cat(overlap_rate_05_06_07)
# # 0.1027592
# 
# createOrReplaceTempView(overlap_05_06_07, "overlap_05_06_07")
# createOrReplaceTempView(cdm_agosto, "cdm_agosto")
# overlap_05_06_07_08 <- sql("select t1.COD_CLIENTE_CIFRATO
#                         from overlap_05_06_07 t1
#                         inner join (select COD_CLIENTE_CIFRATO
#                         from cdm_agosto 
#                         where decile = 1) t2
#                         on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(overlap_05_06_07_08)
# # 9.521
# extid_05_06_07 <- distinct(select(rbind(extid_05_06, extid_07), "COD_CLIENTE_CIFRATO"))
# nrow(extid_05_06_07)
# # 286.325
# overlap_rate_05_06_07_08 <- nrow(overlap_05_06_07_08)/nrow(extid_05_06_07)
# cat(overlap_rate_05_06_07_08)
# # 0.03325242
# 
# createOrReplaceTempView(overlap_05_06_07_08, "overlap_05_06_07_08")
# createOrReplaceTempView(cdm_settembre, "cdm_settembre")
# overlap_05_06_07_08_09 <- sql("select t1.COD_CLIENTE_CIFRATO
#                         from overlap_05_06_07_08 t1
#                         inner join (select COD_CLIENTE_CIFRATO
#                         from cdm_settembre 
#                         where decile = 1) t2
#                         on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(overlap_05_06_07_08_09)
# # 4.274
# extid_05_06_07_08 <- distinct(select(rbind(extid_05_06_07, extid_08), "COD_CLIENTE_CIFRATO"))
# nrow(extid_05_06_07_08)
# # 348.297
# overlap_rate_05_06_07_08_09 <- nrow(overlap_05_06_07_08_09)/nrow(extid_05_06_07_08)
# cat(overlap_rate_05_06_07_08_09)
# # 0.01227114
# 
# 
# 
# 
# ## overlap da giugno ##
# 
# createOrReplaceTempView(cdm_giugno, "cdm_giugno")
# createOrReplaceTempView(cdm_luglio, "cdm_luglio")
# overlap_06_07 <- sql("select t1.COD_CLIENTE_CIFRATO
#                      from cdm_luglio t1
#                      inner join cdm_giugno t2
#                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
#                      where t1.decile = 1 and t2.decile = 1")
# nrow(overlap_06_07)
# # 56.386
# overlap_rate_06_07 <- nrow(overlap_06_07)/nrow(extid_06)
# cat(overlap_rate_06_07)
# # 0.4025702
# 
# createOrReplaceTempView(overlap_06_07, "overlap_06_07")
# createOrReplaceTempView(cdm_agosto, "cdm_agosto")
# overlap_06_07_08 <- sql("select t1.COD_CLIENTE_CIFRATO
#                         from overlap_06_07 t1
#                         inner join (select COD_CLIENTE_CIFRATO
#                         from cdm_agosto 
#                         where decile = 1) t2
#                         on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(overlap_06_07_08)
# # 18.115
# extid_06_07 <- distinct(select(rbind(extid_06, extid_07), "COD_CLIENTE_CIFRATO"))
# nrow(extid_06_07)
# # 210.698
# overlap_rate_06_07_08 <- nrow(overlap_06_07_08)/nrow(extid_06_07)
# cat(overlap_rate_06_07_08)
# # 0.08597614
# 
# createOrReplaceTempView(overlap_06_07_08, "overlap_06_07_08")
# createOrReplaceTempView(cdm_settembre, "cdm_settembre")
# overlap_06_07_08_09 <- sql("select t1.COD_CLIENTE_CIFRATO
#                         from overlap_06_07_08 t1
#                         inner join (select COD_CLIENTE_CIFRATO
#                         from cdm_settembre 
#                         where decile = 1) t2
#                         on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(overlap_06_07_08_09)
# # 7.545
# extid_06_07_08 <- distinct(select(rbind(extid_06_07, extid_08), "COD_CLIENTE_CIFRATO"))
# nrow(extid_06_07_08)
# # 280.666
# overlap_rate_06_07_08_09 <- nrow(overlap_06_07_08_09)/nrow(extid_06_07_08)
# cat(overlap_rate_06_07_08_09)
# # 0.02688249
# 
# 
# 
# 
# ## overlap da luglio ##
# 
# createOrReplaceTempView(cdm_agosto, "cdm_agosto")
# createOrReplaceTempView(cdm_luglio, "cdm_luglio")
# overlap_07_08 <- sql("select t1.COD_CLIENTE_CIFRATO
#                      from cdm_luglio t1
#                      inner join cdm_agosto t2
#                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
#                      where t1.decile = 1 and t2.decile = 1")
# nrow(overlap_07_08)
# # 43.577
# overlap_rate_07_08 <- nrow(overlap_07_08)/nrow(extid_07)
# cat(overlap_rate_07_08)
# # 0.3430747
# 
# createOrReplaceTempView(overlap_07_08, "overlap_07_08")
# createOrReplaceTempView(cdm_settembre, "cdm_settembre")
# overlap_07_08_09 <- sql("select t1.COD_CLIENTE_CIFRATO
#                         from overlap_07_08 t1
#                         inner join (select COD_CLIENTE_CIFRATO
#                         from cdm_settembre 
#                         where decile = 1) t2
#                         on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# nrow(overlap_07_08_09)
# # 14.947
# extid_07_08 <- distinct(select(rbind(extid_07, extid_08), "COD_CLIENTE_CIFRATO"))
# nrow(extid_07_08)
# # 209.490
# overlap_rate_07_08_09 <- nrow(overlap_07_08_09)/nrow(extid_07_08)
# cat(overlap_rate_07_08_09)
# # 0.07134947
# 
# 
# 
# 
# ## overlap da agosto ##
# 
# createOrReplaceTempView(cdm_agosto, "cdm_agosto")
# createOrReplaceTempView(cdm_settembre, "cdm_settembre")
# overlap_08_09 <- sql("select t1.COD_CLIENTE_CIFRATO
#                      from cdm_settembre t1
#                      inner join cdm_agosto t2
#                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
#                      where t1.decile = 1 and t2.decile = 1")
# nrow(overlap_08_09)
# # 44.461
# overlap_rate_08_09 <- nrow(overlap_08_09)/nrow(extid_08)
# cat(overlap_rate_08_09)
# # 0.3527307
# 



##################################################################################################################################################################
##################################################################################################################################################################
## Calcolo overlap mensile del modello CDM sulla lista ON TOP al modello SDM ##
##################################################################################################################################################################
##################################################################################################################################################################



lista_05 <- read.parquet(path_lista_maggio)
View(head(lista_05,100))
nrow(lista_05)
# 4.291.248
lista_06 <- read.parquet(path_lista_giugno)
View(head(lista_06,100))
nrow(lista_06)
# 4.194.143
lista_07 <- read.parquet(path_lista_luglio)
View(head(lista_07,100))
nrow(lista_07)
# 4.202.861
lista_08 <- read.parquet(path_lista_agosto)
View(head(lista_08,100))
nrow(lista_08)
# 4.196.130


createOrReplaceTempView(lista_05, "lista_05")
lista_tot_no_overlap_05 <- sql("select COD_CLIENTE_CIFRATO, decile, fascia_sdm, churn
                               from lista_05
                               where decile == 1 and fascia_sdm <> 1")
View(head(lista_tot_no_overlap_05,1000))
nrow(lista_tot_no_overlap_05)
# 37.000

createOrReplaceTempView(lista_06, "lista_06")
lista_tot_no_overlap_06 <- sql("select COD_CLIENTE_CIFRATO, decile, fascia_sdm, churn
                               from lista_06 
                               where decile == 1 and fascia_sdm <> 1")
View(head(lista_tot_no_overlap_06,1000))
nrow(lista_tot_no_overlap_06)
# 36.855

overlap_05_06 <- distinct(select(rbind(lista_tot_no_overlap_05, lista_tot_no_overlap_06), "COD_CLIENTE_CIFRATO"))
View(head(overlap_05_06,100))
nrow(overlap_05_06)
# 55.463
# no_overlap_05_06 <- nrow(lista_tot_no_overlap_05)+nrow(lista_tot_no_overlap_06)-nrow(overlap_05_06)
# cat(no_overlap_05_06)
# # 18.392


createOrReplaceTempView(lista_07, "lista_07")
lista_tot_no_overlap_07 <- sql("select COD_CLIENTE_CIFRATO, decile, fascia_sdm, churn
                               from lista_07 
                               where decile == 1 and fascia_sdm <> 1")
View(head(lista_tot_no_overlap_07,1000))
nrow(lista_tot_no_overlap_07)
# 34.023

overlap_05_06_07 <- distinct(select(rbind(lista_tot_no_overlap_05, lista_tot_no_overlap_06, lista_tot_no_overlap_07), "COD_CLIENTE_CIFRATO"))
View(head(overlap_05_06_07,100))
nrow(overlap_05_06_07)
# 73.526


createOrReplaceTempView(lista_08, "lista_08")
lista_tot_no_overlap_08 <- sql("select COD_CLIENTE_CIFRATO, decile, fascia_sdm, churn
                               from lista_08 
                               where decile == 1 and fascia_sdm <> 1")
View(head(lista_tot_no_overlap_08,1000))
nrow(lista_tot_no_overlap_08)
# 40.079

overlap_05_06_07_08 <- distinct(select(rbind(lista_tot_no_overlap_05, lista_tot_no_overlap_06, lista_tot_no_overlap_07, lista_tot_no_overlap_08), 
                                       "COD_CLIENTE_CIFRATO"))
View(head(overlap_05_06_07_08,100))
nrow(overlap_05_06_07_08)
# 99.507




#chiudi sessione
sparkR.stop()
