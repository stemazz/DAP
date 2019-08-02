
## Verifica overlap liste modello CHURN DIGITAL vs SDM


#apri sessione
source("connection_R.R")
options(scipen = 1000)



## LISTA modello CHURN DIGITAL

path_lista_cd <- "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180717_enriched.csv"
# "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180717_enriched.csv"
# "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180724_enriched.csv"
# "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180731_enriched.csv"

data_inizio_test <- as.Date('2018-07-17')
data_fine_test <- as.Date('2018-08-21')

path_lista_tot <- "/user/stefano.mazzucca/churn_digital/lista_tot_with_cd_20180717.parquet"
# "/user/stefano.mazzucca/churn_digital/lista_tot_with_cd_20180717.parquet"
# "/user/stefano.mazzucca/churn_digital/lista_tot_with_cd_20180724.parquet"
# "/user/stefano.mazzucca/churn_digital/lista_tot_with_cd_20180731.parquet"


lista_cd <- read.df(path_lista_cd, source = "csv",  header = "true", delimiter = ",")
# View(head(lista,100))
nrow(lista_cd)
# 1.279.135

lista_cd_1 <- withColumn(lista_cd, "decile", cast(lista_cd$decile, "double"))
lista_cd_1 $flg_pred <- ifelse(lista_cd_1$flg_pred == "TRUE", 1, 0)
lista_cd_2 <- withColumn(lista_cd_1, "dat_fine_dt", cast(cast(unix_timestamp(lista_cd_1$DAT_FINE, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))
lista_cd_3 <- filter(lista_cd_2, lista_cd_2$dat_fine_dt >= data_inizio_test | isNull(lista_cd_2$dat_fine_dt))
lista_cd_4 <- withColumn(lista_cd_3, "data_test", lit(data_inizio_test))
lista_cd_5 <- withColumn(lista_cd_4, "gg_dalla_pred_a_effettivo_pdisc", datediff(lista_cd_4$dat_fine_dt, lista_cd_4$data_test))
lista_cd_5$churn <- ifelse(lista_cd_5$dat_fine_dt >= data_inizio_test, # & lista_5$dat_fine_dt <= data_fine_test, 
                        1, 0)
lista_cd_6 <- arrange(lista_cd_5, asc(lista_cd_5$decile), desc(lista_cd_5$flg_pred))
# lista_cd_7 <- select(lista_cd_6, c("flg_pred", "churn"))
View(head(lista_cd_6,100))
nrow(lista_cd_6)
# 1.275.791



## LISTA modello SDM

path_lista_sdm <- "/user/stefano.mazzucca/churn_digital/estraz_cb_attivi_stretti_sdm_luglio_CIFR.csv"

lista_sdm <- read.df(path_lista_sdm, source = "csv",  header = "true", delimiter = ",")
View(head(lista_sdm,100))
nrow(lista_sdm)
# 4.159.744

lista_sdm_1 <- filter(lista_sdm, isNotNull(lista_sdm$FASCIA_SDM_PDISC_M1))
nrow(lista_sdm_1)
# 3.936.947
lista_sdm_2 <- arrange(lista_sdm_1, asc(lista_sdm_1$FASCIA_SDM_PDISC_M1))
lista_sdm_2$churn <- ifelse(isNotNull(lista_sdm_2$dat_pdisc), 1, 0)
lista_sdm_3 <- withColumn(lista_sdm_2, "dat_pdisc_ts", cast(unix_timestamp(lista_sdm_2$dat_pdisc, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'))
lista_sdm_4 <- withColumn(lista_sdm_3, "dat_pdisc_dt", cast(cast(unix_timestamp(lista_sdm_2$dat_pdisc, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'),'date'))
View(head(lista_sdm_4,100))

date <- summarize(lista_sdm_4, min_data_pdisc = min(lista_sdm_4$dat_pdisc_dt), max_data_pdisc = max(lista_sdm_4$dat_pdisc_dt))
View(head(date,100))

# sdm_decile_1 <- filter(lista_sdm_4, lista_sdm_4$FASCIA_SDM_PDISC_M1 == 1)
# # View(head(sdm_decile_1,100))
# nrow(sdm_decile_1)
# # 395.032

# createOrReplaceTempView(lista_sdm_4, "lista_sdm_4")
# sdm_gb <- sql("select FASCIA_SDM_PDISC_M1, churn, count(COD_CLIENTE_CIFRATO)
#                    from lista_sdm_4 
#                    group by FASCIA_SDM_PDISC_M1, churn")
# View(head(sdm_gb,100))




## join

createOrReplaceTempView(lista_sdm_4, "lista_sdm_4")
createOrReplaceTempView(lista_cd_6, "lista_cd_6")

lista_tot <- sql("select distinct t1.*, t2.gg_dalla_pred_a_effettivo_pdisc, t2.decile as fascia_cd, t2.flg_pred as flg_pred_cd
                 from lista_sdm_4 t1
                 left join lista_cd_6 t2
                  on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(lista_tot,100))
nrow(lista_tot)
# 3.948.528

write.parquet(lista_tot, path_lista_tot)

##############################################################################################################################################################

lista_tot <- read.parquet(path_lista_tot)
View(head(lista_tot,100))
nrow(lista_tot)
# 3.948.528

lista_tot_1 <- select(lista_tot, "COD_CLIENTE_CIFRATO", "churn", "dat_pdisc", "dat_pdisc_dt", 
                      "FASCIA_SDM_PDISC_M1", "FASCIA_SDM_PDISC_M4", "fascia_cd", "flg_pred_cd")
lista_tot_2 <- arrange(lista_tot_1, asc(lista_tot_1$fascia_cd), asc(lista_tot_1$FASCIA_SDM_PDISC_M1))
View(head(lista_tot_2,100))

## TOLGO i clienti che hanno "churnato" prima del 17 luglio
lista_tot_3 <- filter(lista_tot_2, lista_tot_2$dat_pdisc_dt >= data_inizio_test | isNull(lista_tot_2$dat_pdisc_dt)) 
lista_tot_4 <- withColumn(lista_tot_3, "data_test", lit(data_inizio_test))
lista_tot_5 <- withColumn(lista_tot_4, "gg_dalla_pred_a_effettivo_pdisc", datediff(lista_tot_4$dat_pdisc_dt, lista_tot_4$data_test))
lista_tot_6 <- withColumn(lista_tot_5, "churn_periodo", lit(0))
lista_tot_6$churn_periodo <- ifelse(isNotNull(lista_tot_2$dat_pdisc_dt) & lista_tot_2$dat_pdisc_dt <= data_fine_test & lista_tot_2$dat_pdisc_dt >= data_inizio_test, 1, 0)

View(head(lista_tot_6,100))
nrow(lista_tot_6)
# 3.926.096

valdist_extid <- distinct(select(lista_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_extid)
# 3.809.196 (126k clienti che "ballano")


#########################################################################
# osservazioni fino al 23 settembre:

createOrReplaceTempView(lista_tot_6, "lista_tot_6")
cd_gb <- sql("select fascia_cd, churn, count(COD_CLIENTE_CIFRATO)
                   from lista_tot_6 
                   group by fascia_cd, churn")
View(head(cd_gb,100))

lista_tot_cd <- filter(lista_tot_6, isNotNull(lista_tot_6$fascia_cd))
nrow(lista_tot_cd)
# 1.172.333
lista_tot_cd_pdisc <- filter(lista_tot_cd, lista_tot_cd$churn == 1)
nrow(lista_tot_cd_pdisc)
# 78.824
pdisc_rate_tot_cd <- nrow(lista_tot_cd_pdisc)/nrow(lista_tot_cd)
cat(pdisc_rate_tot_cd)
# 0.06723687


createOrReplaceTempView(lista_tot_6, "lista_tot_6")
sdm_gb <- sql("select FASCIA_SDM_PDISC_M1, churn, count(COD_CLIENTE_CIFRATO)
               from lista_tot_6 
               group by FASCIA_SDM_PDISC_M1, churn")
View(head(sdm_gb,100))

lista_sdm_pdisc <- filter(lista_tot_6, lista_tot_6$churn == 1)
nrow(lista_sdm_pdisc)
# 245.911
pdisc_rate_tot_sdm <- nrow(lista_sdm_pdisc)/nrow(lista_tot_6)
cat(pdisc_rate_tot_sdm)
# 0.06263499


#########################################################################
######################################################################### 
# osservazioni sul periodo (fino al 21 agosto):

createOrReplaceTempView(lista_tot_6, "lista_tot_6")
cd_gb_p <- sql("select fascia_cd, churn_periodo, count(COD_CLIENTE_CIFRATO)
                   from lista_tot_6 
                   group by fascia_cd, churn_periodo")
View(head(cd_gb_p,100))

lista_tot_cd_p <- filter(lista_tot_6, isNotNull(lista_tot_6$fascia_cd))
nrow(lista_tot_cd_p)
# 1.172.333
lista_tot_cd_pdisc_p <- filter(lista_tot_cd_p, lista_tot_cd_p$churn_periodo == 1)
nrow(lista_tot_cd_pdisc_p)
# 44.544
pdisc_rate_tot_cd_p <- nrow(lista_tot_cd_pdisc_p)/nrow(lista_tot_cd_p)
cat(pdisc_rate_tot_cd_p)
# 0.03799603


createOrReplaceTempView(lista_tot_6, "lista_tot_6")
sdm_gb_p <- sql("select FASCIA_SDM_PDISC_M1, churn_periodo, count(COD_CLIENTE_CIFRATO)
               from lista_tot_6 
               group by FASCIA_SDM_PDISC_M1, churn_periodo")
View(head(sdm_gb_p,100))

lista_sdm_pdisc_p <- filter(lista_tot_6, lista_tot_6$churn_periodo == 1)
nrow(lista_sdm_pdisc_p)
# 134.598
pdisc_rate_tot_sdm_p <- nrow(lista_sdm_pdisc_p)/nrow(lista_tot_6)
cat(pdisc_rate_tot_sdm_p)
# 0.03428291

#########################################################################
#########################################################################
# groupby tot

createOrReplaceTempView(lista_tot_5, "lista_tot_5")
gb1 <- sql("select fascia_cd, FASCIA_SDM_PDISC_M1, 
                    count(COD_CLIENTE_CIFRATO) as count_tot, 
                    sum(churn) as count_pdisc
           from lista_tot_5
           group by fascia_cd, FASCIA_SDM_PDISC_M1")
View(head(gb1,100))


lista_tot_pdisc <- filter(lista_tot_6, lista_tot_6$churn == 1)
nrow(lista_tot_pdisc)
# 245.911

createOrReplaceTempView(lista_tot_pdisc, "lista_tot_pdisc")
gb_pdisc <- sql("select fascia_cd, FASCIA_SDM_PDISC_M1, 
                        count(COD_CLIENTE_CIFRATO) as count
               from lista_tot_pdisc
               group by fascia_cd, FASCIA_SDM_PDISC_M1")
View(head(gb_pdisc,1000))

#########################################################################
#########################################################################
# groupby sul periodo (fion al 21 agosto)

lista_tot_pdisc_periodo <- filter(lista_tot_6, lista_tot_6$churn_periodo == 1)
nrow(lista_tot_pdisc_periodo)
# 134.598

createOrReplaceTempView(lista_tot_pdisc_periodo, "lista_tot_pdisc_periodo")
gb_pdisc_periodo <- sql("select fascia_cd, FASCIA_SDM_PDISC_M1, 
                                count(COD_CLIENTE_CIFRATO) as count
                       from lista_tot_pdisc_periodo
                       group by fascia_cd, FASCIA_SDM_PDISC_M1")
View(head(gb_pdisc_periodo,1000))


createOrReplaceTempView(lista_tot_6, "lista_tot_6")
gb_periodo <- sql("select fascia_cd, FASCIA_SDM_PDISC_M1, 
                          count(COD_CLIENTE_CIFRATO) as count
                  from lista_tot_6
                  group by fascia_cd, FASCIA_SDM_PDISC_M1")
View(head(gb_periodo,1000))



##############################################################################################################################################################
## Verifica sovrapposizione settimanale

# path liste complete
path_lista_20180717 <- "/user/stefano.mazzucca/churn_digital/lista_tot_with_cd_20180717.parquet"
path_lista_20180724 <- "/user/stefano.mazzucca/churn_digital/lista_tot_with_cd_20180724.parquet"
path_lista_20180731 <- "/user/stefano.mazzucca/churn_digital/lista_tot_with_cd_20180731.parquet"

path_def_lista_20180717 <- "/user/stefano.mazzucca/churn_digital/lista_tot_def_cd_20180717.parquet"
path_def_lista_20180724 <- "/user/stefano.mazzucca/churn_digital/lista_tot_def_cd_20180724.parquet"
path_def_lista_20180731 <- "/user/stefano.mazzucca/churn_digital/lista_tot_def_cd_20180731.parquet"


lista_20180717 <- read.parquet(path_lista_20180717)
View(head(lista_20180717,100))
nrow(lista_20180717)
# 3.948.528

data_inizio_test <- as.Date('2018-07-17')
data_fine_test <- as.Date('2018-08-21')

lista_20180717 <- select(lista_20180717, "COD_CLIENTE_CIFRATO", "churn", "dat_pdisc", "dat_pdisc_dt", 
                      "FASCIA_SDM_PDISC_M1", "FASCIA_SDM_PDISC_M4", "fascia_cd", "flg_pred_cd")
lista_20180717 <- arrange(lista_20180717, asc(lista_20180717$fascia_cd), asc(lista_20180717$FASCIA_SDM_PDISC_M1))
## TOLGO i clienti che hanno "churnato" prima del 17 luglio
lista_20180717 <- filter(lista_20180717, lista_20180717$dat_pdisc_dt >= data_inizio_test | isNull(lista_20180717$dat_pdisc_dt)) 
lista_20180717 <- withColumn(lista_20180717, "data_test", lit(data_inizio_test))
lista_20180717 <- withColumn(lista_20180717, "gg_dalla_pred_a_effettivo_pdisc", datediff(lista_20180717$dat_pdisc_dt, lista_20180717$data_test))
lista_20180717 <- withColumn(lista_20180717, "churn_periodo", lit(0))
lista_20180717$churn_periodo <- ifelse(isNotNull(lista_20180717$dat_pdisc_dt) & lista_20180717$dat_pdisc_dt <= data_fine_test & lista_20180717$dat_pdisc_dt >= data_inizio_test, 1, 0)
View(head(lista_20180717,100))
nrow(lista_20180717)
# 3.926.096
valdist_extid <- distinct(select(lista_20180717, "COD_CLIENTE_CIFRATO"))
nrow(valdist_extid)
# 3.809.196 (126k clienti che "ballano")

write.parquet(lista_20180717, path_def_lista_20180717)



lista_20180724 <- read.parquet(path_lista_20180724)
View(head(lista_20180724,100))
nrow(lista_20180724)
# 3.944.441

data_inizio_test <- as.Date('2018-07-24')
data_fine_test <- as.Date('2018-08-21')

lista_20180724 <- select(lista_20180724, "COD_CLIENTE_CIFRATO", "churn", "dat_pdisc", "dat_pdisc_dt", 
                         "FASCIA_SDM_PDISC_M1", "FASCIA_SDM_PDISC_M4", "fascia_cd", "flg_pred_cd")
lista_20180724 <- arrange(lista_20180724, asc(lista_20180724$fascia_cd), asc(lista_20180724$FASCIA_SDM_PDISC_M1))
## TOLGO i clienti che hanno "churnato" prima del 24 luglio
lista_20180724 <- filter(lista_20180724, lista_20180724$dat_pdisc_dt >= data_inizio_test | isNull(lista_20180724$dat_pdisc_dt)) 
lista_20180724 <- withColumn(lista_20180724, "data_test", lit(data_inizio_test))
lista_20180724 <- withColumn(lista_20180724, "gg_dalla_pred_a_effettivo_pdisc", datediff(lista_20180724$dat_pdisc_dt, lista_20180724$data_test))
lista_20180724 <- withColumn(lista_20180724, "churn_periodo", lit(0))
lista_20180724$churn_periodo <- ifelse(isNotNull(lista_20180724$dat_pdisc_dt) & lista_20180724$dat_pdisc_dt <= data_fine_test & lista_20180724$dat_pdisc_dt >= data_inizio_test, 1, 0)
View(head(lista_20180724,100))
nrow(lista_20180724)
# 3.896.255
valdist_extid <- distinct(select(lista_20180724, "COD_CLIENTE_CIFRATO"))
nrow(valdist_extid)
# 3.787.826 (108k clienti che "ballano")

write.parquet(lista_20180724, path_def_lista_20180724)



lista_20180731 <- read.parquet(path_lista_20180731)
View(head(lista_20180731,100))
nrow(lista_20180731)
# 3.937.751

data_inizio_test <- as.Date('2018-07-31')
data_fine_test <- as.Date('2018-08-21')

lista_20180731 <- select(lista_20180731, "COD_CLIENTE_CIFRATO", "churn", "dat_pdisc", "dat_pdisc_dt", 
                         "FASCIA_SDM_PDISC_M1", "FASCIA_SDM_PDISC_M4", "fascia_cd", "flg_pred_cd")
lista_20180731 <- arrange(lista_20180731, asc(lista_20180731$fascia_cd), asc(lista_20180731$FASCIA_SDM_PDISC_M1))
## TOLGO i clienti che hanno "churnato" prima del 24 luglio
lista_20180731 <- filter(lista_20180731, lista_20180731$dat_pdisc_dt >= data_inizio_test | isNull(lista_20180731$dat_pdisc_dt)) 
lista_20180731 <- withColumn(lista_20180731, "data_test", lit(data_inizio_test))
lista_20180731 <- withColumn(lista_20180731, "gg_dalla_pred_a_effettivo_pdisc", datediff(lista_20180731$dat_pdisc_dt, lista_20180731$data_test))
lista_20180731 <- withColumn(lista_20180731, "churn_periodo", lit(0))
lista_20180731$churn_periodo <- ifelse(isNotNull(lista_20180731$dat_pdisc_dt) & lista_20180731$dat_pdisc_dt <= data_fine_test & lista_20180731$dat_pdisc_dt >= data_inizio_test, 1, 0)
View(head(lista_20180731,100))
nrow(lista_20180731)
# 3.837.848
valdist_extid <- distinct(select(lista_20180731, "COD_CLIENTE_CIFRATO"))
nrow(valdist_extid)
# 3.745.351 (92k clienti che "ballano")

write.parquet(lista_20180731, path_def_lista_20180731)


######################################################################
lista_def_20180717 <- read.parquet(path_def_lista_20180717)
View(head(lista_def_20180717,100))
nrow(lista_def_20180717)
# 3.926.096

lista_def_20180724 <- read.parquet(path_def_lista_20180724)
View(head(lista_def_20180724,100))
nrow(lista_def_20180724)
# 3.896.255

lista_def_20180731 <- read.parquet(path_def_lista_20180731)
View(head(lista_def_20180731,100))
nrow(lista_def_20180731)
# 3.837.848


lista_def_20180717_1 <- withColumnRenamed(lista_def_20180717, "churn_periodo", "churn_periodo_20180717")
lista_def_20180717_2 <- withColumnRenamed(lista_def_20180717_1, "fascia_cd", "fascia_cd_20180717")
lista_def_20180717_3 <- select(lista_def_20180717_2, "COD_CLIENTE_CIFRATO", "dat_pdisc_dt", "FASCIA_SDM_PDISC_M1", "fascia_cd_20180717", "churn_periodo_20180717")

lista_def_20180724_1 <- withColumnRenamed(lista_def_20180724, "churn_periodo", "churn_periodo_20180724")
lista_def_20180724_2 <- withColumnRenamed(lista_def_20180724_1, "fascia_cd", "fascia_cd_20180724")
lista_def_20180724_3 <- select(lista_def_20180724_2, "COD_CLIENTE_CIFRATO", "dat_pdisc_dt", "FASCIA_SDM_PDISC_M1", "fascia_cd_20180724", "churn_periodo_20180724")

lista_def_20180731_1 <- withColumnRenamed(lista_def_20180731, "churn_periodo", "churn_periodo_20180731")
lista_def_20180731_2 <- withColumnRenamed(lista_def_20180731_1, "fascia_cd", "fascia_cd_20180731")
lista_def_20180731_3 <- select(lista_def_20180731_2, "COD_CLIENTE_CIFRATO", "dat_pdisc_dt", "FASCIA_SDM_PDISC_M1", "fascia_cd_20180731", "churn_periodo_20180731")

# join
createOrReplaceTempView(lista_def_20180717_3, "lista_def_20180717_3")
createOrReplaceTempView(lista_def_20180724_3, "lista_def_20180724_3")
createOrReplaceTempView(lista_def_20180731_3, "lista_def_20180731_3")

lista_def_tot <- sql("select distinct t1.*, t2.fascia_cd_20180724, t2.churn_periodo_20180724
                     from lista_def_20180717_3 t1
                     left join lista_def_20180724_3 t2
                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO ")
View(head(lista_def_tot,100))
nrow(lista_def_tot)
# 3.924.346

createOrReplaceTempView(lista_def_tot, "lista_def_tot")
lista_def_tot_2 <- sql("select distinct t1.*, t3.fascia_cd_20180731, t3.churn_periodo_20180731
                       from lista_def_tot t1
                       left join lista_def_20180731_3 t3
                        on t1.COD_CLIENTE_CIFRATO = t3.COD_CLIENTE_CIFRATO ")
View(head(lista_def_tot_2,100))
nrow(lista_def_tot_2)
# 3.935.624


write.parquet(lista_def_tot_2, "/user/stefano.mazzucca/churn_digital/lista_tot_def.parquet", mode = "overwrite")

###########################################################################################
###########################################################################################

lista_def_tot <- read.parquet("/user/stefano.mazzucca/churn_digital/lista_tot_def.parquet")
View(head(lista_def_tot,100))
nrow(lista_def_tot)
# 3.935.624

lista_filt_def_tot <- filter(lista_def_tot, isNotNull(lista_def_tot$fascia_cd_20180717) | isNotNull(lista_def_tot$fascia_cd_20180724) | 
                               isNotNull(lista_def_tot$fascia_cd_20180731))
View(head(lista_filt_def_tot,1000))
nrow(lista_filt_def_tot)
# 1.298.396

## overlap tra liste
createOrReplaceTempView(lista_filt_def_tot, "lista_filt_def_tot")
gb_1_lista_def <- sql("select fascia_cd_20180717, fascia_cd_20180724,
                              count(COD_CLIENTE_CIFRATO) as count
                      from lista_filt_def_tot
                      group by fascia_cd_20180717, fascia_cd_20180724")
gb_1_lista_def <- arrange(gb_1_lista_def, asc(gb_1_lista_def$fascia_cd_20180717), asc(gb_1_lista_def$fascia_cd_20180724))
View(head(gb_1_lista_def,1000))
write.df(repartition( gb_1_lista_def, 1), path = "/user/stefano.mazzucca/churn_digital/lista_gb_1.csv", "csv", sep=";", mode = "overwrite", header=TRUE)

createOrReplaceTempView(lista_filt_def_tot, "lista_filt_def_tot")
gb_2_lista_def <- sql("select fascia_cd_20180717, fascia_cd_20180731,
                              count(COD_CLIENTE_CIFRATO) as count
                      from lista_filt_def_tot
                      group by fascia_cd_20180717, fascia_cd_20180731")
gb_2_lista_def <- arrange(gb_2_lista_def, asc(gb_2_lista_def$fascia_cd_20180717), asc(gb_2_lista_def$fascia_cd_20180731))
View(head(gb_2_lista_def,1000))
write.df(repartition( gb_2_lista_def, 1), path = "/user/stefano.mazzucca/churn_digital/lista_gb_2.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


createOrReplaceTempView(lista_filt_def_tot, "lista_filt_def_tot")
gb_3_lista_def <- sql("select fascia_cd_20180724, fascia_cd_20180731,
                              count(COD_CLIENTE_CIFRATO) as count
                      from lista_filt_def_tot
                      group by fascia_cd_20180724, fascia_cd_20180731")
gb_3_lista_def <- arrange(gb_3_lista_def, asc(gb_3_lista_def$fascia_cd_20180724), asc(gb_3_lista_def$fascia_cd_20180731))
View(head(gb_3_lista_def,1000))
write.df(repartition( gb_3_lista_def, 1), path = "/user/stefano.mazzucca/churn_digital/lista_gb_3.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




#chiudi sessione
sparkR.stop()
