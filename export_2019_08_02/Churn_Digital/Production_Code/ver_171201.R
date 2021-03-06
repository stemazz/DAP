
## Verifica pre Cerberus proj ##

source("connection_R.R")
options(scipen = 10000)


path_cb <- "/user/stefano.mazzucca/churn_digital_ver/1712.csv"
path_lista_output <- "/user/stefano.mazzucca/churn_digital_ver/output_modello_CD_171201.csv"

# path_ver <- "/user/stefano.mazzucca/churn_digital_ver/ver_171201.csv"
path_ver_csv <- "/user/stefano.mazzucca/churn_digital_ver/ver_csv_171201.csv"
path_csv1 <- "/user/stefano.mazzucca/churn_digital_ver/csv_pdisc_171201.csv"


cb_1712 <- read.df(path_cb, source = "csv", header = "true", delimiter = ",")
cb_1712 <- filter(cb_1712, isNotNull(cb_1712$COD_CLIENTE_CIFRATO))
cb_1712 <- withColumn(cb_1712, "DAT_PRIMA_ATTIV_CNTR_dt", cast(cast(unix_timestamp(cb_1712$DAT_PRIMA_ATTIV_CNTR, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))
View(head(cb_1712,100))
nrow(cb_1712)
# 3.911.034


list_cdm_1712_1 <- read.df(path_lista_output, source = "csv", header = "true", delimiter = ";")
View(head(list_cdm_1712_1,100))
nrow(list_cdm_1712_1)
# 1.520.899

createOrReplaceTempView(cb_1712, "cb_1712")
createOrReplaceTempView(list_cdm_1712_1, "list_cdm_1712_1")
list_171201 <- sql("select distinct t1.COD_CLIENTE_CIFRATO, t1.DAT_PRIMA_ATTIV_CNTR_dt, t2.decile as decile_cdm, t1.fascia1 as fascia_1_sdm, t1.fascia4 as fascia_4_sdm,
                            t1.pdisc1, t1.pdisc2
                   from cb_1712 t1
                   left join list_cdm_1712_1 t2
                   on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and 
                      t1.DAT_PRIMA_ATTIV_CNTR_dt = t2.DAT_PRIMA_ATTIVAZIONE_dt ")
# View(head(list_171201,100))
# nrow(list_171201)
# # 3.909.844

# write.parquet(list_171201, path_ver)
write.df(repartition( list_171201, 1), path = path_ver_csv, "csv", sep=";", mode = "overwrite", header=TRUE)


# ver_171201 <- read.parquet(path_ver)
ver_171201 <- read.df(path_ver_csv, source = "csv", header = "true", delimiter = ";")
View(head(ver_171201, 100))
nrow(ver_171201)
# 3.909.844


## PIVOT ## 

pivot_tot_171201 <- count(pivot(groupBy(ver_171201, "decile_cdm"), "fascia_1_sdm"))
pivot_tot_171201 <- withColumn(pivot_tot_171201, "decile_cdm",cast(pivot_tot_171201$decile_cdm, "integer"))
pivot_tot_171201 <- arrange(pivot_tot_171201, asc(pivot_tot_171201$decile_cdm))
# View(head(pivot_tot_171201,100))
pivot_tot_171201_ <- as.data.frame(pivot_tot_171201)
pivot_tot_171201_ <- pivot_tot_171201_[order(pivot_tot_171201_$decile_cdm),c(1,2,4,5,6,7,8,9,10,3)]
View(head(pivot_tot_171201_,100))


pdisc_171201 <- filter(ver_171201, isNotNull(ver_171201$pdisc1))
pivot_pdisc_171201 <- count(pivot(groupBy(pdisc_171201, "decile_cdm"), "fascia_1_sdm"))
pivot_pdisc_171201 <- withColumn(pivot_pdisc_171201, "decile_cdm",cast(pivot_pdisc_171201$decile_cdm, "integer"))
pivot_pdisc_171201 <- arrange(pivot_pdisc_171201, asc(pivot_pdisc_171201$decile_cdm))
# View(head(pivot_pdisc_171201,100))
pivot_pdisc_171201_ <- as.data.frame(pivot_pdisc_171201)
pivot_pdisc_171201_ <- pivot_pdisc_171201_[order(pivot_pdisc_171201_$decile_cdm),c(1,2,4,5,6,7,8,9,10,3)]
View(head(pivot_pdisc_171201_,100))













oggetto1 <- filter(ver_171201, isNotNull(ver_171201$pdisc1))
View(head(oggetto1,100))
nrow(oggetto1)
# 13.264

write.df(repartition( oggetto1, 1), path = path_csv1, "csv", sep=";", mode = "overwrite", header=TRUE)







#chiudi sessione
sparkR.stop()
