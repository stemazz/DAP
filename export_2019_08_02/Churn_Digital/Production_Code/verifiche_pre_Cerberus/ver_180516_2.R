
## Verifica pre Cerberus proj ##

source("connection_R.R")
options(scipen = 10000)


path_cb <- "/user/stefano.mazzucca/churn_digital_ver/1805.csv"
path_lista_output <- "/user/stefano.mazzucca/churn_digital_ver/output_modello_CD_180516_2.csv"

# path_ver <- "/user/stefano.mazzucca/churn_digital_ver/ver_180516_2.csv"
path_ver_csv <- "/user/stefano.mazzucca/churn_digital_ver/ver_csv_180516_2.csv"
path_csv1 <- "/user/stefano.mazzucca/churn_digital_ver/csv_pdisc_180516_2.csv"


cb <- read.df(path_cb, source = "csv", header = "true", delimiter = ",")
cb <- filter(cb, isNotNull(cb$COD_CLIENTE_CIFRATO))
cb <- withColumn(cb, "DAT_PRIMA_ATTIV_CNTR_dt", cast(cast(unix_timestamp(cb$DAT_PRIMA_ATTIV_CNTR, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))
View(head(cb,100))
nrow(cb)
# 3.961.801


list_cdm_1 <- read.df(path_lista_output, source = "csv", header = "true", delimiter = ";")
View(head(list_cdm_1,100))
nrow(list_cdm_1)
# 1.562.298

createOrReplaceTempView(cb, "cb")
createOrReplaceTempView(list_cdm_1, "list_cdm_1")
list <- sql("select distinct t1.COD_CLIENTE_CIFRATO, t1.DAT_PRIMA_ATTIV_CNTR_dt, 
                            t2.decile as decile_cdm, t2.prediction as score_cdm, 
            t1.fascia1 as fascia_1_sdm, t1.sdm1 as score_1_sdm,
            t1.fascia4 as fascia_4_sdm, t1.sdm4 as score_4_sdm, 
            t1.pdisc1, t1.pdisc2
            from cb t1
            left join list_cdm_1 t2
            on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and 
            t1.DAT_PRIMA_ATTIV_CNTR_dt = t2.DAT_PRIMA_ATTIVAZIONE_dt ")
# View(head(list,100))
# nrow(list)
# 

# write.parquet(list, path_ver)
write.df(repartition( list, 1), path = path_ver_csv, "csv", sep=";", mode = "overwrite", header=TRUE)


# ver_171201 <- read.parquet(path_ver)
ver <- read.df(path_ver_csv, source = "csv", header = "true", delimiter = ";")
View(head(ver, 100))
nrow(ver)
# 3.961.281


## PIVOT ## 

pivot_tot <- count(pivot(groupBy(ver, "decile_cdm"), "fascia_1_sdm"))
pivot_tot <- withColumn(pivot_tot, "decile_cdm",cast(pivot_tot$decile_cdm, "integer"))
pivot_tot <- arrange(pivot_tot, asc(pivot_tot$decile_cdm))
# View(head(pivot_tot,100))
pivot_tot_ <- as.data.frame(pivot_tot)
pivot_tot_ <- pivot_tot_[order(pivot_tot_$decile_cdm),c(1,2,4,5,6,7,8,9,10,3)]
View(head(pivot_tot_,100))


pdisc <- filter(ver, isNotNull(ver$pdisc2))
pivot_pdisc <- count(pivot(groupBy(pdisc, "decile_cdm"), "fascia_1_sdm"))
pivot_pdisc <- withColumn(pivot_pdisc, "decile_cdm",cast(pivot_pdisc$decile_cdm, "integer"))
pivot_pdisc <- arrange(pivot_pdisc, asc(pivot_pdisc$decile_cdm))
# View(head(pivot_pdisc,100))
pivot_pdisc_ <- as.data.frame(pivot_pdisc)
pivot_pdisc_ <- pivot_pdisc_[order(pivot_pdisc_$decile_cdm),c(1,2,4,5,6,7,8,9,10,3)]
View(head(pivot_pdisc_,100))



rate <- withColumn(ver, "churn_periodo", ifelse(isNotNull(ver$pdisc2), 1, 0))
rate_cdm <- summarize(groupBy(rate, "decile_cdm", "churn_periodo"), count = count(rate$COD_CLIENTE_CIFRATO))
View(head(rate_cdm,100))
rate_sdm <- summarize(groupBy(rate, "fascia_1_sdm", "churn_periodo"), count = count(rate$COD_CLIENTE_CIFRATO))
View(head(rate_sdm,100))




oggetto1 <- filter(ver, isNotNull(ver$pdisc2))
View(head(oggetto1,100))
nrow(oggetto1)
# 22.977








#chiudi sessione
sparkR.stop()
