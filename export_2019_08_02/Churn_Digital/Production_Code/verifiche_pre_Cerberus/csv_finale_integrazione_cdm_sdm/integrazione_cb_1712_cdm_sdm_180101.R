
## join integrazione cdm sdm con logiche trimestrali ##
## es: CB 2017-12 con score cdm e sdm a 2018-01-02-03


source("connection_R.R")
options(scipen = 10000)


path_cb <- "/user/stefano.mazzucca/churn_digital_ver/ver_csv_171201.csv"
path_cdm_sdm <- "/user/stefano.mazzucca/churn_digital_ver/ver_csv_180101.csv"

path_finale_csv <- "/user/stefano.mazzucca/churn_digital_ver/integrazione_cb_1712_cdm_sdm_1801.csv"



cb1712 <- read.df(path_cb, source = "csv", header = "true", delimiter = ";")
View(head(cb1712, 100))
nrow(cb1712)
# 3.910.288

csv <- read.df(path_cdm_sdm, source = "csv", header = "true", delimiter = ";")
View(head(csv, 100))
nrow(csv)
# 3.910.288

createOrReplaceTempView(cb1712, "cb1712")
createOrReplaceTempView(csv, "csv")

join_finale <- sql("select distinct t1.COD_CLIENTE_CIFRATO, t1.DAT_PRIMA_ATTIV_CNTR_dt,
                                  t2.decile_cdm, t2.score_cdm, 
                                  t2.fascia_1_sdm, t2.score_1_sdm, t2.fascia_4_sdm, t2.score_4_sdm, 
                                  t2.pdisc1, t2.pdisc2
                   from cb1712 t1
                   inner join csv t2
                    on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and 
                        t1.DAT_PRIMA_ATTIV_CNTR_dt = t2.DAT_PRIMA_ATTIV_CNTR_dt")
View(head(join_finale,100))
nrow(join_finale)

write.df(repartition( join_finale, 1), path = path_finale_csv, "csv", sep=";", mode = "overwrite", header=TRUE)



#chiudi sessione
sparkR.stop()
