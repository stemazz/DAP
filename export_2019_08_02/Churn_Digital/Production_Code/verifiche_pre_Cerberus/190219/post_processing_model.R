
## post_elaborazione_modello

source("connection_R.R")
options(scipen = 10000)


# path_lista_output <- "/user/stefano.mazzucca/churn_digital_ver/output_modello_CD_190218.csv"
# path_full <- "/user/stefano.mazzucca/churn_digital_ver/2019_02_19/full_201902.parquet"

## Path in SCRITTURA
path_lista_finale <- "/user/stefano.mazzucca/churn_digital_ver/output_finale_modello_CD_190218.csv"


lista_output <- read.df(path_lista_output, source = "csv", header = "true", delimiter = ";")
# nrow(lista_output)
# 1.382.812

createOrReplaceTempView(lista_output, "lista_output")
calcolo_variabili <- sql("select min(prediction) as min_pred, (max(prediction)-min(prediction)) as norm_pred
                          from lista_output")
View(head(calcolo_variabili,100))
# min_pred      norm_pred
# 0.02467003    0.9939041

## Prendere le variabili e sostituirle nella query SQL

lista_output <- sql("select COD_CLIENTE_CIFRATO, DAT_PRIMA_ATTIVAZIONE_dt, 
                              (prediction - 0.02467003)/0.9939041 as prediction_norm,
                              decile
                     from lista_output")
View(head(lista_output,100))
nrow(lista_output)
# 1.382.812


full <- read.parquet(path_full)
# nrow(full)
# 4.439.462

createOrReplaceTempView(lista_output, "lista_output")
createOrReplaceTempView(full, "full")
lista_finale <- sql("select distinct t1.*, t2.COD_CONTRATTO
                    from lista_output t1
                    left join full t2
                    on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO 
                      and t1.DAT_PRIMA_ATTIVAZIONE_dt = t2.DAT_PRIMA_ATTIV_CNTR_dt")
View(head(lista_finale,100))
nrow(lista_finale)
# 1.383.740

write.df(repartition( lista_finale, 1), path = path_lista_finale, "csv", sep=";", mode = "overwrite", header=TRUE)



