
## Digitalizzazione info siti interni


#apri sessione
source("connection_R.R")
options(scipen = 1000)



## Associazione skyid + cluster
predictions_visits <- read.parquet("/user/riccardo.motta/Aggiornamento_Digitalizzazione_20180301_20180831/predictions4.parquet")
View(head(predictions_visits,100))
nrow(predictions_visits)
# 3.006.565
createOrReplaceTempView(predictions_visits, "predictions_visits")
gb_cl <- sql("select prediction, count(label)
             from predictions_visits
             group by prediction")
View(head(gb_cl,100))


predictions_device <- read.parquet("hdfs:///user/riccardo.motta/Aggiornamento_Digitalizzazione_20180301_20180831/predictions_device.parquet")
View(head(predictions_device,100))
nrow(predictions_device)
# 3.006.565



## Altri dati

## kpi raw calcolati dal dap
tmp_matrice <- read.parquet("/user/riccardo.motta/Aggiornamento_Digitalizzazione_20180301_20180831/tmp_matrice1.parquet")
View(head(tmp_matrice,100))
nrow(tmp_matrice)
# 3.006.565


## kpi normalizzati
result <- read.parquet("/user/riccardo.motta/Aggiornamento_Digitalizzazione_20180301_20180831/result1.parquet")
View(head(result,100))
nrow(result)
# 3.006.565


## modello clustering (?)
# model <- read.ml("/user/riccardo.motta/Aggiornamento_Digitalizzazione_20180301_20180831/model5.parquet")


## dati elaborati da passare al modello
parsed_data <- read.parquet("/user/riccardo.motta/Aggiornamento_Digitalizzazione_20180301_20180831/parsedData.parquet")
View(head(parsed_data,100))
nrow(parsed_data)
# 3.006.565






#chiudi sessione
sparkR.stop()
