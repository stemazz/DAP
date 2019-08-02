

source("connection_R.R")
options(scipen = 10000)


path_1 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_181216_2.csv" , source = "csv", header = "true", delimiter = ";")
path_2 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_181201_2.csv" , source = "csv", header = "true", delimiter = ";")
path_3 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_181116_2.csv" , source = "csv", header = "true", delimiter = ";")
path_4 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_181101_2.csv" , source = "csv", header = "true", delimiter = ";")
path_5 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_181016_2.csv" , source = "csv", header = "true", delimiter = ";")
path_6 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_181001_2.csv" , source = "csv", header = "true", delimiter = ";")
path_7 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180916_2.csv" , source = "csv", header = "true", delimiter = ";")
path_8 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180901_2.csv" , source = "csv", header = "true", delimiter = ";")
path_9 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180816_2.csv" , source = "csv", header = "true", delimiter = ";")
path_10 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180801_2.csv" , source = "csv", header = "true", delimiter = ";")
path_11 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180716_2.csv" , source = "csv", header = "true", delimiter = ";")
path_12 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180701_2.csv" , source = "csv", header = "true", delimiter = ";")
path_13 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180616_2.csv" , source = "csv", header = "true", delimiter = ";")
path_14 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180601_2.csv" , source = "csv", header = "true", delimiter = ";")
path_15 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180516_2.csv" , source = "csv", header = "true", delimiter = ";")
path_16 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180501_2.csv" , source = "csv", header = "true", delimiter = ";")
path_17 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180416_2.csv" , source = "csv", header = "true", delimiter = ";")
path_18 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180401_2.csv" , source = "csv", header = "true", delimiter = ";")
path_19 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180316.csv" , source = "csv", header = "true", delimiter = ";")
path_20 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180301.csv" , source = "csv", header = "true", delimiter = ";")
path_21 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180216.csv" , source = "csv", header = "true", delimiter = ";")
path_22 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180201.csv" , source = "csv", header = "true", delimiter = ";")
path_23 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180116.csv" , source = "csv", header = "true", delimiter = ";")
path_24 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_180101.csv" , source = "csv", header = "true", delimiter = ";")
path_25 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_171216.csv" , source = "csv", header = "true", delimiter = ";")
path_26 <-read.df("/user/stefano.mazzucca/churn_digital_ver/ver_csv_171201.csv" , source = "csv", header = "true", delimiter = ";")



join_COD_CLIENTE_CIFRATO <- function(df1, df2){
  df2 <- withColumnRenamed(df2, "COD_CLIENTE_CIFRATO", "COD_CLIENTE_CIFRATO_y")
  df2 <- withColumnRenamed(df2, "DAT_PRIMA_ATTIV_CNTR_dt", "DAT_PRIMA_ATTIV_CNTR_dt_y")
  df2 <- withColumnRenamed(df2, "decile_cdm", "decile_cdm_y")
  df2 <- withColumnRenamed(df2, "score_cdm", "score_cdm_y")
  df2 <- withColumnRenamed(df2, "fascia_1_sdm", "fascia_1_sdm_y")
  df2 <- withColumnRenamed(df2, "score_1_sdm", "score_1_sdm_y")
  df2 <- withColumnRenamed(df2, "fascia_4_sdm", "fascia_4_sdm_y")
  df2 <- withColumnRenamed(df2, "score_4_sdm", "score_4_sdm_y")
  df2 <- withColumnRenamed(df2, "pdisc1", "pdisc1_y")
  df2 <- withColumnRenamed(df2, "pdisc2", "pdisc2_y")
  
  df1 <- merge(df1, df2,  by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_y")
  df1 <- drop(df1, "COD_CLIENTE_CIFRATO_y")
  df1 <- drop(df1, "DAT_PRIMA_ATTIV_CNTR_dt_y")
  df1 <- drop(df1, "decile_cdm_y")
  df1 <- drop(df1, "score_cdm_y")
  df1 <- drop(df1,"fascia_1_sdm_y") 
  df1 <- drop(df1, "score_1_sdm_y")
  df1 <- drop(df1, "fascia_4_sdm_y") 
  df1 <- drop(df1, "score_4_sdm_y") 
  df1 <- drop(df1, "pdisc1_y")
  df1 <- drop(df1, "pdisc2_y")
  
  return(df1)
}


join1 <- join_COD_CLIENTE_CIFRATO(path_1, path_2) 
View(head(join1,100))
nrow(join1)
# 3.807.416

join1 <- join_COD_CLIENTE_CIFRATO(join1, path_2) 

join1 <- join_COD_CLIENTE_CIFRATO(join1, path_3) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_4) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_5) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_6) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_7) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_8) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_9) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_10) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_11) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_12) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_13) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_14) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_15) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_16) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_17) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_18) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_19) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_20) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_21) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_22) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_23) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_24) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_25) 
join1 <- join_COD_CLIENTE_CIFRATO(join1, path_26) 

printSchema(join1)

write.parquet(join1, "/user/stefano.mazzucca/churn_digital_ver/verifica_count_extid_1anno.parquet")


prova <- read.parquet("/user/stefano.mazzucca/churn_digital_ver/verifica_count_extid_1anno.parquet")

## non ha girato!





