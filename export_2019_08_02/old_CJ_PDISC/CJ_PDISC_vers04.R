
# CJ_PDISC_vers04

#apri sessione
source("connection_R.R")
options(scipen = 1000)


# DATA EXPLORATION cod_contratto MISSING from v1.1 WAVERD_MA

pdisc_path <- "/user/stefano.mazzucca/WAVERD_PDISC_interaction_history_V2_Missing.csv"
pdisc_waverd_missing <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
View(head(pdisc_waverd_missing))
nrow(pdisc_waverd_missing)
# 12.173

valdist_waverd_missing_cntr <- distinct(select(pdisc_waverd_missing, "CODICE_CONTRATTO__C"))
nrow(valdist_waverd_missing_cntr)
# 3.563

valdist_waverd_missing <- distinct(select(pdisc_waverd_missing, "CODICE_CLIENTE__C"))
nrow(valdist_waverd_missing)
# 3.559


pdisc_waverd_missing <- withColumn(pdisc_waverd_missing,"CREATEDDATE",cast(cast(unix_timestamp(pdisc_waverd_missing$CREATEDDATE, 'dd/MM/yyyy HH:mm:ss'), 'timestamp'), 'date'))
View(head(pdisc_waverd_missing))



## DISTRIBUZIONI

createOrReplaceTempView(pdisc_waverd_missing,"pdisc_waverd_missing")

waverd_missing_data <- sql("select *, 
                              case when CREATEDDATE >= '2017-10-01' and CREATEDDATE <= '2017-10-31' then 'Ott' 
                              when CREATEDDATE >= '2017-11-01' and CREATEDDATE <= '2017-11-30' then 'Nov' 
                              else 'altro' end as mese_data
                           from pdisc_waverd_missing")
View(head(waverd_missing_data,100))


createOrReplaceTempView(waverd_missing_data,"waverd_missing_data")

waverd_missing_data_distr <- sql("select mese_data, count(*)
                           from waverd_missing_data
                           group by mese_data")
View(head(waverd_missing_data_distr,100))


waverd_missing_data_NA <- sql("select *
                              from waverd_missing_data
                              where mese_data = 'altro'")
View(head(waverd_missing_data_NA,100))
nrow(waverd_missing_data_NA)
# 26



##### JOIN con la base_pdisc "base_pdisc_digital"

createOrReplaceTempView(waverd_missing_data,"waverd_missing_data")
createOrReplaceTempView(base_pdisc_digital,"base_pdisc_digital")

join_pdisc_missing <- sql("select distinct t1.*, t2.DES_STATO_BUSINESS, t2.CREATEDDATE
                     from base_pdisc_digital t1
                     inner join waverd_missing_data t2
                      on t1.COD_CLIENTE_FRUITORE = t2.CODICE_CLIENTE__C")
View(head(join_pdisc_missing,100))
nrow(join_pdisc_missing)
# 3.655



## ALTRE DISTRIBUZIONI

## DES_UTLIMA_CAUSALE_CESSAZIONE

createOrReplaceTempView(join_pdisc_missing,"join_pdisc_missing")

waverd_missing_des_cessazione <- sql("select DES_ULTIMA_CAUSALE_CESSAZIONE, count(COD_CLIENTE_CIFRATO)
                            from join_pdisc_missing
                            group by DES_ULTIMA_CAUSALE_CESSAZIONE")
View(head(waverd_missing_des_cessazione,100))


## DES_AREA_NIELSEN_FRU

waverd_missing_des_area <- summarize(groupBy(join_pdisc_missing, join_pdisc_missing$DES_AREA_NIELSEN_FRU), COUNT = count(join_pdisc_missing$COD_CLIENTE_CIFRATO))
View(head(waverd_missing_des_area,100))


## DES_PACCHETTO_POSS_FROM

waverd_missing_pacchetto_poss <- summarize(groupBy(join_pdisc_missing, join_pdisc_missing$DES_PACCHETTO_POSS_FROM), COUNT = count(join_pdisc_missing$COD_CLIENTE_CIFRATO))
View(head(waverd_missing_pacchetto_poss,100))


## DIGITALIZZAZIONE_BIN

waverd_missing_dig_bin <- summarize(groupBy(join_pdisc_missing, join_pdisc_missing$digitalizzazione_bin), COUNT = count(join_pdisc_missing$COD_CLIENTE_CIFRATO))
View(head(waverd_missing_dig_bin,100))




###########################################################################################################################################################################

# DATA EXPLORATION cod_contratto MISSING

pdisc_path <- "/user/stefano.mazzucca/missing7k_pdisc_Non_presenti_su_interaction_hisotry.csv"
pdisc_missing <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
View(head(pdisc_missing))
nrow(pdisc_missing)
# 3.531 cod_contratto


## JOIN con la base pdisc "base_pdisc_digital"

createOrReplaceTempView(pdisc_missing,"pdisc_missing")
createOrReplaceTempView(base_pdisc_digital,"base_pdisc_digital")

join_missing <- sql("select distinct t1.*, t2.COD_CONTRATTO
                     from base_pdisc_digital t1
                     inner join pdisc_missing t2
                      on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(join_missing,100))
nrow(join_missing)
# 3.531


## DISTRIBUZIONI

createOrReplaceTempView(join_missing,"join_missing")

distr_missing_data <- sql("select *, 
                              case 
                              when data_ingr_pdisc_formdate >= '2017-01-01' and data_ingr_pdisc_formdate <= '2017-01-31' then 'Nov'
                              when data_ingr_pdisc_formdate >= '2017-02-01' and data_ingr_pdisc_formdate <= '2017-02-29' then 'Nov'
                              when data_ingr_pdisc_formdate >= '2017-03-01' and data_ingr_pdisc_formdate <= '2017-03-31' then 'Mar'
                              when data_ingr_pdisc_formdate >= '2017-04-01' and data_ingr_pdisc_formdate <= '2017-04-30' then 'Apr'
                              when data_ingr_pdisc_formdate >= '2017-05-01' and data_ingr_pdisc_formdate <= '2017-05-31' then 'Mag'
                              when data_ingr_pdisc_formdate >= '2017-06-01' and data_ingr_pdisc_formdate <= '2017-06-30' then 'Giu'
                              when data_ingr_pdisc_formdate >= '2017-07-01' and data_ingr_pdisc_formdate <= '2017-07-31' then 'Lug'
                              when data_ingr_pdisc_formdate >= '2017-08-01' and data_ingr_pdisc_formdate <= '2017-08-31' then 'Ago'
                              when data_ingr_pdisc_formdate >= '2017-09-01' and data_ingr_pdisc_formdate <= '2017-09-30' then 'Set' 
                              when data_ingr_pdisc_formdate >= '2017-10-01' and data_ingr_pdisc_formdate <= '2017-10-31' then 'Ott' 
                              when data_ingr_pdisc_formdate >= '2017-11-01' and data_ingr_pdisc_formdate <= '2017-11-30' then 'Nov' 
                              when data_ingr_pdisc_formdate >= '2017-12-01' and data_ingr_pdisc_formdate <= '2017-12-31' then 'Dic'
                              else 'altro' end as mese_data
                           from join_missing")

createOrReplaceTempView(distr_missing_data,"distr_missing_data")

distr_missing_data <- sql("select mese_data, count(*)
                           from distr_missing_data
                           group by mese_data")
View(head(distr_missing_data,100))


## DES_UTLIMA_CAUSALE_CESSAZIONE

missing_des_cessazione <- sql("select DES_ULTIMA_CAUSALE_CESSAZIONE, count(COD_CLIENTE_CIFRATO)
                            from join_missing
                            group by DES_ULTIMA_CAUSALE_CESSAZIONE")
View(head(missing_des_cessazione,100))


## DES_AREA_NIELSEN_FRU

missing_des_area <- summarize(groupBy(join_missing, join_missing$DES_AREA_NIELSEN_FRU), COUNT = count(join_missing$COD_CLIENTE_CIFRATO))
View(head(missing_des_area,100))


## DES_PACCHETTO_POSS_FROM

missing_pacchetto_poss <- summarize(groupBy(join_missing, join_missing$DES_PACCHETTO_POSS_FROM), COUNT = count(join_missing$COD_CLIENTE_CIFRATO))
View(head(missing_pacchetto_poss,100))


## DIGITALIZZAZIONE_BIN

missing_dig_bin <- summarize(groupBy(join_missing, join_missing$digitalizzazione_bin), COUNT = count(join_missing$COD_CLIENTE_CIFRATO))
View(head(missing_dig_bin,100))









#chiudi sessione
sparkR.stop()
