
# CJ_PDISC_vers05

#apri sessione
source("connection_R.R")
options(scipen = 1000)


# Base PDISC estratta da SAS (senza codice cifrato)

pdisc_path <- "/user/stefano.mazzucca/base_pdisc_clear.csv"
pdisc_clear <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
View(head(pdisc_clear))
nrow(pdisc_clear)
# 38.150


chiavi <- read.parquet("hdfs:///STAGE/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2.parquet")
View(head(chiavi,100))


createOrReplaceTempView(pdisc_clear,"pdisc_clear")
createOrReplaceTempView(chiavi,"chiavi")

join_base_2 <- sql("select distinct t1.*, t2.COD_CLIENTE_CIFRATO
                   from pdisc_clear t1
                   left join chiavi t2
                   on t1.COD_CLIENTE_FRUITORE = t2.COD_CLIENTE")
View(head(join_base_2,100))
nrow(join_base_2)
# 38.150


base_pdisc_2 <- filter(join_base_2, "COD_CLIENTE_CIFRATO is not NULL")
View(head(base_pdisc_2,100))
nrow(base_pdisc_2)
# 37.976


base_pdisc_3 <- withColumn(base_pdisc_2,"data_ingr_pdisc_formdate",cast(cast(unix_timestamp(base_pdisc_2$DATA_INGR_PDISC, 'ddMMMyy'), 'timestamp'), 'date'))
View(head(base_pdisc_3,100))
nrow(base_pdisc_3)
# 37.976



## Join tra la base pdisc e i valore di digitalizzaizone degli utenti

base_score_digital <- read.parquet("hdfs:///user/alessandro/clusterfinalscore_conApp.parquet")

View(head(base_score_digital, 100))
nrow(base_score_digital)
# 4.325.832

## Calcolo la "digitalizzazione_bin" a seconda del livello_digitalizzazione

createOrReplaceTempView(base_score_digital, "base_score_digital")

base_score_digital_2 <- sql("select distinct *
                            from base_score_digital")
nrow(base_score_digital_2)
# 4.178.841

base_liv_digital <- sql("select distinct *,
                        case when livello_digitalizzazione = 0 then 0 
                        when livello_digitalizzazione > 0 and livello_digitalizzazione <=3 then 1 
                        when livello_digitalizzazione > 3 and livello_digitalizzazione <=6 then 2 
                        when livello_digitalizzazione > 6 and livello_digitalizzazione <=9 then 3 
                        when livello_digitalizzazione > 9 and livello_digitalizzazione <=12 then 4 
                        when livello_digitalizzazione > 12 and livello_digitalizzazione <=15 then 5
                        when livello_digitalizzazione > 15 and livello_digitalizzazione <=18 then 6
                        when livello_digitalizzazione > 18 and livello_digitalizzazione <=21 then 7
                        when livello_digitalizzazione > 21 and livello_digitalizzazione <=24 then 8
                        when livello_digitalizzazione > 24 and livello_digitalizzazione <=27 then 9
                        when livello_digitalizzazione > 27 and livello_digitalizzazione <=30 then 10
                        else NULL end as digitalizzazione_bin
                        from base_score_digital
                        ")
View(head(base_liv_digital,100))
nrow(base_liv_digital)
# 4.178.841 (ceck OK)


createOrReplaceTempView(base_pdisc_3,"base_pdisc_3")
createOrReplaceTempView(base_liv_digital,"base_liv_digital")

base_pdisc_digital <- sql("select t1.*, t2.digitalizzazione_bin
                          from base_pdisc_3 t1
                          left join base_liv_digital t2
                          on t1.COD_CLIENTE_CIFRATO = t2.skyid")
View(head(base_pdisc_digital, 100))
nrow(base_pdisc_digital)
# 37.976 





## Salvataggio del file in .parquet

write.parquet(base_pdisc_digital,"/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet") # <<<<------------------------ BASE DEFINITIVA utilizzata per agganciarci i cookies



# 
# ## Lista PDISC ricevuta da MARETING AUTOMATION (PRIMA VERSIONE)
# 
# pdisc_path <- "/user/stefano.mazzucca/WAVERD_PDISC_ma_completo.csv"
# pdisc_waverd <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
# View(head(pdisc_waverd,100))
# nrow(pdisc_waverd)
# # 351.554
# 
# createOrReplaceTempView(pdisc_waverd,"pdisc_waverd")
# valdist_waverd <- sql("select distinct CODICE_CLIENTE__C
#                       from pdisc_waverd")
# View(head(valdist_waverd))
# nrow(valdist_waverd)
# # 113.272
# 
# 
# 
# ## Join tra la lista_marketing_automation e la base_pdisc "base_pdisc_digital"
# 
# createOrReplaceTempView(pdisc_waverd,"pdisc_waverd")
# createOrReplaceTempView(base_pdisc_digital,"base_pdisc_digital")
# 
# join_pdisc_ma <- sql("select distinct t1.*, t2.DES_STATO_BUSINESS
#                      from base_pdisc_digital t1
#                      inner join pdisc_waverd t2
#                      on t1.COD_CLIENTE_FRUITORE = t2.CODICE_CLIENTE__C")
# View(head(join_pdisc_ma,100))
# nrow(join_pdisc_ma)
# # 30.890 (solo t1)
# # 87.664 (t1 + DES_STATO_BUSINESS + CANALE__C)
# # 31.219 (rispetto ai 37.984 si perdono: 7.119 clienti)
# 
# valdist_pdisc_ma <- distinct(select(join_pdisc_ma, "COD_CLIENTE_FRUITORE"))
# nrow(valdist_pdisc_ma)
# # 30.678 
# 
# 
# 
# 
# ##### Dei 7k mancanti, aggiancio 3.5k trovati nella Contact History (rimangono fuori 3.5k non trovati)
# 
# pdisc_path <- "/user/stefano.mazzucca/WAVERD_PDISC_interaction_history_V2_Missing.csv"
# pdisc_waverd_missing <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
# View(head(pdisc_waverd_missing))
# nrow(pdisc_waverd_missing)
# # 12.173
# 
# valdist_waverd_missing_cntr <- distinct(select(pdisc_waverd_missing, "CODICE_CONTRATTO__C"))
# nrow(valdist_waverd_missing_cntr)
# # 3.563
# 
# valdist_waverd_missing <- distinct(select(pdisc_waverd_missing, "CODICE_CLIENTE__C"))
# nrow(valdist_waverd_missing)
# # 3.559
# 
# 
# pdisc_waverd_missing <- withColumn(pdisc_waverd_missing,"CREATEDDATE",cast(cast(unix_timestamp(pdisc_waverd_missing$CREATEDDATE, 'dd/MM/yyyy HH:mm:ss'), 'timestamp'), 'date'))
# View(head(pdisc_waverd_missing))
# 
# 
# 
# ### AGGANCIO! ########################################################################################
# 
# createOrReplaceTempView(pdisc_waverd_missing,"pdisc_waverd_missing")
# createOrReplaceTempView(base_pdisc_digital,"base_pdisc_digital")
# 
# join_1 <- sql("select distinct t1.*, t2.DES_STATO_BUSINESS
#                      from base_pdisc_digital t1
#                      inner join pdisc_waverd_missing t2
#                       on t1.COD_CLIENTE_FRUITORE = t2.CODICE_CLIENTE__C")
# View(head(join_1,100))
# nrow(join_1)
# # 3.562 (solo t1)
# # 9.935 (t1 + DES_STATO_BUSINESS + CANALE__C)
# # 3.566 
# 
# 
# createOrReplaceTempView(join_1,"join_1")
# createOrReplaceTempView(join_pdisc_ma,"join_pdisc_ma")
# 
# base_pdisc_def <- sql("select *
#                         from join_1
#                         union 
#                         select *
#                         from join_pdisc_ma")
# View(head(base_pdisc_def,100))
# nrow(base_pdisc_def)
# # 34.785
# 
# valdist_base <- distinct(select(base_pdisc_def, "COD_CLIENTE_FRUITORE"))
# nrow(valdist_base)
# # 34.236  <<<<<<------------------------------------------------------------------------------------------
# 
# valdist_base_cntr <- distinct(select(base_pdisc_def, "COD_CONTRATTO"))
# nrow(valdist_base_cntr)
# # 34.452
# 
# 
# 
# 
# 
# ## Salvataggio del file in .parquet
# 
# write.parquet(base_pdisc_def,"/user/stefano.mazzucca/CJ_PDISC_base_def.parquet")
# 



base_pdisc_def <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
View(head(base_pdisc_def))
nrow(base_pdisc_def)
# 37.976


### VERIFICHE E DISTRIBUZIONI 

ver_dig_bin <- filter(base_pdisc_def, "digitalizzazione_bin is NULL")
nrow(ver_dig_bin)
# 862

# ver_stato_business <- filter(base_pdisc_def, "DES_STATO_BUSINESS is NULL")
# nrow(ver_stato_business)
# # 0

ver_data <- filter(base_pdisc_def, "data_ingr_pdisc_formdate is NULL")
nrow(ver_data)
# 0

createOrReplaceTempView(base_pdisc_def,"base_pdisc_def")

ver_mese_data <- sql("select *, 
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
                           from base_pdisc_def")
View(head(ver_mese_data,100))
nrow(ver_mese_data)
# 37.976

createOrReplaceTempView(ver_mese_data,"ver_mese_data")

distr_data <- sql("select mese_data, count(*)
                           from ver_mese_data
                           group by mese_data")
View(head(distr_data,100))

distr_data_causale <- sql("select mese_data,DES_ULTIMA_CAUSALE_CESSAZIONE, count(*)
                           from ver_mese_data
                           group by mese_data, DES_ULTIMA_CAUSALE_CESSAZIONE")
View(head(distr_data_causale,100))



## DES_UTLIMA_CAUSALE_CESSAZIONE

distr_des_cessazione <- sql("select DES_ULTIMA_CAUSALE_CESSAZIONE, count(COD_CLIENTE_CIFRATO)
                            from base_pdisc_def
                            group by DES_ULTIMA_CAUSALE_CESSAZIONE")
View(head(distr_des_cessazione,100))


## DES_AREA_NIELSEN_FRU

distr_des_area <- summarize(groupBy(base_pdisc_def, base_pdisc_def$DES_AREA_NIELSEN_FRU), COUNT = count(base_pdisc_def$COD_CLIENTE_CIFRATO))
View(head(distr_des_area,100))


## DES_PACCHETTO_POSS_FROM

distr_pacchetto_poss <- summarize(groupBy(base_pdisc_def, base_pdisc_def$DES_PACCHETTO_POSS_FROM), COUNT = count(base_pdisc_def$COD_CLIENTE_CIFRATO))
View(head(distr_pacchetto_poss,100))

createOrReplaceTempView(distr_pacchetto_poss,"distr_pacchetto_poss")
bin_distr <- sql("select *,
                  case when DES_PACCHETTO_POSS_FROM like '%CINEMA + SPORT + CALCIO%' then 'CINEMA + SPORT + CALCIO' 
                      when DES_PACCHETTO_POSS_FROM like '%SPORT + CALCIO%' then 'SPORT + CALCIO' 
                      when DES_PACCHETTO_POSS_FROM like '%CINEMA + CALCIO%' then 'CINEMA + CALCIO'
                      when DES_PACCHETTO_POSS_FROM like '%CINEMA + SPORT%' then 'CINEMA + SPORT'
                      when DES_PACCHETTO_POSS_FROM like '%CINEMA%' then 'CINEMA'
                      when DES_PACCHETTO_POSS_FROM like '%CALCIO%' then 'CALCIO'
                      when DES_PACCHETTO_POSS_FROM like '%SPORT%' then 'SPORT'
                      when DES_PACCHETTO_POSS_FROM like '%MINI%' then 'MINIPACK NEWS'
                      else 'BASE' end as flg_pacc_poss
                 from distr_pacchetto_poss")
View(head(bin_distr,100))
distr_bin_pacc <- summarize(groupBy(bin_distr, bin_distr$flg_pacc_poss), COUNT = sum(bin_distr$COUNT))
View(head(distr_bin_pacc,100))


## DIGITALIZZAZIONE_BIN

distr_dig_bin <- summarize(groupBy(base_pdisc_def, base_pdisc_def$digitalizzazione_bin), COUNT = count(base_pdisc_def$COD_CLIENTE_CIFRATO))
View(head(distr_dig_bin,100))







#chiudi sessione
sparkR.stop()
