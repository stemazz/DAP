
# CJ_PDISC_vers03

#apri sessione
source("connection_R.R")
options(scipen = 1000)


# Base PDISC estratta da SAS (senza codice cifrato)

pdisc_path <- "/user/stefano.mazzucca/base_pdisc_clear.csv"
pdisc_clear <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
View(head(pdisc_clear))
nrow(pdisc_clear)
# 38.150

valdist_cod_contratto <- distinct(select(pdisc_clear, "COD_CONTRATTO"))
nrow(valdist_cod_contratto)
# 38.150

valdist_cod_cliente <- distinct(select(pdisc_clear, "COD_CLIENTE_FRUITORE"))
nrow(valdist_cod_cliente)
# 37.843


## Aggancio alla base i codice_cliente_cifrato

# createOrReplaceTempView(pdisc_clear,"pdisc_clear")
# createOrReplaceTempView(pdisc_completo,"pdisc_completo")
# 
# join_base <- sql("select distinct t1.COD_CONTRATTO, t2.COD_CLIENTE_CIFRATO
#                  from pdisc_clear t1
#                  left join pdisc_completo t2
#                  on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
# View(head(join_base))
# nrow(join_base)
# # 38.168 ######################## NON TORNA!!!!!
# 
# test_cod_contr <- distinct(select(join_base, "COD_CONTRATTO"))
# nrow(test_cod_contr)
# # 38.150
# 
# test_cod_cliente <- distinct(select(join_base, "COD_CLIENTE_CIFRATO"))
# nrow(test_cod_cliente)
# # 37.944


## Aggancio la base con le chiavi fornite (ma non aggiornate)

chiavi <- read.parquet("hdfs:///STAGE/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2.parquet")
View(head(chiavi,100))

# write.parquet(chiavi,"/user/stefano.mazzucca/chiavi_contratto_cliente.parquet")


createOrReplaceTempView(pdisc_clear,"pdisc_clear")
createOrReplaceTempView(chiavi,"chiavi")

join_base_2 <- sql("select distinct t1.*, t2.COD_CLIENTE_CIFRATO
                   from pdisc_clear t1
                   left join chiavi t2
                   on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(join_base_2,100))
nrow(join_base_2)
# 38.150

test_cod_cifrato <- filter(join_base_2, "COD_CLIENTE_CIFRATO is NULL")
nrow(test_cod_cifrato)
# 166 utenti mancanti con l'anonimizzazione!!!

base_pdisc_ok <- filter(join_base_2, "COD_CLIENTE_CIFRATO is not NULL")
View(head(base_pdisc_ok,100))
nrow(base_pdisc_ok)
# 37.984


base_pdisc_2 <- withColumn(base_pdisc_ok,"data_ingr_pdisc_formdate",cast(cast(unix_timestamp(base_pdisc_ok$DATA_INGR_PDISC, 'ddMMMyy'), 'timestamp'), 'date'))
View(head(base_pdisc_2,100))
nrow(base_pdisc_2)
# 37.984



## Salvataggio del file in .parquet

write.parquet(base_pdisc_2,"/user/stefano.mazzucca/CJ_PDISC_base.parquet")




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

# ver_dig <- filter(base_liv_digital, "livello_digitalizzazione is NULL")
# nrow(ver_dig)
# # 0


## JOIN tra base_pdisc e livello di digitalizzazione

createOrReplaceTempView(base_pdisc_2,"base_pdisc_2")
createOrReplaceTempView(base_liv_digital,"base_liv_digital")

base_pdisc_digital <- sql("select t1.*, t2.digitalizzazione_bin
                          from base_pdisc_2 t1
                          left join base_liv_digital t2
                          on t1.COD_CLIENTE_CIFRATO = t2.skyid")
View(head(base_pdisc_digital, 100))
nrow(base_pdisc_digital)
# 37.984 

# ver_dig <- filter(base_pdisc_digital, "digitalizzazione_bin is NULL")
# nrow(ver_dig)
# # 874  (37.984 - 874 = 37.110) utenti senza il parametro di digitalizzazione
# 
# base_pdisc_digital_ok <- filter(base_pdisc_digital, "digitalizzazione_bin is not NULL")
# nrow(base_pdisc_digital_ok)
# # 37.110



## Salvataggio del file in .parquet

write.parquet(base_pdisc_digital,"/user/stefano.mazzucca/CJ_PDISC_base_digital.parquet")





## Lista PDISC ricevuta da MARETING AUTOMATION

pdisc_path <- "/user/stefano.mazzucca/WAVERD_PDISC_ma_completo.csv"
pdisc_waverd <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
View(head(pdisc_waverd,100))
nrow(pdisc_waverd)
# 351.554

createOrReplaceTempView(pdisc_waverd,"pdisc_waverd")
valdist_waverd <- sql("select distinct CODICE_CLIENTE__C
                      from pdisc_waverd")
View(head(valdist_waverd))
nrow(valdist_waverd)
# 113.272


## Join tra la lista_marketing_automation e la base_pdisc

createOrReplaceTempView(pdisc_waverd,"pdisc_waverd")
createOrReplaceTempView(base_pdisc_digital,"base_pdisc_digital")

join_pdisc_ma <- sql("select distinct t1.*, t2.DES_STATO_BUSINESS
                     from base_pdisc_digital t1
                     inner join pdisc_waverd t2
                      on t1.COD_CONTRATTO = t2.CODICE_CONTRATTO__C")
View(head(join_pdisc_ma,100))
nrow(join_pdisc_ma)
# 30.865 (solo t1)
# 86.678 (t1 + DES_STATO_BUSINESS + CANALE__C)
# 30.865 (rispetto ai 37.984 si perdono: 7.119 clienti)

valdist_pdisc_ma <- distinct(select(join_pdisc_ma, "COD_CLIENTE_FRUITORE"))
nrow(valdist_pdisc_ma)
# 30.664 




## DISTRIBUZIONI

## DES_UTLIMA_CAUSALE_CESSAZIONE

createOrReplaceTempView(join_pdisc_ma,"join_pdisc_ma")
pdisc_ma_des_cessazione <- sql("select DES_ULTIMA_CAUSALE_CESSAZIONE, count(COD_CLIENTE_CIFRATO)
                            from join_pdisc_ma
                            group by DES_ULTIMA_CAUSALE_CESSAZIONE")
View(head(pdisc_ma_des_cessazione,100))


## DES_AREA_NIELSEN_FRU

pdisc_ma_des_area <- summarize(groupBy(join_pdisc_ma, join_pdisc_ma$DES_AREA_NIELSEN_FRU), COUNT = count(join_pdisc_ma$COD_CLIENTE_CIFRATO))
View(head(pdisc_ma_des_area,100))


## DES_PACCHETTO_POSS_FROM

pdisc_ma_pacchetto_poss <- summarize(groupBy(join_pdisc_ma, join_pdisc_ma$DES_PACCHETTO_POSS_FROM), COUNT = count(join_pdisc_ma$COD_CLIENTE_CIFRATO))
View(head(pdisc_ma_pacchetto_poss,100))


## DIGITALIZZAZIONE_BIN

pdisc_ma_dig_bin <- summarize(groupBy(join_pdisc_ma, join_pdisc_ma$digitalizzazione_bin), COUNT = count(join_pdisc_ma$COD_CLIENTE_CIFRATO))
View(head(pdisc_ma_dig_bin,100))


######################################################################################################################################################

## VERIFICHE sui dati mancanti

antijoin_pdisc_ma <- sql("select t1.*
                     from base_pdisc_digital t1
                     left join pdisc_waverd t2
                     on t1.COD_CLIENTE_FRUITORE = t2.CODICE_CLIENTE__C where t2.CODICE_CLIENTE__C is NULL")
View(head(antijoin_pdisc_ma,100))
nrow(antijoin_pdisc_ma)
# 7.094

write.df(repartition( antijoin_pdisc_ma, 1),path = "/user/stefano.mazzucca/missing7k_pdisc.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


## DES_UTLIMA_CAUSALE_CESSAZIONE

createOrReplaceTempView(antijoin_pdisc_ma,"antijoin_pdisc_ma")

pdisc_missing_des_cessazione <- sql("select DES_ULTIMA_CAUSALE_CESSAZIONE, count(COD_CLIENTE_CIFRATO)
                            from antijoin_pdisc_ma
                            group by DES_ULTIMA_CAUSALE_CESSAZIONE")
View(head(pdisc_missing_des_cessazione,100))


## data_ingr_pdisc_formdate

pdisc_missing_data_ingr_pdisc <- sql("select distinct *, 
                                case 
                                when data_ingr_pdisc_formdate >= '2017-12-01' and data_ingr_pdisc_formdate <= '2017-12-31' then 1 
                                else 0 end as prova_data
                                from antijoin_pdisc_ma")
View(head(pdisc_missing_data_ingr_pdisc,100))

data_filter <- filter(pdisc_missing_data_ingr_pdisc, "prova_data = 0")
View(head(data_filter,100))
nrow(data_filter)
# 6.553

##############################################################################################################################################################
















#chiudi sessione
sparkR.stop()

