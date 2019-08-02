
# CJ_PDISC vers.01

#apri sessione
source("connection_R.R")
options(scipen = 1000)


## Base PDISC estrapolata da SAS (42.526 records)

pdisc_path <- "/user/stefano.mazzucca/JOIN_pdisc.csv"
pdisc_clear_df <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
View(head(pdisc_clear_df))
nrow(pdisc_clear_df)
# 42.526

## Chiavi per associare gli external_id

chiavi <- read.parquet("hdfs:///STAGE/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2.parquet")
View(head(chiavi,100))

## Join per avere la base completa PDISC

createOrReplaceTempView(pdisc_clear_df,"pdisc_clear_df")
createOrReplaceTempView(chiavi,"chiavi")

pdisc_join <- sql("select t1.*, t2.COD_CLIENTE_CIFRATO, t2.COD_CONTRATTO_CIFRATO
                  from pdisc_clear_df t1
                  left join chiavi t2
                  on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(pdisc_join,100))
nrow(pdisc_join)
# 42.526

pdisc_inner_join <- sql("select t1.*, t2.COD_CLIENTE_CIFRATO, t2.COD_CONTRATTO_CIFRATO
                  from pdisc_clear_df t1
                  inner join chiavi t2
                  on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(pdisc_inner_join,100))
nrow(pdisc_inner_join)
# 42.335


## Verifica dei 200 "vuoti"

verifica_ <- sql("select count(COD_CONTRATTO)
                 from pdisc_join
                 where COD_CLIENTE_CIFRATO is NULL")
View(head(verifica_,100))
# 191


## Seconda verifica sui COD_CLIENTE_CIFRATO

pdisc_cod_cifrato_2 <- filter(pdisc_join, "COD_CLIENTE_CIFRATO is not NULL")
nrow(pdisc_cod_cifrato_2)
# 42.335

pdisc_cod_cifrato_3 <- filter(pdisc_join, "COD_CLIENTE_CIFRATO is NULL")
nrow(pdisc_cod_cifrato_3)
# 191


## Salvataggio del file in .parquet

write.parquet(pdisc_inner_join,"/user/stefano.mazzucca/CJ_PDISC_base.parquet")





#Prove di raggruppamento 

## DES_UTLIMA_CAUSALE_CESSAZIONE

createOrReplaceTempView(pdisc_inner_join,"pdisc_inner_join")
pdisc_des_cessazione <- sql("select DES_ULTIMA_CAUSALE_CESSAZIONE, count(COD_CLIENTE_CIFRATO)
                            from pdisc_inner_join
                            group by DES_ULTIMA_CAUSALE_CESSAZIONE")
View(head(pdisc_des_cessazione,100))

#oppure (con summarize)

pdisc_des_cessazione_2 <-summarize(groupBy(pdisc_inner_join, pdisc_inner_join$DES_ULTIMA_CAUSALE_CESSAZIONE), COUNT = count(pdisc_inner_join$DES_ULTIMA_CAUSALE_CESSAZIONE))
View(head(pdisc_des_cessazione_2,100))
pdisc_des_cessazione_3 <-summarize(groupBy(pdisc_inner_join, pdisc_inner_join$DES_ULTIMA_CAUSALE_CESSAZIONE), COUNT = count(pdisc_inner_join$COD_CLIENTE_CIFRATO))
View(head(pdisc_des_cessazione_3,100))


## DES_AREA_NIELSEN_FRU

pdisc_des_area_nielsen <- summarize(groupBy(pdisc_inner_join, pdisc_inner_join$DES_AREA_NIELSEN_FRU), COUNT = count(pdisc_inner_join$COD_CLIENTE_CIFRATO))
View(head(pdisc_des_area_nielsen,100))


## DES_PACCHETTO_POSS_FROM

pdisc_pacchetto_poss <- summarize(groupBy(pdisc_inner_join, pdisc_inner_join$DES_PACCHETTO_POSS_FROM), COUNT = count(pdisc_inner_join$COD_CLIENTE_CIFRATO))
View(head(pdisc_pacchetto_poss,100))

pdisc_pacchetto_poss_2 <- summarize(groupBy(pdisc_inner_join, pdisc_inner_join$DES_PACCHETTO_POSS_FROM), COUNT = count(pdisc_inner_join$DES_PACCHETTO_POSS_FROM))
View(head(pdisc_pacchetto_poss_2,100))

ver2 <- filter(pdisc_inner_join, "DES_PACCHETTO_POSS_FROM == 'MINIPACK NEWS'")
View(head(ver2,100))
nrow(ver2)
# 673

createOrReplaceTempView(pdisc_inner_join,"pdisc_inner_join")
ver3 <- sql("select DES_PACCHETTO_POSS_FROM, count(*) as n
            from pdisc_inner_join
            group by DES_PACCHETTO_POSS_FROM")
View(head(ver3,100))

ver4 <- sql("select DES_PACCHETTO_POSS_FROM, count(COD_CONTRATTO) as n
            from pdisc_inner_join
            where DES_PACCHETTO_POSS_FROM like 'MINIPACK%'")
View(head(ver4,100))




###########################################################################

## Base PDISC COMPLETA estrapolata da SAS (42.526 records)


pdisc_path <- "/user/stefano.mazzucca/JOIN_pdisc_COMPLETA.csv"
pdisc_completo <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
View(head(pdisc_completo))
nrow(pdisc_completo)
# 42.526

ver <- sql("select distinct COD_CONTRATTO, COD_CLIENTE_CIFRATO
           from pdisc_completo")
View(head(ver,100))
nrow(ver)
# 38.470 ??????????????????????????????????????????????????????????????????????????????????????????????
#39.804 dal file iniziale!!


## Ceck sulla PRIMA_DATA_ATTIVAZIONE_CONTRATTO

verifica1_ <- filter(pdisc_completo,"DAT_PRIMA_ATTIV_CNTR='31DEC9999:00:00:00'")
View(head(verifica1_, 100))
nrow(verifica1_)
# 3.708


#Verifica external_id con nessun missing

ver5 <- filter(pdisc_completo, "COD_CLIENTE_CIFRATO is not NULL")
nrow(ver5)
# 42.526 (ceck OK)


#Prove di raggruppamento 

## DES_UTLIMA_CAUSALE_CESSAZIONE

createOrReplaceTempView(pdisc_completo,"pdisc_completo")
pdisc_c_des_cessazione <- sql("select DES_ULTIMA_CAUSALE_CESSAZIONE, count(COD_CLIENTE_CIFRATO)
                            from pdisc_completo
                            group by DES_ULTIMA_CAUSALE_CESSAZIONE")
View(head(pdisc_c_des_cessazione,100))

## DES_AREA_NIELSEN_FRU

pdisc_c_des_area_nielsen <- summarize(groupBy(pdisc_completo, pdisc_completo$DES_AREA_NIELSEN_FRU), COUNT = count(pdisc_completo$COD_CLIENTE_CIFRATO))
View(head(pdisc_c_des_area_nielsen,100))

## DES_PACCHETTO_POSS_FROM

pdisc_c_pacchetto_poss <- summarize(groupBy(pdisc_completo, pdisc_completo$DES_PACCHETTO_POSS_FROM), COUNT = count(pdisc_completo$COD_CLIENTE_CIFRATO))
View(head(pdisc_c_pacchetto_poss,100))







#chiudi sessione
sparkR.stop()


