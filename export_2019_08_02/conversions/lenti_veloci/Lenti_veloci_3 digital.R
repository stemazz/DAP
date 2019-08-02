
## Lenti-veloci (3)
## Conversioni DIGITALI


#apri sessione
source("connection_R.R")
options(scipen = 1000)



attivazioni_digital_p2cAmedeo <- read.parquet("/user/silvia/p2c_length_digitali_puri")
View(head(attivazioni_digital_p2cAmedeo,300))
#printSchema(attivazioni_digital_p2cAmedeo)
nrow(attivazioni_digital_p2cAmedeo) 
# 82.304

## Converto date
attivazioni_digital_p2cAmedeo_2 <- withColumn(attivazioni_digital_p2cAmedeo,"DAT_CONVERSION_ts",cast(unix_timestamp(attivazioni_digital_p2cAmedeo$Conversion, 'yyyy-MM-dd'),'timestamp'))
attivazioni_digital_p2cAmedeo_2 <- withColumn(attivazioni_digital_p2cAmedeo_2, "DAT_CONVERSION_dt", cast(attivazioni_digital_p2cAmedeo_2$DAT_CONVERSION_ts, "date"))
attivazioni_digital_p2cAmedeo_2 <- withColumn(attivazioni_digital_p2cAmedeo_2,"First_touch_ts",cast(unix_timestamp(attivazioni_digital_p2cAmedeo_2$First_touch, 'yyyy-MM-dd'),'timestamp'))
attivazioni_digital_p2cAmedeo_2 <- withColumn(attivazioni_digital_p2cAmedeo_2, "First_touch_dt", cast(attivazioni_digital_p2cAmedeo_2$First_touch_ts, "date"))

View(head(attivazioni_digital_p2cAmedeo_2,300))
#printSchema(attivazioni_digital_p2cAmedeo_2)

attivazioni_digital_p2cAmedeo_3 <- select(attivazioni_digital_p2cAmedeo_2,"device-name","browser-name","os-name",
                                          "CookieID","skyid","OrderId","DAT_CONVERSION_dt","First_touch_dt")

createOrReplaceTempView(attivazioni_digital_p2cAmedeo_3,"attivazioni_digital_p2cAmedeo_3")
attivazioni_digital_p2cAmedeo_4 <- sql("select *,
                                       datediff(DAT_CONVERSION_dt,First_touch_dt) as diff_gg
                                       from attivazioni_digital_p2cAmedeo_3")
View(head(attivazioni_digital_p2cAmedeo_4,300))

## Crea classificazione
createOrReplaceTempView(attivazioni_digital_p2cAmedeo_4,"attivazioni_digital_p2cAmedeo_4")
attivazioni_digital_p2cAmedeo_5 <- sql("select *,
                                       case when diff_gg<=7 then '1_<=7gg'
                                       when diff_gg<=14 then '2_<=14gg'
                                       when diff_gg<=21 then '3_<=21gg'
                                       when diff_gg<=28 then '4_<=28gg'
                                       when diff_gg<=60 then '5_<=60gg'
                                       when diff_gg>60 then '6_>60gg'
                                       else 'NA' end as FASCIA_P2C
                                       from attivazioni_digital_p2cAmedeo_4")
View(head(attivazioni_digital_p2cAmedeo_5,2000))
nrow(attivazioni_digital_p2cAmedeo_5) 
# 82.304

## Crea variabile dicotomica lenti/veloci
createOrReplaceTempView(attivazioni_digital_p2cAmedeo_5,"attivazioni_digital_p2cAmedeo_5")
attivazioni_digital_p2cAmedeo_6 <- sql("select *,
                                          case when FASCIA_P2C='1_<=7gg' or FASCIA_P2C='2_<=14gg' or FASCIA_P2C='3_<=21gg' or FASCIA_P2C='4_<=28gg' then 'VELOCI'
                                          when FASCIA_P2C='NA' then 'NA'
                                          else 'LENTI' end as classificazione_cliente
                                        from attivazioni_digital_p2cAmedeo_5")
View(head(attivazioni_digital_p2cAmedeo_6,200))
nrow(attivazioni_digital_p2cAmedeo_6) 
# 82.304


############## carico estrazioni da BI ################################################################################################################

codici_BI <- read.df("/user/valentina/LentiVSveloci_estrai lista_20180731.csv", source = "csv", header = "true", delimiter = ";")
View(head(codici_BI,200))
nrow(codici_BI) 
# 27.102

createOrReplaceTempView(attivazioni_digital_p2cAmedeo_6,"attivazioni_digital_p2cAmedeo_6")
createOrReplaceTempView(codici_BI,"codici_BI")
join_1 <- sql("select distinct t1.*,
             t2.FASCIA_P2C, t2.classificazione_cliente
             from codici_BI t1 inner join attivazioni_digital_p2cAmedeo_6 t2
             on t1.orderID =t2.OrderId")
View(head(join_1,200))
nrow(join_1) 
# 25.377


write.df(repartition( join_1, 1),path = "/user/valentina/20180731_Export_lista_Ricerca_p2c_FULL.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
write.parquet(join_1, "/user/stefano.mazzucca/lenti_veloci_digital.parquet")



## Aggancio le info socio-demo ai convertenti digitali ------------------------------------------------------

lv_digital <- read.parquet("/user/stefano.mazzucca/lenti_veloci_digital.parquet")
View(head(lv_digital,100))
nrow(lv_digital)
# 25.377

lv_digital_1 <- withColumn(lv_digital,"data_stipula_dt",cast(cast(unix_timestamp(lv_digital$data_stip, 'dd/MM/yyyy'),'timestamp'),'date'))
View(head(lv_digital_1,100))


## Rianggancio le info iniziali
conversions_all <- read.df("/user/stefano.mazzucca/20180726_stipulati_Nov17_Giu18.csv", source = "csv", header = "true", delimiter = ";")
View(head(conversions_all,100))
nrow(conversions_all)
# 403.360 

conversions_all_1 <- withColumn(conversions_all,"data_stipula_dt",cast(cast(unix_timestamp(conversions_all$DATA_STIPULA, 'dd/MM/yyyy'),'timestamp'),'date'))
View(head(conversions_all_1,100))


createOrReplaceTempView(lv_digital_1, "lv_digital_1")
createOrReplaceTempView(conversions_all_1, "conversions_all_1")
lv_digital_2 <- sql("select distinct t1.*, 
                              t2.FASCIA_P2C, t2.classificazione_cliente
                     from conversions_all_1  t1
                     inner join lv_digital_1 t2
                     on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and 
                      t1.data_stipula_dt = t2.data_stipula_dt")
View(head(lv_digital_2,100))
nrow(lv_digital_2)
# 25.889
## ATTENZIONE !! vs 25.377 dei convertenti digitali... Ci sono dei doppioni!!


write.parquet(lv_digital_2, "/user/stefano.mazzucca/lenti_veloci_digital_info.parquet")



lv_digital <- read.parquet("/user/stefano.mazzucca/lenti_veloci_digital_info.parquet")
View(head(lv_digital,100))
nrow(lv_digital)
# 25.889


#chiudi sessione
sparkR.stop()
