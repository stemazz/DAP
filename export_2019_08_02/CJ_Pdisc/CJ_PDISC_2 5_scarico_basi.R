

# CJ_PDISC_2
# scarico basi



#apri sessione
source("connection_R.R")
options(scipen = 1000)


#################################################################################################################
#################################################################################################################
# scarico i clienti che sono entrati in pdisc lug/ago/sep 2017
#################################################################################################################
#################################################################################################################

cambi_stato <- read.parquet("hdfs:///STAGE/CMDM/DettaglioMovimentiStatoBusiness/delta/vs_cambio_stato_business_SHOT_2.parquet")
cambi_stato_v2 <- select(cambi_stato,
                         "DAT_INIZIO","DAT_FINE","COD_CONTRATTO",
                         "DES_STATO_BUSINESS_PRE","DES_STATO_BUSINESS_DA","DES_STATO_BUSINESS_A")

cambi_stato_v3 <- withColumn(cambi_stato_v2,"DAT_INIZIO_ts",cast(unix_timestamp(cambi_stato_v2$DAT_INIZIO, 'dd/MM/yyyy'),'timestamp'))
cambi_stato_v4 <- withColumn(cambi_stato_v3,"DAT_FINE_ts",cast(unix_timestamp(cambi_stato_v3$DAT_FINE, 'dd/MM/yyyy'),'timestamp'))

cambi_stato_v5 <- withColumn(cambi_stato_v4, "DAT_INIZIO_dt", cast(cambi_stato_v4$DAT_INIZIO_ts, "date"))
cambi_stato_v6 <- withColumn(cambi_stato_v5, "DAT_FINE_dt", cast(cambi_stato_v5$DAT_FINE_ts, "date"))

cambi_stato_v7 <- select(cambi_stato_v6,"COD_CONTRATTO","DES_STATO_BUSINESS_PRE","DES_STATO_BUSINESS_DA","DES_STATO_BUSINESS_A","DAT_INIZIO_dt","DAT_FINE_dt")

cambi_stato_v8 <- filter(cambi_stato_v7,"DES_STATO_BUSINESS_DA='Active Pdisc' ")

cambi_stato_v9 <- arrange(cambi_stato_v8,cambi_stato_v8$COD_CONTRATTO,cambi_stato_v8$DAT_INIZIO_dt)

cambi_stato_v10 <- filter(cambi_stato_v9,"DAT_INIZIO_dt>='2017-07-01'") 

cambi_stato_v11 <- arrange(cambi_stato_v10,cambi_stato_v10$DAT_INIZIO_dt)

cambi_stato_v12 <- filter(cambi_stato_v11,"DAT_INIZIO_dt<='2017-09-30'") 
nrow(cambi_stato_v12) #215379

cambi_stato_v13 <- filter(cambi_stato_v12,"DAT_FINE_dt!='9999-12-31'")
nrow(cambi_stato_v13) #124116

cambi_stato_v14 <- filter(cambi_stato_v13,"DES_STATO_BUSINESS_PRE='Active' or  DES_STATO_BUSINESS_PRE='Active Pre Pdisc' or DES_STATO_BUSINESS_PRE='Active Suspended' ")
nrow(cambi_stato_v14) #122.547

createOrReplaceTempView(cambi_stato_v14,"cambi_stato_v14")

cambi_stato_v15 <- sql("select *,
                       case when DES_STATO_BUSINESS_A='Active' then 1 else 0 end as flg_recuperato,
                       case when DES_STATO_BUSINESS_A='Disconnected' then 1 else 0 end as flg_disconnesso,
                       case when DES_STATO_BUSINESS_A='Active Suspended' then 1 else 0 end as flg_sospeso
                       from cambi_stato_v14 ")
nrow(cambi_stato_v15)  #122547



################### prendo un po di info sul contratto dalle foto della sto  

sto_201706_v1 <- read.parquet('hdfs:///STAGE/CMDM/AmbienteAnalisiStoricizzato/delta/fat_contratto_sto_SHOT_2_201706.parquet')
sto_201707_v1 <- read.parquet('hdfs:///STAGE/CMDM/AmbienteAnalisiStoricizzato/delta/fat_contratto_sto_SHOT_2_201707.parquet')
sto_201708_v1 <- read.parquet('hdfs:///STAGE/CMDM/AmbienteAnalisiStoricizzato/delta/fat_contratto_sto_SHOT_2_201708.parquet')
sto_201709_v1 <- read.parquet('hdfs:///STAGE/CMDM/AmbienteAnalisiStoricizzato/delta/fat_contratto_sto_SHOT_2_201709.parquet')


#giugno
sto_201706_v2 <- filter( select(sto_201706_v1,c("COD_CONTRATTO","DAT_PRIMA_ATTIV_CNTR","DES_PACCHETTO_POSSEDUTO",
                                                "DES_TIPO_CONTRATTO","COD_STB_PRINCIPALE","DES_STATO_BUSINESS",
                                                "VAL_ARPU_TEORICO","VAL_ARPU_NET_DISCOUNT","VAL_ARPU_AVG_TEORICO_3M","VAL_ARPU_AVG_NET_DISCOUNT_3M"  
)), "DES_TIPO_CONTRATTO='RESIDENZIALE' and DES_STATO_BUSINESS like '%Active%'")
nrow(sto_201706_v2) #4315227
sto_201706_v3 <- withColumn(sto_201706_v2,"DATA_STO",lit(201706))


#luglio
sto_201707_v2 <- filter( select(sto_201707_v1,c("COD_CONTRATTO","DAT_PRIMA_ATTIV_CNTR","DES_PACCHETTO_POSSEDUTO",
                                                "DES_TIPO_CONTRATTO","COD_STB_PRINCIPALE","DES_STATO_BUSINESS",
                                                "VAL_ARPU_TEORICO","VAL_ARPU_NET_DISCOUNT","VAL_ARPU_AVG_TEORICO_3M","VAL_ARPU_AVG_NET_DISCOUNT_3M"  
)), "DES_TIPO_CONTRATTO='RESIDENZIALE' and DES_STATO_BUSINESS like '%Active%'")
nrow(sto_201707_v2) #4304154
sto_201707_v3 <- withColumn(sto_201707_v2,"DATA_STO",lit(201707))



#agosto
sto_201708_v2 <- filter( select(sto_201708_v1,c("COD_CONTRATTO","DAT_PRIMA_ATTIV_CNTR","DES_PACCHETTO_POSSEDUTO",
                                                "DES_TIPO_CONTRATTO","COD_STB_PRINCIPALE","DES_STATO_BUSINESS",
                                                "VAL_ARPU_TEORICO","VAL_ARPU_NET_DISCOUNT","VAL_ARPU_AVG_TEORICO_3M","VAL_ARPU_AVG_NET_DISCOUNT_3M"  
)), "DES_TIPO_CONTRATTO='RESIDENZIALE' and DES_STATO_BUSINESS like '%Active%'")
nrow(sto_201708_v2) #4312173
sto_201708_v3 <- withColumn(sto_201708_v2,"DATA_STO",lit(201708))



#settembre
sto_201709_v2 <- filter( select(sto_201709_v1,c("COD_CONTRATTO","DAT_PRIMA_ATTIV_CNTR","DES_PACCHETTO_POSSEDUTO",
                                                "DES_TIPO_CONTRATTO","COD_STB_PRINCIPALE","DES_STATO_BUSINESS",
                                                "VAL_ARPU_TEORICO","VAL_ARPU_NET_DISCOUNT","VAL_ARPU_AVG_TEORICO_3M","VAL_ARPU_AVG_NET_DISCOUNT_3M"  
)), "DES_TIPO_CONTRATTO='RESIDENZIALE' and DES_STATO_BUSINESS like '%Active%'")
nrow(sto_201709_v2) #4324206
sto_201709_v3 <- withColumn(sto_201709_v2,"DATA_STO",lit(201709))


#append 
append_sto_1 <- union(sto_201706_v3,sto_201707_v3) 
append_sto_2 <- union(append_sto_1,sto_201708_v3)
append_sto_3 <- union(append_sto_2,sto_201709_v3)
nrow(append_sto_3)  #17255760


#recupero la chiave
chiavi <- read.parquet("hdfs:///STAGE/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2.parquet")

createOrReplaceTempView(chiavi,"chiavi")
createOrReplaceTempView(append_sto_3,"append_sto_3")

append_sto_4 <- sql("select t1.*,
                    t2.COD_CLIENTE,t2.COD_CLIENTE_CIFRATO,t2.COD_CONTRATTO_CIFRATO
                    from append_sto_3 t1 inner join chiavi t2
                    on t1.COD_CONTRATTO=t2.COD_CONTRATTO")
nrow(append_sto_4) #17254224


####### vado in join con la base per capire le info del contratto
createOrReplaceTempView(cambi_stato_v15,"cambi_stato_v15")
createOrReplaceTempView(append_sto_4,"append_sto_4")

cambi_stato_v16 <- sql("select t1.*, t2.DATA_STO,
                       t2.DAT_PRIMA_ATTIV_CNTR,t2.DES_PACCHETTO_POSSEDUTO,t2.COD_STB_PRINCIPALE,
                       t2.VAL_ARPU_TEORICO,t2.VAL_ARPU_NET_DISCOUNT,t2.VAL_ARPU_AVG_TEORICO_3M,t2.VAL_ARPU_AVG_NET_DISCOUNT_3M
                       from cambi_stato_v15 t1 left join append_sto_4 t2
                       on t1.COD_CONTRATTO=t2.COD_CONTRATTO_CIFRATO
                       order by t1.COD_CONTRATTO,t2.DATA_STO")
nrow(cambi_stato_v16) #397379

# conta <- distinct(select(cambi_stato_v16,"COD_CONTRATTO"))
# nrow(conta) #119.125 prima 122.547 

#seleziono la prima riga per cod_contratto
createOrReplaceTempView(cambi_stato_v16,"cambi_stato_v16")
cambi_stato_v17 <- sql("SELECT FIRST(COD_CONTRATTO) as COD_CONTRATTO,
                       FIRST(DES_STATO_BUSINESS_PRE) as DES_STATO_BUSINESS_PRE,
                       FIRST(DES_STATO_BUSINESS_DA) as DES_STATO_BUSINESS_DA,
                       FIRST(DES_STATO_BUSINESS_A) as DES_STATO_BUSINESS_A,
                       FIRST(DAT_INIZIO_dt) as DAT_INIZIO_dt,
                       FIRST(DAT_FINE_dt) as DAT_FINE_dt,
                       FIRST(flg_recuperato) as flg_recuperato,
                       FIRST(flg_disconnesso) as flg_disconnesso,
                       FIRST(flg_sospeso) as flg_sospeso,
                       FIRST(DAT_PRIMA_ATTIV_CNTR) as DAT_PRIMA_ATTIV_CNTR,
                       FIRST(DES_PACCHETTO_POSSEDUTO) as DES_PACCHETTO_POSSEDUTO,
                       FIRST(COD_STB_PRINCIPALE) as COD_STB_PRINCIPALE,
                       FIRST(VAL_ARPU_TEORICO) as VAL_ARPU_TEORICO,
                       FIRST(VAL_ARPU_NET_DISCOUNT) as VAL_ARPU_NET_DISCOUNT,
                       FIRST(VAL_ARPU_AVG_TEORICO_3M) as VAL_ARPU_AVG_TEORICO_3M,
                       FIRST(VAL_ARPU_AVG_NET_DISCOUNT_3M) as VAL_ARPU_AVG_NET_DISCOUNT_3M
                       FROM cambi_stato_v16
                       GROUP BY COD_CONTRATTO")
nrow(cambi_stato_v17)  #119125

#View(head(cambi_stato_v17, 1000))



#################################################################################################################
#################################################################################################################
# aggiungo il modello di rischio pdisc
#################################################################################################################
#################################################################################################################

FAT_SCORE_SDM_STO_v1 <- read.parquet("hdfs:///STAGE/CMDM/ModelliScoring/delta/fat_score_sdm_sto_SHOT_2.parquet")

FAT_SCORE_SDM_STO_v2 <- select(FAT_SCORE_SDM_STO_v1,
                               "COD_CONTRATTO","DAT_MESE_RIFERIMENTO","FASCIA_SDM_PDISC_M1","FASCIA_SDM_PDISC_M2","FASCIA_SDM_PDISC_M3","FASCIA_SDM_PDISC_M4")


#devo spaccare le query per mettere tutto x riga e non fare duplicati

FAT_SCORE_SDM_STO_giu <- filter(FAT_SCORE_SDM_STO_v2,"DAT_MESE_RIFERIMENTO='1/6/2017'")

FAT_SCORE_SDM_STO_lug <- filter(FAT_SCORE_SDM_STO_v2,"DAT_MESE_RIFERIMENTO='1/7/2017'")

FAT_SCORE_SDM_STO_ago <- filter(FAT_SCORE_SDM_STO_v2,"DAT_MESE_RIFERIMENTO='1/8/2017' ")


#giugno
createOrReplaceTempView(FAT_SCORE_SDM_STO_giu,"FAT_SCORE_SDM_STO_giu")
createOrReplaceTempView(cambi_stato_v17,"cambi_stato_v17")

cambi_stato_v18 <- sql("select t1.*,
                       t2.FASCIA_SDM_PDISC_M1 as FASCIA_SDM_PDISC_M1_06,
                       t2.FASCIA_SDM_PDISC_M2 as FASCIA_SDM_PDISC_M2_06,
                       t2.FASCIA_SDM_PDISC_M3 as FASCIA_SDM_PDISC_M3_06,
                       t2.FASCIA_SDM_PDISC_M4 as FASCIA_SDM_PDISC_M4_06
                       from cambi_stato_v17 t1 left join FAT_SCORE_SDM_STO_giu t2
                       on t1.COD_CONTRATTO=t2.COD_CONTRATTO")
nrow(cambi_stato_v18) #119125


#luglio
createOrReplaceTempView(FAT_SCORE_SDM_STO_lug,"FAT_SCORE_SDM_STO_lug")
createOrReplaceTempView(cambi_stato_v18,"cambi_stato_v18")

cambi_stato_v19 <- sql("select t1.*,
                       t2.FASCIA_SDM_PDISC_M1 as FASCIA_SDM_PDISC_M1_07,
                       t2.FASCIA_SDM_PDISC_M2 as FASCIA_SDM_PDISC_M2_07,
                       t2.FASCIA_SDM_PDISC_M3 as FASCIA_SDM_PDISC_M3_07,
                       t2.FASCIA_SDM_PDISC_M4 as FASCIA_SDM_PDISC_M4_07
                       from cambi_stato_v18 t1 left join FAT_SCORE_SDM_STO_lug t2
                       on t1.COD_CONTRATTO=t2.COD_CONTRATTO")
nrow(cambi_stato_v19)  #119125



#agosto
createOrReplaceTempView(FAT_SCORE_SDM_STO_ago,"FAT_SCORE_SDM_STO_ago")
createOrReplaceTempView(cambi_stato_v19,"cambi_stato_v19")

cambi_stato_v20 <- sql("select t1.*,
                       t2.FASCIA_SDM_PDISC_M1 as FASCIA_SDM_PDISC_M1_08,
                       t2.FASCIA_SDM_PDISC_M2 as FASCIA_SDM_PDISC_M2_08,
                       t2.FASCIA_SDM_PDISC_M3 as FASCIA_SDM_PDISC_M3_08,
                       t2.FASCIA_SDM_PDISC_M4 as FASCIA_SDM_PDISC_M4_08
                       from cambi_stato_v19 t1 left join FAT_SCORE_SDM_STO_ago t2
                       on t1.COD_CONTRATTO=t2.COD_CONTRATTO")
nrow(cambi_stato_v20)  #119125


### elabora e rinomina
createOrReplaceTempView(cambi_stato_v20,"cambi_stato_v20")

cambi_stato_v21 <- sql("select *,
                       case when DAT_INIZIO_dt>='2017-07-01' and DAT_INIZIO_dt<='2017-07-31' and FASCIA_SDM_PDISC_M1_06 is not NULL then FASCIA_SDM_PDISC_M1_06 
                       when DAT_INIZIO_dt>='2017-07-01' and DAT_INIZIO_dt<='2017-07-31' and FASCIA_SDM_PDISC_M1_06 is NULL and FASCIA_SDM_PDISC_M1_07 is not NULL then FASCIA_SDM_PDISC_M1_07
                       when DAT_INIZIO_dt>='2017-08-01' and DAT_INIZIO_dt<='2017-08-31' and FASCIA_SDM_PDISC_M1_07 is not NULL then FASCIA_SDM_PDISC_M1_07 
                       when DAT_INIZIO_dt>='2017-08-01' and DAT_INIZIO_dt<='2017-08-31' and FASCIA_SDM_PDISC_M1_07 is NULL and FASCIA_SDM_PDISC_M1_06 is not NULL then FASCIA_SDM_PDISC_M1_06
                       when DAT_INIZIO_dt>='2017-09-01' and DAT_INIZIO_dt<='2017-09-30' and FASCIA_SDM_PDISC_M1_08 is not NULL then FASCIA_SDM_PDISC_M1_08 
                       when DAT_INIZIO_dt>='2017-09-01' and DAT_INIZIO_dt<='2017-09-30' and FASCIA_SDM_PDISC_M1_08 is NULL and FASCIA_SDM_PDISC_M1_07 is not NULL then FASCIA_SDM_PDISC_M1_07
                       else NULL end as FASCIA_SDM_PDISC_M1,
                       
                       case when DAT_INIZIO_dt>='2017-07-01' and DAT_INIZIO_dt<='2017-07-31' and FASCIA_SDM_PDISC_M4_06 is not NULL then FASCIA_SDM_PDISC_M4_06 
                       when DAT_INIZIO_dt>='2017-07-01' and DAT_INIZIO_dt<='2017-07-31' and FASCIA_SDM_PDISC_M4_06 is NULL and FASCIA_SDM_PDISC_M4_07 is not NULL then FASCIA_SDM_PDISC_M4_07
                       when DAT_INIZIO_dt>='2017-08-01' and DAT_INIZIO_dt<='2017-08-31' and FASCIA_SDM_PDISC_M4_07 is not NULL then FASCIA_SDM_PDISC_M4_07 
                       when DAT_INIZIO_dt>='2017-08-01' and DAT_INIZIO_dt<='2017-08-31' and FASCIA_SDM_PDISC_M4_07 is NULL and FASCIA_SDM_PDISC_M4_06 is not NULL then FASCIA_SDM_PDISC_M4_06
                       when DAT_INIZIO_dt>='2017-09-01' and DAT_INIZIO_dt<='2017-09-30' and FASCIA_SDM_PDISC_M4_08 is not NULL then FASCIA_SDM_PDISC_M4_08 
                       when DAT_INIZIO_dt>='2017-09-01' and DAT_INIZIO_dt<='2017-09-30' and FASCIA_SDM_PDISC_M4_08 is NULL and FASCIA_SDM_PDISC_M4_07 is not NULL then FASCIA_SDM_PDISC_M4_07
                       else NULL end as FASCIA_SDM_PDISC_M4
                       from cambi_stato_v20")

#View(head(cambi_stato_v21,1000))

createOrReplaceTempView(cambi_stato_v21,"cambi_stato_v21")

cambi_stato_v22 <- sql("select
                       COD_CONTRATTO,DES_STATO_BUSINESS_PRE,DES_STATO_BUSINESS_DA,DES_STATO_BUSINESS_A,        
                       DAT_INIZIO_dt as DATA_INGRESSO_PDISC,
                       DAT_FINE_dt as DATA_USCITA_PDISC,
                       flg_recuperato,flg_disconnesso,flg_sospeso,
                       DAT_PRIMA_ATTIV_CNTR,DES_PACCHETTO_POSSEDUTO,COD_STB_PRINCIPALE,          
                       VAL_ARPU_TEORICO,VAL_ARPU_NET_DISCOUNT,VAL_ARPU_AVG_TEORICO_3M,VAL_ARPU_AVG_NET_DISCOUNT_3M,
                       FASCIA_SDM_PDISC_M1,FASCIA_SDM_PDISC_M4                          
                       from cambi_stato_v21")
nrow(cambi_stato_v22) #119.125 



#################################################################################################################
#################################################################################################################
#  aggiungo le chiavi per prossimi match
#################################################################################################################
#################################################################################################################

chiavi <- read.parquet("hdfs:///STAGE/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2.parquet")

createOrReplaceTempView(chiavi,"chiavi")
createOrReplaceTempView(cambi_stato_v22,"cambi_stato_v22")

cambi_stato_v23 <- sql("select t1.*,
                       t2.COD_CONTRATTO as COD_CONTRATTO_INchiaro,
                       t2.COD_CLIENTE,
                       t2.COD_CLIENTE_CIFRATO as SKY_ID
                       from cambi_stato_v22 t1 inner join chiavi t2
                       on t1.COD_CONTRATTO=t2.COD_CONTRATTO_CIFRATO")
nrow(cambi_stato_v23) #119.116



#################################################################################################################
#################################################################################################################
#  aggiungo la digitalizzazione
#################################################################################################################
#################################################################################################################

base_score_digital <- read.parquet("hdfs:///user/alessandro/clusterfinalscore_conApp.parquet")

createOrReplaceTempView(base_score_digital, "base_score_digital")

base_score_digital_2 <- sql("select distinct * from base_score_digital")
nrow(base_score_digital_2) # 4.178.841

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

#join
createOrReplaceTempView(base_liv_digital,"base_liv_digital")
createOrReplaceTempView(cambi_stato_v23,"cambi_stato_v23")

cambi_stato_v24 <- sql("select t1.*,
                       t2.digitalizzazione_bin
                       from cambi_stato_v23 t1 left join base_liv_digital t2
                       on t1.SKY_ID=t2.skyid")
nrow(cambi_stato_v24)  #119116



#################################################################################################################
#################################################################################################################
# codifico variabili e salvo
#################################################################################################################
#################################################################################################################

#pacchetto
createOrReplaceTempView(cambi_stato_v24,"cambi_stato_v24")
cambi_stato_v25 <- sql("select *,
                       case when des_pacchetto_posseduto like '%CINEMA%' then 1 else 0 end as flg_cinema,
                       case when des_pacchetto_posseduto like '%CALCIO%' then 1 else 0 end as flg_calcio,
                       case when des_pacchetto_posseduto like '%SPORT%' then 1 else 0 end as flg_sport
                       from cambi_stato_v24")


createOrReplaceTempView(cambi_stato_v25,"cambi_stato_v25")
cambi_stato_v26 <- sql("select *,
                       case when flg_cinema=1 and flg_calcio=1 and flg_sport=1 then 'CINEMA + SPORT + CALCIO'
                       when flg_cinema=0 and flg_calcio=1 and flg_sport=1 then 'SPORT + CALCIO'
                       when flg_cinema=1 and flg_calcio=1 and flg_sport=0 then 'CINEMA + CALCIO'
                       when flg_cinema=1 and flg_calcio=1 and flg_sport=0 then 'CINEMA + CALCIO'
                       when flg_cinema=1 and flg_calcio=0 and flg_sport=1 then 'CINEMA + SPORT'
                       when flg_cinema=1 and flg_calcio=0 and flg_sport=0 then 'CINEMA'
                       when flg_cinema=0 and flg_calcio=1 and flg_sport=0 then 'CALCIO'
                       when flg_cinema=0 and flg_calcio=0 and flg_sport=1 then 'SPORT'
                       else 'BASE' end as tipo_pack
                       from cambi_stato_v25")

#View(head(cambi_stato_v26,200))

cambi_stato_v27 <- select(cambi_stato_v26,
                          "COD_CONTRATTO","COD_CONTRATTO_INchiaro","COD_CLIENTE","SKY_ID",
                          "DES_STATO_BUSINESS_PRE","DES_STATO_BUSINESS_DA","DES_STATO_BUSINESS_A",        
                          "DATA_INGRESSO_PDISC","DATA_USCITA_PDISC","flg_recuperato","flg_disconnesso","flg_sospeso",
                          "DAT_PRIMA_ATTIV_CNTR","VAL_ARPU_TEORICO","VAL_ARPU_NET_DISCOUNT","VAL_ARPU_AVG_TEORICO_3M","VAL_ARPU_AVG_NET_DISCOUNT_3M",
                          "FASCIA_SDM_PDISC_M1","FASCIA_SDM_PDISC_M4","digitalizzazione_bin","tipo_pack")

nrow(cambi_stato_v27) #119116

write.parquet(cambi_stato_v27,"/user/valentina/ModChurnOnLine_base_Pdisc.parquet")


# ## controllo x quanti clienti manca il livello di digitalizzazione
# base_pdisc <- read.parquet("/user/valentina/ModChurnOnLine_base_Pdisc.parquet")
# createOrReplaceTempView(base_pdisc,"base_pdisc")
# count_digitaliz <- sql("select digitalizzazione_bin,count(COD_CONTRATTO) as num_cntr
#                        from base_pdisc
#                        group by digitalizzazione_bin")
# View(head(count_digitaliz,200))



#################################################################################################################
#################################################################################################################
# aggiungo i cookies adobe
#################################################################################################################
#################################################################################################################

base_pdisc <- read.parquet("/user/valentina/ModChurnOnLine_base_Pdisc.parquet")
nrow(base_pdisc)  #119116

base_pdisc_skyid <- distinct(select(base_pdisc,"SKY_ID"))


skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated"))

#join
createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
createOrReplaceTempView(base_pdisc_skyid,"base_pdisc_skyid")

recupera_cookies <- sql("select *
                        from base_pdisc_skyid  t1 left join skyitdev_df_3 t2
                        on t1.SKY_ID=t2.external_id_post_evar")

write.parquet(recupera_cookies,"/user/valentina/ModChurnOnLine_base_Pdisc_cookies_adobe.parquet")





#################################################################################################################
#################################################################################################################
# associazione cookies adform dalla tabella dei tracking point
#################################################################################################################
#################################################################################################################

base_pdisc <- read.parquet("/user/valentina/ModChurnOnLine_base_Pdisc.parquet")
base_pdisc_skyid <- distinct(select(base_pdisc,"SKY_ID"))


adform_tp_1 <- read.parquet('hdfs:///STAGE/adform/table=Trackingpoint/Trackingpoint.parquet')

createOrReplaceTempView(adform_tp_1, "adform_tp_1")
adform_tp_2 <- sql( "SELECT regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                    cookieid, 
                    'device-name',
                    'os-name', 
                    'browser-name'
                    from adform_tp_1
                    where(customvars.systemvariables like '%13%' and cookieid <> '0' )
                    having (skyid <> '' and length(skyid)= 48)")


createOrReplaceTempView(adform_tp_2,"adform_tp_2")
adform_tp_3 <- sql("select  skyid,cookieid,
                   'device-name' as device_name,
                   'os-name' as os_name,
                   'browser-name' as browser_name 
                   from adform_tp_2")


########## metto in join le due basi per associare i cookies agli external_id

createOrReplaceTempView(adform_tp_3,"adform_tp_3")
createOrReplaceTempView(base_pdisc_skyid,"base_pdisc_skyid")

recupera_cookies_1 <- sql("select distinct t1.SKY_ID,
                          t2.skyid as skyid_adfomr,        
                          t2.cookieid,
                          t2.device_name,
                          t2.os_name,
                          t2.browser_name                          
                          from base_pdisc_skyid t1 left join adform_tp_3 t2
                          on t1.SKY_ID=t2.skyid")

write.parquet(recupera_cookies_1,"/user/valentina/ModChurnOnLine_base_Pdisc_cookies_adform.parquet")



#################################################################################################################
#################################################################################################################
#################################################################################################################
#################################################################################################################



#################################################################################################################
#################################################################################################################
# scarico base no pdisc a giugno 2017
#################################################################################################################
#################################################################################################################

base_sto_201706 <- read.parquet("hdfs:///STAGE/CMDM/AmbienteAnalisiStoricizzato/delta/fat_contratto_sto_SHOT_2_201706.parquet")

base_sto_201706_v2 <- filter( select(base_sto_201706,c("COD_CONTRATTO","DAT_PRIMA_ATTIV_CNTR","DES_PACCHETTO_POSSEDUTO",
                                                       "VAL_ARPU_NET_DISCOUNT","VAL_ARPU_AVG_TEORICO_3M","VAL_ARPU_AVG_NET_DISCOUNT_3M",
                                                       "DES_STATO_BUSINESS","DES_TIPO_CONTRATTO")), "DES_TIPO_CONTRATTO='RESIDENZIALE'")

base_sto_201706_v3 <- filter(base_sto_201706_v2,"DES_STATO_BUSINESS= 'Active'")


#pacchetto
createOrReplaceTempView(base_sto_201706_v3,"base_sto_201706_v3")
base_sto_201706_v4 <- sql("select *,
                          case when des_pacchetto_posseduto like '%CINEMA%' then 1 else 0 end as flg_cinema,
                          case when des_pacchetto_posseduto like '%CALCIO%' then 1 else 0 end as flg_calcio,
                          case when des_pacchetto_posseduto like '%SPORT%' then 1 else 0 end as flg_sport
                          from base_sto_201706_v3")


createOrReplaceTempView(base_sto_201706_v4,"base_sto_201706_v4")
base_sto_201706_v5 <- sql("select *,
                          case when flg_cinema=1 and flg_calcio=1 and flg_sport=1 then 'CINEMA + SPORT + CALCIO'
                          when flg_cinema=0 and flg_calcio=1 and flg_sport=1 then 'SPORT + CALCIO'
                          when flg_cinema=1 and flg_calcio=1 and flg_sport=0 then 'CINEMA + CALCIO'
                          when flg_cinema=1 and flg_calcio=1 and flg_sport=0 then 'CINEMA + CALCIO'
                          when flg_cinema=1 and flg_calcio=0 and flg_sport=1 then 'CINEMA + SPORT'
                          when flg_cinema=1 and flg_calcio=0 and flg_sport=0 then 'CINEMA'
                          when flg_cinema=0 and flg_calcio=1 and flg_sport=0 then 'CALCIO'
                          when flg_cinema=0 and flg_calcio=0 and flg_sport=1 then 'SPORT'
                          else 'BASE' end as tipo_pack
                          from base_sto_201706_v4")

nrow(base_sto_201706_v5) # 4139330

base_sto_201706_v6 <- select(base_sto_201706_v5,
                             "COD_CONTRATTO","DAT_PRIMA_ATTIV_CNTR","VAL_ARPU_NET_DISCOUNT",       
                             "VAL_ARPU_AVG_TEORICO_3M","VAL_ARPU_AVG_NET_DISCOUNT_3M","tipo_pack" )


## metto in join le chiavi per recuperare lo skyid
chiavi <- read.parquet("hdfs:///STAGE/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2.parquet")

createOrReplaceTempView(base_sto_201706_v6,"base_sto_201706_v6")
createOrReplaceTempView(chiavi,"chiavi")

base_sto_201706_v7 <- sql("select t1.*, t2.COD_CONTRATTO_CIFRATO, t2.COD_CLIENTE_CIFRATO as SKY_ID,
                          t2.COD_CLIENTE
                          from base_sto_201706_v6 t1 left join chiavi t2
                          on t1.COD_CONTRATTO=t2.COD_CONTRATTO")

createOrReplaceTempView(base_sto_201706_v7,"base_sto_201706_v7")
base_sto_201706_v8 <- sql("select COD_CONTRATTO as COD_CONTRATTO_INchiaro,SKY_ID,COD_CLIENTE,
                          COD_CONTRATTO_CIFRATO as COD_CONTRATTO,
                          DAT_PRIMA_ATTIV_CNTR,VAL_ARPU_NET_DISCOUNT,VAL_ARPU_AVG_TEORICO_3M,     
                          VAL_ARPU_AVG_NET_DISCOUNT_3M,tipo_pack,SKY_ID 
                          from base_sto_201706_v7")

nrow(base_sto_201706_v8) #4139330


#elimino da questa base i pdisc
createOrReplaceTempView(base_sto_201706_v8,"base_sto_201706_v8")

base_pdisc <- read.parquet("/user/valentina/ModChurnOnLine_base_Pdisc.parquet")
createOrReplaceTempView(base_pdisc,"base_pdisc")


base_sto_201706_v9 <- sql("select t1.*, 
                          t2.COD_CONTRATTO as cod_2
                          from base_sto_201706_v8 t1 left join base_pdisc t2
                          on t1.COD_CONTRATTO=t2.COD_CONTRATTO")
View(head(base_sto_201706_v9,2000))

base_sto_201706_v10 <- filter(base_sto_201706_v9,"cod_2 is NULL")
nrow(base_sto_201706_v10)  #4039609



#### aggiungo il modello pdisc

FAT_SCORE_SDM_STO_v1 <- read.parquet("hdfs:///STAGE/CMDM/ModelliScoring/delta/fat_score_sdm_sto_SHOT_2.parquet")

FAT_SCORE_SDM_STO_v2 <- select(FAT_SCORE_SDM_STO_v1,
                               "COD_CONTRATTO","DAT_MESE_RIFERIMENTO","FASCIA_SDM_PDISC_M1","FASCIA_SDM_PDISC_M4")

FAT_SCORE_SDM_STO_mag <- filter(FAT_SCORE_SDM_STO_v2,"DAT_MESE_RIFERIMENTO='1/5/2017' ")

createOrReplaceTempView(FAT_SCORE_SDM_STO_mag,"FAT_SCORE_SDM_STO_mag")
createOrReplaceTempView(base_sto_201706_v10,"base_sto_201706_v10")

base_sto_201706_v11 <- sql("select t1.*,
                           t2.FASCIA_SDM_PDISC_M1 as FASCIA_SDM_PDISC_M1_05,
                           t2.FASCIA_SDM_PDISC_M4 as FASCIA_SDM_PDISC_M4_05
                           from base_sto_201706_v10 t1 left join FAT_SCORE_SDM_STO_mag t2
                           on t1.COD_CONTRATTO=t2.COD_CONTRATTO")

nrow(base_sto_201706_v11)  #4039609

#View(head(base_sto_201706_v11,1000))

base_sto_201706_v12 <- select(base_sto_201706_v11,
                              "COD_CONTRATTO","COD_CONTRATTO_INchiaro","SKY_ID","COD_CLIENTE",                                
                              "DAT_PRIMA_ATTIV_CNTR","VAL_ARPU_NET_DISCOUNT","VAL_ARPU_AVG_TEORICO_3M","VAL_ARPU_AVG_NET_DISCOUNT_3M",
                              "tipo_pack","FASCIA_SDM_PDISC_M1_05","FASCIA_SDM_PDISC_M4_05")

write.parquet(base_sto_201706_v12,"/user/valentina/ModChurnOnLine_base_NO_Pdisc.parquet")



#################################################################################################################
#################################################################################################################
# aggiungo i cookies adobe
#################################################################################################################
#################################################################################################################

base_NO_pdisc <- read.parquet("/user/valentina/ModChurnOnLine_base_NO_Pdisc.parquet")
nrow(base_NO_pdisc)  #4039609

base_NO_pdisc_skyid <- distinct(select(base_NO_pdisc,"SKY_ID"))

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated"))

#join
createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
createOrReplaceTempView(base_NO_pdisc_skyid,"base_NO_pdisc_skyid")

recupera_cookies <- sql("select *
                        from base_NO_pdisc_skyid  t1 left join skyitdev_df_3 t2
                        on t1.SKY_ID=t2.external_id_post_evar")

write.parquet(recupera_cookies,"/user/valentina/ModChurnOnLine_base_NO_Pdisc_cookies_adobe.parquet")










#chiudi sessione
sparkR.stop()

