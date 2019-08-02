source("connection_R.r")
Sys.setlocale(category = "LC_ALL", locale = "English_United States.1252")
library(magrittr)
#### LETTURA TABELLE #####
cmdm= read.df("/STAGE/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2.parquet", "parquet")
ambiente = read.df("/STAGE/CMDM/AmbienteAnalisiStoricizzato/delta/fat_contratto_sto*.parquet", "parquet")
createOrReplaceTempView(ambiente, "ambiente")
createOrReplaceTempView(cmdm, "cmdm")

#### FILTRA CLIENTI ATTIVI E AGGIUNGI INFO. SULLO STORICO #####
clienti_attivi <- sql("SELECT COD_CONTRATTO, ID_PARCO_SKY from ambiente
                      where DAT_MESE_RIFERIMENTO='1/9/2017' and DES_STATO_BUSINESS in ('Active','Active Pdisc','Active Pre Pdisc','Active Suspended') and  DES_TIPO_CONTRATTO='RESIDENZIALE'")
createOrReplaceTempView(clienti_attivi, "clienti_attivi")

join <- sql("SELECT c.COD_CONTRATTO, c.COD_CLIENTE, c.COD_CLIENTE_CIFRATO, c.COD_CONTRATTO_CIFRATO 
            FROM cmdm c 
            INNER JOIN clienti_attivi att ON c.COD_CONTRATTO=att.COD_CONTRATTO")
#write.parquet(join, "/user/jacopoa/attivi.parquet")    
createOrReplaceTempView(join, "join")

storico <- sql("SELECT COD_CONTRATTO,
               COD_PROVINCIA_FRU,
               COD_SESSO_CLIENTE_FRU,
               COD_STB_PRINCIPALE,
               DAT_MESE_RIFERIMENTO,
               DAT_NASCITA_CLIENTE_FRU,
               DAT_PRIMA_ATTIV_CNTR,
               DES_AREA_NIELSEN_FRU,
               DES_CANALE_LOYALTY,
               DES_CANALE_VENDITA,
               DES_CANALE_VENDITA_DETT,
               DES_CLUSTER_POLIS_L1_2011,
               DES_DECODER_PRINCIPALE,
               DES_FORMATO_FATTURA,
               DES_INVIO_FATTURA,
               DES_MOD_PAG,
               DES_NOME_REG_FRU,
               DES_PACCHETTO_POSSEDUTO,
               DES_STATO_BUSINESS,
               DES_TIPO_CONTRATTO,
               FLG_BOX_SETS_ATTIVO,
               FLG_CALCIO_PACPOS,
               FLG_CAP_CRITICO,
               FLG_CINEMA_PACPOS,
               FLG_CONTRATTO_BUNDLE,
               FLG_HD,
               FLG_HD_PACPOS,
               FLG_IPTV,
               FLG_LISTINO_RETENTION,
               FLG_LISTINO_RETENTION_GEO,
               FLG_MATCH_DTH_RIVER,
               FLG_MV_1,
               FLG_MYSKY,
               FLG_MYSKYHD,
               FLG_PAG_FRU_DIVERSI,
               FLG_PULLVOD_ATTIVO,
               FLG_RESISTANT_CALCIO,
               FLG_RESISTANT_CINEMA,
               FLG_RESISTANT_SPORT,
               FLG_SKYFAMIGLIA_PACPOS,
               FLG_SKYGO,
               FLG_SKYGO_PLUS_ATTIVO,
               FLG_SKYTV_PACPOS,
               FLG_SPORT_PACPOS,
               NUM_AGEING_CNTR_AA,
               NUM_CONT_PHONE_INBOUND_14D,
               NUM_CONT_PHONE_INBOUND_180D,
               NUM_CONT_PHONE_INBOUND_30D,
               NUM_CONT_PHONE_INBOUND_360D,
               NUM_CONT_PHONE_INBOUND_60D,
               NUM_CONT_PHONE_INBOUND_7D,
               NUM_CONT_PHONE_INBOUND_90D,
               NUM_CONT_WEB_14D,
               NUM_CONT_WEB_180D,
               NUM_CONT_WEB_30D,
               NUM_CONT_WEB_360D,
               NUM_CONT_WEB_60D,
               NUM_CONT_WEB_7D,
               NUM_CONT_WEB_90D,
               NUM_DEVICE_SKYGO,
               NUM_PACCHETTI_MV,
               VAL_ARPU_NET_DISCOUNT,
               VAL_ARPU_TEORICO
               FROM ambiente
               where DAT_MESE_RIFERIMENTO='1/9/2017' and DES_STATO_BUSINESS in ('Active','Active Pdisc','Active Pre Pdisc','Active Suspended') and  DES_TIPO_CONTRATTO='RESIDENZIALE'")


storico <- withColumn(storico, "pacchetto", lit("BASE"))

storico$pacchetto = ifelse(contains(storico$DES_PACCHETTO_POSSEDUTO, c("CINEMA")), 'CINEMA', storico$pacchetto)
storico$pacchetto = ifelse(contains(storico$DES_PACCHETTO_POSSEDUTO, c("CALCIO")), 'CALCIO', storico$pacchetto)
storico$pacchetto = ifelse(contains(storico$DES_PACCHETTO_POSSEDUTO, c("SPORT")), 'SPORT', storico$pacchetto)
storico$pacchetto = ifelse(contains(storico$DES_PACCHETTO_POSSEDUTO, c("MINI")), 'MINIPACK NEWS', storico$pacchetto)
storico$pacchetto = ifelse(contains(storico$DES_PACCHETTO_POSSEDUTO, c("SPORT + CALCIO")), 'SPORT + CALCIO', storico$pacchetto)
storico$pacchetto = ifelse(contains(storico$DES_PACCHETTO_POSSEDUTO, c("CINEMA + CALCIO")), 'CINEMA + CALCIO', storico$pacchetto)
storico$pacchetto = ifelse(contains(storico$DES_PACCHETTO_POSSEDUTO, c("CINEMA + SPORT")), 'CINEMA + SPORT', storico$pacchetto)
storico$pacchetto = ifelse(contains(storico$DES_PACCHETTO_POSSEDUTO, c("CINEMA + SPORT + CALCIO")), 'CINEMA + SPORT + CALCIO', storico$pacchetto)

storico <- withColumn(storico, "visione_stb", lit("SD"))
storico$visione_stb = ifelse(contains(storico$FLG_HD, c("1")), 'HD', storico$visione_stb)
storico$visione_stb = ifelse(contains(storico$FLG_MYSKY, c("1")), 'MYSKY', storico$visione_stb)
storico$visione_stb = ifelse(contains(storico$FLG_MYSKYHD, c("1")), 'MYSKYHD', storico$visione_stb)

storico <- withColumn(storico, "flg_tier2", lit("0"))
storico$flg_tier2 = ifelse(contains(storico$FLG_LISTINO_RETENTION, c("1")), '1', storico$flg_tier2)
storico$flg_tier2 = ifelse(contains(storico$FLG_LISTINO_RETENTION_GEO, c("1")), '1', storico$flg_tier2)

storico <- withColumn(storico, "decoder_principale", lit("STD"))
storico$decoder_principale = ifelse(contains(substr(storico$COD_STB_PRINCIPALE, 1,1), c("H")), 'HD', storico$decoder_principale)
storico$decoder_principale = ifelse(contains(substr(storico$COD_STB_PRINCIPALE, 1,1), c("P")), 'MySky', storico$decoder_principale)
storico$decoder_principale = ifelse(contains(substr(storico$COD_STB_PRINCIPALE, 1,1), c("V")), 'MYSKYHD Fusion', storico$decoder_principale)
storico$decoder_principale = ifelse(contains(storico$COD_STB_PRINCIPALE, c("V1")), 'MYSKYHD No Fusion', storico$decoder_principale)
storico$decoder_principale = ifelse(contains(storico$COD_STB_PRINCIPALE, c("V2")), 'MYSKYHD No Fusion', storico$decoder_principale)
storico$decoder_principale = ifelse(contains(storico$COD_STB_PRINCIPALE, c("VE")), 'MYSKYHD No Fusion', storico$decoder_principale)

createOrReplaceTempView(storico, "storico")

join_1 <- sql("SELECT c.COD_CLIENTE, c.COD_CLIENTE_CIFRATO, c.COD_CONTRATTO_CIFRATO, att.* FROM cmdm c INNER JOIN storico att ON c.COD_CONTRATTO=att.COD_CONTRATTO")
createOrReplaceTempView(join_1, "join_1")
#c1 <- sql("SELECT COUNT(*) FROM join_1")

#### AGGIUNGI EXTRA #####

skyextra <- read.df("/STAGE/CMDM/DettaglioSkyExtraReward/delta/fat_iscrizioni_skyextra_sto_SHOT_2.parquet", "parquet")
skyextra <- fillna(skyextra, '31/12/9999')
createOrReplaceTempView(skyextra, "skyextra")
skyextra <- sql("SELECT * FROM skyextra WHERE DAT_USCITA='31/12/9999'")
skyextra$flg_iscritto_extra <- lit(1)
createOrReplaceTempView(skyextra, "skyextra")

join_2 <- sql("SELECT j.*,  ext.DES_CANALE, ext.flg_iscritto_extra FROM join_1 j LEFT JOIN skyextra ext ON j.COD_CONTRATTO_CIFRATO=ext.COD_CONTRATTO")
createOrReplaceTempView(join_2, "join_2")
#c2 <- sql("SELECT COUNT(*) FROM join_2")
#View(head(c2))
join_2 <- fillna(join_2, 0)

#### AGGIUNGI CLUSTER CMDM BASATO SU ARPU #####

cluster <- read.df("/STAGE/CMDM/MovimentazioneClusterArpuDevelopment/full/fat_mov_cluster_arpu_dev_SHOT_2.parquet", "parquet")
createOrReplaceTempView(cluster, "cluster")
cluster <- sql("SELECT * FROM cluster WHERE DAT_FINE='31/12/9999'")
createOrReplaceTempView(cluster, "cluster")
join_3 <- sql("SELECT j.*, clu.DES_CLUSTER_ARPU_DEV FROM join_2 j LEFT JOIN cluster clu ON j.COD_CONTRATTO_CIFRATO=clu.COD_CONTRATTO")
write.parquet(join_3, "/user/jacopoa/bigcmdm.parquet")  

join_3 <- read.df("/user/jacopoa/bigcmdm.parquet", "parquet")
#tablecmdm <- read.df("/user/jacopoa/bigcmdm_official.parquet")
#### AGGIUNGI TENURE + ETA' + MANIPOLAZIONI VARIE #####

createOrReplaceTempView(join_3, "join_3")
#c4 <- sql("SELECT COUNT(*) FROM join_3")
persist(join_3, "MEMORY_ONLY")
#createOrReplaceTempView(c4, "c4")
#View(head(c4))
join_3 <- sql("SELECT *, split(DAT_NASCITA_CLIENTE_FRU, '/')[0] as part1, split(DAT_NASCITA_CLIENTE_FRU, '/')[1] as part2, split(DAT_NASCITA_CLIENTE_FRU, '/')[2] as part3 FROM join_3")

join_3$zero <- lit("0")
join_3$length1 <- length(join_3$part1)
join_3$length2 <- length(join_3$part2)
join_3$part1 = ifelse(join_3$length1==1, concat(join_3$zero, join_3$part1), join_3$part1)
join_3$part2 = ifelse(join_3$length2==1, concat(join_3$zero, join_3$part2), join_3$part2)
join_3$slash <- lit("-")
join_3$DAT_NASCITA_CLIENTE_FRU_2 <- concat(join_3$part3, join_3$slash, join_3$part2, join_3$slash, join_3$part1)
join_3$zero <- NULL
join_3$length1 <- NULL
join_3$length2 <- NULL
join_3$part1 <- NULL
join_3$part2 <- NULL
join_3$part3 <- NULL
join_3$slash <- NULL
join_3 <- withColumn(join_3, "DAT_NASCITA_CLIENTE_FRU_2", cast(join_3$DAT_NASCITA_CLIENTE_FRU_2, "date"))
createOrReplaceTempView(join_3, "join_3")
join_3 <- sql("select *, floor(datediff(current_date(),
              TO_DATE(CAST(UNIX_TIMESTAMP(DAT_NASCITA_CLIENTE_FRU_2,'yyyy-MM-dd') AS TIMESTAMP)))/365) as eta
              from join_3")

join_3$classe_eta <- lit("A: <30")
join_3$classe_eta <- ifelse(join_3$eta<30, "A: <30", join_3$classe_eta)
join_3$classe_eta <- ifelse(join_3$eta<=40 & join_3$eta>30, "B: 31-40", join_3$classe_eta)
join_3$classe_eta <- ifelse(join_3$eta<=50 & join_3$eta>40, "C: 41-50", join_3$classe_eta)
join_3$classe_eta <- ifelse(join_3$eta<=60 & join_3$eta>50, "D: 51-60", join_3$classe_eta)
join_3$classe_eta <- ifelse(join_3$eta<=70 & join_3$eta>60, "E: 61-70", join_3$classe_eta)
join_3$classe_eta <- ifelse(join_3$eta<=80 & join_3$eta>70, "F: 71-80", join_3$classe_eta)
join_3$classe_eta <- ifelse(join_3$eta>80, "G: >80", join_3$classe_eta)

createOrReplaceTempView(join_3, "join_3")
join_3 <- sql("SELECT *, split(DAT_PRIMA_ATTIV_CNTR, '/')[0] as part1, split(DAT_PRIMA_ATTIV_CNTR, '/')[1] as part2, split(DAT_PRIMA_ATTIV_CNTR, '/')[2] as part3 FROM join_3")

join_3$zero <- lit("0")
join_3$length1 <- length(join_3$part1)
join_3$length2 <- length(join_3$part2)
join_3$part1 = ifelse(join_3$length1==1, concat(join_3$zero, join_3$part1), join_3$part1)
join_3$part2 = ifelse(join_3$length2==1, concat(join_3$zero, join_3$part2), join_3$part2)
join_3$slash <- lit("-")
join_3$DAT_PRIMA_ATTIV_CNTR_2 <- concat(join_3$part3, join_3$slash, join_3$part2, join_3$slash, join_3$part1)
join_3$zero <- NULL
join_3$length1 <- NULL
join_3$length2 <- NULL
join_3$part1 <- NULL
join_3$part2 <- NULL
join_3$part3 <- NULL
join_3$slash <- NULL
join_3 <- withColumn(join_3, "DAT_PRIMA_ATTIV_CNTR_2", cast(join_3$DAT_PRIMA_ATTIV_CNTR_2, "date"))
createOrReplaceTempView(join_3, "join_3")
join_3 <- sql("select *, floor(datediff(current_date(),
              TO_DATE(CAST(UNIX_TIMESTAMP(DAT_PRIMA_ATTIV_CNTR_2,'yyyy-MM-dd') AS TIMESTAMP)))/30) as tenure_month
              from join_3")

join_3 <- withColumn(join_3, "NUM_AGEING_CNTR_AA", cast(join_3$NUM_AGEING_CNTR_AA, "double"))
join_3$cl_tenure <- lit("1. <1")
join_3$cl_tenure = ifelse(join_3$tenure_month<36 & join_3$tenure_month>= 12, "2. da 1 a 3", join_3$cl_tenure)
join_3$cl_tenure = ifelse(join_3$tenure_month>=36 & join_3$tenure_month<72, "3. da 3 a 6", join_3$cl_tenure)
join_3$cl_tenure = ifelse(join_3$tenure_month>=72 & join_3$tenure_month<120, "4. da 6 a 10", join_3$cl_tenure)
join_3$cl_tenure = ifelse(join_3$tenure_month>=120, "5. >10", join_3$cl_tenure)

persist(join_3, "MEMORY_ONLY")
#View(head(join_3, 100))
count_join <- sql("SELECT COUNT(*) FROM join_3")
#View(head(count_join))
write.parquet(join_3, "/user/jacopoa/bigcmdm_official.parquet")  

#### TABELLA BASE CMDM PER DIGITALIZZAZIONE CREATA. MANCA TUTTA LA PARTE SULLE ESIGENZE ####
