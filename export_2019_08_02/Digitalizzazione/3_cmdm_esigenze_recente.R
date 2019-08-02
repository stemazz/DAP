library(magrittr)

####### TABELLA ESIGENZE RECENTI + UNIONE CON LA VECCHIA #########

vs_esigenze_dett_timest_recente <- read.parquet("/STAGE/CMDM/DettaglioEsigenze/delta/vs_esigenze_dett_SHOT_2.parquet")

#provincia <- read.parquet("/user/jacopoa/sky_fuori_provincia.csv")

persist(vs_esigenze_dett_timest_recente, "MEMORY_ONLY")
createOrReplaceTempView(vs_esigenze_dett_timest_recente, "vs_esigenze_dett_timest_recente")


#provo a creae una tabella con tutti i filtri (tranne quello temporale)
estrai_esig_all_temp_recente <- sql("select COD_CONTRATTO, COD_ESIGENZA, DAT_APERTURA, DAT_CHIUSURA, DES_STATO, DES_CATEGORIA, DES_SUBCATEGORIA,
                                    DES_MOTIVO,DES_TIPO_CONTATTO,DES_CANALE_CNT
                                    from vs_esigenze_dett_timest_recente
                                    where
                                    DES_MOTIVO not in ('INFO CONSEGNA SWAP P2P3' , 'PRENOTAZIONE INVIO SMC SWAP P2 P3' , 'SWAP COMMERCIALE P2/P3' ,
                                    'BLUE SCREEN SWAP P2 P3' , 'PRENOTAZIONE INVIO SMC SWAP P2' , 'CHIEDE ATTIVAZIONE SMC P2P3' ,
                                    'SWAP P2 P3'  ) and
                                    DES_TIPO_CONTATTO in ('CALL ME NOW' , 'INBOUND') and
                                    DES_CANALE_CNT not in ('Undefined' ,  'NONE' , ' ' , 'N/A' , 'NA' )
                                    ")
#View(head(estrai_esig_all_temp_recente,1000))
printSchema(estrai_esig_all_temp_recente)
createOrReplaceTempView(estrai_esig_all_temp_recente, "estrai_esig_all_temp_recente")

#seleziona solo esigenze aperte con canali d'interesse
estrai_esig_cod_canale_recente <- sql("select * from (select COD_CONTRATTO, COD_ESIGENZA, DAT_APERTURA, DAT_CHIUSURA, DES_STATO, DES_CATEGORIA, DES_SUBCATEGORIA,
                                      DES_MOTIVO,DES_TIPO_CONTATTO,DES_CANALE_CNT,
                                      case when DES_CANALE_CNT in ('WEBMAIL','WEBEASY','WEB - RISOLVI ONLINE','WEB','MOBILE','EMAIL','CHAT') then 'WEB'
                                      when DES_CANALE_CNT in ('UPSELLING RETAIL','SKY SERVICE','SKY CENTER') then 'RETAIL'
                                      when DES_CANALE_CNT in ('TELEFONO','IVR-SELFCARE','IVR SELFCARE','IVR','CRM') then 'TELEFONO'
                                      when DES_CANALE_CNT in ('SMS-SELFCARE','SMS') then 'SMS'
                                      else NULL end as CANALE_CONTATTO_COD
                                      from estrai_esig_all_temp_recente)
                                      where CANALE_CONTATTO_COD is not NULL
                                      ")
#View(head(estrai_esig_cod_canale_recente,1000))
createOrReplaceTempView(estrai_esig_cod_canale_recente, "estrai_esig_cod_canale_recente")

#aggiungi alla tabella un flg per indicare se il cliente ha aperto un'esigenza di UPGRADE (info e/o attivaz) + flg per apertura esig tecnica
estrai_esig_cod_canale_recente$FLG_ESIG_UPGRADE = ifelse(contains(estrai_esig_cod_canale_recente$DES_MOTIVO, c("UPGRADE")), '1','0')
#View(head(estrai_esig_cod_canale_recente,10000))

estrai_esig_cod_canale_recente$FLG_ESIG_TECNICA = ifelse(contains(estrai_esig_cod_canale_recente$DES_CATEGORIA, c("GESTIONE TECNICA")), '1','0')
#View(head(estrai_esig_cod_canale_recente,10000))

printSchema(estrai_esig_cod_canale_recente)


#### converto la data apertura  (che ha gli zeri!!!!)
estrai_esig_cod_canale2_recente2 <- withColumn(estrai_esig_cod_canale_recente,"DAT_APERTURA_dt",cast(unix_timestamp(estrai_esig_cod_canale_recente$DAT_APERTURA, 'dd/MM/yyyy'),'timestamp'))
estrai_esig_cod_canale2_recente3 <- withColumn(estrai_esig_cod_canale2_recente2,"DAT_CHIUSURA_dt",cast(unix_timestamp(estrai_esig_cod_canale2_recente2$DAT_CHIUSURA, 'dd/MM/yyyy'),'timestamp'))
#View(head(estrai_esig_cod_canale2_recente3,10000))
#printSchema(estrai_esig_cod_canale2_recente3)
createOrReplaceTempView(estrai_esig_cod_canale2_recente3, "estrai_esig_cod_canale2_recente3")


###faccio un filtro sugli ultimi 3 anni di storico
tab_esig_fin1_recente <- sql("select *
                             from estrai_esig_cod_canale2_recente3
                             where (DAT_APERTURA_dt <> '9999-12-31' and DAT_APERTURA_dt>='2015-10-01')
                             ")

#faccio un check visualizzando il min e max delle date presenti
verifica_date_apertura_recente <- agg(
  tab_esig_fin1_recente,
  max = max(tab_esig_fin1_recente$DAT_APERTURA_dt),
  min = min(tab_esig_fin1_recente$DAT_APERTURA_dt)
)
#View(head(verifica_date_apertura_recente))   #min:2015-10-01 / max 2017-03-21



tab_esig_fin1_recente <- withColumn(tab_esig_fin1_recente, "FLG_ESIG_UPGRADE", cast(tab_esig_fin1_recente$FLG_ESIG_UPGRADE, "double"))
tab_esig_fin1_recente <- withColumn(tab_esig_fin1_recente, "FLG_ESIG_TECNICA", cast(tab_esig_fin1_recente$FLG_ESIG_TECNICA, "double"))
write.parquet(tab_esig_fin1_recente, "/user/jacopoa/esigenze_recente.parquet")
persist(tab_esig_fin1_recente, "MEMORY_AND_DISK")


esigenze_recente <- read.parquet("/user/jacopoa/esigenze_recente.parquet")
esigenze <- read.parquet("/user/jacopoa/esigenze.parquet")
#View(head(esigenze_recente, 1000))


vs_esigenze_dett_timest <- union(esigenze, esigenze_recente)
tab_esig_fin <- distinct(vs_esigenze_dett_timest)
persist(tab_esig_fin, "MEMORY_ONLY")
createOrReplaceTempView(tab_esig_fin, "tab_esig_fin")

#### CONTEGGIO DELLE ESIGENZE #####

count_upgrade_total_no_filter <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_total_no FROM tab_esig_fin GROUP BY 1")
count_upgrade_telefono_no_filter <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_telefono_no FROM tab_esig_fin WHERE CANALE_CONTATTO_COD='TELEFONO' GROUP BY 1")
count_upgrade_sms_no_filter <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_sms_no FROM tab_esig_fin WHERE CANALE_CONTATTO_COD='SMS' GROUP BY 1")
count_upgrade_retail_no_filter <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_retail_no FROM tab_esig_fin WHERE CANALE_CONTATTO_COD='RETAIL' GROUP BY 1")
count_upgrade_web_no_filter <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_web_no FROM tab_esig_fin WHERE CANALE_CONTATTO_COD='WEB' GROUP BY 1")

count_upgrade_total <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_total FROM tab_esig_fin WHERE FLG_ESIG_UPGRADE=1 GROUP BY 1")
count_upgrade_telefono <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_telefono FROM tab_esig_fin WHERE FLG_ESIG_UPGRADE=1 AND CANALE_CONTATTO_COD='TELEFONO' GROUP BY 1")
count_upgrade_sms <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_sms FROM tab_esig_fin WHERE FLG_ESIG_UPGRADE=1 AND CANALE_CONTATTO_COD='SMS' GROUP BY 1")
count_upgrade_retail <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_retail FROM tab_esig_fin WHERE FLG_ESIG_UPGRADE=1 AND CANALE_CONTATTO_COD='RETAIL' GROUP BY 1")
count_upgrade_web <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_web FROM tab_esig_fin WHERE FLG_ESIG_UPGRADE=1 AND CANALE_CONTATTO_COD='WEB' GROUP BY 1")

count_tecnica_total <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as tecnica_total FROM tab_esig_fin WHERE FLG_ESIG_TECNICA=1 GROUP BY 1")
count_tecnica_telefono <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as tecnica_telefono FROM tab_esig_fin WHERE FLG_ESIG_TECNICA=1 AND CANALE_CONTATTO_COD='TELEFONO' GROUP BY 1")
count_tecnica_sms <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as tecnica_sms FROM tab_esig_fin WHERE FLG_ESIG_TECNICA=1 AND CANALE_CONTATTO_COD='SMS' GROUP BY 1")
count_tecnica_retail <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as tecnica_retail FROM tab_esig_fin WHERE FLG_ESIG_TECNICA=1 AND CANALE_CONTATTO_COD='RETAIL' GROUP BY 1")
count_tecnica_web <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as tecnica_web FROM tab_esig_fin WHERE FLG_ESIG_TECNICA=1 AND CANALE_CONTATTO_COD='WEB' GROUP BY 1")


visitor_rollup_no_filter_upgrade <- list(count_upgrade_total_no_filter, count_upgrade_telefono_no_filter) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_no_filter_upgrade$COD_CONTRATTO_y <- NULL

visitor_rollup_no_filter_upgrade <- list(visitor_rollup_no_filter_upgrade, count_upgrade_sms_no_filter) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_no_filter_upgrade$COD_CONTRATTO_y <- NULL

visitor_rollup_no_filter_upgrade <- list(visitor_rollup_no_filter_upgrade, count_upgrade_retail_no_filter) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_no_filter_upgrade$COD_CONTRATTO_y <- NULL

visitor_rollup_no_filter_upgrade <- list(visitor_rollup_no_filter_upgrade, count_upgrade_web_no_filter) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_no_filter_upgrade$COD_CONTRATTO_y <- NULL

##########################


visitor_rollup_tecnica <- list(count_tecnica_total, count_tecnica_telefono) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_tecnica$COD_CONTRATTO_y <- NULL

visitor_rollup_tecnica <- list(visitor_rollup_tecnica, count_tecnica_sms) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_tecnica$COD_CONTRATTO_y <- NULL

visitor_rollup_tecnica <- list(visitor_rollup_tecnica, count_tecnica_retail) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_tecnica$COD_CONTRATTO_y <- NULL

visitor_rollup_tecnica <- list(visitor_rollup_tecnica, count_tecnica_web) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_tecnica$COD_CONTRATTO_y <- NULL

########################################

visitor_rollup_upgrade <- list(count_upgrade_total, count_upgrade_telefono) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_upgrade$COD_CONTRATTO_y <- NULL

visitor_rollup_upgrade <- list(visitor_rollup_upgrade, count_upgrade_sms) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_upgrade$COD_CONTRATTO_y <- NULL

visitor_rollup_upgrade <- list(visitor_rollup_upgrade, count_upgrade_retail) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)

visitor_rollup_upgrade$COD_CONTRATTO_y <- NULL

visitor_rollup_upgrade <- list(visitor_rollup_upgrade, count_upgrade_web) %>%
  Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
visitor_rollup_upgrade$COD_CONTRATTO_y <- NULL

#View(head(visitor_rollup_upgrade, 100))

write.parquet(visitor_rollup_upgrade, "/user/jacopoa/visitor_rollup_upgrade.parquet")
write.parquet(visitor_rollup_tecnica, "/user/jacopoa/visitor_rollup_tecnica.parquet")
write.parquet(visitor_rollup_no_filter_upgrade, "/user/jacopoa/visitor_rollup_no_filter_upgrade.parquet")


bigcmdm <- read.parquet("/user/jacopoa/bigcmdm_official.parquet")
visitor_rollup_upgrade <- read.parquet("/user/jacopoa/visitor_rollup_upgrade.parquet")
visitor_rollup_tecnica <- read.parquet("/user/jacopoa/visitor_rollup_tecnica.parquet")
visitor_rollup_no_filter_upgrade <- read.parquet("/user/jacopoa/visitor_rollup_no_filter_upgrade.parquet")

#View(head(bigcmdm, 100))

######### UNIONE DEI CONTEGGI DELLE ESIGENZE ALLA MAXI TABLE ###########
createOrReplaceTempView(bigcmdm, "bigcmdm")
count_maxi_cmdm <- sql("SELECT COUNT(*) FROM bigcmdm")
#View(head(count_maxi_cmdm))

visitor_rollup_tecnica <- withColumnRenamed(visitor_rollup_tecnica, "COD_CONTRATTO_x", "COD_CONTRATTO_tecnica")
visitor_rollup_upgrade <- withColumnRenamed(visitor_rollup_upgrade, "COD_CONTRATTO_x", "COD_CONTRATTO_upgrade")
visitor_rollup_no_filter_upgrade <- withColumnRenamed(visitor_rollup_no_filter_upgrade, "COD_CONTRATTO_x", "COD_CONTRATTO_no_filter_upgrade")

createOrReplaceTempView(visitor_rollup_tecnica, "visitor_rollup_tecnica")
createOrReplaceTempView(visitor_rollup_upgrade, "visitor_rollup_upgrade")
createOrReplaceTempView(visitor_rollup_no_filter_upgrade, "visitor_rollup_no_filter_upgrade")

maxi_table_cmdm <- sql("SELECT b.*, vrt.*, vru.*, vrfu.* FROM bigcmdm b
                       LEFT JOIN visitor_rollup_tecnica vrt
                       on b.COD_CONTRATTO_CIFRATO=vrt.COD_CONTRATTO_tecnica
                       LEFT JOIN visitor_rollup_upgrade vru
                       on b.COD_CONTRATTO_CIFRATO=vru.COD_CONTRATTO_upgrade
                       LEFT JOIN visitor_rollup_no_filter_upgrade vrfu
                       on b.COD_CONTRATTO_CIFRATO=vrfu.COD_CONTRATTO_no_filter_upgrade")

# maxi_table_cmdm <-  sql("SELECT b.*, vrt.* FROM bigcmdm b
#     LEFT JOIN visitor_rollup_tecnica vrt
#     on b.COD_CONTRATTO_CIFRATO=vrt.COD_CONTRATTO_tecnica")
# createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")
# 
# maxi_table_cmdm <-  sql("SELECT b.*, vru.* FROM maxi_table_cmdm b
#     LEFT JOIN visitor_rollup_upgrade vru
#     on b.COD_CONTRATTO_CIFRATO=vru.COD_CONTRATTO_upgrade")
# 
# createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")
# 
# maxi_table_cmdm <-  sql("SELECT b.*, vrfu.* FROM maxi_table_cmdm b
#     LEFT JOIN visitor_rollup_no_filter_upgrade vrfu
#     on b.COD_CONTRATTO_CIFRATO=vrfu.COD_CONTRATTO_no_filter_upgrade")
# createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")

maxi_table_cmdm <- distinct(maxi_table_cmdm)
createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")
#count_maxi <- sql("SELECT COUNT(*) FROM maxi_table_cmdm")
#View(head(count_maxi))
persist(maxi_table_cmdm, "MEMORY_ONLY")
#View(head(maxi_table_cmdm, 100))
maxi_table_cmdm <- withColumnRenamed(maxi_table_cmdm, "upgrade_total_no", "total_esigenze")
maxi_table_cmdm <- withColumnRenamed(maxi_table_cmdm, "upgrade_telefono_no", "total_esigenze_telefono")
maxi_table_cmdm <- withColumnRenamed(maxi_table_cmdm, "upgrade_sms_no", "total_esigenze_sms")
maxi_table_cmdm <- withColumnRenamed(maxi_table_cmdm, "upgrade_retail_no", "total_esigenze_retail")
maxi_table_cmdm <- withColumnRenamed(maxi_table_cmdm, "upgrade_web_no", "total_esigenze_web")
maxi_table_cmdm <- withColumnRenamed(maxi_table_cmdm, "COD_CONTRATTO_no_filter_upgrade", "COD_CONTRATTO_total_esigenze")
createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")

write.parquet(maxi_table_cmdm, "/user/jacopoa/maxi_table_cmdm_official.parquet")

#count <- sql("SELECT COUNT(*) FROM maxi_table_cmdm")
maxi_table_cmdm <- read.parquet("/user/jacopoa/maxi_table_cmdm_official.parquet")
maxi_table_cmdm <- fillna(maxi_table_cmdm, 0)
createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")
#printSchema(maxi_table_cmdm)
#View(head(maxi_table_cmdm, 100))

######## CREAZIONE INUTILI #########
inutili <- subset(maxi_table_cmdm, maxi_table_cmdm$total_esigenze==0)
#View(head(inutili, 100))
createOrReplaceTempView(inutili, "inutili")
maxi_table_cmdm_filter <- sql("SELECT m.* FROM maxi_table_cmdm m LEFT JOIN inutili i ON m.COD_CONTRATTO_CIFRATO=i.COD_CONTRATTO_CIFRATO WHERE i.COD_CONTRATTO_CIFRATO IS NULL")
#View(head(maxi_table_cmdm_filter, 100))
createOrReplaceTempView(maxi_table_cmdm_filter, "maxi_table_cmdm_filter")
#count <- sql("SELECT COUNT(*) FROM maxi_table_cmdm_filter")
#View(head(count))

####### DATA MANIPULATION FOR CLUSTER ###########

maxi_table_cmdm$total_esigenze_rel <- bround(maxi_table_cmdm$total_esigenze / maxi_table_cmdm$total_esigenze, 2)
maxi_table_cmdm$total_esigenze_telefono_rel <- bround(maxi_table_cmdm$total_esigenze_telefono / maxi_table_cmdm$total_esigenze, 2)
maxi_table_cmdm$total_esigenze_sms_rel <- bround(maxi_table_cmdm$total_esigenze_sms / maxi_table_cmdm$total_esigenze, 2)
maxi_table_cmdm$total_esigenze_retail_rel <- bround(maxi_table_cmdm$total_esigenze_retail / maxi_table_cmdm$total_esigenze, 2)
maxi_table_cmdm$total_esigenze_web_rel <- bround(maxi_table_cmdm$total_esigenze_web / maxi_table_cmdm$total_esigenze, 2)
#View(head(maxi_table_cmdm, 100))

maxi_table_cmdm_filter$total_esigenze_rel <- bround(maxi_table_cmdm_filter$total_esigenze / maxi_table_cmdm_filter$total_esigenze, 2)
maxi_table_cmdm_filter$total_esigenze_telefono_rel <- bround(maxi_table_cmdm_filter$total_esigenze_telefono / maxi_table_cmdm_filter$total_esigenze, 2)
maxi_table_cmdm_filter$total_esigenze_sms_rel <- bround(maxi_table_cmdm_filter$total_esigenze_sms / maxi_table_cmdm_filter$total_esigenze, 2)
maxi_table_cmdm_filter$total_esigenze_retail_rel <- bround(maxi_table_cmdm_filter$total_esigenze_retail / maxi_table_cmdm_filter$total_esigenze, 2)
maxi_table_cmdm_filter$total_esigenze_web_rel <- bround(maxi_table_cmdm_filter$total_esigenze_web / maxi_table_cmdm_filter$total_esigenze, 2)
#View(head(maxi_table_cmdm_filter, 100))

maxi_table_cmdm$upgrade_total_rel <- bround(maxi_table_cmdm$upgrade_total / maxi_table_cmdm$upgrade_total, 2)
maxi_table_cmdm$upgrade_telefono_rel <- bround(maxi_table_cmdm$upgrade_telefono / maxi_table_cmdm$upgrade_total, 2)
maxi_table_cmdm$upgrade_sms_rel <- bround(maxi_table_cmdm$upgrade_sms / maxi_table_cmdm$upgrade_total, 2)
maxi_table_cmdm$upgrade_retail_rel <- bround(maxi_table_cmdm$upgrade_retail / maxi_table_cmdm$upgrade_total, 2)
maxi_table_cmdm$upgrade_web_rel <- bround(maxi_table_cmdm$upgrade_web / maxi_table_cmdm$upgrade_total, 2)

maxi_table_cmdm_filter$upgrade_total_rel <- bround(maxi_table_cmdm_filter$upgrade_total / maxi_table_cmdm_filter$upgrade_total, 2)
maxi_table_cmdm_filter$upgrade_telefono_rel <- bround(maxi_table_cmdm_filter$upgrade_telefono / maxi_table_cmdm_filter$upgrade_total, 2)
maxi_table_cmdm_filter$upgrade_sms_rel <- bround(maxi_table_cmdm_filter$upgrade_sms / maxi_table_cmdm_filter$upgrade_total, 2)
maxi_table_cmdm_filter$upgrade_retail_rel <- bround(maxi_table_cmdm_filter$upgrade_retail / maxi_table_cmdm_filter$upgrade_total, 2)
maxi_table_cmdm_filter$upgrade_web_rel <- bround(maxi_table_cmdm_filter$upgrade_web / maxi_table_cmdm_filter$upgrade_total, 2)

maxi_table_cmdm$tecnica_total_rel <- bround(maxi_table_cmdm$tecnica_total / maxi_table_cmdm$tecnica_total, 2)
maxi_table_cmdm$tecnica_telefono_rel <- bround(maxi_table_cmdm$tecnica_telefono / maxi_table_cmdm$tecnica_total, 2)
maxi_table_cmdm$tecnica_sms_rel <- bround(maxi_table_cmdm$tecnica_sms / maxi_table_cmdm$tecnica_total, 2)
maxi_table_cmdm$tecnica_retail_rel <- bround(maxi_table_cmdm$tecnica_retail / maxi_table_cmdm$tecnica_total, 2)
maxi_table_cmdm$tecnica_web_rel <- bround(maxi_table_cmdm$tecnica_web / maxi_table_cmdm$tecnica_total, 2)

maxi_table_cmdm_filter$tecnica_total_rel <- bround(maxi_table_cmdm_filter$tecnica_total / maxi_table_cmdm_filter$tecnica_total, 2)
maxi_table_cmdm_filter$tecnica_telefono_rel <- bround(maxi_table_cmdm_filter$tecnica_telefono / maxi_table_cmdm_filter$tecnica_total, 2)
maxi_table_cmdm_filter$tecnica_sms_rel <- bround(maxi_table_cmdm_filter$tecnica_sms / maxi_table_cmdm_filter$tecnica_total, 2)
maxi_table_cmdm_filter$tecnica_retail_rel <- bround(maxi_table_cmdm_filter$tecnica_retail / maxi_table_cmdm_filter$tecnica_total, 2)
maxi_table_cmdm_filter$tecnica_web_rel <- bround(maxi_table_cmdm_filter$tecnica_web / maxi_table_cmdm_filter$tecnica_total, 2)

write.parquet(inutili, "/user/jacopoa/inutili.parquet")
write.parquet(maxi_table_cmdm_filter, "/user/jacopoa/maxi_table_cmdm_official_filter.parquet")
write.parquet(maxi_table_cmdm, "/user/jacopoa/maxi_table_cmdm_official_v2.parquet")


########### AMEDEO #################

maxi_table_cmdm <- read.parquet("/user/jacopoa/maxi_table_cmdm_official_v2.parquet")

write.parquet(maxi_table_cmdm, "/user/jacopoa/maxi_table_cmdm_official_v3.parquet")

##### AGGIUNTA INFO SU CAMPAGNE by Valentina ####
contact_response <- read.parquet('hdfs:///STAGE/CMDM/DettaglioContattiResponseCampagneAM/delta/fat_contact_response.parquet')

createOrReplaceTempView(contact_response,'contact_response')
contact_response_dt <- withColumn(contact_response,"LISTCREATEDATE_dt",cast(unix_timestamp(contact_response$LISTCREATEDATE, 'dd/MM/yyyy'),'timestamp'))
#View(head(contact_response_dt,30))

createOrReplaceTempView(contact_response_dt,'contact_response_dt')
contact_response_dt_2 <- sql("select CAMPAIGNCODE,CELLID,CHANNEL,COD_CLIENTE,COD_CONTRATTO,COD_TIPO_RESPONSE,CONTACTDATETIME,
                             CONTACTEDCHANNEL,CONTACTSTATUSID,DAT_PRIMO_CLICK_DEM,FLG_CLICK_DEM,LISTCREATEDATE_dt,
                             OFFERCODEACCEPTED
                             from contact_response_dt
                             where CHANNEL in ('EMAIL','RETAIL SERVICE','PHONE INBOUND','SMS')
                             and CONTACTSTATUSID='2'
                             and LISTCREATEDATE_dt<='2017-09-30'
                             and LISTCREATEDATE_dt>='2015-09-01'
                             and CAMPAIGNCODE not in ('S_OTH_CREAODS_06953','S_OTH_SWAPSMC_06929','S_INF_SWAPSMC_06909','S_OTH_SWAPSMC_06940')
                             and COD_TIPO_RESPONSE <> ''
                             ")

# View(head(contact_response_dt_2,100))

createOrReplaceTempView(contact_response_dt_2,'contact_response_dt_2')
contact_response_dt_3 <-sql("select *,
                            case when CHANNEL='EMAIL' then 'EMAIL'
                            when CHANNEL='RETAIL SERVICE' then 'RETAIL'
                            when CHANNEL='SMS' then 'SMS'
                            when CHANNEL='PHONE INBOUND' then 'PHONE'
                            else NULL end as canale_campagne
                            from contact_response_dt_2
                            ")
# cache(contact_response_dt_3)
# ------->conteggi x cliente x canale
createOrReplaceTempView(contact_response_dt_3,'contact_response_dt_3')

count_SMS <- sql("select COD_CONTRATTO, count(distinct CELLID) as num_campagne_SMS
                 from contact_response_dt_3
                 where canale_campagne='SMS'
                 group by COD_CONTRATTO")

count_EMAIL <- sql("select COD_CONTRATTO, count(distinct CELLID) as num_campagne_EMAIL
                   from contact_response_dt_3
                   where canale_campagne='EMAIL'
                   group by COD_CONTRATTO")

count_RETAIL <- sql("select COD_CONTRATTO, count(distinct CELLID) as num_campagne_RETAIL
                    from contact_response_dt_3
                    where canale_campagne='RETAIL'
                    group by COD_CONTRATTO")

count_PHONE <- sql("select COD_CONTRATTO, count(distinct CELLID) as num_campagne_PHONE
                   from contact_response_dt_3
                   where canale_campagne='PHONE'
                   group by COD_CONTRATTO")

createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")
createOrReplaceTempView(count_PHONE, "count_PHONE")
createOrReplaceTempView(count_SMS, "count_SMS")
createOrReplaceTempView(count_RETAIL, "count_RETAIL")
createOrReplaceTempView(count_EMAIL, "count_EMAIL")


maxi_table_cmdm <- sql("SELECT maxi_table_cmdm.*, count_PHONE.num_campagne_PHONE 
                       from maxi_table_cmdm left join count_PHONE 
                       on maxi_table_cmdm.COD_CONTRATTO = count_PHONE.COD_CONTRATTO")

createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")
maxi_table_cmdm <- sql("SELECT maxi_table_cmdm.*, count_SMS.num_campagne_SMS 
                       from maxi_table_cmdm left join count_SMS 
                       on maxi_table_cmdm.COD_CONTRATTO = count_SMS.COD_CONTRATTO")

createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")
maxi_table_cmdm <- sql("SELECT maxi_table_cmdm.*, count_RETAIL.num_campagne_RETAIL 
                       from maxi_table_cmdm 
                       left join count_RETAIL on maxi_table_cmdm.COD_CONTRATTO = count_RETAIL.COD_CONTRATTO")

createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")
maxi_table_cmdm <- sql("SELECT maxi_table_cmdm.*, count_EMAIL.num_campagne_EMAIL 
                       from maxi_table_cmdm 
                       left join count_EMAIL on maxi_table_cmdm.COD_CONTRATTO = count_EMAIL.COD_CONTRATTO")

##### ISCRIZIONE WSC #####
vs_fat_cliente <- read.parquet("/STAGE/CMDM/AnagraficaClienti/full/vs_fat_cliente_SHOT_2.parquet")
dap_stg_contratto_cliente <- read.parquet("/STAGE/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2.parquet")

createOrReplaceTempView(vs_fat_cliente, "vs_fat_cliente")
# test <- sql("select DAT_REGISTRAZIONE_WEB, count(*) from vs_fat_cliente group by 1")
createOrReplaceTempView(dap_stg_contratto_cliente, "dap_stg_contratto_cliente")

# MATCH FRA CODICE CLIENTI DIVERSA CIFRATURA
pass <- sql("SELECT * FROM vs_fat_cliente left join dap_stg_contratto_cliente
            on vs_fat_cliente.COD_CLIENTE = dap_stg_contratto_cliente.COD_CLIENTE")

# #test1 <- sql("select maxi_table_cmdm.COD_CLIENTE, vs_fat_cliente.COD_CLIENTE 
# #             from maxi_table_cmdm inner join vs_fat_cliente 
# #             on maxi_table_cmdm.COD_CLIENTE =  vs_fat_cliente.COD_CLIENTE")
# ##  0 rows

createOrReplaceTempView(pass, "pass")
createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")

maxi_table_cmdm <- sql("SELECT maxi_table_cmdm.*, 
                       pass.DAT_REGISTRAZIONE_WEB,
                       CASE WHEN pass.DAT_REGISTRAZIONE_WEB IS NULL THEN 0 ELSE 1 END as FLG_ISCRIZIONE_WSC
                       FROM  maxi_table_cmdm LEFT JOIN pass
                       ON maxi_table_cmdm.COD_CONTRATTO_CIFRATO = pass.COD_CONTRATTO_CIFRATO
                       ")

{
  # createOrReplaceTempView(maxi_table_cmdm_v2, "maxi_table_cmdm_v2")
  # extract <- sql("SELECT COD_CLIENTE, COD_CLIENTE_CIFRATO, COD_CONTRATTO_CIFRATO, DAT_REGISTRAZIONE_WEB
  #                from maxi_table_cmdm_v2 LIMIT 1000")
  # 
  # persist(extract, "MEMORY_ONLY")
  # cache(extract)
  # write.parquet(extract, "/user/amedeo.bellodi/extract_1000_DAT_REGISTRAZIONE_WEB.parquet")
}

cache(maxi_table_cmdm)
#nrow(maxi_table_cmdm)

write.parquet(maxi_table_cmdm, "/user/riccardo.motta/maxi_table_cmdm_def_1112.parquet")

##### DATA PER CLUSTER #####
maxi_table_cmdm <- read.parquet("/user/riccardo.motta/maxi_table_cmdm_def_1112.parquet")
createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")

data <- sql("SELECT COD_CONTRATTO_CIFRATO,
            COD_CLIENTE_CIFRATO,
            DES_CANALE_VENDITA_DETT,
            DES_CANALE AS DES_CANALE_ISCRIZIONE_EXTRA,
            DES_FORMATO_FATTURA,
            FLG_SKYGO,
            flg_iscritto_extra AS FLG_ISCRITTO_EXTRA,
            FLG_ISCRIZIONE_WSC,
            NUM_CONT_PHONE_INBOUND_360D,
            NUM_CONT_WEB_360D,
            DES_CANALE,
            total_esigenze,
            total_esigenze_rel,
            upgrade_total_rel,
            tecnica_total_rel,
            total_esigenze_telefono_rel,
            total_esigenze_sms_rel,
            total_esigenze_retail_rel,
            total_esigenze_web_rel,
            upgrade_telefono_rel,
            upgrade_sms_rel,
            upgrade_retail_rel,
            upgrade_web_rel, 
            tecnica_telefono_rel,
            tecnica_sms_rel,
            tecnica_retail_rel,
            tecnica_web_rel,
            num_campagne_EMAIL,
            num_campagne_SMS,
            num_campagne_RETAIL,
            num_campagne_PHONE
            FROM maxi_table_cmdm")

data$DES_CANALE_VENDITA_DETT_cl <- lit("OTHER")
data$DES_CANALE_VENDITA_DETT_cl = ifelse(contains(data$DES_CANALE_VENDITA_DETT, c("TELESELLING")), 'TELEFONO', data$DES_CANALE_VENDITA_DETT_cl)
data$DES_CANALE_VENDITA_DETT_cl = ifelse(contains(data$DES_CANALE_VENDITA_DETT, c("SKY CENTER")), 'PDV', data$DES_CANALE_VENDITA_DETT_cl)
data$DES_CANALE_VENDITA_DETT_cl = ifelse(contains(data$DES_CANALE_VENDITA_DETT, c("WEBSELLING")), 'WEB', data$DES_CANALE_VENDITA_DETT_cl)
data$DES_CANALE_VENDITA_DETT_cl = ifelse(contains(data$DES_CANALE_VENDITA_DETT, c("SKY SERVICE")), 'PDV', data$DES_CANALE_VENDITA_DETT_cl)

data$FLG_DES_CANALE_VENDITA_DETT_WEB_cl = ifelse(contains(data$DES_CANALE_VENDITA_DETT_cl, c("WEB")), 1, 0)
data$FLG_DES_CANALE_VENDITA_DETT_PHONE_cl = ifelse(contains(data$DES_CANALE_VENDITA_DETT_cl, c("TELEFONO")), 1, 0)
data$FLG_DES_CANALE_VENDITA_DETT_PDV_cl = ifelse(contains(data$DES_CANALE_VENDITA_DETT_cl, c("PDV")), 1, 0)

data$DES_CANALE_ISCRIZIONE_EXTRA_cl <- lit("NA")
data$DES_CANALE_ISCRIZIONE_EXTRA_cl = ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA, c("APP")), 'APP_WSC', data$DES_CANALE_ISCRIZIONE_EXTRA_cl)
data$DES_CANALE_ISCRIZIONE_EXTRA_cl = ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA, c("WSC")), 'APP_WSC', data$DES_CANALE_ISCRIZIONE_EXTRA_cl)
data$DES_CANALE_ISCRIZIONE_EXTRA_cl = ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA, c("Retail")), 'PDV', data$DES_CANALE_ISCRIZIONE_EXTRA_cl)
data$DES_CANALE_ISCRIZIONE_EXTRA_cl= ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA, c("SKY SERVICE")), 'PDV', data$DES_CANALE_ISCRIZIONE_EXTRA_cl)
data$DES_CANALE_ISCRIZIONE_EXTRA_cl= ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA, c("IVR")), 'OTHER', data$DES_CANALE_ISCRIZIONE_EXTRA_cl)
data$DES_CANALE_ISCRIZIONE_EXTRA_cl= ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA, c("ISCRIZIONE DA STB")), 'OTHER', data$DES_CANALE_ISCRIZIONE_EXTRA_cl)
data$DES_CANALE_ISCRIZIONE_EXTRA_cl= ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA, c("CRM")), 'OTHER', data$DES_CANALE_ISCRIZIONE_EXTRA_cl)
data$DES_CANALE_ISCRIZIONE_EXTRA_cl= ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA, c("ACS")), 'OTHER', data$DES_CANALE_ISCRIZIONE_EXTRA_cl)

data$FLG_DES_CANALE_ISCRIZIONE_EXTRA_PDV_cl = ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA_cl, c("PDV")), 1, 0)
data$FLG_DES_CANALE_ISCRIZIONE_EXTRA_WSC_cl = ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA_cl, c("APP_WSC")), 1, 0)
data$FLG_DES_CANALE_ISCRIZIONE_EXTRA_OTHER_cl = ifelse(contains(data$DES_CANALE_ISCRIZIONE_EXTRA_cl, c("OTHER")), 1, 0)

data$DES_FORMATO_FATTURA_cl <- lit("NESSUNO")
data$DES_FORMATO_FATTURA_cl = ifelse(contains(data$DES_FORMATO_FATTURA, c("ELETTRONICO")), 'ELETTRONICO', data$DES_FORMATO_FATTURA_cl)
data$DES_FORMATO_FATTURA_cl = ifelse(contains(data$DES_FORMATO_FATTURA, c("E-MAIL")), 'ELETTRONICO', data$DES_FORMATO_FATTURA_cl)
data$DES_FORMATO_FATTURA_cl = ifelse(contains(data$DES_FORMATO_FATTURA, c("CARTACEO")), 'CARTACEO', data$DES_FORMATO_FATTURA_cl)

data$FLG_DES_FORMATO_FATTURA_ELETTRONICA_cl = ifelse(contains(data$DES_FORMATO_FATTURA_cl, c("ELETTRONICO")), 1, 0)

data$FLG_ESIGENZE <- ifelse(data$total_esigenze_rel==1, 1, 0)
data$FLG_UPGRADE <- ifelse(data$upgrade_total_rel ==1, 1, 0)
data$FLG_TECNICA <- ifelse(data$tecnica_total_rel ==1, 1, 0)

createOrReplaceTempView(data, "data")

data <- sql("SELECT COD_CONTRATTO_CIFRATO,
            COD_CLIENTE_CIFRATO,
            FLG_DES_CANALE_VENDITA_DETT_WEB_cl,
            FLG_DES_CANALE_VENDITA_DETT_PHONE_cl,
            FLG_DES_CANALE_VENDITA_DETT_PDV_cl,
            FLG_DES_CANALE_ISCRIZIONE_EXTRA_PDV_cl,
            FLG_DES_CANALE_ISCRIZIONE_EXTRA_WSC_cl,
            FLG_DES_CANALE_ISCRIZIONE_EXTRA_OTHER_cl,
            FLG_DES_FORMATO_FATTURA_ELETTRONICA_cl,
            FLG_SKYGO,
            NUM_CONT_PHONE_INBOUND_360D,
            NUM_CONT_WEB_360D,
            FLG_ISCRITTO_EXTRA,
            FLG_ISCRIZIONE_WSC,
            FLG_ESIGENZE,
            FLG_UPGRADE,
            FLG_TECNICA,
            total_esigenze,
            total_esigenze_telefono_rel,
            total_esigenze_sms_rel,
            total_esigenze_retail_rel,
            total_esigenze_web_rel,
            upgrade_telefono_rel,
            upgrade_sms_rel,
            upgrade_retail_rel,
            upgrade_web_rel, 
            tecnica_telefono_rel,
            tecnica_sms_rel,
            tecnica_retail_rel,
            tecnica_web_rel,
            num_campagne_EMAIL,
            num_campagne_SMS,
            num_campagne_RETAIL,
            num_campagne_PHONE
            FROM data")

data <- fillna(data, 0.00)

data$NUM_CONT_PHONE_INBOUND_360D <- bround(data$NUM_CONT_PHONE_INBOUND_360D, 2)
data$NUM_CONT_WEB_360D <- bround(data$NUM_CONT_WEB_360D, 2)

data$num_campagne_tot <- data$num_campagne_EMAIL + data$num_campagne_PHONE + data$num_campagne_RETAIL + data$num_campagne_SMS
data$num_campagne_EMAIL_rel <- data$num_campagne_EMAIL / data$num_campagne_tot
data$num_campagne_PHONE_rel <- data$num_campagne_PHONE / data$num_campagne_tot
data$num_campagne_RETAIL_rel <- data$num_campagne_RETAIL / data$num_campagne_tot
data$num_campagne_SMS_rel <- data$num_campagne_SMS / data$num_campagne_tot

data <- fillna(data, 0.00)

createOrReplaceTempView(data, "data")
data <- sql("SELECT COD_CONTRATTO_CIFRATO,
            COD_CLIENTE_CIFRATO,
            FLG_DES_CANALE_VENDITA_DETT_WEB_cl,
            -- FLG_DES_CANALE_VENDITA_DETT_PHONE_cl,
            -- FLG_DES_CANALE_VENDITA_DETT_PDV_cl,
            FLG_DES_CANALE_ISCRIZIONE_EXTRA_PDV_cl,
            FLG_DES_CANALE_ISCRIZIONE_EXTRA_WSC_cl,
            FLG_DES_CANALE_ISCRIZIONE_EXTRA_OTHER_cl,
            FLG_DES_FORMATO_FATTURA_ELETTRONICA_cl,
            FLG_SKYGO,
            -- NUM_CONT_PHONE_INBOUND_360D,
            -- NUM_CONT_WEB_360D,
            FLG_ISCRITTO_EXTRA,
            FLG_ISCRIZIONE_WSC,
            FLG_ESIGENZE,
            FLG_UPGRADE,
            FLG_TECNICA,
            -- total_esigenze,
            total_esigenze_telefono_rel,
            -- total_esigenze_sms_rel,
            -- total_esigenze_retail_rel,
            total_esigenze_web_rel,
            upgrade_telefono_rel,
            upgrade_sms_rel,
            upgrade_retail_rel,
            upgrade_web_rel, 
            tecnica_telefono_rel,
            tecnica_sms_rel,
            tecnica_retail_rel,
            tecnica_web_rel,
            num_campagne_EMAIL_rel,
            num_campagne_SMS_rel,
            num_campagne_RETAIL_rel,
            num_campagne_PHONE_rel
            FROM data")

createOrReplaceTempView(data, "data")
data <- sql("select * from data 
            where NOT(FLG_ESIGENZE = 0 AND FLG_ISCRITTO_EXTRA = 0 AND FLG_DES_CANALE_VENDITA_DETT_WEB_cl = 0) ")

# View(sql("select FLG_ESIGENZE, FLG_ISCRITTO_EXTRA, FLG_DES_CANALE_VENDITA_DETT_WEB_cl, FLG_DES_FORMATO_FATTURA_ELETTRONICA_cl, COUNT(*) from data GROUP BY 1,2,3,4"))
# collect(select(data, countDistinct(data$COD_CLIENTE_CIFRATO)))

cache(data)
#nrow(data)

createOrReplaceTempView(data, "data")
# View(sql("select DES_CANALE_VENDITA_DETT_cl, count(*) from data group by 1"))
#View(sql("select FLG_ESIGENZE, FLG_ISCRITTO_EXTRA, COUNT(*) FROM data group by 1,2"))

write.parquet(data, "/user/riccardo.motta/data_cluster_v8_1112.parquet")

data <- read.parquet("/user/riccardo.motta/data_cluster_v8_1112.parquet")
createOrReplaceTempView(data, "demo")
a <- sql("SELECT * FROM demo")

#### CLUSTER VERA E PROPRIA ####

## Schiaccio su skyid sommando
data <- sql("SELECT COD_CLIENTE_CIFRATO,
            MAX(FLG_DES_CANALE_VENDITA_DETT_WEB_cl) AS FLG_DES_CANALE_VENDITA_DETT_WEB_cl,
            MAX(FLG_DES_CANALE_ISCRIZIONE_EXTRA_PDV_cl) AS FLG_DES_CANALE_ISCRIZIONE_EXTRA_PDV_cl,
            MAX(FLG_DES_CANALE_ISCRIZIONE_EXTRA_WSC_cl) AS FLG_DES_CANALE_ISCRIZIONE_EXTRA_WSC_cl,
            MAX(FLG_DES_CANALE_ISCRIZIONE_EXTRA_OTHER_cl) AS FLG_DES_CANALE_ISCRIZIONE_EXTRA_OTHER_cl,
            MAX(FLG_DES_FORMATO_FATTURA_ELETTRONICA_cl) AS FLG_DES_FORMATO_FATTURA_ELETTRONICA_cl,
            MAX(FLG_SKYGO) AS FLG_SKYGO,
            MAX(FLG_ISCRITTO_EXTRA) AS FLG_ISCRITTO_EXTRA,
            MAX(FLG_ISCRIZIONE_WSC) AS FLG_ISCRIZIONE_WSC,
            MAX(FLG_ESIGENZE) AS FLG_ESIGENZE,
            MAX(FLG_UPGRADE) AS FLG_UPGRADE,
            MAX(FLG_TECNICA) AS FLG_TECNICA,
            SUM(total_esigenze_telefono_rel)/(SUM(total_esigenze_telefono_rel) + SUM(total_esigenze_web_rel)) AS total_esigenze_telefono_rel,
            SUM(total_esigenze_web_rel)/(SUM(total_esigenze_telefono_rel) + SUM(total_esigenze_web_rel)) AS total_esigenze_web_rel,
            MAX(upgrade_telefono_rel) AS upgrade_telefono_rel,
            MAX(upgrade_sms_rel) AS upgrade_sms_rel,
            MAX(upgrade_retail_rel) AS upgrade_retail_rel,
            MAX(upgrade_web_rel) AS upgrade_web_rel,
            SUM(tecnica_telefono_rel)/(SUM(tecnica_telefono_rel) + SUM(tecnica_sms_rel) + SUM(tecnica_retail_rel) + SUM(tecnica_web_rel)) AS tecnica_telefono_rel,
            SUM(tecnica_sms_rel)/(SUM(tecnica_telefono_rel) + SUM(tecnica_sms_rel) + SUM(tecnica_retail_rel) + SUM(tecnica_web_rel)) AS tecnica_sms_rel,
            SUM(tecnica_retail_rel)/(SUM(tecnica_telefono_rel) + SUM(tecnica_sms_rel) + SUM(tecnica_retail_rel) + SUM(tecnica_web_rel)) AS tecnica_retail_rel,
            SUM(tecnica_web_rel)/(SUM(tecnica_telefono_rel) + SUM(tecnica_sms_rel) + SUM(tecnica_retail_rel) + SUM(tecnica_web_rel)) AS tecnica_web_rel,
            SUM(num_campagne_EMAIL_rel)/(SUM(num_campagne_EMAIL_rel) + SUM(num_campagne_SMS_rel) + SUM(num_campagne_RETAIL_rel) + SUM(num_campagne_PHONE_rel)) AS num_campagne_EMAIL_rel,
            SUM(num_campagne_SMS_rel)/(SUM(num_campagne_EMAIL_rel) + SUM(num_campagne_SMS_rel) + SUM(num_campagne_RETAIL_rel) + SUM(num_campagne_PHONE_rel)) AS num_campagne_SMS_rel,
            SUM(num_campagne_RETAIL_rel)/(SUM(num_campagne_EMAIL_rel) + SUM(num_campagne_SMS_rel) + SUM(num_campagne_RETAIL_rel) + SUM(num_campagne_PHONE_rel)) AS num_campagne_RETAIL_rel,
            SUM(num_campagne_PHONE_rel)/(SUM(num_campagne_EMAIL_rel) + SUM(num_campagne_SMS_rel) + SUM(num_campagne_RETAIL_rel) + SUM(num_campagne_PHONE_rel)) AS num_campagne_PHONE_rel
            FROM demo
            GROUP BY 1")
data <- fillna(data, 0)
#View(head(data, 100))

write.parquet(data, "/user/riccardo.motta/data_cluster_grouped_1112.parquet")
data <- read.parquet("/user/riccardo.motta/data_cluster_grouped_1112.parquet")
#cache(data)
#nrow(data)

collect(select(data, countDistinct(data$COD_CLIENTE_CIFRATO)))

# 
# cluster5 <- spark.kmeans(data[,-1], ~ ., k = 5,
#                          maxIter = 50, initMode = "k-means||")
# cluster6 <- spark.kmeans(data[,-1], ~ ., k = 6,
#                          maxIter = 50, initMode = "k-means||")

cluster7 <- spark.kmeans(data[,-1], ~ ., k = 7,
                         maxIter = 50, initMode = "k-means||")

res <- as.data.frame(summary(cluster7)$coefficients)
res <- as.DataFrame(res)
write.df(repartition(res, 1), path = "hdfs:///user/riccardo.motta/res_cluster_1112.csv", "csv", mode = "overwrite", header=TRUE)
res <- as.data.frame(res)
point_cluster0 <- sum(res[1,c(1:25)])
point_cluster1 <- sum(res[2,c(1:25)])
point_cluster2 <- sum(res[3,c(1:25)])
point_cluster3 <- sum(res[4,c(1:25)])
point_cluster4 <- sum(res[5,c(1:25)])
point_cluster5 <- sum(res[6,c(1:25)])
point_cluster6 <- sum(res[7,c(1:25)])


fitted <- predict(cluster7, data)
cache(fitted)

createOrReplaceTempView(fitted, "fitted")
sizes <- sql("SELECT prediction AS cluster,
             count(*) as tot
             FROM fitted 
             GROUP BY 1 ORDER BY 1")
#View(sizes)

{
  # description_canaleV <- sql("SELECT prediction AS cluster, 
  #                            DES_CANALE_VENDITA_DETT_cl,
  #                            count(*) as num
  #                            FROM fitted 
  #                            GROUP BY 1,2  order BY 1")
  # 
  # description_canaleV <- merge(description_canaleV, sizes, by="cluster")
  # description_canaleV$rel <- description_canaleV$num / description_canaleV$tot
  # description_canaleV <- orderBy(description_canaleV, "cluster_x", "rel")
  # View(description_canaleV)
  # 
  # description_canaleE <- sql("SELECT prediction AS cluster, 
  #                            DES_CANALE_ISCRIZIONE_EXTRA_cl,
  #                            count(*) as num
  #                            FROM fitted 
  #                            GROUP BY 1,2  order BY 1")
  # 
  # description_canaleE <- merge(description_canaleE, sizes, by="cluster")
  # description_canaleE$rel <- description_canaleE$num / description_canaleE$tot
  # description_canaleE <- orderBy(description_canaleE, "cluster_x", "rel")
  # View(description_canaleE)
  # 
  # description_fattura <- sql("SELECT prediction AS cluster, 
  #                            DES_FORMATO_FATTURA_cl,
  #                            count(*) as num
  #                            FROM fitted 
  #                            GROUP BY 1,2  order BY 1")
  # 
  # description_fattura <- merge(description_fattura, sizes, by="cluster")
  # description_fattura$rel <- description_fattura$num / description_fattura$tot
  # description_fattura <- orderBy(description_fattura, "cluster_x", "rel")
  # View(description_fattura)
}

description_skygo <- sql("SELECT prediction AS cluster, 
                         FLG_SKYGO,
                         count(*) as num
                         FROM fitted 
                         GROUP BY 1,2  order BY 1")

description_skygo <- merge(description_skygo, sizes, by="cluster")
description_skygo$rel <- description_skygo$num / description_skygo$tot
description_skygo <- orderBy(description_skygo, "cluster_x", "rel")
#View(description_skygo)

description_extra <- sql("SELECT prediction AS cluster, 
                         FLG_ISCRITTO_EXTRA,
                         count(*) as num
                         FROM fitted 
                         GROUP BY 1,2  order BY 1")

description_extra <- merge(description_extra, sizes, by="cluster")
description_extra$rel <- description_extra$num / description_extra$tot
description_extra <- orderBy(description_extra, "cluster_x", "rel")
#View(description_extra)

description_wsc <- sql("SELECT prediction AS cluster, 
                       FLG_ISCRIZIONE_WSC,
                       count(*) as num
                       FROM fitted 
                       GROUP BY 1,2  order BY 1")

description_wsc <- merge(description_wsc, sizes, by="cluster")
description_wsc$rel <- description_wsc$num / description_wsc$tot
description_wsc <- orderBy(description_wsc, "cluster_x", "rel")
#View(description_wsc)

description_esigenze <- sql("SELECT prediction AS cluster, 
                            FLG_ESIGENZE,
                            FLG_UPGRADE, 
                            FLG_TECNICA,
                            count(*) as num
                            FROM fitted 
                            GROUP BY 1,2,3,4  order BY 1")

description_esigenze <- merge(description_esigenze, sizes, by="cluster")
description_esigenze$rel <- description_esigenze$num / description_esigenze$tot
description_esigenze <- orderBy(description_esigenze, "cluster_x", "rel")
#View(description_esigenze)

description_esigenze <- sql("SELECT prediction AS cluster, 
                            FLG_ESIGENZE,
                            count(*) as num
                            FROM fitted 
                            GROUP BY 1,2  order BY 1")

description_esigenze <- merge(description_esigenze, sizes, by="cluster")
description_esigenze$rel <- description_esigenze$num / description_esigenze$tot
description_esigenze <- orderBy(description_esigenze, "cluster_x", "rel")
#View(description_esigenze)

description_num <- avg(groupBy(fitted[,2:ncol(fitted)], "prediction"))
description_num <- orderBy(description_num, "prediction")
#View(description_num)

#### AUDIENCE ####

createOrReplaceTempView(fitted, "fitted")
createOrReplaceTempView(maxi_table_cmdm, "maxi_table_cmdm")

audience <- sql("SELECT maxi_table_cmdm.*, fitted.prediction 
                from maxi_table_cmdm left join fitted
                on maxi_table_cmdm.COD_CLIENTE_CIFRATO = fitted.COD_CLIENTE_CIFRATO")

audience$score <- lit(0)
audience <- withColumn(audience,"score",ifelse(audience$prediction == 0, point_cluster0, audience$score ))
audience <- withColumn(audience,"score",ifelse(audience$prediction == 1, point_cluster1, audience$score )) 
audience <- withColumn(audience,"score",ifelse(audience$prediction == 2, point_cluster2, audience$score ))    
audience <- withColumn(audience,"score",ifelse(audience$prediction == 3, point_cluster3, audience$score )) 
audience <- withColumn(audience,"score",ifelse(audience$prediction == 4, point_cluster4, audience$score )) 
audience <- withColumn(audience,"score",ifelse(audience$prediction == 5, point_cluster5, audience$score )) 
audience <- withColumn(audience,"score",ifelse(audience$prediction == 6, point_cluster6, audience$score ))  


createOrReplaceTempView(audience, "audience")

write.parquet(audience, "/user/riccardo.motta/maxi_table_cmdm_cluster_score_1112.parquet")

a <- read.parquet("/user/jacopoa/maxi_table_cmdm_cluster_score_1112.parquet")

audience_description <- sql("SELECT prediction, 
  DES_AREA_NIELSEN_FRU, 
  DES_CLUSTER_POLIS_L1_2011, 
  classe_eta, 
  pacchetto,
  FLG_PULLVOD_ATTIVO,
  FLG_MV_1,
  count(*) 
  from audience 
  group by 1,2,3,4,5,6,7")

audience_description2 <- sql("SELECT prediction, 
  AVG(tenure_month),
  AVG(total_esigenze)
  from audience 
  group by 1")

write.df(repartition(audience_description, 1), path = "hdfs:///user/riccardo.motta/audience_description.csv", "csv", mode = "overwrite", header=TRUE)
#View(audience_description2)


