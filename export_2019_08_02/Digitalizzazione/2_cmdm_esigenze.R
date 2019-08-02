library(magrittr)
############ *****************  VALUTAZIONE TABELLA ESIGENZE DETT ************************************* #################
#carica la tabella con formato timestamp x la data apertura
vs_esigenze_dett_timest <- read.parquet("/STAGE/CMDM/DettaglioEsigenze/delta/vs_esigenze_dett_with_timestamp.parquet")
vs_esigenze_dett_timest$DAT_APERTURA_COMPLETA <- NULL
persist(vs_esigenze_dett_timest, "MEMORY_ONLY")
createOrReplaceTempView(vs_esigenze_dett_timest, "vs_esigenze_dett_timest")


#provo a creae una tabella con tutti i filtri (tranne quello temporale)
estrai_esig_all_temp <- sql("select COD_CONTRATTO, COD_ESIGENZA, DAT_APERTURA, DAT_CHIUSURA, DES_STATO, DES_CATEGORIA, DES_SUBCATEGORIA,
                            DES_MOTIVO,DES_TIPO_CONTATTO,DES_CANALE_CNT
                            from vs_esigenze_dett_timest
                            where
                            DES_MOTIVO not in ('INFO CONSEGNA SWAP P2P3' , 'PRENOTAZIONE INVIO SMC SWAP P2 P3' , 'SWAP COMMERCIALE P2/P3' ,
                            'BLUE SCREEN SWAP P2 P3' , 'PRENOTAZIONE INVIO SMC SWAP P2' , 'CHIEDE ATTIVAZIONE SMC P2P3' ,
                            'SWAP P2 P3'  ) and
                            DES_TIPO_CONTATTO in ('CALL ME NOW' , 'INBOUND') and
                            DES_CANALE_CNT not in ('Undefined' ,  'NONE' , ' ' , 'N/A' , 'NA' )
                            ")
#View(head(estrai_esig_all_temp,1000))
#printSchema(estrai_esig_all_temp)
createOrReplaceTempView(estrai_esig_all_temp, "estrai_esig_all_temp")

#seleziona solo esigenze aperte con canali d'interesse
estrai_esig_cod_canale <- sql("select * from (select COD_CONTRATTO, COD_ESIGENZA, DAT_APERTURA, DAT_CHIUSURA, DES_STATO, DES_CATEGORIA, DES_SUBCATEGORIA,
                              DES_MOTIVO,DES_TIPO_CONTATTO,DES_CANALE_CNT,
                              case when DES_CANALE_CNT in ('WEBMAIL','WEBEASY','WEB - RISOLVI ONLINE','WEB','MOBILE','EMAIL','CHAT') then 'WEB'
                              when DES_CANALE_CNT in ('UPSELLING RETAIL','SKY SERVICE','SKY CENTER') then 'RETAIL'
                              when DES_CANALE_CNT in ('TELEFONO','IVR-SELFCARE','IVR SELFCARE','IVR','CRM') then 'TELEFONO'
                              when DES_CANALE_CNT in ('SMS-SELFCARE','SMS') then 'SMS'
                              else NULL end as CANALE_CONTATTO_COD
                              from estrai_esig_all_temp)
                              where CANALE_CONTATTO_COD is not NULL
                              ")
#View(head(estrai_esig_cod_canale,1000))
createOrReplaceTempView(estrai_esig_cod_canale, "estrai_esig_cod_canale")

#aggiungi alla tabella un flg per indicare se il cliente ha aperto un'esigenza di UPGRADE (info e/o attivaz) + flg per apertura esig tecnica
estrai_esig_cod_canale$FLG_ESIG_UPGRADE = ifelse(contains(estrai_esig_cod_canale$DES_MOTIVO, c("UPGRADE")), '1','0')
#View(head(estrai_esig_cod_canale,10000))

estrai_esig_cod_canale$FLG_ESIG_TECNICA = ifelse(contains(estrai_esig_cod_canale$DES_CATEGORIA, c("GESTIONE TECNICA")), '1','0')
#View(head(estrai_esig_cod_canale,10000))

#printSchema(estrai_esig_cod_canale)


#### converto la data apertura  (che ha gli zeri!!!!)
estrai_esig_cod_canale2 <- withColumn(estrai_esig_cod_canale,"DAT_APERTURA_dt",cast(unix_timestamp(estrai_esig_cod_canale$DAT_APERTURA, 'dd/MM/yyyy'),'timestamp'))
estrai_esig_cod_canale3 <- withColumn(estrai_esig_cod_canale2,"DAT_CHIUSURA_dt",cast(unix_timestamp(estrai_esig_cod_canale2$DAT_CHIUSURA, 'dd/MM/yyyy'),'timestamp'))
#View(head(estrai_esig_cod_canale3,10000))
#printSchema(estrai_esig_cod_canale3)
createOrReplaceTempView(estrai_esig_cod_canale3, "estrai_esig_cod_canale3")


###faccio un filtro sugli ultimi 3 anni di storico
tab_esig_fin1 <- sql("select *
                     from estrai_esig_cod_canale3
                     where (DAT_APERTURA_dt <>'9999-12-31' and DAT_APERTURA_dt>='2015-10-01')
                     ")

#faccio un check visualizzando il min e max delle date presenti
verifica_date_apertura <- agg(
  tab_esig_fin1,
  max = max(tab_esig_fin1$DAT_APERTURA_dt),
  min = min(tab_esig_fin1$DAT_APERTURA_dt)
)
#View(head(verifica_date_apertura))   #min:2015-10-01 / max 2017-03-21



tab_esig_fin1 <- withColumn(tab_esig_fin1, "FLG_ESIG_UPGRADE", cast(tab_esig_fin1$FLG_ESIG_UPGRADE, "double"))
tab_esig_fin1 <- withColumn(tab_esig_fin1, "FLG_ESIG_TECNICA", cast(tab_esig_fin1$FLG_ESIG_TECNICA, "double"))
tab_esig_fin1 <- persist(tab_esig_fin1, "MEMORY_AND_DISK")
createOrReplaceTempView(tab_esig_fin1, "tab_esig_fin1")
write.parquet(tab_esig_fin1, "/user/jacopoa/esigenze.parquet")

# test <- sql("SELECT DISTINCT COD_CONTRATTO, CANALE_CONTATTO_COD FROM tab_esig_fin1 WHERE FLG_ESIG_UPGRADE=1 and COD_CONTRATTO='00005e35dea6d1fe0119bba1999e38ff9c07602c'")
# 
# count_upgrade_total_no_filter <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_total_no FROM tab_esig_fin1 GROUP BY 1")
# count_upgrade_telefono_no_filter <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_telefono_no FROM tab_esig_fin1 WHERE CANALE_CONTATTO_COD='TELEFONO' GROUP BY 1")
# count_upgrade_sms_no_filter <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_sms_no FROM tab_esig_fin1 WHERE CANALE_CONTATTO_COD='SMS' GROUP BY 1")
# count_upgrade_retail_no_filter <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_retail_no FROM tab_esig_fin1 WHERE CANALE_CONTATTO_COD='RETAIL' GROUP BY 1")
# count_upgrade_web_no_filter <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_web_no FROM tab_esig_fin1 WHERE CANALE_CONTATTO_COD='WEB' GROUP BY 1")
# 
# count_upgrade_total <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_total FROM tab_esig_fin1 WHERE FLG_ESIG_UPGRADE=1 GROUP BY 1")
# count_upgrade_telefono <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_telefono FROM tab_esig_fin1 WHERE FLG_ESIG_UPGRADE=1 AND CANALE_CONTATTO_COD='TELEFONO' GROUP BY 1")
# count_upgrade_sms <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_sms FROM tab_esig_fin1 WHERE FLG_ESIG_UPGRADE=1 AND CANALE_CONTATTO_COD='SMS' GROUP BY 1")
# count_upgrade_retail <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_retail FROM tab_esig_fin1 WHERE FLG_ESIG_UPGRADE=1 AND CANALE_CONTATTO_COD='RETAIL' GROUP BY 1")
# count_upgrade_web <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as upgrade_web FROM tab_esig_fin1 WHERE FLG_ESIG_UPGRADE=1 AND CANALE_CONTATTO_COD='WEB' GROUP BY 1")
# 
# count_tecnica_total <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as tecnica_total FROM tab_esig_fin1 WHERE FLG_ESIG_TECNICA=1 GROUP BY 1")
# count_tecnica_telefono <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as tecnica_telefono FROM tab_esig_fin1 WHERE FLG_ESIG_TECNICA=1 AND CANALE_CONTATTO_COD='TELEFONO' GROUP BY 1")
# count_tecnica_sms <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as tecnica_sms FROM tab_esig_fin1 WHERE FLG_ESIG_TECNICA=1 AND CANALE_CONTATTO_COD='SMS' GROUP BY 1")
# count_tecnica_retail <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as tecnica_retail FROM tab_esig_fin1 WHERE FLG_ESIG_TECNICA=1 AND CANALE_CONTATTO_COD='RETAIL' GROUP BY 1")
# count_tecnica_web <- sql("SELECT DISTINCT COD_CONTRATTO, count(CANALE_CONTATTO_COD) as tecnica_web FROM tab_esig_fin1 WHERE FLG_ESIG_TECNICA=1 AND CANALE_CONTATTO_COD='WEB' GROUP BY 1")
# 
# 
# #############
# 
# 
# visitor_rollup_no_filter_upgrade <- list(count_upgrade_total_no_filter, count_upgrade_telefono_no_filter) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_no_filter_upgrade$COD_CONTRATTO_y <- NULL
# 
# visitor_rollup_no_filter_upgrade <- list(visitor_rollup_no_filter_upgrade, count_upgrade_sms_no_filter) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_no_filter_upgrade$COD_CONTRATTO_y <- NULL
# 
# visitor_rollup_no_filter_upgrade <- list(visitor_rollup_no_filter_upgrade, count_upgrade_retail_no_filter) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_no_filter_upgrade$COD_CONTRATTO_y <- NULL
# 
# visitor_rollup_no_filter_upgrade <- list(visitor_rollup_no_filter_upgrade, count_upgrade_web_no_filter) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_no_filter_upgrade$COD_CONTRATTO_y <- NULL
# 
# ##########################
# 
# 
# visitor_rollup_tecnica <- list(count_tecnica_total, count_tecnica_telefono) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_tecnica$COD_CONTRATTO_y <- NULL
# 
# visitor_rollup_tecnica <- list(visitor_rollup_tecnica, count_tecnica_sms) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_tecnica$COD_CONTRATTO_y <- NULL
# 
# visitor_rollup_tecnica <- list(visitor_rollup_tecnica, count_tecnica_retail) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_tecnica$COD_CONTRATTO_y <- NULL
# 
# visitor_rollup_tecnica <- list(visitor_rollup_tecnica, count_tecnica_web) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_tecnica$COD_CONTRATTO_y <- NULL
# 
# ########################################
# 
# visitor_rollup_upgrade <- list(count_upgrade_total, count_upgrade_telefono) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_upgrade$COD_CONTRATTO_y <- NULL
# 
# visitor_rollup_upgrade <- list(visitor_rollup_upgrade, count_upgrade_sms) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_upgrade$COD_CONTRATTO_y <- NULL
# 
# visitor_rollup_upgrade <- list(visitor_rollup_upgrade, count_upgrade_retail) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# 
# visitor_rollup_upgrade$COD_CONTRATTO_y <- NULL
# 
# visitor_rollup_upgrade <- list(visitor_rollup_upgrade, count_upgrade_web) %>%
#   Reduce(function(...) merge(..., all.x=TRUE, by="COD_CONTRATTO"), .)
# visitor_rollup_upgrade$COD_CONTRATTO_y <- NULL
# 
# ###########################################
# 
# maxi_table_cmdm <- sql("SELECT j.*, vrt.*, vru.*, vrfu.* FROM join_3 
#     LEFT JOIN visitor_rollup_tecnica vrt
#     on =vrt.COD_CONTRATTO
#     LEFT JOIN visitor_rollup_upgrade vru
#     on =vru.COD_CONTRATTO
#     LEFT JOIN visitor_rollup_no_filter_upgrade vrfu
#     on =vrfu.COD_CONTRATTO")
# 
# visitor_rollup <- fillna(visitor_rollup, 0)
# 
# visitor_rollup2 <- list(visitor_rollup_no_filter, visitor_rollup) %>%
#   Reduce(function(...) merge(...,by="COD_CONTRATTO_x"), .)
# visitor_rollup2$COD_CONTRATTO_x_y <- NULL
# visitor_rollup2 <- fillna(visitor_rollup2, 0)
# createOrReplaceTempView(visitor_rollup2, "visitor_rollup2")
# 
# testjoin <- sql("SELECT COUNT(*) FROM visitor_rollup2")
# 
# count_upgrade <- sql("SELECT DISTINCT CANALE_CONTATTO_COD, count(*) as count_upgrade FROM tab_esig_fin1 WHERE FLG_ESIG_UPGRADE=1 GROUP BY 1")
# count_tecnica <- sql("SELECT DISTINCT CANALE_CONTATTO_COD, count(*) as count_tecnica FROM tab_esig_fin1 WHERE FLG_ESIG_TECNICA=1 GROUP BY 1")
# 
# createOrReplaceTempView(count_upgrade_total, "count_upgrade_total")
# createOrReplaceTempView(count_tecnica, "count_tecnica")
# 
# count_esigenze <- sql("SELECT t.*, u.count_upgrade FROM count_tecnica t LEFT JOIN count_upgrade u on t.CANALE_CONTATTO_COD=u.CANALE_CONTATTO_COD")