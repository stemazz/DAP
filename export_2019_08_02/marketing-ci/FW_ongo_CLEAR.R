# "FW_ongo" con i COD_CONTRATTO in CLEAR:


cm_ansto_clear <- read.df("/CLEAR/STAGE/CMDM/AmbienteAnalisiStoricizzato/delta/fat_contratto_sto-ORIG.parquet","parquet")

#printSchema(cm_ansto_clear)
#nrow(cm_ansto_clear)
#[1] 170.593.979

#cb_NO_FW_clear <- subset(cm_ansto_clear, cm_ansto_clear$COD_CLUSTER_BUNDLE_MATCH == "2")
#View(head(cb_NO_FW_clear))
#nrow(cb_NO_FW_clear)
#[1] 131.213.117



#a=distinct(select(cm_anto_clear, list('FLG_CONTRATTO_BUNDLE', 'FLG_TIPO_ADESIONE_BUNDLE')))
#View(head(a))


#SELEZIONE DEI CODICI CONTRATTO CHE NON HANNO HOME PACK E NON HANNO FASTWEB 
cb_NO_FW_NO_HP_clear <- filter(cm_ansto_clear, cm_ansto_clear$FLG_CONTRATTO_BUNDLE=="0" & cm_ansto_clear$COD_CLUSTER_BUNDLE_MATCH=="2")
#nrow(cb_NO_FW_NO_HP_clear)
#[1] 130.335.140
# NUMERO DI CODICI CONTRATTO DISTINTI CHE RIENTRANO NELLA CATEGORIA SENZA FASTWEB
cb_NO_FW_NO_HP_valdist_clear <- distinct(select(cm_ansto_clear, "COD_CONTRATTO"))
#nrow(cb_NO_FW_NO_HP_valdist_clear)
#[1] 7.539.193
#View(head(cb_NO_FW_NO_HP_valdist_clear))


# #ci mette troppo tempo
# cb_test <- arrange(cb_NO_FW_NO_HP_clear, cb_NO_FW_NO_HP_clear$COD_CONTRATTO)
# View(head(cb_test))


#Prendo i primi 6 valori di COD_SEZ_CENS_2011_FRU che corrispondono al codice ISTAT comunale (codice presente nelle tabella da conforntare)
b <- SparkR::select(cb_NO_FW_NO_HP_clear, "DES_COD_SEZ_CENS_2011_FRU")
#nrow(b)
#[1] 130.335.140

#aa <- SparkR::substr(cb_NO_FW_NO_HP_clear$DES_COD_SEZ_CENS_2011_FRU, 1, 6) #NON funziona!
#SELEZIONE DEI CODICI CONTRATTO CON I PRIMI 6 valori di COD_SEZ_CENS_2011_FRU (=codice istat comunale)
bb <- SparkR::select(cb_NO_FW_NO_HP_clear ,cb_NO_FW_NO_HP_clear$COD_CONTRATTO, substr(cb_NO_FW_NO_HP_clear$DES_COD_SEZ_CENS_2011_FRU, 1, 6))
bb <- SparkR::rename(bb, COD_ISTAT = bb[[2]])
bb <- withColumnRenamed(bb, "COD_CONTRATTO", "COD_CONTRATTO_bb")


#NUMERO DEI CODICI CONTRATTO INTERESSATI IN COMUNI ITALIANI DISTINTI
bb_sel <- SparkR::distinct(SparkR::select(bb, "COD_CONTRATTO_bb", "COD_ISTAT"))
#nrow(bb_sel)
#[1] 10.866.856

#NUMERI DI CODICI CONTRATTO DISTINTI
test_cb <- distinct(select(cb_NO_FW_NO_HP_clear, "COD_CONTRATTO"))
#nrow(test_cb)
#[1] 6.231.217 (vs 130.335.140 di "cb_NO_FW_NO_HP_clear")


#cb_selezionata <- withColumn(cb_NO_FW_NO_HP_clear, "cod_istat", bb$COD_ISTAT) #NON funziona!
#SELEZIONE DEI CODICI CONTRATTO CON IL CODICE ISTAT esatto RELATIVO AL COMUNE ITALIANO INTERESSATO
cb_selezionata <- join(bb_sel, cb_NO_FW_NO_HP_clear, bb_sel$COD_CONTRATTO_bb==cb_NO_FW_NO_HP_clear$COD_CONTRATTO, "inner")
#nrow(cb_selezionata)
#[1] 246.066.848

#per velocizzare la join: utilizzo test_cb
cb_selezionata2 <- join(bb_sel, test_cb, bb_sel$COD_CONTRATTO_bb==test_cb$COD_CONTRATTO, "inner")
#nrow(cb_selezionata2)
#[1] 10.866.856


#----------------------------------------------------------------------------------------------------------------------------------------------
#RAGGRUPPO CODICI CONTRATTO DISTINTI PER COMUNI DISTINTI (NON serve pi?? perch?? ho selezionato gi?? prima i codici contratto unici: in "cb_selezionata2")
cb_sel_dist <- SparkR::groupBy(cb_selezionata, cb_selezionata$COD_CONTRATTO, cb_selezionata$COD_ISTAT)
nrow(cb_sel_dist)
#[1] 
#----------------------------------------------------------------------------------------------------------------------------------------------



#IMPORTAZIONE TABELLE zone Fibra attiva:

#TABELLA DI CENTRALI NGA (Fibra) ATTIVE IN ITALIA DIVISE PER COMUNE (codice istat comunale)
df_NGA_attiv <- read.csv("Centrali NGA attive 09-ott-2017.csv", header = TRUE, sep = ";")
#SELEZIONE DEI codici istat della tabella (ultimi 6 numeri di ISTAT.Comune.OLT)
col_cod <- substr(df_NGA_attiv$ISTAT.Comune.OLT, nchar(df_NGA_attiv$ISTAT.Comune.OLT)-5, nchar(df_NGA_attiv$ISTAT.Comune.OLT))
#SELEZIONE DEI SOLI CODICI UNICI (per Comune)
col_cod_distval_NGAattiv <- createDataFrame(unique(data.frame(col_cod)))
#nrow(col_cod_distval_NGAattiv)
#[1] 1501

#codice vecchio (non utilizzo pi??)
#df_NGA_attiv <- cbind(df_NGA_attiv,col_cod)
#View(head(df_NGA_attiv))
#df_NGA_attiv_valdist <- distinct(select(df_NGA_attiv, 'col_cod'))


#TABELLA DI CENTRALI FTTCab (Fibra) ATTIVE IN ITALIA DIVISE PER COMUNE (codice istat comunale)
df_FTTCab_attiv <- read.csv("cop_FTTCab_attiva_soloCOD.csv", header = TRUE, sep = ";")
#SELEZIONE DEI codici istat della tabella (ultimi 6 numeri di ISTAT.Comune.OLT)
col_cod <- substr(df_FTTCab_attiv$ISTAT.Comune.OLT, nchar(df_FTTCab_attiv$ISTAT.Comune.OLT)-5, nchar(df_FTTCab_attiv$ISTAT.Comune.OLT))
#SELEZIONE DEI SOLI CODICI UNICI (per Comune)
col_cod_distval_FTTCabattiv <- createDataFrame(unique(data.frame(col_cod)))
#nrow(col_cod_distval_FTTCabattiv)
#[1] 1499

#IMPORTAZIONE TABELLE zone Fibra pianificate
#TABELLA DI CENTRALI NGA (Fibra) PIANIFICATE IN ITALIA DIVISE PER COMUNE (codice istat comunale)
df_NGA_pianif <- read.csv("Centrali NGA pianificate 09-ott-2017.csv", header = TRUE, sep = ";")
#SELEZIONE DEI codici istat della tabella (ultimi 6 numeri di ISTAT.Comune.OLT)
col_cod <- substr(df_NGA_pianif$ISTAT.Comune.OLT, nchar(df_NGA_pianif$ISTAT.Comune.OLT)-5, nchar(df_NGA_pianif$ISTAT.Comune.OLT))
#SELEZIONE DEI SOLI CODICI UNICI (per Comune)
col_cod_distval_NGApianif <- createDataFrame(unique(data.frame(col_cod)))
#nrow(col_cod_distval_NGApianif)
#[1] 257

#TABELLA DI CENTRALI NGA (Fibra) PIANIFICATE IN ITALIA DIVISE PER COMUNE (codice istat comunale)
df_FTTCab_pianif <- read.csv("Copertura pianificata FTTCab 09-ott-2017.csv", header = TRUE, sep = ";")
#SELEZIONE DEI codici istat della tabella (ultimi 6 numeri di ISTAT.Comune.OLT)
col_cod <- substr(df_FTTCab_pianif$ISTAT.Comune.OLT, nchar(df_FTTCab_pianif$ISTAT.Comune.OLT)-5, nchar(df_FTTCab_pianif$ISTAT.Comune.OLT))
#SELEZIONE DEI SOLI CODICI UNICI (per Comune)
col_cod_distval_FTTCabpianif <- createDataFrame(unique(data.frame(col_cod)))
#nrow(col_cod_distval_FTTCabpianif)
#[1] 965


#Quindi le zone coperte da Fibra o in pianificazione di esserlo sono:
Zona_fibra <- rbind(col_cod_distval_NGAattiv, col_cod_distval_FTTCabattiv, col_cod_distval_NGApianif, col_cod_distval_FTTCabpianif)
#nrow(Zona_fibra)
#[1] 4222
Zona_fibra_distval <- SparkR::distinct(SparkR::select(Zona_fibra, "col_cod"))
#nrow(Zona_fibra_distval)
#[1] 1.699

Zona_fibra_pianif <- rbind(col_cod_distval_NGApianif, col_cod_distval_FTTCabpianif)
#nrow(Zona_fibra_pianif)
#[1] 1222
Zona_fibra_pianif_distval <- SparkR::distinct(SparkR::select(Zona_fibra_pianif, "col_cod"))
#nrow(Zona_fibra_pianif_distval)
#[1] 1068

Zona_fibra_attiv <- rbind(col_cod_distval_NGAattiv, col_cod_distval_FTTCabattiv)
#nrow(Zona_fibra_attiv)
#[1] 3000
Zona_fibra_attiv_distval <- SparkR::distinct(SparkR::select(Zona_fibra_attiv, "col_cod"))
#nrow(Zona_fibra_attiv_distval)
#[1] 1501




#SELEZIONE DEI CODICI CONTRATTO POTENZIALMENTE INTERESSATI (nei Comuni raggiunti da Fibra)
test_finale <- join(Zona_fibra_distval, cb_selezionata2, Zona_fibra_distval$col_cod==cb_selezionata2$COD_ISTAT, "inner")
#nrow(test_finale) 
#[1] 3.937.174


#SELEZIONE DEI CODICI CONTRATTO POTENZIALMENTE INTERESSATI (nei soli Comuni in cui ?? pianificata la Fibra)
test_finale2 <- join(Zona_fibra_pianif_distval, cb_selezionata2, Zona_fibra_pianif_distval$col_cod==cb_selezionata2$COD_ISTAT, "inner")
#nrow(test_finale2) 
#[1] 2.988.274


#SELEZIONE DEI CODICI CONTRATTO POTENZIALMENTE INTERESSATI (nei soli Comuni in cui ?? gi?? attiva la Fibra)
test_finale3 <- join(Zona_fibra_attiv_distval, cb_selezionata2, Zona_fibra_attiv_distval$col_cod==cb_selezionata2$COD_ISTAT, "inner")
#nrow(test_finale3)
#[1] 3.787.410




#ANALISI SULLA VELOCIT?? DI DOWNLOAD DEI CODICI CONTRATTO

vod_skyod_view_clear <- read.df("/CLEAR/STAGE/VoD/SkyOnDemand_viewing/full/SkyOnDemand_viewing.parquet", "parquet")
#nrow(vod_skyod_view_clear)
#[1] 258.167.612

vod_dwnl <- select(vod_skyod_view_clear, list("COD_CONTRATTO", "NUM_DWNL_BYTE", "FLG_DWNL_COMPLETO", "NUM_DURATA_DWNL_MS"))
#nrow(vod_dwnl)
#[1] 258.167.612
#numero di cod_contratto distinti:
vod_dwnl_valdist <- distinct(select(vod_dwnl, "COD_CONTRATTO"))
#nrow(vod_dwnl_valdist)
#[1] 1.224.577  <<-----------------------------num. di contratti distinti presenti in questa tabella!

#CALCOLO DELLA VELOCIT?? DI DOWNLOAD
vod_dwnl <- mutate(vod_dwnl, vel_dwnl = (vod_dwnl$NUM_DWNL_BYTE / vod_dwnl$NUM_DURATA_DWNL_MS) / 1000)





#SELEZIONE DEI CONTRATTI CHE HANNO VEL >= 30Mbps (=3,75 MByte/sec)
vod_dwnl_fibra <- subset(vod_dwnl, vod_dwnl$vel_dwnl >= 3.75)
#nrow(vod_dwnl_fibra)
#[1] 2.791.116
vod_dwnl_fibra_valdist <- distinct(select(vod_dwnl_fibra, "COD_CONTRATTO"))
#nrow(vod_dwnl_fibra_valdist)
#[1] 97.154

vod_dwnl_fibra_valdist <- withColumnRenamed(vod_dwnl_fibra_valdist, "COD_CONTRATTO", "NEW_COD_CONTRATTO")



#SELEZIONE DEI CODICE CONTRATTO CHE HANNO VEL_DWNL ALTE MA SENZA FASTWEB
jj <- join(vod_dwnl_fibra_valdist, test_finale, vod_dwnl_fibra_valdist$NEW_COD_CONTRATTO == test_finale$COD_CONTRATTO)
#nrow(jj)
#[1] 48.338 (Zona_Fibra_completa)  <<-------------------------------------------------------------------------------------------------
jjbis <- join(vod_dwnl_fibra_valdist, test_finale2, vod_dwnl_fibra_valdist$NEW_COD_CONTRATTO == test_finale2$COD_CONTRATTO)
#nrow(jjbis)
#[1] 36.303 (Zona_fibra_soloPIANIFICATA)
jjtris <- join(vod_dwnl_fibra_valdist, test_finale3, vod_dwnl_fibra_valdist$NEW_COD_CONTRATTO == test_finale3$COD_CONTRATTO)
#nrow(jjtris)
#[1] 48.108 (Zona_fibra_soloATTIVA)


#SELEZIONE DEI CONTRATTI CHE HANNO VEL < 30Mbps (=3,75 MByte/sec)
vod_dwnl_adsl <- subset(vod_dwnl, vod_dwnl$vel_dwnl < 3.75)
#nrow(vod_dwnl_adsl)
#[1] 163.534.500
vod_dwnl_adsl_valdist <- distinct(select(vod_dwnl_adsl, "COD_CONTRATTO"))
#nrow(vod_dwnl_adsl_valdist)
#[1] 1.214.576

vod_dwnl_adsl_valdist <- withColumnRenamed(vod_dwnl_adsl_valdist, "COD_CONTRATTO", "NEW_COD_CONTRATTO")


#SELEZIONE DEI CODICE CONTRATTO CHE HANNO VEL_DWNL BASSE E COMUNQUE SENZA FASTWEB
jj2 <- join(vod_dwnl_adsl_valdist, test_finale, vod_dwnl_adsl_valdist$NEW_COD_CONTRATTO == test_finale$COD_CONTRATTO)
#nrow(jj2)
#[1] 670.211 (Zona_Fibra_completa)  <<-------------------------------------------------------------------------------------------------
jj2bis <- join(vod_dwnl_adsl_valdist, test_finale2, vod_dwnl_adsl_valdist$NEW_COD_CONTRATTO == test_finale2$COD_CONTRATTO)
#nrow(jj2)
#[1] 507.240 (Zona_fibra_soloPIANIFICATA)
jj2tris <- join(vod_dwnl_adsl_valdist, test_finale3, vod_dwnl_adsl_valdist$NEW_COD_CONTRATTO == test_finale3$COD_CONTRATTO)
#nrow(jj2tris)
#[1] 644.968 (Zona_fibra_soloATTIVA)





#VERIFICA SUI CONTRATTI ATTIVI AD OGGI
cm_mov_business <- view_df(cm_mov_business)
#nrow(cm_mov_business)
#[1] 63.527.663

cm_distval <- distinct(select(cm_mov_business, "COD_CONTRATTO"))
#nrow(cm_distval)
#[1] 15.510.984 #Questa tabella contiene 15 milioni di codici contratto

cm_bus_distval <- distinct(select(cm_mov_business, cm_mov_business$DES_STATO_BUSINESS_DA, cm_mov_business$DES_STATO_BUSINESS_A))
#View(cm_bus_distval)


cm_attiv <- select(cm_mov_business, "COD_CONTRATTO", "DAT_INIZIO", "DAT_FINE", "DES_STATO_BUSINESS_DA", "DES_STATO_BUSINESS_A")
#View(head(cm_attiv))

#SELEZIONE DEI COD_CONTRATTO ATTIVI OGGI
cm_attiv_today <- subset(cm_attiv, cm_attiv$DES_STATO_BUSINESS_DA == "Active" & cm_attiv$DES_STATO_BUSINESS_A == "")
#View(head(cm_attiv_today))
#nrow(cm_attiv_today)
#[1] 4.247.804  <<------------------------------------------------------------------------------------------------- NUMERO DI CONTRATTI ATTIVI OGGI
test <- distinct(select(cm_attiv_today, "COD_CONTRATTO"))
#nrow(test)
#[1] 4.247.804




#SELEZIONE LISTA DEFINITIVA

jj <- withColumnRenamed(jj, "COD_CONTRATTO", "COD_CONTRATTO_join") 
jj2 <- withColumnRenamed(jj2, "COD_CONTRATTO", "COD_CONTRATTO_join")

#FILTRO SUI COD_CONTRATTO ATTIVI OGGI:
final_cb <- join(cm_attiv_today, jj, cm_attiv_today$COD_CONTRATTO == jj$COD_CONTRATTO_join)
#nrow(final_cb)
#[1] 47.099  <<----------------------------- LISTA COD_CONTRATTO CHE HANNO VEL_DWNL ALTE MA SENZA FASTWEB, RAGGIUNTI DA FIBRA OTTICA (NEL COMUNE DI RESIDENZA).
test <- distinct(select(final_cb, "COD_CONTRATTO"))
#nrow(test)
#[1] 46.379

final_cb2 <- join(cm_attiv_today, jj2, cm_attiv_today$COD_CONTRATTO == jj2$COD_CONTRATTO_join)
#nrow(final_cb2)
#[1] 637.669  <<------------------ LISTA COD_CONTRATTO CHE HANNO VEL_DWNL BASSE E COMUNQUE SENZA FASTWEB, RAGGIUNTI DA FIBRA OTTICA (NEL COMUNE DI RESIDENZA).
test <- distinct(select(final_cb2, "COD_CONTRATTO"))
#nrow(test)
#[1] 630.420
