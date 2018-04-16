#TVSamsung_VOD(+Skygo) --CLEAR

#--- Skygo ---

kpi_skygo <- read.df("/CLEAR/STAGE/CMDM/DettaglioServizioSkyGo/full/fat_kpi_servizio_skygo_sto.parquet", "parquet")
#View(head(kpi_skygo))
#nrow(kpi_skygo)
#[1] 67.128.125

#FLG_ACCESSO_SKYGO
#distval_kpi_skygo <- distinct(select(cm_kpi_skygo, "FLG_ACCESSO_SKYGO"))
#View(distval_kpi_skygo)

distval_kpi_skygo  <- summarize(groupBy(kpi_skygo, kpi_skygo$FLG_ACCESSO_SKYGO), count = n(kpi_skygo$FLG_ACCESSO_SKYGO))
#View(distval_kpi_skygo)
# 0: 6.115.726
# FLG_ACCESSO_SKYGO: 1
# 1: 61.012.398


# 
# #--- Test su kpi_skygo ---
# 
# dist_cntr_skygo <-distinct(select(kpi_skygo, "COD_CONTRATTO"))
# #nrow(dist_cntr_skygo)
# #[1] 2.585.172
# 
# 
# distval_kpi_skygo2  <- summarize(groupBy(kpi_skygo, kpi_skygo$COD_CONTRATTO, kpi_skygo$FLG_ACCESSO_SKYGO), count = n(kpi_skygo$FLG_ACCESSO_SKYGO))
# #View(head(distval_kpi_skygo2))
# #nrow(distval_kpi_skygo2)
# #[1] 2.862.528
# 
# 
# distval_skygo_flg0 <- subset(distval_kpi_skygo2, distval_kpi_skygo2$FLG_ACCESSO_SKYGO == 0)
# #nrow(distval_skygo_flg0)
# #[1] 508.413
# 
# distval_skygo_flg1 <- subset(distval_kpi_skygo2, distval_kpi_skygo2$FLG_ACCESSO_SKYGO == 1)
# #nrow(distval_skygo_flg1)
# #[1] 2.354.114
# #---------------------
# #2.862.528 - 2.585.172
# #[1] 277.356
# 
# flg0 <- select(distval_skygo_flg0, "COD_CONTRATTO")
# #nrow(flg0)
# #[1] 508.413
# flg1 <- select(distval_skygo_flg1, "COD_CONTRATTO")
# #nrow(flg1)
# #[1] 2.354.114
# #--------------------
# flg0_flg1 <- intersect(flg0, flg1)
# #nrow(flg0_flg1)
# #[1] 277.356  <<------- NUMERO COD_CONTRATTO CHE HANNO FLG=0 & FLG=1
# 
# 
# 
# # 
# # #JOIN per capire i soli COD C_CONTRATTO che hanno attivato skygo senza accedervi:
# # flg0 <- withColumnRenamed(flg0, "COD_CONTRATTO", "CODICE_CONTRATTO")
# # 
# # cntr_skygo_flg_accesso <- join(flg0, flg0_flg1, flg0$CODICE_CONTRATTO==flg0_flg1$COD_CONTRATTO, "outer")
# # nrow(cntr_skygo_flg_accesso)
# # #[1] 508.413
# # View(head(cntr_skygo_flg_accesso))
# # ff <- filter(cntr_skygo_flg_accesso, isnull(cntr_skygo_flg_accesso$CODICE_CONTRATTO))# & isNotNull(cntr_skygo_flg_accesso$COD_CONTRATTO))
# # nrow(ff)
# # #[1]  <<--------- ATTENZIONE!
# # 



#--- VOD ---

#--- kpi_vod ---

kpi_vod <- read.df("/CLEAR/STAGE/CMDM/DettaglioServizioVODSTB/full/fat_kpi_servizio_vod_sto.parquet","parquet")
#View(head(kpi_vod))
#nrow(kpi_vod)
#[1] 60.230.045

distval_kpi_vod  <- summarize(groupBy(kpi_vod, kpi_vod$FLG_ACCESSO_VOD_STB), count = n(kpi_vod$FLG_ACCESSO_VOD_STB))
#View(distval_kpi_vod) #  <<----- Tutti i clienti della tabella hanno fatto accesso al VOD
# 1: 60.230.044
# FLG_ACCESSO_VOD_STB: 1

distval_cod_cntr <- distinct(select(kpi_vod, "COD_CONTRATTO"))
#nrow(distval_cod_cntr)
#[1] 2.625.099


#------------ Verifica su kpi_vod che i COD_CONTRATTO che "vedono" nell'ultimo mese abbiano (prima data - ultima data) visione nell'ultimo mese:
#FLG_ACCESSO/DWNL ULTIMO MESE con DAT_ULTIMO_ACCESSO/DWNL
kpi_vod_DAT <- withColumn(kpi_vod, "DAT_PRIMO_ACCESSO_VOD_STB", cast(cast(unix_timestamp(kpi_vod$DAT_PRIMO_ACCESSO_VOD_STB, 'dd/MM/yyyy'), 'timestamp'), 'date'))
kpi_vod_DAT <- withColumn(kpi_vod_DAT, "DAT_ULTIMO_ACCESSO_VOD_STB", cast(cast(unix_timestamp(kpi_vod_DAT$DAT_ULTIMO_ACCESSO_VOD_STB, 'dd/MM/yyyy'), 'timestamp'), 'date'))
kpi_vod_DAT <- withColumn(kpi_vod_DAT, "DAT_ULTIMO_DOWNLOAD_VOD_STB", cast(cast(unix_timestamp(kpi_vod_DAT$DAT_ULTIMO_DOWNLOAD_VOD_STB, 'dd/MM/yyyy'), 'timestamp'), 'date'))
#View(head(kpi_vod_DAT))

first_dwnl_vod_date <- summarize(kpi_vod_DAT, prima_data=min(kpi_vod_DAT$DAT_PRIMO_ACCESSO_VOD_STB)) 
#View(first_dwnl_vod_date)
# 1990-01-01
last_dwnl_vod_date <- summarize(kpi_vod_DAT, ultima_data=max(kpi_vod_DAT$DAT_ULTIMO_DOWNLOAD_VOD_STB)) 
#View(last_dwnl_vod_date)
# 2017-03-19

last_month_dwnl_vod_list <- subset(kpi_vod_DAT, kpi_vod_DAT$DAT_ULTIMO_DOWNLOAD_VOD_STB >= "2017-02-19" & kpi_vod_DAT$DAT_ULTIMO_DOWNLOAD_VOD_STB <= "2017-03-19")
#View(head(last_month_dwnl_vod_list))
#nrow(last_month_dwnl_vod_list)
#[1] 2.937.026

distval_dwnl_vod_last_month <- distinct(select(last_month_dwnl_vod_list, "COD_CONTRATTO"))
#nrow(distval_dwnl_vod_last_month)
#[1] 1.661.667


#----------------------------------------------- Verifica flg_dwnl_ultimo_mese = lista_last_month
kpi_vod_flg_dwnl_mese <- subset(kpi_vod_DAT, kpi_vod_DAT$FLG_DOWNLOAD_VOD_STB_MESE == "1")
#nrow(kpi_vod_flg_dwnl_mese)
#[1] 39.258.735
distval_kpi_vod_flg_dwnl_mese <- distinct(select(kpi_vod_flg_dwnl_mese, "COD_CONTRATTO"))
#nrow(distval_kpi_vod_flg_dwnl_mese)
#[1] 2.426.869

kpi_vod_flg_dwnl_last30gg <- subset(kpi_vod_DAT, kpi_vod_DAT$FLG_DOWNLOAD_VOD_STB_LAST30GG == "1")
#nrow(kpi_vod_flg_dwnl_last30gg)
#[1] 39.579.578
distval_kpi_vod_flg_dwnl_last30gg <- distinct(select(kpi_vod_flg_dwnl_last30gg, "COD_CONTRATTO"))
#nrow(distval_kpi_vod_flg_dwnl_last30gg)
#[1] 2.428.111





#--- vod_viewing ---

vod_viewing <- read.df("/CLEAR/STAGE/VoD/SkyOnDemand_viewing/full/SkyOnDemand_viewing.parquet", "parquet")
#View(head(vod_viewing))
#printSchema(vod_viewing)
#nrow(vod_viewing)
#[1] 258.167.612

distval_vod_viewing <- distinct(select(vod_viewing, "COD_CONTRATTO"))
#nrow(distval_vod_viewing)
#[1] 1.224.577

vod_viewing_DAT <- withColumn(vod_viewing,"#DATA_COMPETENZA",cast(cast(unix_timestamp(vod_viewing$'#DATA_COMPETENZA', 'MM/dd/yyyy HH:mm:ss'), 'timestamp'), 'date'))
vod_viewing_DAT <- withColumn(vod_viewing_DAT,"DAT_ACTION",cast(unix_timestamp(vod_viewing_DAT$DAT_ACTION, 'MM/dd/yyyy HH:mm:ss'), 'timestamp'))
#View(head(vod_viewing_DAT))

first_vod_date <- summarize(vod_viewing_DAT, prima_data=min(vod_viewing_DAT$'#DATA_COMPETENZA')) 
#View(first_vod_date)
# 2016-04-01
last_vod_date <- summarize(vod_viewing_DAT, ultima_data=max(vod_viewing_DAT$'#DATA_COMPETENZA')) 
#View(last_vod_date)
# 2017-04-30

last_month_vod_list <- subset(vod_viewing_DAT, vod_viewing_DAT$'#DATA_COMPETENZA' >= "2017-03-30" & vod_viewing_DAT$'#DATA_COMPETENZA' <= "2017-04-30")
#View(head(last_month_vod_list))
#nrow(last_month_vod_list)
#[1] 24.436.777
distval_vod_viewing_last_month <- distinct(select(last_month_vod_list, "COD_CONTRATTO"))
#nrow(distval_vod_viewing_last_month)
#[1] 972.656


#----------------------------------------- Verififca che tutti i COD_CONTRATTO in "last_month" abbiano FLG_DWNL_EFFETTUATO = 1
last_month_vod_flg_dwnld <- subset(last_month_vod_list, last_month_vod_list$FLG_DWNL_EFFETTUATO == "1")
#nrow(last_month_vod_flg_dwnld)
#[1] 14.143.085
distval_vod_flg_dwnl_last_month <- distinct(select(last_month_vod_flg_dwnld, "COD_CONTRATTO"))
#nrow(distval_vod_flg_dwnl_last_month)
#[1] 946.056

distval_vod_flg_dwnl_last_month <- withColumnRenamed(distval_vod_flg_dwnl_last_month, "COD_CONTRATTO", "COD_CONTRATTO_jj")
num_cb_vod_dwnld_last_month <- join(distval_vod_viewing_last_month, distval_vod_flg_dwnl_last_month, distval_vod_viewing_last_month$COD_CONTRATTO == distval_vod_flg_dwnl_last_month$COD_CONTRATTO_jj)
#nrow(num_cb_vod_dwnld_last_month)
#[1] 946.056  <<---- TUTTI I CLIENTI CHE hanno FLG_DWNL=1 nell'ULTIMO MESE


#----------------------------------------------------------------------------------------------------------------------------------
#Contro verifica: 972.656 - 946.056 = (26.600) sono i CLINETI con NUM_DWNL_BYTE = NA 
last_month_vod_NO_flg <- subset(last_month_vod_list, isNull(last_month_vod_list$NUM_DWNL_BYTE))
#nrow(last_month_vod_NO_flg)
#[1] 10.293.692

distval_vod_NO_flg_last_month <- distinct(select(last_month_vod_NO_flg, "COD_CONTRATTO"))
#nrow(distval_vod_NO_flg_last_month)
#[1] 819.944 (????? perch?? non: 26.600 ?????)

#Riprova:
riprova <- subset(last_month_vod_list, last_month_vod_list$FLG_DWNL_EFFETTUATO == 1 & isNull(last_month_vod_list$NUM_DWNL_BYTE))
#nrow(riprova)
#[1] 0
#----------------------------------------------------------------------------------------------------------------------------------





# 
# #--- Verificare quanti clienti utilizzano sia skygo, sia vod: ---
# 
# dist_cntr_vod <- withColumnRenamed(dist_cntr_vod, "COD_CONTRATTO", "COD_CONTRATTO_join")
# 
# test_join <- join(dist_cntr_skygo, dist_cntr_vod, dist_cntr_skygo$COD_CONTRATTO == dist_cntr_vod$COD_CONTRATTO_join)
# #nrow(test_join)
# #[1] 1.769.754
# 
# 
# test_union <- union(dist_cntr_skygo, dist_cntr_vod) 
# #nrow(test_union)
# #[1] 5.210.271
# test_intersect <- intersect(dist_cntr_skygo, dist_cntr_vod)
# #nrow(test_intersect)
# #[1] 1.769.754
# 
# #test_diff <- diff(test_union,test_intersect) #  <<------ ??????
# 





