

#loading
vod_viewing <- read.df("hdfs:///STAGE/VoD/SkyOnDemand_viewing/full/SkyOnDemand_viewing.parquet","parquet")
vod_schedule <- read.df("hdfs:///STAGE/VoD/VodSchedule/full/Palinsesto_VOD.parquet","parquet")
vod_viewing <- withColumn(vod_viewing,"DES_TITOLO",upper(vod_viewing$DES_TITOLO))


#preprocessing
vod_tds1=filter(x = vod_viewing,condition = "(DES_TITOLO like '%IL TRONO DI SPADE EP%' or DES_TITOLO like '%IL TRONO DI SPADE (%' or DES_TITOLO like '%IL TRONO DI SPADE -%') and FLG_DWNL_COMPLETO=1")
vod_tds1_cnt <- collect(agg(
  groupBy(vod_tds1, "COD_CONTRATTO","DES_TITOLO"), 
  NUM_PROPERTIES=n(vod_tds1$COD_CONTRATTO)
))

View(vod_tds1_cnt)

vod_tds1_cnt[grepl("EP.01", vod_tds1_cnt$DES_TITOLO, ignore.case=FALSE),]$DES_TITOLO <- "EPISODIO01"
View(vod_tds1_cnt)

vod_tds1_cnt_1=data.frame(table(vod_tds1_cnt$COD_CONTRATTO,vod_tds1_cnt$DES_TITOLO))
colnames(vod_tds1_cnt_1)=c('COD_CONTRATTO','DES_TITOLO','count')
View(vod_tds1_cnt_1)

vod_tds1_cnt_1_1=vod_tds1_cnt_1[vod_tds1_cnt_1$count>0,]
View(vod_tds1_cnt_1_1)

vod_tds1_final=data.frame(table(vod_tds1_cnt_1_1$COD_CONTRATTO))
colnames(vod_tds1_final)=c('COD_CONTRATTO','count')
View(vod_tds1_final)

#ottengo i download della serie selezionata per ogni utente(cod_contratto)!!

rm(vod_tds1_cnt,vod_tds1_cnt_1,vod_tds1_cnt_1_1)



# 
# #uguale per il Trono di Spade, stagine 2:
# vod_tds2=filter(x = vod_viewing,condition = "DES_TITOLO like '%IL TRONO DI SPADE 2%' and FLG_DWNL_COMPLETO=1")
# vod_tds2_cnt <- collect(agg(
#   groupBy(vod_tds2, "COD_CONTRATTO","DES_TITOLO"), 
#   NUM_PROPERTIES=n(vod_tds2$COD_CONTRATTO)
# ))
# 
# 
# #stagione 3:
# vod_tds3=filter(x = vod_viewing,condition = "DES_TITOLO like '%IL TRONO DI SPADE 3%' and FLG_DWNL_COMPLETO=1")
# vod_tds3_cnt <- collect(agg(
#   groupBy(vod_tds3, "COD_CONTRATTO","DES_TITOLO"), 
#   NUM_PROPERTIES=n(vod_tds3$COD_CONTRATTO)
# ))
# 
# 
# 
# 
# vod_tds4=filter(x = vod_viewing,condition = "DES_TITOLO like '%IL TRONO DI SPADE 4%' and FLG_DWNL_COMPLETO=1")
# vod_tds4_cnt <- collect(agg(
#   groupBy(vod_tds4, "COD_CONTRATTO","DES_TITOLO"), 
#   NUM_PROPERTIES=n(vod_tds4$COD_CONTRATTO)
# ))
# 
# 
# 
# 
# vod_tds5=filter(x = vod_viewing,condition = "DES_TITOLO like '%IL TRONO DI SPADE 5%' and FLG_DWNL_COMPLETO=1")
# vod_tds5_cnt <- collect(agg(
#   groupBy(vod_tds5, "COD_CONTRATTO","DES_TITOLO"), 
#   NUM_PROPERTIES=n(vod_tds5$COD_CONTRATTO)
# ))
# 
# 
# 
# 
# vod_tds6=filter(x = vod_viewing,condition = "DES_TITOLO like '%IL TRONO DI SPADE 6%' and FLG_DWNL_COMPLETO=1")
# vod_tds6_cnt <- collect(agg(
#   groupBy(vod_tds6, "COD_CONTRATTO","DES_TITOLO"),
#   NUM_PROPERTIES=n(vod_tds6$COD_CONTRATTO)
# ))




#Adesso prendo la serie tv "1992":
vod_92=filter(x = vod_viewing,condition = "DES_TITOLO like '%1992%' and FLG_DWNL_COMPLETO=1")
vod_92_cnt <- collect(agg(
  groupBy(vod_92, "COD_CONTRATTO","DES_TITOLO"), 
  NUM_PROPERTIES=n(vod_92$COD_CONTRATTO)
))

vod_92_cnt_1=data.frame(table(vod_92_cnt$COD_CONTRATTO,vod_92_cnt$DES_TITOLO))
colnames(vod_92_cnt_1)=c('COD_CONTRATTO','DES_TITOLO','count')
vod_92_cnt_1_1=vod_92_cnt_1[vod_92_cnt_1$count>0,]
vod_92_final=data.frame(table(vod_92_cnt_1_1$COD_CONTRATTO))
colnames(vod_92_final)=c('COD_CONTRATTO','count')
rm(vod_92_cnt,vod_92_cnt_1,vod_92_cnt_1_1)




#aggiunta info

#1992
max(vod_92_final$count)
#2-5-7-10
vod_92_final$bins <- cut(vod_92_final$count, breaks=c(0,2,5,7,10), labels=c("low_user","medium_low_user","medium_high_user","high_user"))

View((vod_92_final))





write.table(x=vod_92_final,file = '~/1992-prova.csv',quote = T,sep = ',',na = '',row.names = F,col.names = T)




#View(head(vod_viewing))
vod_viewing <- withColumn(vod_viewing,"DAT_ACTION",cast(cast(unix_timestamp(vod_viewing$DAT_ACTION, 'MM/dd/yyyy'), 'timestamp'), 'date'))
max_date <- collect(select(vod_viewing, max(vod_viewing$DAT_ACTION)))
min_date <- collect(select(vod_viewing, min(vod_viewing$DAT_ACTION)))



#-------------------------------------------------------------------------------------


prova <- read.df("/STAGE/columns_mapping/COD_CONTRATTO.parquet","parquet")

distVals <- collect(distinct(select(prova, 'DES_INTERESSE_LIVELLO_2')))

