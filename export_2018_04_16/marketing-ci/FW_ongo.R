#on Rstudio:
# 
# supp <- read.csv('C:\\Users\\mazzucas\\Desktop\\Stefano\\modello EXTRA\\sas\\cb_sky_NO_FW.csv', header = TRUE, sep = ";")
# 
# supp2 <- read.csv('C:\\Users\\mazzucas\\Desktop\\Stefano\\modello EXTRA\\sas\\Fat_stato_mv_connected.csv', header = TRUE, sep = ",")
# 
# 
# distval <- distinct(select(supp, 'DES_TECNOLOGIA_BUNDLE_MATCH'))
# View(distval)
# 
# sub_ADSL_supp <- subset(supp, supp$DES_TECNOLOGIA_BUNDLE_MATCH == "ADSL WS" | supp$DES_TECNOLOGIA_BUNDLE_MATCH == "ADSL FW" | 
#                           supp$DES_TECNOLOGIA_BUNDLE_MATCH == "ADSL_FTTS" | supp$DES_TECNOLOGIA_BUNDLE_MATCH == "")
# #View(head(sub_ADSL_supp))
# nrow(sub_ADSL_supp)
# 
# sub_FIBRA_supp <- subset(supp, supp$DES_TECNOLOGIA_BUNDLE_MATCH == "ULL_VULA" | supp$DES_TECNOLOGIA_BUNDLE_MATCH == "VPLUS" | 
#                            supp$DES_TECNOLOGIA_BUNDLE_MATCH == "Fibra (FTTS)" | supp$DES_TECNOLOGIA_BUNDLE_MATCH == "Fibra (FTTH)" |
#                            supp$DES_TECNOLOGIA_BUNDLE_MATCH == "VULA" | supp$DES_TECNOLOGIA_BUNDLE_MATCH == "Fibra (GPON)" )
# #View(head(sub_FIBRA_supp))
# nrow(sub_FIBRA_supp)
# 
# cb_NOFW_conFibra <- select(sub_FIBRA_supp, 'COD_CONTRATTO', 'COD_CLIENTE_FRUITORE', 'DES_COD_SEZ_CENS_2001_FRU', 'DES_COD_SEZ_CENS_2011_FRU')
# cb_NOFW_conFibra_2 <- lapply(cb_NOFW_conFibra, as.character)
# #View(head(test))
# nrow(cb_NOFW_conFibra)
# 
# distinct_val_cens_2001 <- distinct(select(cb_NOFW_conFibra, 'DES_COD_SEZ_CENS_2001_FRU'))
# nrow(distinct_val_cens_2001) #per capire quanti codici "unici" ho nella tabella
# 
# distinct_val_cens_2011 <- distinct(select(cb_NOFW_conFibra, 'DES_COD_SEZ_CENS_2011_FRU'))
# nrow(distinct_val_cens_2011) #per capire quanti codici "unici" ho nella tabella
# 
# 
# 
# cb_flg_NOHP <-  subset(supp2, supp2$FLG_HOME_PACK == "N")
# #View(head(cb_flg_NOHP))
# nrow(cb_flg_NOHP)
# 
# 
# # JOIn tra NO Fastweb e NO HomePack, con la possibilit?? di avere la FIBRA a casa
# c <- inner_join(cb_NOFW_conFibra, cb_flg_NOHP, by = "COD_CONTRATTO")
# nrow(c)
# 
# #View(head(cb_NOFW_conFibra))
# #View(head(cb_flg_NOHP))
# 
# names(cb_NOFW_conFibra)[names(cb_NOFW_conFibra)=="COD_CLIENTE_FRUITORE"] <- "COD_CLIENTE"
# 
# d <- inner_join(cb_NOFW_conFibra, cb_flg_NOHP, by = "COD_CLIENTE")
# nrow(d)

#------------------------------------------------------------------------------------------------------------------------------------


cm_ansto <- view_df(cm_ansto)
#printSchema(cm_ansto)
#nrow(cm_ansto)
#[1] 170.593.979

cb_NO_FW <- subset(cm_ansto, cm_ansto$COD_CLUSTER_BUNDLE_MATCH == "2")
#View(head(cb_NO_FW))
#nrow(cb_NO_FW)
#[1] 131.213.117

#NON c'?? il campo:
#sub_ADSL <- subset(cb_NO_FW, cb_NO_FW$DES_TECNOLOGIA_BUNDLE_MATCH == "ADSL WS" | cb_NO_FW$DES_TECNOLOGIA_BUNDLE_MATCH == "ADSL FW" | 
#                    cb_NO_FW$DES_TECNOLOGIA_BUNDLE_MATCH == "ADSL_FTTS" | cb_NO_FW$DES_TECNOLOGIA_BUNDLE_MATCH == "")


 
# View(collect(distinct(select(cb_NO_FW, 'DES_DETTAGLIO_PACCHETTO'))))
# View(collect(distinct(select(cb_NO_FW, 'FLG_TIPO_ADESIONE_BUNDLE'))))
# View(collect(distinct(select(cb_NO_FW, 'COD_CLUSTER_BUNDLE_MATCH'))))
# a=select(cb_NO_FW, 'DAT_RICH_CESS_FW')

# View(collect(distinct(select(cb_NO_FW, 'VAL_BALANCE_SCADUTO_FW_DAILY'))))
# View(collect(distinct(select(cb_NO_FW, 'DES_CONNETTIVITA_PACC_POSS'))))


df_NGA_attiv <- read.csv("Centrali NGA attive 09-ott-2017.csv", header = TRUE, sep = ";")
#View(head(df_NGA_attiv))

#View(collect(distinct(select(cb_NO_FW, 'COD_CAP_FRU')))) #prova per verificare join tra CAP



a=distinct(select(cm_ansto, list('FLG_CONTRATTO_BUNDLE', 'FLG_TIPO_ADESIONE_BUNDLE')))
#View(head(a))


cb_NO_FW_NO_HP <- filter(cm_ansto, cm_ansto$FLG_CONTRATTO_BUNDLE=="0" & cm_ansto$COD_CLUSTER_BUNDLE_MATCH=="2")
#nrow(cb_NO_FW_NO_HP)
#[1] 130.335.140
cb_NO_FW_NO_HP_valdist <- distinct(select(cm_ansto, "COD_CONTRATTO"))
#nrow(cb_NO_FW_NO_HP_valdist)
#[1] 7.539.193
#View(head(cb_NO_FW_NO_HP_valdist))


#DA QUI IN POI DECODIFICA COD_CONTRATTO <<---------------------------------------

#decod <- view_df(decod)






# NO----------------------------------------------------------------------------------------
# b=subset(cm_ansto, cm_ansto$FLG_CONTRATTO_BUNDLE == "0")      #flag=1 clienti con HOME PACK
# c=subset(cm_ansto, cm_ansto$COD_CLUSTER_BUNDLE_MATCH == "2")  #cod=2 clienti SENZA FW
# #nrow(b)
# #[1] 157.878.017
# #nrow(c)
# #[1] 131.213.117
# #nrow(cb_NO_FW)=131.213.117
# 
# 
# #FLG_CONTRATTO_BUNDLE se clienti hanno HP oppure no
# #FLG_TIPO_ADESIONE_BUNDLE (?)
# #COD_CLUSTER_BUNDLE_MATCH (2=NO_FW+SI_SKY, 3=SI_FW+SI_SKY)
# 
# 
# e=withColumnRenamed(c,"COD_CONTRATTO","NEW_COD_CONTRATTO")
# #View(head(e))
# d=join(b, e, b$COD_CONTRATTO==e$NEW_COD_CONTRATTO)
# View(head(d))
# nrow(d)
# #[1] 3.230.628.845


