

clienti <- read.parquet("/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_nopdisc_182.parquet")
View(head(clienti,100))




#FILE SALVATI:
#NAVIGAZIONI SITO NO-PDISC TRAINING 
write.parquet(join_nopdisc_1,"/user/valentina/churnOnline_rifaiMod_scarico_navigaz_NoPdisc_trainingModello.parquet")

#NAVIGAZIONI SITO SI-PDISC TRAINING
write.parquet(join_pdisc_1,"/user/valentina/churnOnline_rifaiMod_scarico_navigaz_SiPdisc_trainingModello.parquet")



#TEST MENSILI:

#NAVIGAZIONE SITO SI-PDISC 
write.parquet(join_1,"/user/valentina/churnOnline_scaricoBI_xtest_navigazSkyITDev_OK.parquet")
#NAVIGAZIONE APP SI-PDISC 
churnOnline_scaricoBI_xtest_navigazAPP <- read.parquet("/user/valentina/churnOnline_scaricoBI_xtest_navigazAPP.parquet")
View(head(churnOnline_scaricoBI_xtest_navigazAPP,100))
nrow(churnOnline_scaricoBI_xtest_navigazAPP) 
#9.819.593

#NAVIGAZIONE SITO NO-PDISC (BASE AD APRILE):
write.parquet(join_1,"/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_navigazSkyITDev_ok.parquet")
#NAVIGAZIONE APP NO-PDISC: 
write.parquet(join_base_app,"/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_navigazAPP.parquet")





#questa e' la base con i SI PDISC:
base_test <- read.parquet("/user/valentina/churnOnline_scaricoBI_xtest_Pdisc.parquet")
View(head(base_test,100))
#questa e' la base dei NO Pdsic
scarico_base_xTest_Attivi_ad_Apr2018_4 <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin.parquet")
View(head(scarico_base_xTest_Attivi_ad_Apr2018_4,100))








cookie_sito_pdisc_17 <- read.parquet("/user/riccardo.motta/churn_digital/skyitdev.parquet")

filt <- filter(cookie_sito_pdisc_17, "page_url_post_evar LIKE '%query=recess%'")
View(head(filt,100))
nrow(filt)
#3229

cookie_sito_pdisc_17 <- read.parquet("/user/valentina/churnOnline_rifaiMod_scarico_navigaz_SiPdisc_trainingModello.parquet") 
View(head(cookie_sito_pdisc_17))

filt <- filter(cookie_sito_pdisc_17, "page_url_pulita_post_prop LIKE '%query=recess%'")
nrow(filt)
#3229


sito_completo <- read.parquet("/user/stefano.mazzucca/scarico_corporate_guidatv_20180712.parquet")
View(head(sito_completo,1000))
nrow(sito_completo)
# 664.318.214

filt_2 <- filter(sito_completo, "page_url_pulita_post_prop LIKE '%query=recess%'")
nrow(filt_2)
# 










prova <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_navigazSkyITDev_ok.parquet")
View(head(prova,100))




nopdisc18_fixed <- read.parquet("/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_nopdisc18_fixed.parquet")
View(head(nopdisc18_fixed,100))

df_kpi_nopdisc_18 <- read.parquet("/user/riccardo.motta/churn_digital/kpi_luglio_18/df_kpi_nopdisc_18.parquet")
View(head(df_kpi_nopdisc_18,100))








full_17lug <- read.df("/user/stefano.mazzucca/20180718_richiesta_estrazione_cb_attiva.txt", source = "csv" ,header = "true", delimiter = "|")
View(head(full_17lug,100))
nrow(full_17lug)
# 4.158.508

write.parquet(full_17lug, "/user/stefano.mazzucca/CD_full_17lug.parquet")

prova <- read.parquet("/user/stefano.mazzucca/CD_full_17lug.parquet")
View(head(prova,100))







