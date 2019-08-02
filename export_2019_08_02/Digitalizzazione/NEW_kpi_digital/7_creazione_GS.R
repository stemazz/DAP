

source("connection_R.R")
options(scipen = 10000)


path_df_variabile_24 = '/user/matteo.ballerinipuviani/dig_gen_giu_2018_df_pulito.parquet'

path_golden_generico_lug_dic =  "/user/emma.cambieri/TRIPLETTA_GOLDEN_SET/golden_generico_lug_dic.parquet"

path_golden_fan_lug_dic =  "/user/emma.cambieri/TRIPLETTA_GOLDEN_SET/golden_fan_lug_dic.parquet"

path_golden_social_lug_dic =  "/user/emma.cambieri/TRIPLETTA_GOLDEN_SET/golden_social_lug_dic.parquet"


path_impression_finale_pulito = "/user/emma.cambieri/impression_finale_pulito_20180101_20180630.parquet"

path_gs_adform <- "/user/emma.cambieri/GOLDEN_SET_DIGITALIZZAZIONE_2019_FINALI/golden_adform_1_e_0scelti_20180101_20180630.parquet"



lugl_dic = read.parquet(path_df_variabile_24)
# nrow(lugl_dic)

##### CREAZIONE GOLDEN SET 1 e 0 #####
# 
# # 0:
# 
# zeri = filter(lugl_dic, lugl_dic$tipo_device == 'desktop' & lugl_dic$visite_app == 0 & lugl_dic$visite_web == 1)
# nrow(zeri) #169.768
# 
# # 1 generico:
# 
# uni_generico = filter(lugl_dic, lugl_dic$ricorrenza_totale == 'ricorrente' & lugl_dic$percentile_score_utilizzo == 10 | (lugl_dic$cookie_skyitdev > 6))
# nrow(uni_generico) #322.120
# 
# # 1 fan editoriale, xfactor e appsport:
# 
# uni_fan = filter(lugl_dic, lugl_dic$ricorrenza_editoriale == 1 | lugl_dic$avg_visite_mese_editoriale > 4)
# nrow(uni_fan) # 430.027
# 
# # 1 app-social:
# 
# uni_social = filter(lugl_dic, lugl_dic$ricorrenza_app == 1 | (lugl_dic$social_network =='social' & lugl_dic$ricorrenza_totale == 'ricorrente') | lugl_dic$avg_visite_mese_app > 4)
# nrow(uni_social) #413.440



golden_generico <- withColumn(lugl_dic, "score_digital", ifelse((lugl_dic$ricorrenza_totale == 'ricorrente' & lugl_dic$percentile_score_utilizzo == 10 | (lugl_dic$cookie_skyitdev > 6)), 1,
                                                                ifelse(lugl_dic$tipo_device == 'desktop' & lugl_dic$visite_app == 0 & lugl_dic$visite_web == 1, 0, NULL )))

# nrow(golden_generico)
# prova=filter(golden_generico, golden_generico$score_digital ==0)                              
# nrow(prova)


golden_fan <- withColumn(lugl_dic, "score_digital", ifelse(( lugl_dic$ricorrenza_editoriale == 1 | lugl_dic$avg_visite_mese_editoriale > 4), 1,
                                                           ifelse(lugl_dic$tipo_device == 'desktop' & lugl_dic$visite_app == 0 & lugl_dic$visite_web == 1, 0, NULL )))

# nrow(golden_fan)
# prova=filter(golden_fan, golden_fan$score_digital ==1)                              
# nrow(prova)


golden_social <- withColumn(lugl_dic, "score_digital", ifelse(( lugl_dic$ricorrenza_app == 1 | (lugl_dic$social_network =='social' & lugl_dic$ricorrenza_totale == 'ricorrente') | lugl_dic$avg_visite_mese_app > 4), 1,
                                                              ifelse(lugl_dic$tipo_device == 'desktop' & lugl_dic$visite_app == 0 & lugl_dic$visite_web == 1, 0, NULL )))

# nrow(golden_social)
# prova=filter(golden_social, golden_social$score_digital ==0)                              
# nrow(prova)


write.parquet(golden_generico, path_golden_generico_lug_dic)

write.parquet(golden_fan, path_golden_fan_lug_dic)

write.parquet(golden_social, path_golden_social_lug_dic)






adform=read.parquet(path_impression_finale_pulito)


##### GOLDEN SET IMPRESSION ADFORM #####

# adform_gs_1 = filter(adform, (adform$flag_social == 1 &  adform$numero_impression_social > 3) | (adform$numero_device > 4 & adform$avg_impression_giorno > 15))
# # nrow(adform_gs_1) # 96.912
# 
# adform_gs_0 = filter(adform, (adform$numero_mesi == 1 & adform$avg_impression_mese < 5 & adform$perc_impression_desktop == 1))
# # nrow(adform_gs_0) #71.046

adform = withColumn(adform, 'score_digital', ifelse((adform$flag_social == 1 & adform$numero_impression_social > 3 | (adform$numero_device > 4 & adform$avg_impression_giorno > 15)), 1 ,
                                                   ifelse(adform$numero_mesi == 1 & adform$avg_impression_mese < 5 & adform$perc_impression_desktop == 1, 0, NULL)))

write.parquet(adform, path_gs_adform)





