
# 07_golden_sets #


source("connection_R.R")
options(scipen = 10000)

source("Digitalizzazione/NEW_kpi_digital/Production_Code/00_Utils_kpi_digital.R")



# Golden set Adobe #

df_variabile_25 <- read.parquet(path_df_variabile_25) #adobe pulito

golden_generico <- withColumn(df_variabile_25, "score_digital", ifelse((df_variabile_25$ricorrenza_totale == 'ricorrente' & df_variabile_25$percentile_score_utilizzo == 10 | (df_variabile_25$cookie_skyitdev > 6)), 1,
                                                                       ifelse(df_variabile_25$tipo_device == 'desktop' & df_variabile_25$visite_app == 0 & df_variabile_25$visite_web == 1, 0, NULL )))

golden_fan <- withColumn(df_variabile_25, "score_digital", ifelse(( df_variabile_25$ricorrenza_editoriale == 1 | df_variabile_25$avg_visite_mese_editoriale > 4), 1,
                                                                  ifelse(df_variabile_25$tipo_device == 'desktop' & df_variabile_25$visite_app == 0 & df_variabile_25$visite_web == 1, 0, NULL )))

golden_social <- withColumn(df_variabile_25, "score_digital", ifelse(( df_variabile_25$ricorrenza_app == 1 | (df_variabile_25$social_network =='social' & df_variabile_25$ricorrenza_totale == 'ricorrente') | df_variabile_25$avg_visite_mese_app > 4), 1,
                                                                     ifelse(df_variabile_25$tipo_device == 'desktop' & df_variabile_25$visite_app == 0 & df_variabile_25$visite_web == 1, 0, NULL )))

write.parquet(golden_generico,path_golden_generico)

write.parquet(golden_fan,path_golden_fan)

write.parquet(golden_social,path_golden_social)



# Golden set Adform #

adform = read.parquet(path_impression_finale_pulito)

golden_adform= withColumn(adform, 'score_digital', ifelse((adform$flag_social == 1 & adform$numero_impression_social > 3 | (adform$numero_device > 4 & adform$avg_impression_giorno > 15)), 1 ,
                                                          ifelse(adform$numero_mesi == 1 & adform$avg_impression_mese < 5 & adform$perc_impression_desktop == 1, 0, NULL)))

write.parquet(golden_adform, path_golden_adform)
