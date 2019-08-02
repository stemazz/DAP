
## KPI e score finali del NEW_KPI_DIGITAL MODEL

source("connection_R.R")
options(scipen = 10000)


## Range temporale LUG - DIC 2018 ##

# path_score_digitale_pesato_adobe_adform_filtro_e_variabili_CMDM_v3 = '/user/emma.cambieri/SCORE_DIGITALE_PESATO_ADFORM_ADOBE_FILTRO_E_VARIABILI_CMDM_v3.parquet'
path_score_digitale_pesato_adobe_adform_filtro_e_variabili_CMDM_v3 = '/user/emma.cambieri/ext_id_decili_ripesati_e_variabili_20190415.parquet'

score_dig <- read.parquet(path_score_digitale_pesato_adobe_adform_filtro_e_variabili_CMDM_v3)
View(head(score_dig,100))
nrow(score_dig)
# 2.692.058

score_dig_2 <- select(score_dig, score_dig$ext_id_totale, score_dig$decile_ripesato_rounded, score_dig$decile_generico, score_dig$decile_fan, score_dig$decile_social, 
                      score_dig$adobe_decile, score_dig$adform_decile)
View(head(score_dig_2,100))
nrow(score_dig_2)
# 2.692.058

score_digital_finale <- select(score_dig_2, "ext_id_totale", "decile_ripesato_rounded")
score_digital_finale <- withColumnRenamed(score_digital_finale, "ext_id_totale", "external_id")
score_digital_finale <- withColumnRenamed(score_digital_finale, "decile_ripesato_rounded", "decile_kpi_digital")

View(head(score_digital_finale,100))

write.parquet(score_digital_finale, "/user/stefano.mazzucca/new_kpi_digital/score_finale_ext_id.parquet", mode = "overwrite")



prova <- read.parquet("/user/stefano.mazzucca/new_kpi_digital/score_finale_ext_id.parquet")
View(head(prova,100))
nrow(prova)
# 2.692.058

prova_2 <- arrange(summarize(groupBy(prova, "decile_kpi_digital"), count = count(prova$external_id)), "decile_kpi_digital")
View(head(prova_2,100))




