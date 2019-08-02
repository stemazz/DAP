

source("connection_R.R")
options(scipen = 10000)


cb_online <- read.parquet("/user/stefano.mazzucca/new_kpi_digital/score_finale_ext_id.parquet")
View(head(cb_online,100))
nrow(cb_online)
# 2.692.058

count <- distinct(select(cb_online, "external_id"))
nrow(count)
# 2.622.817

count_1 <- summarize(groupBy(cb_online, "external_id"), count = count(cb_online$external_id), 
                     decile_max = max(cb_online$decile_kpi_digital), decile_min = min(cb_online$decile_kpi_digital))
count_2 <- arrange(count_1, desc(count_1$count))
View(head(count_2,100))

filtro <- filter(count_2, count_2$count > 1)
nrow(filtro)
# 64.758
prova <- summarize(filtro, somma = sum(filtro$count))
View(head(prova))
# 133.999


##########################################################################################################################################################
##########################################################################################################################################################
##########################################################################################################################################################
##########################################################################################################################################################


path_CB_completa_per_descrittive_20190506 = '/user/emma.cambieri/CB_completa_per_descrittive_20190506.parquet'
finale <- read.parquet(path_CB_completa_per_descrittive_20190506)
View(head(finale,100))
nrow(finale)
# 4.461.940

finale_online <- filter(finale, finale$flag_online == 1 & finale$flag_prima_di_luglio==1)
View(head(finale_online,100))
nrow(finale_online)
# 2.615.489

# cb_crm <- read.df("/user/sonia.pasquini/High_digital_cluster_rt_01feb18_01feb19.csv", source = "csv", header = T, delimiter = ";")
# View(head(cb_crm,100))
# nrow(cb_crm)
# # 737.139
# valdist_extid <- distinct(select(cb_crm, cb_crm$external_id))
# nrow(valdist_extid)
# # 455.329


finale_filt <- select(finale, "skyid",
                      "FASCIA_AGEING",
                      "des_decoder_principale",
                      "DES_STATO_BUSINESS",
                      "num_gestite_om_360d",
                      "num_accessi_APP",
                      "num_accessi_WEB",
                      "num_contatti_BO",
                      "num_contatti_CHAT",
                      "num_contatti_IVR",
                      "comp_contatto",
                      "adobe_decile",
                      "adform_decile",
                      "decile_ripesato_rounded",
                      "flag_online",
                      "decile_generico",
                      "decile_fan",
                      "decile_social")
View(head(finale_filt,100))
nrow(finale_filt)
# 4.461.940

count_decili <- summarize(groupBy(finale_filt, finale_filt$decile_ripesato_rounded), count = count(finale_filt$skyid))
View(head(count_decili,100))

count_gestite_per_decile <- summarize(groupBy(finale_filt, finale_filt$decile_ripesato_rounded), count = sum(finale_filt$num_gestite_om_360d))
View(head(count_gestite_per_decile,100))

count_IVR_per_decile <- summarize(groupBy(finale_filt, finale_filt$decile_ripesato_rounded), count = sum(finale_filt$num_contatti_IVR))
View(head(count_IVR_per_decile,100))

count_chat_per_decile <- summarize(groupBy(finale_filt, finale_filt$decile_ripesato_rounded), count = sum(finale_filt$num_contatti_CHAT))
View(head(count_chat_per_decile,100))




