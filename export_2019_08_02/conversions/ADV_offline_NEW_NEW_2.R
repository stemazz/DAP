
## Impatto ADV su vendite offline NEW 2

# Kpi navigazioni su sito pre-conversione
# "/user/valentina/IMPATTO_ADV_sommarizzaz_sito.parquet"

# Scarico completto delle navigazioni associate agli ext_id target
# write.parquet(scarico_naigazione_2,"/user/valentina/IMPATTO_ADV_scarico_naigazione.parquet")




#apri sessione
source("connection_R.R")
options(scipen = 1000)



## Base elabortata da file estrazione BI: 

skyid_tot <- read.parquet("/user/stefano.mazzucca/p2c_nd_skyid_tot.parquet")
View(head(skyid_tot,100))
nrow(skyid_tot)
# 128.322


## Aggangio le info socio-demo  alla base #################################################################################################################
conversions_all_filter <- read.parquet("/user/stefano.mazzucca/conversion_all_filtered.parquet")
View(head(conversions_all_filter,100))
nrow(conversions_all_filter)
# 128.829


createOrReplaceTempView(skyid_tot, "skyid_tot")
createOrReplaceTempView(conversions_all_filter, "conversions_all_filter")

skyid_tot_info <- sql("select t1.*, 
                              t2.DES_AREA_NIELSEN, t2.DES_CLUSTER_POLIS_I, t2.DES_CLUSTER_POLIS_II, t2.DES_NOME_PROV, t2.DES_NOME_REG, t2.NUM_FAM_AREA_NIE, 
                              t2.NUM_FAM_PROV, t2.NUM_FAM_REG, t2.FASCIA_ETA, t2.FLG_HD, t2.FLG_PVR, t2.FLG_HDMYSKY, t2.FLG_SKY_Q_PLUS,
                              t2.FLG_STB_AMIDALA, t2.FLG_STB_HD, t2.FLG_STB_PVR, t2.FLG_STB_PVRHD, t2.FLG_STB_SD, t2.FLG_STB_SKY_Q_PLATINUM,
                              t2.DES_APPARATO, t2.FLG_ANYWHERE, t2.FLG_SKYGO_PLUS_S, t2.FLG_ANYTIME_OFF, t2.DES_TIPO_PAG, t2.PACK_STIPULA,
                              t2.FASCIA_EXTRA, t2.FLG_ISCR_WEB_SELFCARE, t2.FLG_ISCRIZIONE, t2.DAT_ISCRIZIONE, t2.FASCIA_SDM_PDISC_M4,
                              t2.VAL_ARPU_TEOR_PACK_BASE_REALE, t2.VAL_ARPU_NET_DISCOUNT_REALE, t2.FLG_STATISTICHE, t2.FLG_COMUNICAZ_TERZI, t2.FLG_MULTI,
                              t2.NUM_COD_VIS, t2.PROMO_INGRESSO, t2.FLG_TBY, t2.DAT_CONV_TBY, t2.DES_STATO_TBY, t2.DES_STATO_CNTR, t2.DES_CLS_UTENZA_SALES,
                              t2.DES_CATEGORIA_CANALE, t2.DES_NOM_CAN_PADRE, t2.DES_NOME_CANALE, t2.DES_NOME_QUAL, t2.flg_digital, t2.num_week 
                      from skyid_tot t1
                      left join conversions_all_filter t2
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t1.data_stipula_dt = t2.data_stipula_dt and 
                           t1.DAT_PRIMA_ATTIVAZIONE = t2.DAT_PRIMA_ATTIVAZIONE")
View(head(skyid_tot_info,100))
nrow(skyid_tot_info)
# 128.829


createOrReplaceTempView(skyid_tot_info, "skyid_tot_info")

skyid_tot_info_1 <- sql("select *, 
                              case when num_tot_interactions < 10 then '<10'
                              when num_tot_interactions < 20 then '<20' 
                              when num_tot_interactions < 30 then '<30' 
                              when num_tot_interactions < 40 then '<40' 
                              when num_tot_interactions < 50 then '<50' 
                              when num_tot_interactions < 100 then '<100' 
                              when num_tot_interactions < 200 then '<200' 
                              when num_tot_interactions < 300 then '<300'
                              when num_tot_interactions < 400 then '<400' 
                              when num_tot_interactions < 500 then '<500' 
                              when num_tot_interactions < 1000 then '<1000' 
                              else '>1000' end as bin_interactions
                        from skyid_tot_info")
View(head(skyid_tot_info_1,100))
nrow(skyid_tot_info_1)
# 128.829


write.parquet(skyid_tot_info_1,"/user/stefano.mazzucca/p2c_nd_base_completa.parquet")
write.df(repartition( skyid_tot_info_1, 1), path = "/user/stefano.mazzucca/p2c_nd_base_completa.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


skyid_tot_info <- read.parquet("/user/stefano.mazzucca/p2c_nd_base_completa.parquet")
View(head(skyid_tot_info,100))
nrow(skyid_tot_info)
# 128.829


#############################################################################################################################################################

## Utenti intercettati da impatto ADV pre conversione

skyid_tot_navig <- filter(skyid_tot_info, "min_data_interactions is NOT NULL")
View(head(skyid_tot_navig,100))
nrow(skyid_tot_navig)
# 39.699


## Utenti che hanno convertito in maniera NON digitale, intercettate da ADV on line prima della conversione!

conv_flg_digital_0 <- filter(skyid_tot_info, "flg_digital = 0")
View(head(conv_flg_digital_0,100))
nrow(conv_flg_digital_0)
# 110.155


createOrReplaceTempView(skyid_tot_navig, "skyid_tot_navig")
createOrReplaceTempView(conv_flg_digital_0, "conv_flg_digital_0")

target_conv_nav_flg0 <- sql("select distinct t1.*
                            from skyid_tot_navig t1
                            inner join conv_flg_digital_0 t2
                              on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t1.data_stipula_dt = t2.data_stipula_dt 
                            ") # and t1.DAT_PRIMA_ATTIVAZIONE = t2.DAT_PRIMA_ATTIVAZIONE
View(head(target_conv_nav_flg0,100))
nrow(target_conv_nav_flg0)
# 31.863



target_mai_attivato <- filter(target_conv_nav_flg0, "DAT_PRIMA_ATTIVAZIONE like '01/01/1900'")
nrow(target_mai_attivato)
# 1.811


#############################################################################################################################################################
#############################################################################################################################################################
## Analisi ##
#############################################################################################################################################################
#############################################################################################################################################################

target_conv_nav_flg0 <- withColumn(target_conv_nav_flg0, "days_p2c_first_interaction", datediff(target_conv_nav_flg0$data_stipula_dt, target_conv_nav_flg0$min_data_interactions))

dist_day_first_interaction <- summarize(groupBy(target_conv_nav_flg0, "days_p2c_first_interaction"), count = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
dist_day_first_interaction_2 <- arrange(dist_day_first_interaction, asc(dist_day_first_interaction$days_p2c_first_interaction))
View(head(dist_day_first_interaction_2, 10000))

#write.df(repartition( dist_day_first_interaction_2, 1), path = "/user/stefano.mazzucca/NEW_exp_distribution_first_interaction.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



createOrReplaceTempView(target_conv_nav_flg0, "target_conv_nav_flg0")

target_conv_nav_flg0 <- sql("select *,
                              case when days_p2c_first_interaction <= 7 then '<7'
                              when days_p2c_first_interaction > 7 and days_p2c_first_interaction <= 14 then '<14'
                              when days_p2c_first_interaction > 14 and days_p2c_first_interaction <= 21 then '<21'
                              when days_p2c_first_interaction > 21 and days_p2c_first_interaction <= 28 then '<28'
                              when days_p2c_first_interaction > 28 and days_p2c_first_interaction <= 60 then '<60'
                              else '>60' end as bin_temp_conv
                            from target_conv_nav_flg0")
View(head(target_conv_nav_flg0,100))
nrow(target_conv_nav_flg0)
# 31.863






target_conv_nav_flg0 <- withColumn(target_conv_nav_flg0, "days_p2c_last_interaction", datediff(target_conv_nav_flg0$data_stipula_dt, target_conv_nav_flg0$max_data_interactions))

dist_day_last_interaction <- summarize(groupBy(target_conv_nav_flg0, "days_p2c_last_interaction"), count = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
dist_day_last_interaction_2 <- arrange(dist_day_last_interaction, asc(dist_day_last_interaction$days_p2c_last_interaction))
View(head(dist_day_last_interaction_2, 10000))

#write.df(repartition( dist_day_last_interaction_2, 1), path = "/user/stefano.mazzucca/NEW_exp_distribution_last_interaction.csv", "csv", sep=";", mode = "overwrite", header=TRUE)






write.parquet(target_conv_nav_flg0, "/user/stefano.mazzucca/p2c_nd_target_NO_digital.parquet")
write.df(repartition( target_conv_nav_flg0, 1), path = "/user/stefano.mazzucca/p2c_nd_target_NO_digital.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


target_conv_nav_flg0 <- read.parquet("/user/stefano.mazzucca/p2c_nd_target_NO_digital.parquet")
View(head(target_conv_nav_flg0,100))
nrow(target_conv_nav_flg0)
# 31.863





distr_bin_temp <- summarize(groupBy(target_conv_nav_flg0, "bin_temp_conv"), count = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
distr_bin_temp_2 <- arrange(distr_bin_temp, "bin_temp_conv")
View(head(distr_bin_temp_2,100))



distr_weekFY_conv <- summarize(groupBy(target_conv_nav_flg0, "num_week", "bin_temp_conv"), count_weekFY = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
distr_weekFY_conv_2 <- arrange(distr_weekFY_conv, asc(distr_weekFY_conv$num_week), desc(distr_weekFY_conv$bin_temp_conv))
View(head(distr_weekFY_conv_2,100))







distr_weekFY_conv <- summarize(groupBy(target_conv_nav_flg0, "num_week", "bin_interactions"), count_weekFY = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
distr_weekFY_conv_2 <- arrange(distr_weekFY_conv, asc(distr_weekFY_conv$num_week), desc(distr_weekFY_conv$bin_interactions))
View(head(distr_weekFY_conv_2,1000))


distr_bin_temp <- summarize(groupBy(target_conv_nav_flg0, "bin_interactions"), count = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
distr_bin_temp_2 <- arrange(distr_bin_temp, "bin_interactions")
View(head(distr_bin_temp_2,100))



distr_week_FY <- summarize(groupBy(target_conv_nav_flg0, "num_week"), count = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
distr_week_FY_2 <- arrange(distr_week_FY, "num_week")
View(head(distr_week_FY_2,100))



distr_channel <- summarize(groupBy(target_conv_nav_flg0, "DES_CATEGORIA_CANALE"), count = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
#distr_channel_2 <- arrange(distr_channel, "DES_CANALE_VENDITA")
View(head(distr_channel,100))




distr_eta <- summarize(groupBy(target_conv_nav_flg0, "FASCIA_ETA"), count = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
distr_eta_2 <- arrange(distr_eta, "FASCIA_ETA")
View(head(distr_eta_2,100))





distr_area <- summarize(groupBy(target_conv_nav_flg0, "DES_AREA_NIELSEN"), count = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
View(head(distr_area,100))



distr_regione <- summarize(groupBy(target_conv_nav_flg0, "DES_NOME_REG"), count = count(target_conv_nav_flg0$COD_CLIENTE_CIFRATO))
View(head(distr_regione,100))







#chiudi sessione
sparkR.stop()
