
## Impatto ADV su vendite offline NEW 4

#apri sessione
source("connection_R.R")
options(scipen = 1000)




skyid_tot_info <- read.parquet("/user/stefano.mazzucca/p2c_nd_base_completa.parquet")
View(head(skyid_tot_info,100))
nrow(skyid_tot_info)
# 128.829

skyid_tot_attiv <- filter(skyid_tot_info, "flg_digital = 0 and dat_prima_attivazione_dt <> '1900-01-01'")
nrow(skyid_tot_attiv)
# 89.477



createOrReplaceTempView(skyid_tot_info, "skyid_tot_info")

skyid_tot_info_1 <- sql("select *, 
                              case when flg_digital = 1 then 'digital'
                              when (flg_digital = 0 and min_data_interactions is NOT NULL) then 'no_digital_ibridi'
                              when (flg_digital = 0 and min_data_interactions is NULL) then 'no_digital_altri_canali'
                              else NULL end as flg_profilo
                        from skyid_tot_info")
View(head(skyid_tot_info_1,1000))
nrow(skyid_tot_info_1)
# 128.829

## calcolo media VAL_ARPU_GROSS e NET per i 3 profili ##

createOrReplaceTempView(skyid_tot_info_1, "skyid_tot_info_1")

media_ARPU_3_profili <- sql("select flg_profilo, 
                                    mean(VAL_ARPU_TEOR_PACK_BASE_REALE) as media_val_arpu_teorico,
                                    mean(VAL_ARPU_NET_DISCOUNT_REALE) as media_val_arpu_net
                            from skyid_tot_info_1
                            group by flg_profilo")
View(head(media_ARPU_3_profili,100))

#######################################################

## calcolo media VAL_ARPU_GROSS e NET per i 3 profili SENZA T&B ##

skyid_tot_info_2 <- filter(skyid_tot_info_1, "PROMO_INGRESSO not like '%PROVA%'")
nrow(skyid_tot_info_2)
# 94.622

createOrReplaceTempView(skyid_tot_info_2, "skyid_tot_info_2")

media_ARPU_3_profili_noteb <- sql("select flg_profilo, 
                                    mean(VAL_ARPU_TEOR_PACK_BASE_REALE) as media_val_arpu_teorico,
                                    mean(VAL_ARPU_NET_DISCOUNT_REALE) as media_val_arpu_net
                            from skyid_tot_info_2
                            group by flg_profilo")
View(head(media_ARPU_3_profili_noteb,100))

##################################################################

write.df(repartition( skyid_tot_info_1, 1), path = "/user/stefano.mazzucca/p2c_nd_target_tot.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
write.parquet(skyid_tot_info_1, "/user/stefano.mazzucca/p2c_nd_base_completa_2.parquet")



###########################################################################################################################################################

digital_conv <- filter(skyid_tot_info, "flg_digital = 1")
View(head(digital_conv, 100))
nrow(digital_conv)
# 18.674

# cookieid_skyid_adform <- read.parquet("/user/stefano.mazzucca/p2c_nd_coo_skyid.parquet")
# View(head(cookieid_skyid_adform,100))
# nrow(cookieid_skyid_adform)
# # 29.518.853
# 
# createOrReplaceTempView(digital_conv, "digital_conv")
# createOrReplaceTempView(cookieid_skyid_adform, "cookieid_skyid_adform")
# 
# join <- sql("select distinct COD_CLIENTE_CIFRATO
#             from digital_conv t1
#             inner join cookieid_skyid_adform t2
#               on t1.COD_CLIENTE_CIFRATO = t2.skyid")
# nrow(join)
# # 11.022

write.parquet(digital_conv, "/user/stefano.mazzucca/p2c_nd_target_digital.parquet")



no_digital_ibridi <- filter(skyid_tot_info, "flg_digital = 0 and min_data_interactions is NOT NULL")
View(head(no_digital_ibridi,100))
nrow(no_digital_ibridi)
# 31.868

write.parquet(no_digital_ibridi, "/user/stefano.mazzucca/p2c_nd_target_no_dig_ibridi.parquet")



no_digital_altri_canali <- filter(skyid_tot_info, "flg_digital = 0 and min_data_interactions is NULL")
View(head(no_digital_altri_canali,100))
nrow(no_digital_altri_canali)
# 78.287

write.parquet(no_digital_altri_canali, "/user/stefano.mazzucca/p2c_nd_target_no_dig_altri_ch.parquet")




verifica <- rbind(digital_conv, no_digital_ibridi, no_digital_altri_canali)
nrow(verifica)
# 128.829 (Ceck OK!)


###########################################################################################################################################################
## 3 profili ## 
###########################################################################################################################################################

digital_conv <- read.parquet("/user/stefano.mazzucca/p2c_nd_target_digital.parquet")
View(head(digital_conv,100))
nrow(digital_conv)
# 18.674

no_digital_ibridi <- read.parquet("/user/stefano.mazzucca/p2c_nd_target_no_dig_ibridi.parquet")
View(head(no_digital_ibridi,100))
nrow(no_digital_ibridi)
# 31.868

no_digital_altri_canali <- read.parquet("/user/stefano.mazzucca/p2c_nd_target_no_dig_altri_ch.parquet")
View(head(no_digital_altri_canali,100))
nrow(no_digital_altri_canali)
# 78.287



###########################################################################################################################################################
## profilo DIGITAL ## 
###########################################################################################################################################################

digital_conv <- read.parquet("/user/stefano.mazzucca/p2c_nd_target_digital.parquet")
View(head(digital_conv,100))
nrow(digital_conv)
# 18.674

# digital_conv <- withColumn(digital_conv, "days_p2c_first_interaction", datediff(digital_conv$data_stipula_dt, digital_conv$min_data_interactions))
# 
# dist_day_first_interaction <- summarize(groupBy(digital_conv, "days_p2c_first_interaction"), count = count(digital_conv$COD_CLIENTE_CIFRATO))
# dist_day_first_interaction_2 <- arrange(dist_day_first_interaction, asc(dist_day_first_interaction$days_p2c_first_interaction))
# View(head(dist_day_first_interaction_2, 10000))
# 
# #write.df(repartition( dist_day_first_interaction_2, 1), path = "/user/stefano.mazzucca/NEW_exp_distribution_first_interaction.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
# 
# createOrReplaceTempView(digital_conv, "digital_conv")
# 
# digital_conv <- sql("select *,
#                               case when days_p2c_first_interaction <= 7 then '<7'
#                               when days_p2c_first_interaction > 7 and days_p2c_first_interaction <= 14 then '<14'
#                               when days_p2c_first_interaction > 14 and days_p2c_first_interaction <= 21 then '<21'
#                               when days_p2c_first_interaction > 21 and days_p2c_first_interaction <= 28 then '<28'
#                               when days_p2c_first_interaction > 28 and days_p2c_first_interaction <= 60 then '<60'
#                               else '>60' end as bin_temp_conv
#                             from digital_conv")
# View(head(digital_conv,100))
# nrow(digital_conv)
# 18.674
# 
# digital_conv <- withColumn(digital_conv, "days_p2c_last_interaction", datediff(digital_conv$data_stipula_dt, digital_conv$max_data_interactions))
# 
# dist_day_last_interaction <- summarize(groupBy(digital_conv, "days_p2c_last_interaction"), count = count(digital_conv$COD_CLIENTE_CIFRATO))
# dist_day_last_interaction_2 <- arrange(dist_day_last_interaction, asc(dist_day_last_interaction$days_p2c_last_interaction))
# View(head(dist_day_last_interaction_2, 10000))
# 
# #write.df(repartition( dist_day_last_interaction_2, 1), path = "/user/stefano.mazzucca/NEW_exp_distribution_last_interaction.csv", "csv", sep=";", mode = "overwrite", header=TRUE)

write.df(repartition( digital_conv, 1), path = "/user/stefano.mazzucca/p2c_nd_target_digital.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



digital_conv$bin_temp_conv_def <- ifelse(digital_conv$bin_temp_conv == '>60' & isNull(digital_conv$days_p2c_first_interaction) == TRUE, 
                                         'NA', digital_conv$bin_temp_conv)

distr_bin_temp <- summarize(groupBy(digital_conv, "bin_temp_conv_def"), count = count(digital_conv$COD_CLIENTE_CIFRATO))
distr_bin_temp_2 <- arrange(distr_bin_temp, "bin_temp_conv_def")
View(head(distr_bin_temp_2,100))


distr_weekFY_conv <- summarize(groupBy(digital_conv, "num_week", "bin_temp_conv_def"), count_weekFY = count(digital_conv$COD_CLIENTE_CIFRATO))
distr_weekFY_conv_2 <- arrange(distr_weekFY_conv, asc(distr_weekFY_conv$num_week), desc(distr_weekFY_conv$bin_temp_conv_def))
View(head(distr_weekFY_conv_2,100))



digital_conv$bin_interactions_def <- ifelse(digital_conv$bin_interactions == '<10' & isNull(digital_conv$min_date_imp) == TRUE, 
                                         'NA', digital_conv$bin_interactions)

distr_weekFY_conv <- summarize(groupBy(digital_conv, "num_week", "bin_interactions_def"), count_weekFY = count(digital_conv$COD_CLIENTE_CIFRATO))
distr_weekFY_conv_2 <- arrange(distr_weekFY_conv, asc(distr_weekFY_conv$num_week), desc(distr_weekFY_conv$bin_interactions_def))
View(head(distr_weekFY_conv_2,1000))


distr_bin_temp <- summarize(groupBy(digital_conv, "bin_interactions_def"), count = count(digital_conv$COD_CLIENTE_CIFRATO))
distr_bin_temp_2 <- arrange(distr_bin_temp, "bin_interactions_def")
View(head(distr_bin_temp_2,100))


distr_week_FY <- summarize(groupBy(digital_conv, "num_week"), count = count(digital_conv$COD_CLIENTE_CIFRATO))
distr_week_FY_2 <- arrange(distr_week_FY, "num_week")
View(head(distr_week_FY_2,100))







distr_channel <- summarize(groupBy(digital_conv, "DES_CATEGORIA_CANALE"), count = count(digital_conv$COD_CLIENTE_CIFRATO))
#distr_channel_2 <- arrange(distr_channel, "DES_CANALE_VENDITA")
View(head(distr_channel,100))


distr_eta <- summarize(groupBy(digital_conv, "FASCIA_ETA"), count = count(digital_conv$COD_CLIENTE_CIFRATO))
distr_eta_2 <- arrange(distr_eta, "FASCIA_ETA")
View(head(distr_eta_2,100))


distr_area <- summarize(groupBy(digital_conv, "DES_AREA_NIELSEN"), count = count(digital_conv$COD_CLIENTE_CIFRATO))
View(head(distr_area,100))


distr_regione <- summarize(groupBy(digital_conv, "DES_NOME_REG"), count = count(digital_conv$COD_CLIENTE_CIFRATO))
View(head(distr_regione,100))



###########################################################################################################################################################
## profilo NO DIGITAL ALTRI CANALI ## 
###########################################################################################################################################################

no_digital_altri_canali <- read.parquet("/user/stefano.mazzucca/p2c_nd_target_no_dig_altri_ch.parquet")
View(head(no_digital_altri_canali,100))
nrow(no_digital_altri_canali)
# 78.287

write.df(repartition( no_digital_altri_canali, 1), path = "/user/stefano.mazzucca/p2c_nd_target_no_digital_altro.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



distr_week_FY <- summarize(groupBy(no_digital_altri_canali, "num_week"), count = count(no_digital_altri_canali$COD_CLIENTE_CIFRATO))
distr_week_FY_2 <- arrange(distr_week_FY, "num_week")
View(head(distr_week_FY_2,100))





distr_channel <- summarize(groupBy(no_digital_altri_canali, "DES_CATEGORIA_CANALE"), count = count(no_digital_altri_canali$COD_CLIENTE_CIFRATO))
#distr_channel_2 <- arrange(distr_channel, "DES_CANALE_VENDITA")
View(head(distr_channel,100))


distr_eta <- summarize(groupBy(no_digital_altri_canali, "FASCIA_ETA"), count = count(no_digital_altri_canali$COD_CLIENTE_CIFRATO))
distr_eta_2 <- arrange(distr_eta, "FASCIA_ETA")
View(head(distr_eta_2,100))


distr_area <- summarize(groupBy(no_digital_altri_canali, "DES_AREA_NIELSEN"), count = count(no_digital_altri_canali$COD_CLIENTE_CIFRATO))
View(head(distr_area,100))


distr_regione <- summarize(groupBy(no_digital_altri_canali, "DES_NOME_REG"), count = count(no_digital_altri_canali$COD_CLIENTE_CIFRATO))
View(head(distr_regione,100))








#chiudi sessione
sparkR.stop()
