
## Impatto ADV su vendite offline 4


## Comprendere la distribuzione delle conversion offline degli utenti TRY & BUY



#apri sessione
source("connection_R.R")
options(scipen = 1000)


# prova <- read.df("/user/stefano.mazzucca/p2c_no_digital_def.csv", source = "csv", header = "true", delimiter = ";")
# prova_2 <- arrange(prova, "skyid")
# View(head(prova_2, 100))
# nrow(prova_2)
# # 31.773

p2c_no_digital_completo_def <- read.parquet("/user/stefano.mazzucca/p2c_no_digital_def.parquet")
#p2c_no_digital_completo_def_2 <- arrange(p2c_no_digital_completo_def, "skyid")
View(head(p2c_no_digital_completo_def,100))
nrow(p2c_no_digital_completo_def)
# 31.773


filter_tby <- filter(p2c_no_digital_completo_def, "FLG_TBY = 1 or FLG_TBY = 0")
View(head(filter_tby,100))
nrow(filter_tby)
# 6.237


export_filter_tby <- select(filter_tby, "skyid", "days_p2c_first_interaction", "days_p2c_last_interaction")
#write.df(repartition( export_filter_tby, 1), path = "/user/stefano.mazzucca/exp_filter_tby_first_last_interaction.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



dist_day_first_interaction <- summarize(groupBy(filter_tby, "days_p2c_first_interaction"), count = count(filter_tby$skyid))
dist_day_first_interaction_2 <- arrange(dist_day_first_interaction, asc(dist_day_first_interaction$days_p2c_first_interaction))
View(head(dist_day_first_interaction_2, 10000))

#write.df(repartition( dist_day_first_interaction_2, 1), path = "/user/stefano.mazzucca/exp_filter_tby_distribution_first_interaction.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



dist_day_last_interaction <- summarize(groupBy(filter_tby, "days_p2c_last_interaction"), count = count(filter_tby$skyid))
dist_day_last_interaction_2 <- arrange(dist_day_last_interaction, asc(dist_day_last_interaction$days_p2c_last_interaction))
View(head(dist_day_last_interaction_2, 10000))

#write.df(repartition( dist_day_last_interaction_2, 1), path = "/user/stefano.mazzucca/exp_filter_tby_distribution_last_interaction.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




distr_weekFY_conv <- summarize(groupBy(filter_tby, "week_FY", "bin_temp_conv"), count_weekFY = count(filter_tby$skyid))
distr_weekFY_conv_2 <- arrange(distr_weekFY_conv, asc(distr_weekFY_conv$week_FY), desc(distr_weekFY_conv$bin_temp_conv))
View(head(distr_weekFY_conv_2,100))



distr_bin_temp <- summarize(groupBy(filter_tby, "bin_temp_conv"), count = count(filter_tby$skyid))
distr_bin_temp_2 <- arrange(distr_bin_temp, "bin_temp_conv")
View(head(distr_bin_temp_2,100))



distr_week_FY <- summarize(groupBy(filter_tby, "week_FY"), count = count(filter_tby$skyid))
distr_week_FY_2 <- arrange(distr_week_FY, "week_FY")
View(head(distr_week_FY_2,100))



distr_channel <- summarize(groupBy(filter_tby, "DES_CANALE_VENDITA"), count = count(filter_tby$skyid))
#distr_channel_2 <- arrange(distr_channel, "DES_CANALE_VENDITA")
View(head(distr_channel,100))


distr_interazioni <- summarize(groupBy(filter_tby, "bin_interactions"), count = count(filter_tby$skyid))
#distr_interazioni_2 <- arrange(distr_interazioni, "bin_interactions")
View(head(distr_interazioni,100))










#chiudi sessione
sparkR.stop()
