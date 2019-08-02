
## Impatto ADV su vendite offline 2


# Distribuzione lunghezza del path2conversion NON digitale
# Gap tra ultimo ADV digitale viz e conversione
# Definire intensita' per interazioni (impressions & clicks) e capire la distribuzione


#apri sessione
source("connection_R.R")
options(scipen = 1000)


conv_no_dig_day <- read.df("/user/stefano.mazzucca/conversion_no_digital_by_day.csv", source = "csv", header = "true", delimiter = ";")
View(head(conv_no_dig_day,100))
nrow(conv_no_dig_day)
# 31.794

conv_no_dig_day_1 <- withColumn(conv_no_dig_day, "data_prima_attiv_cntr_dt", cast(conv_no_dig_day$data_prima_attiv_cntr_dt, 'date'))
conv_no_dig_day_2 <- withColumn(conv_no_dig_day_1, "imp_prima_data_nav", cast(conv_no_dig_day_1$imp_prima_data_nav, 'date'))
conv_no_dig_day_3 <- withColumn(conv_no_dig_day_2, "clic_prima_data_nav", cast(conv_no_dig_day_2$clic_prima_data_nav, 'date'))
conv_no_dig_day_4 <- withColumn(conv_no_dig_day_3, "imp_ultima_data_nav", cast(conv_no_dig_day_3$imp_ultima_data_nav, 'date'))
conv_no_dig_day_5 <- withColumn(conv_no_dig_day_4, "clic_ultima_data_nav", cast(conv_no_dig_day_4$clic_ultima_data_nav, 'date'))
conv_no_dig_day_6 <- withColumn(conv_no_dig_day_5, "prima_data_interactions", cast(conv_no_dig_day_5$prima_data_interactions, 'date'))
conv_no_dig_day_7 <- withColumn(conv_no_dig_day_6, "ultima_data_interactions", cast(conv_no_dig_day_6$ultima_data_interactions, 'date'))

View(head(conv_no_dig_day_7,100))
nrow(conv_no_dig_day_7)
# 31.794


## Distribuzione lunghezza P2C non digitale: prima_data_interactions -> data_prima_attiv_cntr_dt #################################################################################

p2c_no_digital <- withColumn(conv_no_dig_day_7, "days_p2c_first_interaction", datediff(conv_no_dig_day_7$data_prima_attiv_cntr_dt, conv_no_dig_day_7$prima_data_interactions))

View(head(p2c_no_digital,100))


## Distribuzione lunghezza P2C non digitale: clic_prima_data_nav -> data_prima_attiv_cntr_dt #################################################################################

p2c_no_digital_1 <- withColumn(p2c_no_digital, "days_p2c_first_clic", datediff(p2c_no_digital$data_prima_attiv_cntr_dt, p2c_no_digital$clic_prima_data_nav))

View(head(p2c_no_digital_1,100))


## Distribuzione lunghezza P2C non digitale: imp_prima_data_nav -> data_prima_attiv_cntr_dt #################################################################################

p2c_no_digital_2 <- withColumn(p2c_no_digital_1, "days_p2c_first_imp", datediff(p2c_no_digital_1$data_prima_attiv_cntr_dt, p2c_no_digital_1$imp_prima_data_nav))

View(head(p2c_no_digital_2,100))


## Distribuzione lunghezza P2C non digitale: ultima_data_interactions -> data_prima_attiv_cntr_dt #################################################################################

p2c_no_digital_3 <- withColumn(p2c_no_digital_2, "days_p2c_last_interaction", datediff(p2c_no_digital_2$data_prima_attiv_cntr_dt, p2c_no_digital_2$ultima_data_interactions))

View(head(p2c_no_digital_3,100))


## Distribuzione lunghezza P2C non digitale: clic_ultima_data_nav -> data_prima_attiv_cntr_dt #################################################################################

p2c_no_digital_4 <- withColumn(p2c_no_digital_3, "days_p2c_last_clic", datediff(p2c_no_digital_3$data_prima_attiv_cntr_dt, p2c_no_digital_3$clic_ultima_data_nav))

View(head(p2c_no_digital_4,100))


## Distribuzione lunghezza P2C non digitale: imp_ultima_data_nav -> data_prima_attiv_cntr_dt #################################################################################

p2c_no_digital_5 <- withColumn(p2c_no_digital_4, "days_p2c_last_imp", datediff(p2c_no_digital_4$data_prima_attiv_cntr_dt, p2c_no_digital_4$imp_ultima_data_nav))

View(head(p2c_no_digital_5,100))



write.parquet(p2c_no_digital_5, "/user/stefano.mazzucca/p2c_no_digital_by_day.parquet")
write.df(repartition( p2c_no_digital_5, 1), path = "/user/stefano.mazzucca/p2c_no_digital_by_day.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





p2c_no_digital <- read.parquet("/user/stefano.mazzucca/p2c_no_digital_by_day.parquet")
View(head(p2c_no_digital,100))
nrow(p2c_no_digital)
# 31.794


## Altre elaborazioni #########################################################################################################################################################

p2c_no_digital_wd <- withColumn(p2c_no_digital, "weekday_attiv_cntr", date_format(p2c_no_digital$data_prima_attiv_cntr_dt, 'EEEE'))
View(head(p2c_no_digital_wd,100))
nrow(p2c_no_digital_wd)
# 31.794

createOrReplaceTempView(p2c_no_digital_wd, "p2c_no_digital_wd")

p2c_no_digital_wd_1 <- sql("select *,
                              case when days_p2c_first_interaction <= 7 then '<7'
                              when days_p2c_first_interaction > 7 and days_p2c_first_interaction <= 14 then '<14'
                              when days_p2c_first_interaction > 14 and days_p2c_first_interaction <= 21 then '<21'
                              when days_p2c_first_interaction > 21 and days_p2c_first_interaction <= 28 then '<28'
                              when days_p2c_first_interaction > 28 and days_p2c_first_interaction <= 60 then '<60'
                              else '>60' end as bin_temp_conv
                           from p2c_no_digital_wd")
View(head(p2c_no_digital_wd_1,100))
nrow(p2c_no_digital_wd_1)
# 31.794

distr_bin_temp_conv <- summarize(groupBy(p2c_no_digital_wd_1, "bin_temp_conv"), count_bin = count(p2c_no_digital_wd_1$skyid))
View(distr_bin_temp_conv)



p2c_no_digital_wd_1$week_FY <- ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-02-26', 'W35', 
                                      ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-02-19', 'W34',
                                             ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-02-12', 'W33',
                                                    ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-02-05', 'W32',
                                                           ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-01-29', 'W31',
                                                                  ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-01-22', 'W30',
                                                                         ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-01-15', 'W29',
                                                                                ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-01-08', 'W28',
                                                                                       ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-01-01', 'W27',
                                                                                              ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2017-12-25', 'W26',
                                                                                                     ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2017-12-18', 'W25',
                                                                                                            ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2017-12-11', 'W24',
                                                                                                                   ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2017-12-04', 'W23',
                                                                                                                          ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2017-12-01', 'W22', 
                                                                                                                                 'undefined'))))))))))))))

View(head(p2c_no_digital_wd_1,100))
nrow(p2c_no_digital_wd_1)
# 31.794

# valdist_skyid_data <- distinct(select(p2c_no_digital_wd_1, "skyid", "data_prima_attiv_cntr_dt"))
# nrow(valdist_skyid_data)
# # 31.794


write.df(repartition( p2c_no_digital_wd_1, 1), path = "/user/stefano.mazzucca/p2c_no_digital_weekFY.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



## weekFY / bin_temp_conv

distr_weekFY_conv <- summarize(groupBy(p2c_no_digital_wd_1, "week_FY", "bin_temp_conv"), count_weekFY = count(p2c_no_digital_wd_1$skyid))
View(head(distr_weekFY_conv,100))


## volume interazioni pre_conversione / weekFY

distr_interactions <- summarize(groupBy(p2c_no_digital_wd_1, "week_FY"), 
                                media_interactions = mean(p2c_no_digital_wd_1$num_tot_interactions))
View(head(distr_interactions,100))
#  mediana_interactions = median(p2c_no_digital_wd_1$num_tot_interactions)





#################################################################################################################################################################################
## Plot distribuzioni su ggplot2 ###############################################################################################################################################
#################################################################################################################################################################################

library(ggplot2)
library(gridExtra)

test_min_first_clic <- summarize(p2c_no_digital, min_first_clic = min(p2c_no_digital$days_p2c_first_clic))
View(head(test_min_first_clic))
# -62
test_min_first_clic_2 <- filter(p2c_no_digital, "days_p2c_first_clic < 0")
View(head(test_min_first_clic_2,100))
nrow(test_min_first_clic_2)
# 7 (Probabilmente questi 7 casi sono "doppioni" in cui la seconda data di conversion porta con se navigazioni post-data di attivazione!)


p2c_no_digital_2 <- filter(p2c_no_digital, "days_p2c_last_interaction >= 0")
nrow(p2c_no_digital_2)
# 31.781 (13 casi...)
tbl_plot <- as.data.frame(p2c_no_digital_2)
View(head(tbl_plot,100))
nrow(tbl_plot)
# 31.781


p2c_no_digital_3 <- filter(p2c_no_digital, "days_p2c_first_clic is NOT NULL and days_p2c_first_clic >= 0
                                            and days_p2c_last_interaction >= 0 ")
nrow(p2c_no_digital_3)
# 10.379
tbl_plot_2 <- as.data.frame(p2c_no_digital_3)
View(head(tbl_plot_2,100))
nrow(tbl_plot_2)
# 10.379


## Plot distribution ###########################################################################################################################################################

g1 <- ggplot(tbl_plot, aes(x = reorder(seq(1, length(days_p2c_first_interaction)), days_p2c_first_interaction), y = days_p2c_first_interaction)) +
        geom_point(color = "red", size = .1) +
        coord_flip() +
        theme(axis.text.y = element_blank()) +
        labs(title = "days_p2c_first_interaction",
             x = "numero di conversioni (31k)",
             y = "days_from_first_interaction")

g2 <- ggplot(tbl_plot_2, aes(x = reorder(seq(1, length(days_p2c_first_clic)), days_p2c_first_clic), y = days_p2c_first_clic)) +
        geom_point(color = "green", size = .1) +
        coord_flip() +
        theme(axis.text.y = element_blank()) +
        labs(title = "days_p2c_first_clic",
             x = "numero di conversioni (10k)",
             y = "days_from_first_clic")

grid.arrange(g1, g2, ncol=1, nrow =2)


g3 <- ggplot(tbl_plot, aes(x = reorder(seq(1, length(days_p2c_last_interaction)), days_p2c_last_interaction), y = days_p2c_last_interaction)) +
        geom_point(color = "red", size = .1) +
        coord_flip() +
        theme(axis.text.y = element_blank()) +
        labs(title = "days_p2c_last_interaction",
             x = "numero di conversioni (31k)",
             y = "days_from_last_interaction")

g4 <- ggplot(tbl_plot_2, aes(x = reorder(seq(1, length(days_p2c_last_clic)), days_p2c_last_clic), y = days_p2c_last_clic)) +
        geom_point(color = "green", size = .1) +
        coord_flip() +
        theme(axis.text.y = element_blank()) +
        labs(title = "days_p2c_last_clic",
             x = "numero di conversioni (10k)",
             y = "days_from_last_clic")

grid.arrange(g3, g4, ncol=1, nrow =2)







## export per report #########################################################################################################################################################

p2c_no_digital_completo_def <- read.parquet("/user/stefano.mazzucca/p2c_no_digital_def.parquet")


prova <- summarize(groupBy(p2c_no_digital_completo_def, "days_p2c_last_interaction"), count = count(p2c_no_digital_completo_def$skyid))
prova_2 <- arrange(prova, asc(prova$days_p2c_last_interaction))
View(head(prova_2, 10000))

write.df(repartition( prova_2, 1), path = "/user/stefano.mazzucca/p2c_last_int.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



test_min_max <- summarize(p2c_no_digital_5, min_attiv = min(p2c_no_digital_5$data_prima_attiv_cntr_dt), max_attiv = max(p2c_no_digital_5$data_prima_attiv_cntr_dt))
View(test_min_max)

###############################################################################################################################################################################






#chiudi sessione
sparkR.stop()