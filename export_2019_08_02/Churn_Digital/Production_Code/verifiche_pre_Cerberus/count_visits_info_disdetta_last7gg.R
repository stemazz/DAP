
## count visits su "info disdetta"

source("connection_R.R")
options(scipen = 10000)

data_7gg <- as.Date("2018-12-09") # data uguale a 7 gg prima della data in cui gira il CDM
path_output_cdm <- "/user/stefano.mazzucca/churn_digital_ver/ver_csv_181216_2.csv"
path_nav <- "/user/stefano.mazzucca/churn_digital_ver/2018_12/nav_sito_181216.parquet"

path_csv_finale <- "/user/stefano.mazzucca/churn_digital_ver/ver_csv_181216_with_count_infodisdetta.csv"


cb_sky <- read.df(path_output_cdm, source = "csv", header = "true", delimiter = ";")
# View(head(cb_sky,100))
# nrow(cb_sky)
# # 3.910.288

nav_sito <- read.parquet(path_nav)
# View(head(nav_sito,100))
# nrow(nav_sito)
# # 49.094.254
nav_info_disdetta <- filter(nav_sito, nav_sito$terzo_livello_post_prop == "info disdetta")
nav_info_disdetta_2 <- filter(nav_info_disdetta, nav_info_disdetta$date_time_dt >= data_7gg)
# View(head(nav_info_disdetta_2,100))
# nrow(nav_info_disdetta_2)
# # 15.160
count_visit_info_disdetta <- summarize(groupBy(nav_info_disdetta_2, "COD_CLIENTE_CIFRATO", "DAT_PRIMA_ATTIV_CNTR_dt"), 
                                       count_visits_7gg = countDistinct(concat(nav_info_disdetta_2$post_visid_concatenated, nav_info_disdetta_2$visit_num)))
# View(head(count_visit_info_disdetta,100))
# nrow(count_visit_info_disdetta)
# # 6.889 

createOrReplaceTempView(cb_sky, "cb_sky")
createOrReplaceTempView(count_visit_info_disdetta, "count_visit_info_disdetta")

ver_cb_sky <- sql("select distinct t1.*, t2.count_visits_7gg
                  from cb_sky t1
                  left join count_visit_info_disdetta t2
                    on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and 
                      t1.DAT_PRIMA_ATTIV_CNTR_dt = t2.DAT_PRIMA_ATTIV_CNTR_dt")
# View(head(ver_cb_sky,100))
# nrow(ver_cb_sky)
# # 3.910.288

ver_cb_sky$count_visits_7gg <- ifelse(isNull(ver_cb_sky$count_visits_7gg), 0, ver_cb_sky$count_visits_7gg)

## WARNING WARNING WARNING:
## Eseguire il comdnado successivo solo per i modelli che girano nella seconda meta' del mese xxxx_xx_16
## inserisco count_visits_7gg = 0 per i clienti che hanno gia' inviato disdetta 
ver_cb_sky$count_visits_7gg <- ifelse(isNotNull(ver_cb_sky$pdisc1), 0, ver_cb_sky$count_visits_7gg)

write.df(repartition( ver_cb_sky, 1), path = path_csv_finale, "csv", sep=";", mode = "overwrite", header=TRUE)


prova <- read.df(path_csv_finale, source = "csv", header = TRUE, delimiter = ";")
View(head(prova,100))
nrow(prova)
prova2 <- filter(prova, prova$count_visits_7gg != 0)
View(head(prova2,100))
nrow(prova2)


