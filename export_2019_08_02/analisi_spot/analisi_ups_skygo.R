
## Analisi spot upselling e skygo (Silvia)

source("connection_R.R")
options(scipen = 10000)



path_upselling <- "/user/stefano.mazzucca/up_post_disservizio_crm/20181106_2_estrazione upselling fy18_19.txt"

ups_tot <- read.df(path_upselling, source = "csv", header = "true", delimiter = "$")
ups_tot_1 <- withColumn(ups_tot, "dat_attivazione_dt", cast(cast(unix_timestamp(ups_tot$dat_attivazione, 'dd/MM/yyyy'), 'timestamp'), 'date'))



ups_last <- filter(ups_tot_1, ups_tot_1$dat_attivazione_dt >= '2018-07-01' & ups_tot_1$dat_attivazione_dt <= '2018-10-31')
nrow(ups_last)
# 1.221.496

ups_last_2 <- filter(ups_last, prova$flg_upgrade == 1)
nrow(ups_last_2)
# 571.383

ups_last_nodigital <- filter(ups_last_2, ups_last_2$des_canale_contatto != 'WEB' &
                               ups_last_2$des_canale_contatto != 'WEBMAIL' &
                               ups_last_2$des_canale_contatto != 'MOBILE' &
                               ups_last_2$des_canale_contatto != 'CHAT' &
                               ups_last_2$des_canale_contatto != 'IVR-SELFCARE' &
                               ups_last_2$des_canale_contatto != 'STB')
nrow(ups_last_nodigital)
# 388.279


write.df(repartition( ups_last_nodigital, 1), path = "/user/stefano.mazzucca/export_upsel_nodigital_lug_ott_18.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


