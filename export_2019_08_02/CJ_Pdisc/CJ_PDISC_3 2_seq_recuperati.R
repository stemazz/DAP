


#apri sessione
source("connection_R.R")
options(scipen = 1000)




seq_post_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_seq_nav_post_pdisc.parquet")
View(head(seq_post_pdisc,100))
nrow(seq_post_pdisc)
# 944.075

valdist_ext_id <- distinct(select(seq_post_pdisc, "COD_CLIENTE_CIFRATO"))
View(head(valdist_ext_id,100))
nrow(valdist_ext_id)
# 14.449


createOrReplaceTempView(seq_post_pdisc, "seq_post_pdisc")

test <- sql("select min(date_time_dt) as min_data, max(date_time_dt) as max_data
            from seq_post_pdisc")
View(head(test,1000))


valdist_app_ext_id <- filter(seq_post_pdisc, "canale like'%app%'")
valdist_app_ext_id_2 <- distinct(select(valdist_app_ext_id, "COD_CLIENTE_CIFRATO"))
nrow(valdist_app_ext_id_2)
# 6.095

valdist_wsc_ext_id <- filter(seq_post_pdisc, "canale like'%sito_wsc%'")
valdist_wsc_ext_id_2 <- distinct(select(valdist_wsc_ext_id, "COD_CLIENTE_CIFRATO"))
nrow(valdist_wsc_ext_id_2)
# 8.612

valdist_pubb_ext_id <- filter(seq_post_pdisc, "canale like'%sito_pubblico%'")
valdist_pubb_ext_id_2 <- distinct(select(valdist_pubb_ext_id, "COD_CLIENTE_CIFRATO"))
nrow(valdist_pubb_ext_id_2)
# 12.397









#chiudi sessione
sparkR.stop()