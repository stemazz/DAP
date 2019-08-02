
## CB per kpi digitalizzazione

source("connection_R.R")
options(scipen = 10000)


cb_digit <- read.df("/user/stefano.mazzucca/20190211_richiesta dati per analisi comportamentale CB.csv", source = "csv", header = "true", delimiter = "|")
View(head(cb_digit,100))
nrow(cb_digit)
# 4.613.306
printSchema(cb_digit)
# |-- cod_contratto
# cod_cliente_cifrato
# dat_prima_attivazione
# des_stato_cntr
# des_tipo_pag
# des_area_nielsen
# des_mod_srv
# des_nome_prov
# des_nome_reg
# flg_bb
# flg_calcio
# flg_cinema
# flg_sport
# flg_hd_pack
# flg_sky_famiglia
# flg_skyq_black
# flg_sky_q_plus
# flg_vod
# flg_skygo
# flg_skygo_plus
# flg_iscrizione
# dat_iscrizione
# flg_iscr_web_selfcare
# dat_nascita
# flg_sesso
# promo_ingresso
# des_categoria_canale
# des_nom_can_padre
# des_nome_canale
# des_nome_qual
# des_apparato_stb
# num_pdisc_1_mesi
# num_pdisc_3_mesi
# num_pdisc_6_mesi
# num_downgrade_1_mese
# num_downgrade_3_mese
# num_downgrade_6_mese
# num_upgrade_1_mese
# num_upgrade_3_mese
# num_upgrade_6_mese
# |-- num_equal_1_mese
# |-- num_equal_3_mese
# |-- num_equal_6_mese




#chiudi sessione
sparkR.stop()
