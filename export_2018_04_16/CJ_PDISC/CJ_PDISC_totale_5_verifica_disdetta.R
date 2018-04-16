
# CJ_PDISC_totale
# Verifica su "DISDETTA"

#apri sessione
source("connection_R.R")
options(scipen = 1000)



# http://www.sky.it/assistenza/conosci/informazioni-sull-abbonamento/termini-e-condizioni/come-posso-dare-la-disdetta-del-mio-contratto.html





CJ_PDISC_pag_sito <- read.parquet("/user/valentina/CJ_PDISC_scarico_pag_sito.parquet")
View(head(CJ_PDISC_pag_sito,100))
nrow(CJ_PDISC_pag_sito)
# 4.492.336


ver_disdetta_1 <- filter(CJ_PDISC_pag_sito, "page_url_post_evar LIKE '%disdetta%'")
nrow(ver_disdetta_1)
# 76.438

valdist_ver_disdetta <- distinct(select(ver_disdetta_1, "page_url_post_evar", "secondo_livello_post_evar", "terzo_livello_post_evar"))
View(head(valdist_ver_disdetta,1000))





CJ_PDISC_pag_sito_2 <-withColumn(CJ_PDISC_pag_sito, "up", upper(CJ_PDISC_pag_sito$page_url_post_evar))
View(head(CJ_PDISC_pag_sito_2,1000))

filt_assist_ricerca <- filter(CJ_PDISC_pag_sito_2, "terzo_livello_post_evar LIKE '%ricerca%' and secondo_livello_post_evar LIKE '%assistenza%'")
View(head(filt_assist_ricerca,1000))
nrow(filt_assist_ricerca)
# 10.204

ver_disdetta_2 <- filter(filt_assist_ricerca, "up LIKE '%DISDETTA%'")
nrow(ver_disdetta_2)
# 2.044


valdist_cl <- distinct(select(CJ_PDISC_pag_sito_2, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl)
# 22.045


CJ_PDISC_pag_sito_3 <- withColumn(CJ_PDISC_pag_sito_2, "dat_pdisc_meno_31gg", date_sub(CJ_PDISC_pag_sito_2$data_ingr_pdisc_formdate,31))
CJ_PDISC_pag_sito_3$flg_31gg <- ifelse(CJ_PDISC_pag_sito_3$date_time_dt >= CJ_PDISC_pag_sito_3$dat_pdisc_meno_31gg,1,0)

View(head(CJ_PDISC_pag_sito_3,1000))


CJ_PDISC_pag_sito_4 <- filter(CJ_PDISC_pag_sito_3, "flg_31gg = 1")

valdist_cl_last_month <- distinct(select(CJ_PDISC_pag_sito_4, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_last_month)
# 17.503



ver_disdetta_3 <- filter(CJ_PDISC_pag_sito_4, "terzo_livello_post_evar LIKE '%ricerca%' and secondo_livello_post_evar LIKE '%assistenza%'")
cl <- distinct(select(ver_disdetta_3, "COD_CLIENTE_CIFRATO"))
nrow(cl)
# 1.813

ver_disdetta_4 <- filter(ver_disdetta_3, "up LIKE '%DISDETTA%'")
cl_2 <- distinct(select(ver_disdetta_4, "COD_CLIENTE_CIFRATO"))
nrow(cl_2)
# 836



ver_disdetta_5 <- filter(CJ_PDISC_pag_sito_4, "page_url_post_evar LIKE 
                         '%assistenza/conosci/informazioni-sull-abbonamento/termini-e-condizioni/come-posso-dare-la-disdetta-del-mio-contratto.html%'")
cl_3 <- distinct(select(ver_disdetta_5, "COD_CLIENTE_CIFRATO"))
nrow(cl_3)
# 2.967


