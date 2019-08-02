
## Refresh analisi sui clienti pdisc 
## nel periodo post-invio della disdetta


source("connection_R.R")
options(scipen = 10000)




navigazione_pdisc <- read.parquet("/user/valentina/20180928_AnalisiPdisc_ScaricoNavigaz_SkyItDev_noFilter.parquet")
View(head(navigazione_pdisc,100))
nrow(navigazione_pdisc)
# 82.819.808

# WEB: 
recupera_cookies_navigati <- read.parquet("/user/jacopoa/recupera_cookies_navigati_pdisc.parquet")
View(head(recupera_cookies_navigati,100))
# APP: 
recupera_cookies_appwsc_navigati <- read.parquet("/user/jacopoa/recupera_cookies_appwsc_navigati.parquet")
View(head(recupera_cookies_appwsc_navigati,100))




prova <- read.parquet("/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_corporate_20181015.parquet")
View(head(prova,100))




#chiudi sessione
sparkR.stop()
