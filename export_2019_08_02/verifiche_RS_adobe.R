
## TEST Adobe RS ## 


source("connection_R.R")
options(scipen = 10000)


prova_appriver <- read.parquet("/STAGE/adobe/reportsuite=skyappriver.prod")
prova_appriver <- filter(prova_appriver, prova_appriver$date >= '20190101')
View(head(prova_appriver,100))
nrow(prova_appriver) # 160
# NOWTV (web, ps4, smarttv)


prova_nowtvprod <- read.parquet("/STAGE/adobe/reportsuite=skyskynowtvitaprod")
prova_nowtvprod <- select(prova_nowtvprod, "page_url_post_prop", "mapped_browser_type_value", "mapped_os_value", "mapped_browser_value", "date")
prova_nowtvprod <- filter(prova_nowtvprod, prova_nowtvprod$date >= '20190101')
View(head(prova_nowtvprod,100))
nrow(prova_nowtvprod) # 14.463
# NOWTV (smrttv)



#chiudi sessione
sparkR.stop()


# prova <- read.parquet("/user/stefano.mazzucca/churn_digital_ver/2018_11/nav_sito_181101.parquet")
# View(head(prova,100))
# 
# prova_filt <- filter(prova, prova$terzo_livello_post_prop == 'faidate fatture pagamenti')
# View(head(prova_filt,100))
# tt <- summarize(groupBy(prova_filt, "terzo_livello_post_prop"), count = countDistinct(prova_filt$page_url_post_evar))
# View(head(tt,100))
# tt2 <- summarize(groupBy(prova_filt, "terzo_livello_post_prop"), count = countDistinct(prova_filt$page_name_post_evar))
# View(head(tt2,100))


prova <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
prova2 <- select(prova, "post_page_event", "post_page_event_value",
                 "videos_post_evar",
                 "video_domain_post_evar",
                 "video_url_post_evar",
                 "video_lenght_post_evar",
                 "canali_video_post_evar",
                 "sottocanali_video_post_evar",
                 "terzo_livello_video_post_evar",
                 "post_evar88" # tipologia player video
                 # eventi legati alla visione del video: 
                 # event76 (25% fruizione), event79 (50% fruizione), event74 (75% fruizione) e event73 (fruizione completa)
                 # event142 (secondi spesi sul video) SOLO PER VIDEO FLUID
                 # event72 (video time viewd)secondi spedi sul video) PER I VIDEO jwp SUI SITI SKY
                 )
View(head(prova2,100))



kids <- read.parquet("/STAGE/adobe/reportsuite=skyappkids.prod")
View(head(kids,100))
nrow(kids)
# 129.776.637

test_data <- summarize(kids, min_data = min(kids$date), max_data = max(kids$date))
View(head(test_data,100))
# min_data    max_data
# 20160826    20190311



prova <- read.df("/user/stefano.mazzucca/20190211_richiesta dati per analisi comportamentale CB.csv", source = "csv", header = "true", delimiter = "|")
View(head(prova,100))


