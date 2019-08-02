#TVSamsung_DMP --CLEAR

cm_dmp <- read.df("/CLEAR/STAGE/CMDM/DettaglioDMPDataWeb/full/fat_dmp_interessi_l3l4l5_dett.parquet", "parquet")
#View(head(cm_dmp))


cm_dmp_interessi <- read.df("/CLEAR/STAGE/CMDM/DettaglioDMPDataWeb/full/dim_dmp_interessi.parquet", "parquet")
#View(head(cm_dmp_interessi))

filter_interessi <- filter(cm_dmp_interessi, cm_dmp_interessi$DES_INTERESSE_LIVELLO_4=="apparecchi TV" | 
                             cm_dmp_interessi$DES_INTERESSE_LIVELLO_4=="TV set" | #corrispettivo inglese di "apparecchi TV"
                             cm_dmp_interessi$DES_INTERESSE_LIVELLO_4=="home theater" | 
                             cm_dmp_interessi$DES_INTERESSE_LIVELLO_4=="home video/DVD")
#View(head(filter_interessi))


filter_interessi <- withColumnRenamed(filter_interessi, "COD_INTERESSE", "CODICE_INTERESSE")

list_cb_tech_dmp <- join(cm_dmp, filter_interessi, cm_dmp$COD_INTERESSE == filter_interessi$CODICE_INTERESSE)
#View(head(list_cb_tech_dmp))
#nrow(list_cb_tech_dmp)
#[1] 48.813

distval_cntr_tech_dmp <- distinct(select(list_cb_tech_dmp, "COD_CONTRATTO"))
#nrow(distval_cntr_tech_dmp)
#[1] 38.855

#printSchema(list_cb_tech_dmp)

list_cb_tech_dmp_DAT <- withColumn(list_cb_tech_dmp,"DAT_RILEVAZIONE",cast(cast(unix_timestamp(list_cb_tech_dmp$DAT_RILEVAZIONE, 'dd/MM/yyyy'), 'timestamp'), 'date'))
list_cb_tech_dmp_DAT <- withColumn(list_cb_tech_dmp_DAT,"DAT_VISUALIZZAZIONE",cast(cast(unix_timestamp(list_cb_tech_dmp_DAT$DAT_VISUALIZZAZIONE, 'dd/MM/yyyy'), 'timestamp'), 'date'))
#View(head(list_cb_tech_dmp_DAT))
#nrow(list_cb_tech_dmp_DAT)
#[1] 48.813

#prova_min <- min(list_cb_tech_dmp_DAT$DAT_VISUALIZZAZIONE) # NON FUNZIONA
date_list <- arrange(list_cb_tech_dmp_DAT, list_cb_tech_dmp_DAT$DAT_VISUALIZZAZIONE, list_cb_tech_dmp_DAT$COD_CONTRATTO)
#View(head(date_list))

first_dmp_date <- summarize(date_list, prima_data=first(date_list$DAT_VISUALIZZAZIONE)) 
#View(first_dmp_date)
# 2016-10-23
last_dmp_date <- summarize(date_list, ultima_data=last(date_list$DAT_VISUALIZZAZIONE))
#View(last_dmp_date)
# 2017-03-12
last_month_dmp_list <- subset(date_list, date_list$DAT_VISUALIZZAZIONE >= "2017-02-12" & date_list$DAT_VISUALIZZAZIONE <= "2017-03-12")
#View(head(last_month_dmp_list))
#nrow(last_month_dmp_list)
#[1] 14.114

distval_dmp_last_month <- distinct(select(last_month_dmp_list, "COD_CONTRATTO"))
#nrow(distval_dmp_last_month)
#[1] 11.786





# TEST: join (intersezione) tra vod_last_month e dmp_last_month per vedere quanti utenti della CB sono in entrambi i "gruppi":
distval_dmp_last_month <- withColumnRenamed(distval_dmp_last_month, "COD_CONTRATTO", "COD_CONTRATTO_jj")
num_cb_vod_dmp <- join(distval_vod_viewing_last_month, distval_dmp_last_month, distval_vod_viewing_last_month$COD_CONTRATTO == distval_dmp_last_month$COD_CONTRATTO_jj)
#nrow(num_cb_vod_dmp)
#[1] 3.034




