
prova_skyitdev <- read.df("/STAGE/adobe/reportsuite=skyitdev/table=hitdata","parquet")

View(head(prova_skyitdev))
nrow(prova_skyitdev)
# [1] 3.594.269.296

prova2 <- sample(prova_skyitdev, FALSE, 0.0000001)

View(head(prova2))
nrow(prova2)
# [1] 359


distval_prova2 <- distinct(select(prova2, "connection_type"))
View(distval_prova2)


DAT_prova2 <- withColumn(prova2,"date_time",cast(cast(unix_timestamp(prova2p$DAT_RILEVAZIONE, 'yyy/MM/dd'), 'timestamp'), 'date'))

date_list_prova2 <- arrange(prova2, prova2$DAT_VISUALIZZAZIONE, prova2$COD_CONTRATTO)

first_date_prova2 <- summarize(prova2, prima_data=first(prova2$DAT_VISUALIZZAZIONE)) 
#uguale a?
first_date_prova2 <- summarize(prova2, prima_data=first(first_date_prova2$DAT_VISUALIZZAZIONE)) 

