
# CJ_PDISC_vers01_post_pdisc

#apri sessione
source("connection_R.R")
options(scipen = 1000)


## Importo i dati di Lorenzo e aggrego i 2 file per codice contratto

pdisc_path <- "/user/stefano.mazzucca/Ret_DIRECT_SCADENZE_GENNAIO_MKTGAUTO.txt"
pdisc_ma_1 <- read.df(pdisc_path, source = "csv", header = "true", delimiter = "\t")
View(head(pdisc_ma_1,100))
nrow(pdisc_ma_1)
# 327.986

valdist_ma_1 <- distinct(select(pdisc_ma_1, "COD_CONTRATTO"))
nrow(valdist_ma_1)
# 29.777



pdisc_path <- "/user/stefano.mazzucca/Ret_OB_SCADENZE_GENNAIO_MKTGAUTO.txt"
pdisc_ma_COB <- read.df(pdisc_path, source = "csv", header = "true", delimiter = "\t")
View(head(pdisc_ma_COB,100))
nrow(pdisc_ma_COB)
# 131.424

valdist_ma_COB <- distinct(select(pdisc_ma_COB, "COD_CONTRATTO"))
nrow(valdist_ma_COB)
# 28.634



###


##????
pdisc_ma_1 <- withColumn(pdisc_ma_1,"DAT_CREAZIONE_CONTATTO_formdate",cast(cast(unix_timestamp(pdisc_ma_1$DAT_CREAZIONE_CONTATTO, 'dd-MM-yyyy'), 'timestamp'), 'date'))
View(head(pdisc_ma_1,100))



###################### Seleziono parametri e unisco i 2 dataset


pdisc_ma_2 <- withColumn(pdisc_ma_1,"data", cast(cast(unix_timestamp(pdisc_ma_1$DAT_CREAZIONE_CONTATTO, 'dd-MM-yyyy'), 'timestamp'), 'date')) #formato date dd-MM-yyyy
pdisc_ma_3 <- withColumnRenamed(pdisc_ma_2, "DES_CANALE_MC", "canale")
pdisc_ma_4 <- select(pdisc_ma_3, "COD_CONTRATTO", "data", "canale") 

View(head(pdisc_ma_4,100))
nrow(pdisc_ma_4)
# 327.986


pdisc_ma_COB_1 <- filter(pdisc_ma_COB, "CONTACTSTATUSID = '2'") # seleziono solo le chiamate che hanno ricevuto risposta!
pdisc_ma_COB_2 <- withColumn(pdisc_ma_COB_1, "data", cast(cast(unix_timestamp(pdisc_ma_COB$CONTACTDATETIME, 'dd-MM-yyyy HH:mm:ss'), 'timestamp'), 'date')) #formato timestamp dd-MM-yyyy HH:mm:ss
pdisc_ma_COB_3 <- withColumnRenamed(pdisc_ma_COB_2, "CHANNEL", "canale")
pdisc_ma_COB_4 <- select(pdisc_ma_COB_3, "COD_CONTRATTO", "data", "canale")

View(head(pdisc_ma_COB_4,100))
nrow(pdisc_ma_COB_4)
# 70.990


pdisc_ma_tot <- union(pdisc_ma_4, pdisc_ma_COB_4)

# createOrReplaceTempView(pdisc_ma_tot,"pdisc_ma_tot")
# 
# pdisc_ma_tot_2 <- sql("select * 
#                       from pdisc_ma_tot 
#                       order by data")

pdisc_ma_tot_2 <- arrange(pdisc_ma_tot, "data")

View(head(pdisc_ma_tot_2,1000))
nrow(pdisc_ma_tot_2)
# 398.976

# prova <- filter(pdisc_ma_tot_2, pdisc_ma_tot_2$canale == 'PHONE OUTBOUND')
# View(head(prova,100))
# nrow(prova)
# # 70.990



## Importo la base_pdisc completa

base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
View(head(base_pdisc,100))
nrow(base_pdisc)
# 37.976



## Costruisco sulla base pdisc i record di contatto con i clienti

createOrReplaceTempView(base_pdisc,"base_pdisc")
createOrReplaceTempView(pdisc_ma_tot_2,"pdisc_ma_tot_2")

pdisc_agganciati <- sql("select distinct t1.COD_CONTRATTO
                        from base_pdisc t1
                        inner join pdisc_ma_tot_2 t2
                          on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(pdisc_agganciati,100))
nrow(pdisc_agganciati)
# 27.723


pdisc_contatti <- sql("select t1.COD_CONTRATTO, t1.COD_CLIENTE_CIFRATO, t1.DES_ULTIMA_CAUSALE_CESSAZIONE, 
                                t1.DAT_RICH_CESSAZIONE_CNTR, t1.data_ingr_pdisc_formdate, t1.DES_PACCHETTO_POSS_FROM, t1.DES_AREA_NIELSEN_FRU,
                                t1.cl_tenure, t1.digitalizzazione_bin, 
                                t2.data, t2.canale
                      from base_pdisc t1
                      inner join pdisc_ma_tot_2 t2
                        on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(pdisc_contatti,100))
nrow(pdisc_contatti)
# 370.767 

# t1.post_visid_concatenated MANCA!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# rispetto ai 459.410 record di Marketing Automation, si perdono alcuni "contatti"



#### Salvataggio ####

write.parquet(pdisc_contatti,"/user/stefano.mazzucca/CJ_PDISC_contatti_marketing_automation.parquet")

# Leggi tabella
pdisc_contatti <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_contatti_marketing_automation.parquet")
View(head(pdisc_contatti,100))
nrow(pdisc_contatti)
# 370.767


# export in csv
write.df(repartition( pdisc_contatti, 1),path = "/user/stefano.mazzucca/CJ_PISC_contatti_ma.csv", "csv", sep=";", mode = "overwrite", header=TRUE)

######################



ver_valdist_contratti <- distinct(select(pdisc_contatti, "COD_CONTRATTO"))
nrow(ver_valdist_contratti)
# 27.723 (= pdisc_agganciati) ceck OK

ver_valdist_clienti <- distinct(select(pdisc_contatti, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_clienti)
# 27.541



## Valutazione sugli utenti MANCANTI

createOrReplaceTempView(base_pdisc,"base_pdisc")
#createOrReplaceTempView(pdisc_ma_tot_2,"pdisc_ma_tot_2")
createOrReplaceTempView(pdisc_contatti, "pdisc_contatti")

pdisc_no_agganciati <- sql("select t1.COD_CONTRATTO
                            from base_pdisc t1
                            left join pdisc_contatti t2
                              on t1.COD_CONTRATTO = t2.COD_CONTRATTO where t2.COD_CONTRATTO is NULL")
View(head(pdisc_no_agganciati,100))
nrow(pdisc_no_agganciati)
# 10.253


createOrReplaceTempView(base_pdisc,"base_pdisc")
#createOrReplaceTempView(pdisc_ma_tot_2,"pdisc_ma_tot_2")
createOrReplaceTempView(pdisc_contatti, "pdisc_contatti")

pdisc_no_agganciati_des <- sql("select t1.COD_CONTRATTO, t1. DES_ULTIMA_CAUSALE_CESSAZIONE
                            from base_pdisc t1
                            left join pdisc_contatti t2
                              on t1.COD_CONTRATTO = t2.COD_CONTRATTO where t2.COD_CONTRATTO is NULL")
View(head(pdisc_no_agganciati_des,100))
nrow(pdisc_no_agganciati_des)
# 10.253

## Aggancio le info ai NO_AGGANCIATI

createOrReplaceTempView(pdisc_no_agganciati,"pdisc_no_agganciati")
createOrReplaceTempView(base_pdisc,"base_pdisc")
pdisc_no_agganciati_info <- sql("select t1.COD_CONTRATTO, t1.COD_CLIENTE_CIFRATO, t1.DES_ULTIMA_CAUSALE_CESSAZIONE, 
                                          t1.DAT_RICH_CESSAZIONE_CNTR, t1.data_ingr_pdisc_formdate, t1.DES_PACCHETTO_POSS_FROM, t1.DES_AREA_NIELSEN_FRU,
                                          t1.cl_tenure, t1.digitalizzazione_bin
                                from base_pdisc t1
                                inner join pdisc_no_agganciati t2
                                  on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(pdisc_no_agganciati_info,1000))
nrow(pdisc_no_agganciati_info)
# 10.253


################################################################################################################################################################################
## Verifica sul flag_comunicazione_terzi
################################################################################################################################################################################

path <- "/user/stefano.mazzucca/FLG_COM_TERZI.csv"
base_flg_com_ter <- read.df(path, source = "csv", header = "true", delimiter = ";")
View(head(base_flg_com_ter,100))
nrow(base_flg_com_ter)
# 37.976

createOrReplaceTempView(base_flg_com_ter, "base_flg_com_ter")
createOrReplaceTempView(pdisc_no_agganciati_info, "pdisc_no_agganciati_info")
join_flg <- sql("select distinct t1.COD_CLIENTE_CIFRATO, t1.FLG_COMUNICAZIONE_TERZI, t2.DES_ULTIMA_CAUSALE_CESSAZIONE, t2.data_ingr_pdisc_formdate,
                                      t2.DES_PACCHETTO_POSS_FROM, t2.DES_AREA_NIELSEN_FRU, t2.cl_tenure, t2.digitalizzazione_bin,
                                      case when t2.COD_CLIENTE_CIFRATO is NOT NULL then 0 
                                            when t2.COD_CLIENTE_CIFRATO is NULL then 1
                                            else NULL end as flg_agganciato_MA
                from base_flg_com_ter t1
                left join pdisc_no_agganciati_info t2
                  on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                order by t1.FLG_COMUNICAZIONE_TERZI")
View(head(join_flg,100))
nrow(join_flg)
# 37.775

createOrReplaceTempView(join_flg, "join_flg")
count_tot_flg <- sql("select FLG_COMUNICAZIONE_TERZI, 
                          count(COD_CLIENTE_CIFRATO)
                     from join_flg
                     group by FLG_COMUNICAZIONE_TERZI")
View(head(count_tot_flg,100))
# FLG_COMUNICAZIONE_TERZI count(*)
#     0                   6088
#     NA                  1
#     1                   31686



createOrReplaceTempView(base_flg_com_ter, "base_flg_com_ter")
createOrReplaceTempView(pdisc_no_agganciati_info, "pdisc_no_agganciati_info")
sub_join_flg <- sql("select distinct t1.COD_CLIENTE_CIFRATO, t1.FLG_COMUNICAZIONE_TERZI, t2.DES_ULTIMA_CAUSALE_CESSAZIONE, t2.data_ingr_pdisc_formdate,
                                      t2.DES_PACCHETTO_POSS_FROM, t2.DES_AREA_NIELSEN_FRU, t2.cl_tenure, t2.digitalizzazione_bin,
                                      case when t2.COD_CLIENTE_CIFRATO is NOT NULL then 0 
                                            when t2.COD_CLIENTE_CIFRATO is NULL then 1
                                            else NULL end as flg_agganciato_MA
                from base_flg_com_ter t1
                inner join pdisc_no_agganciati_info t2
                  on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                order by t1.FLG_COMUNICAZIONE_TERZI")
View(head(sub_join_flg,100))
nrow(sub_join_flg)
# 10.251

createOrReplaceTempView(sub_join_flg, "sub_join_flg")
count_flg <- sql("select FLG_COMUNICAZIONE_TERZI, 
                          count(COD_CLIENTE_CIFRATO)
                 from sub_join_flg
                 group by FLG_COMUNICAZIONE_TERZI")
View(head(count_flg,100))
# FLG_COMUNICAZIONE_TERZI  count(*)
#     0                     5121
#     NA                    1
#     1                     5129






#chiudi sessione
sparkR.stop()
