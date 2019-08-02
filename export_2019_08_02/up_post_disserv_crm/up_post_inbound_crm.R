
## up_post_disserv_crm_1

## Analisi dei clienti che hanno effettuato UPSELLING in Q1
## dopo un disservizio inbound crm (chiamata inbound 'abbandonata')
## verificare livello di digtializzazione per capire se sono stati condizionati a fare upselling su web o se erano gia' predisposti!

## periodo di osservazione: 01Lug2018 - 13Set2018
## focus su Week 7 e Week 8


source("connection_R.R")
options(scipen = 10000)

# path in lettura
path_inb_disserv <- "/user/stefano.mazzucca/up_post_disservizio_crm/20180919_PTO_1_abbandonate.txt"
path_inb_tot <- "/user/stefano.mazzucca/up_post_disservizio_crm/20180919_PTO_2_totali.txt"

path_up_wsc <- "/user/stefano.mazzucca/up_post_disservizio_crm/upselling_dig_wsc_GIU-SET_def.csv"
path_up_app <- "/user/stefano.mazzucca/up_post_disservizio_crm/upselling_dig_app_GIU-SET_def.csv"

# path in scrittura
path_ups_digital <- "/user/stefano.mazzucca/up_post_disservizio_crm/upselling_digital_tot_giu_set.parquet"

path_ups_post_diss_crm_gb <- "/user/stefano.mazzucca/up_post_disservizio_crm/upselling_digital_grouby.parquet"



## UPSELLING DIGITALI

up_wsc <- read.df(path_up_wsc, source = "csv",  header = "true", delimiter = ";")
View(head(up_wsc,100))
nrow(up_wsc)
# 379.324
up_wsc_1 <- select(up_wsc, "external_id", "data", "Orders")
up_wsc_2 <- filter(up_wsc_1, isNotNull(up_wsc_1$external_id) & up_wsc_1$Orders >= 1)
up_wsc_3 <- withColumn(up_wsc_2, "data_dt", cast(cast(unix_timestamp(up_wsc_2$data, 'dd/MM/yyyy'), 'timestamp'), 'date'))
up_wsc_4 <- withColumn(up_wsc_3, "canale", lit("sito_wsc"))
View(head(up_wsc_4,100))
nrow(up_wsc_4)
# 70.890

up_app <- read.df(path_up_app, source = "csv",  header = "true", delimiter = ";")
View(head(up_app,100))
nrow(up_app)
# 395.531
up_app_1 <- select(up_app, "external_id", "data", "Orders")
up_app_2 <- filter(up_app_1, isNotNull(up_app_1$external_id) & up_app_1$Orders >= 1)
up_app_3 <- withColumn(up_app_2, "data_dt", cast(cast(unix_timestamp(up_app_2$data, 'dd/MM/yyyy'), 'timestamp'), 'date'))
up_app_4 <- withColumn(up_app_3, "canale", lit("app_wsc"))
View(head(up_app_4,100))
nrow(up_app_4)
# 49.411

ups_digital <- rbind(up_wsc_4, up_app_4)
ups_digital_1 <- arrange(ups_digital, asc(ups_digital$data_dt))
ups_digital_2 <- distinct(select(ups_digital_1, "external_id", "Orders", "data_dt", "canale"))
ups_digital_3 <- filter(ups_digital_2, ups_digital_2$data_dt >= '2018-07-02') # & ups_digital_2$data_dt <= '2018-09-12')
View(head(ups_digital_3,100))
nrow(ups_digital_3)
# 110.528

ver_dat <- summarize(ups_digital_3, min_data = min(ups_digital_3$data_dt), max_data = max(ups_digital_3$data_dt))
View(head(ver_dat))
# min_data        max_data
# 2018-07-02      2018-09-13

write.parquet(ups_digital_3, path_ups_digital, mode = "overwrite")



## CHIAMATE CRM in INBOUND

inbound_disserv <- read.df(path_inb_disserv, source = "csv",  header = "true", delimiter = "$")
View(head(inbound_disserv,100))
nrow(inbound_disserv)
# 673.042
valdist_extid <- distinct(select(inbound_disserv, "COD_CLIENTE_CIFRATO"))
nrow(valdist_extid)
# 360.412

inbound_tot <- read.df(path_inb_tot, source = "csv",  header = "true", delimiter = "$")
View(head(inbound_tot,100))
nrow(inbound_tot)
# 3.086.479

# createOrReplaceTempView(inbound_disserv, "inbound_disserv")
# createOrReplaceTempView(inbound_tot, "inbound_tot")
# 
# inner_join <- sql("select distinct t1.*
#                   from inbound_disserv t1
#                   inner join inbound_tot t2
#                   on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and 
#                     t1.COD_CONN_ID_BASE = t2.COD_CONN_ID_BASE")
# View(head(inner_join,100))
# nrow(inner_join)
# # 673.042

inbound_tot_1 <- withColumn(inbound_tot, "date_time_ts", cast(unix_timestamp(inbound_tot$DAT_START_TIME, 'dd/MM/yyyy HH:mm:ss'), 'timestamp'))
inbound_tot_2 <- withColumn(inbound_tot_1, "date_time_dt", cast(cast(unix_timestamp(inbound_tot_1$DAT_START_TIME, 'dd/MM/yyyy HH:mm:ss'), 'timestamp'),'date'))
inbound_tot_3 <- arrange(inbound_tot_2, asc(inbound_tot_2$COD_CLIENTE_CIFRATO), asc(inbound_tot_2$date_time_ts))
# View(head(inbound_tot_3,100))
inbound_tot_3$week_FY <- ifelse(inbound_tot_3$date_time_dt >= '2018-09-10', 'W11', 
                                      ifelse(inbound_tot_3$date_time_dt >= '2018-09-03', 'W10',
                                             ifelse(inbound_tot_3$date_time_dt >= '2018-08-27', 'W9',
                                                    ifelse(inbound_tot_3$date_time_dt >= '2018-08-20', 'W8',
                                                           ifelse(inbound_tot_3$date_time_dt >= '2018-08-13', 'W7',
                                                                  ifelse(inbound_tot_3$date_time_dt >= '2018-08-06', 'W6',
                                                                         ifelse(inbound_tot_3$date_time_dt >= '2018-07-30', 'W5',
                                                                                ifelse(inbound_tot_3$date_time_dt >= '2018-07-23', 'W4',
                                                                                       ifelse(inbound_tot_3$date_time_dt >= '2018-07-16', 'W3',
                                                                                              ifelse(inbound_tot_3$date_time_dt >= '2018-07-09', 'W2',
                                                                                                     ifelse(inbound_tot_3$date_time_dt >= '2018-07-02', 'W1',
                                                                                                            ifelse(inbound_tot_3$date_time_dt >= '2018-06-25', 'W52',
                                                                                                                   'undefined'))))))))))))
inbound_tot_4 <- filter(inbound_tot_3, inbound_tot_3$date_time_dt >= '2018-07-02' & inbound_tot_3$date_time_dt <= '2018-09-06')
inbound_tot_5 <- arrange(inbound_tot_4, asc(inbound_tot_4$COD_CLIENTE_CIFRATO), asc(inbound_tot_4$date_time_ts))
View(head(inbound_tot_5,100))
nrow(inbound_tot_5)
# 2.855.235
ver_dat <- summarize(inbound_tot_5, min_data = min(inbound_tot_5$date_time_ts), max_data = max(inbound_tot_5$date_time_ts))
View(head(ver_dat))
# min_data                max_data
# 2018-07-02 08:18:33     2018-09-06 22:29:54


################################################################################################################################################################
## Analisi ## 

ups_digital <- read.parquet(path_ups_digital)
View(head(ups_digital,100))
nrow(ups_digital)
# 110.528
valdist_extid <- distinct(select(ups_digital, "external_id"))
nrow(valdist_extid)
# 101.660
valdist_extid_dat <- distinct(select(ups_digital, "external_id", "data_dt"))
nrow(valdist_extid_dat)
# 110.111
valdist_extid_dat_canale <- distinct(select(ups_digital, "external_id", "data_dt", "canale"))
nrow(valdist_extid_dat_canale)
# 110.528

# # inner join per trovare i cod_clienti che matchano
# createOrReplaceTempView(inbound_tot_5, "inbound_tot_5")
# createOrReplaceTempView(ups_digital, "ups_digital")
# overlap <- sql("select distinct t2.external_id
#                from inbound_tot_5 t1
#                inner join ups_digital t2
#                 on t1.COD_CLIENTE_CIFRATO = t2.external_id ")
# View(head(overlap,100))
# nrow(overlap)
# # 37.630

# join per trovare i clienti impattati da disservizio crm

createOrReplaceTempView(inbound_tot_5, "inbound_tot_5")
createOrReplaceTempView(ups_digital, "ups_digital")
ups_crm_tot <- sql("select distinct t1.*, t2.*
                   from inbound_tot_5 t1
                   inner join ups_digital t2
                    on t1.COD_CLIENTE_CIFRATO = t2.external_id ")
View(head(ups_crm_tot,100))
nrow(ups_crm_tot)
# 123.655

# valdist_extid_ups_crm_tot <- distinct(select(ups_crm_tot, ups_crm_tot$COD_CLIENTE_CIFRATO))
# nrow(valdist_extid_ups_crm_tot)
# # 37.630

ups_crm_tot_1 <- withColumn(ups_crm_tot, "dat_1_week_post_call", date_add(ups_crm_tot$date_time_dt, 7))
ups_crm_tot_2 <- filter(ups_crm_tot_1, (ups_crm_tot_1$data_dt <= ups_crm_tot_1$dat_1_week_post_call & ups_crm_tot_1$data_dt >= ups_crm_tot_1$date_time_dt))
nrow(ups_crm_tot_2)
# 33.107

createOrReplaceTempView(ups_crm_tot_2, "ups_crm_tot_2")
ups_crm_tot_3 <- sql("select COD_CLIENTE_CIFRATO, 
                              count(distinct(date_time_ts)) as num_chiamate_inbound,
                              min(date_time_ts) as prima_chiamata_ts, 
                              min(date_time_dt) as prima_chiamata_dt,
                              max(date_time_ts) as ultima_chiamata_ts,
                              max(date_time_dt) as ultima_chiamata_dt,
                              sum(NUM_SECONDI_CHIAMATA) as somma_secondi_chiamate, 
                              sum(FLG_ABBAND) as sum_flg_abbandono,
                              last(DES_CAT_ESG) as des_cat_esg,
                              last(DES_SUB_CAT_ESG) as des_sub_cat_esg,
                              last(DES_X_MOTIVO) as des_x_motivo,
                              last(DES_X_DETTAGLIO_MOTIVO) as des_x_dettaglio_motivo,
                              last(DES_TIPOLOGIA_X_MOTIVO_ESG) as des_tipologia_x_motivo_esg,
                              max(Orders) as ordini_ups,
                              max(data_dt) as data_ups,
                              last(canale) as canale_ups,
                              min(dat_1_week_post_call) as min_dat_1_week_post_call,
                              max(dat_1_week_post_call) as max_dat_1_week_post_call
                     from ups_crm_tot_2
                     group by COD_CLIENTE_CIFRATO")
View(head(ups_crm_tot_3,100))
nrow(ups_crm_tot_3)
# 16.967

write.parquet(ups_crm_tot_3, path_ups_post_diss_crm_gb, mode = "overwrite")


#############################################################################################################################################################

ups_post_diss_crm <- read.parquet(path_ups_post_diss_crm_gb)
View(head(ups_post_diss_crm,100))
nrow(ups_post_diss_crm)
# 16.967

filt_ups <- filter(ups_post_diss_crm, ups_post_diss_crm$sum_flg_abbandono >= 1)
View(head(filt_ups,100))
nrow(filt_ups)
# 6.810

# filt_prova <- filter(filt_ups, filt_ups$data_ups <= filt_ups$min_dat_1_week_post_call)
# nrow(filt_prova)
# # 6.688
# filt_prova_2 <- filter(filt_ups, filt_ups$data_ups <= filt_ups$max_dat_1_week_post_call)
# nrow(filt_prova_2)
# # 6.810




#chiudi sessione
sparkR.stop()
