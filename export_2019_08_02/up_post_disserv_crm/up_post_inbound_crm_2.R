

## up_post_disserv_crm_2

## Analisi dei clienti che hanno effettuato UPSELLING in Q1
## dopo un disservizio inbound crm (chiamata inbound 'abbandonata')
## verificare livello di digtializzazione per capire se sono stati condizionati a fare upselling su web o se erano gia' predisposti!

## periodo di osservazione: 01Lug2018 - 13Set2018
## focus su Week 7 e Week 8


source("connection_R.R")
options(scipen = 10000)


# path in lettura
path_inb_tot <- "/user/stefano.mazzucca/up_post_disservizio_crm/20180919_PTO_2_totali.txt"
path_ups_digital <- "/user/stefano.mazzucca/up_post_disservizio_crm/upselling_digital_tot_giu_set.parquet"

path_kpi_digitalizzazione <- "/user/silvia/digitalizzazione_parziale_navigazione_201810"

# path_ups_post_diss_crm_gb <- "/user/stefano.mazzucca/up_post_disservizio_crm/upselling_digital_grouby.parquet"

# path in scrittura
path_lista_crm_ups_tot <- "/user/stefano.mazzucca/up_post_disservizio_crm/dataset_crm_ups_digital_tot.parquet"
path_lista_crm_ups_w7 <- "/user/stefano.mazzucca/up_post_disservizio_crm/dataset_crm_ups_digital_w7.parquet"
path_lista_crm_ups_w8 <- "/user/stefano.mazzucca/up_post_disservizio_crm/dataset_crm_ups_digital_w8.parquet"


## Elaborazioni ##################################################################################################

ups_digital <- read.parquet(path_ups_digital)
View(head(ups_digital,100))
nrow(ups_digital)
# 110.528
ver_dat <- summarize(ups_digital, min_data = min(ups_digital$data_dt), max_data = max(ups_digital$data_dt))
View(head(ver_dat))
# min_data          max_data
# 2018-07-02        2018-09-13

# ups_digital_w7 <- filter(ups_digital, ups_digital$data_dt >= '2018-08-13' & ups_digital$data_dt <= '2018-08-19')
# nrow(ups_digital_w7)
# # 19.576
# ups_digital_w8 <- filter(ups_digital, ups_digital$data_dt >= '2018-08-20' & ups_digital$data_dt <= '2018-08-26')
# nrow(ups_digital_w8)
# # 16.857


kpi_digit <- read.parquet("/user/silvia/digitalizzazione_parziale_navigazione_201810")
View(head(kpi_digit,100))
nrow(kpi_digit)
# 4.506.444


## join ups_digital con kpi_digit
createOrReplaceTempView(ups_digital, "ups_digital")
createOrReplaceTempView(kpi_digit, "kpi_digit")

ups_digital_1 <- sql("select distinct t1.*, t2.livello_digitalizzazione
                     from ups_digital t1
                     left join kpi_digit t2
                     on t1.external_id = t2.skyid")
View(head(ups_digital_1,100))
nrow(ups_digital_1)
# 110.528

createOrReplaceTempView(ups_digital_1, "ups_digital_1")
distr_liv_digit <- sql("select livello_digitalizzazione, count(external_id) as count
                             from ups_digital_1
                             group by livello_digitalizzazione")
View(head(distr_liv_digit,100))



inbound_tot <- read.df(path_inb_tot, source = "csv",  header = "true", delimiter = "$")
View(head(inbound_tot,100))
nrow(inbound_tot)
# 3.086.479

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
inbound_tot_4 <- filter(inbound_tot_3, inbound_tot_3$date_time_dt >= '2018-07-02' & inbound_tot_3$date_time_dt <= '2018-08-30') #### VARIAZIONE '2018-09-06'
inbound_tot_5 <- arrange(inbound_tot_4, asc(inbound_tot_4$COD_CLIENTE_CIFRATO), asc(inbound_tot_4$date_time_ts))
# ver_dat <- summarize(inbound_tot_5, min_data = min(inbound_tot_5$date_time_ts), max_data = max(inbound_tot_5$date_time_ts))
# View(head(ver_dat))
# # min_data                max_data
# # 2018-07-02 08:18:33     2018-09-06 22:29:54
inbound_tot_6 <- withColumn(inbound_tot_5, "dat_2_week_post_call", date_add(inbound_tot_5$date_time_dt, 14))
View(head(inbound_tot_6,100))
nrow(inbound_tot_6)
# 2.855.235
call_abbandonate <- filter(inbound_tot_6, "FLG_ABBAND = 1")
nrow(call_abbandonate)
# 668.147

## FOCUS ########################################################################################################################################################
inbound_w7 <- filter(inbound_tot_6, inbound_tot_6$week_FY == 'W7')
nrow(inbound_w7)
# 445.464
w7_abb <- filter(inbound_w7, "FLG_ABBAND == 1")
nrow(w7_abb)
# 190.682 (vs 260k dati di Ilaria Spirito)
unici_extid_w7_abb <- distinct(select(w7_abb, "COD_CLIENTE_CIFRATO"))
nrow(unici_extid_w7_abb)
# 112.469 (vs 144k)

inbound_w8 <- filter(inbound_tot_6, inbound_tot_6$week_FY == 'W8')
nrow(inbound_w8)
# 397.147
w8_abb <- filter(inbound_w8, "FLG_ABBAND == 1")
nrow(w8_abb)
# 83.889 (vs 120k dati di Ilaria Spirito)
unici_extid_w8_abb <- distinct(select(w8_abb, "COD_CLIENTE_CIFRATO"))
nrow(unici_extid_w8_abb)
# 59.960 (vs 79k)
################################################################################################################################################################

valdist_clienti <- distinct(select(inbound_tot_6, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti)
# 1.081.401
createOrReplaceTempView(inbound_tot_6, "inbound_tot_6")
groupby_cl <- sql("select COD_CLIENTE_CIFRATO, count(distinct(date_time_ts) as count_call)
                  from inbound_tot_6
                  group by COD_CLIENTE_CIFRATO")
View(head(groupby_cl,100))
call_num <- summarize(groupby_cl, media_chiamate_per_utente = mean(groupby_cl$count_call))
View(head(call_num))
# media_chiamate_per_utente:    2.596551


# join tra clienti tot che hanno effettuato upselling digitale e clienti che hanno chiamato inbound
createOrReplaceTempView(inbound_tot_6, "inbound_tot_6")
createOrReplaceTempView(ups_digital_1, "ups_digital_1")
crm_ups_tot <- sql("select distinct t1.*, t2.*
                   from inbound_tot_6 t1
                   left join ups_digital_1 t2
                    on t1.COD_CLIENTE_CIFRATO = t2.external_id ")
View(head(crm_ups_tot,100))
nrow(crm_ups_tot)
# 2.867.467 (vs 2.855.235 del crm)


write.parquet(crm_ups_tot, path_lista_crm_ups_tot, mode = "overwrite")



# join tra clienti w7 che hanno effettuato upselling digitale e clienti che hanno chiamato inbound
createOrReplaceTempView(inbound_w7, "inbound_w7")
createOrReplaceTempView(ups_digital_1, "ups_digital_1")
crm_ups_w7 <- sql("select distinct t1.*, t2.*
                   from inbound_w7 t1
                   left join ups_digital_1 t2
                    on t1.COD_CLIENTE_CIFRATO = t2.external_id ")
View(head(crm_ups_w7,100))
nrow(crm_ups_w7)
# 447.483 (vs 2.867.467 del tot)


write.parquet(crm_ups_w7, path_lista_crm_ups_w7, mode = "overwrite")



# join tra clienti w8 che hanno effettuato upselling digitale e clienti che hanno chiamato inbound
createOrReplaceTempView(inbound_w8, "inbound_w8")
createOrReplaceTempView(ups_digital_1, "ups_digital_1")
crm_ups_w8 <- sql("select distinct t1.*, t2.*
                  from inbound_w8 t1
                  left join ups_digital_1 t2
                  on t1.COD_CLIENTE_CIFRATO = t2.external_id ")
View(head(crm_ups_w8,100))
nrow(crm_ups_w8)
# 398.513 (vs 2.867.467 del tot)


write.parquet(crm_ups_w8, path_lista_crm_ups_w8, mode = "overwrite")


#############################################################################################################################################################

## Analisi sul tot

crm_ups_tot <- read.parquet(path_lista_crm_ups_tot)
View(head(crm_ups_tot,100))
nrow(crm_ups_tot)
# 2.867.467

## filtro i clienti target (quelli che effettuano ups digital) impattati
crm_ups_digit <- filter(crm_ups_tot, isNotNull(crm_ups_tot$external_id))
nrow(crm_ups_digit)
# 123.655 # 112.194

valdist_clienti <- distinct(select(crm_ups_digit, "external_id"))
nrow(valdist_clienti)
# 37.631 # 34.876

createOrReplaceTempView(crm_ups_digit, "crm_ups_digit")
distr_liv_digit_38k <- sql("select livello_digitalizzazione, count(distinct external_id) as count
                             from crm_ups_digit
                             group by livello_digitalizzazione")
View(head(distr_liv_digit_38k,100))


## filtro per periodo temporale: ups digital nella settimana successiva al disservizio
crm_ups_target <- filter(crm_ups_digit, crm_ups_digit$data_dt <= crm_ups_digit$dat_1_week_post_call)
View(head(crm_ups_target,100))
nrow(crm_ups_target)
# 72.793
crm_ups_target_1 <- filter(crm_ups_target, crm_ups_target$data_dt >= crm_ups_target$date_time_dt)
View(head(crm_ups_target_1,100))
nrow(crm_ups_target_1)
# 33.107 # 42.015


valdist_clienti <- distinct(select(crm_ups_target_1, "external_id"))
nrow(valdist_clienti)
# 16.967 # 19.107

createOrReplaceTempView(crm_ups_target_1, "crm_ups_target_1")
info_target_liv_digit_17k <- sql("select livello_digitalizzazione, count(distinct external_id) as count
                             from crm_ups_target_1
                             group by livello_digitalizzazione")
View(head(info_target_liv_digit_17k,100))


createOrReplaceTempView(crm_ups_target_1, "crm_ups_target_1")
info_target_tot <- sql("select COD_CLIENTE_CIFRATO, 
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
                              max(dat_1_week_post_call) as max_dat_1_week_post_call,
                              max(livello_digitalizzazione) as livello_digitalizzazione,
                              last(week_FY) as week_FY
                     from crm_ups_target_1
                     group by COD_CLIENTE_CIFRATO")
View(head(info_target_tot,100))
nrow(info_target_tot)
# 16.967


filt_ups_abbandono <- filter(info_target_tot, info_target_tot$sum_flg_abbandono >= 1)
View(head(filt_ups_abbandono,100))
nrow(filt_ups_abbandono)
# 6.810

filt_ups_0_chiamata <- filter(info_target_tot, info_target_tot$somma_secondi_chiamate == 0)
View(head(filt_ups_0_chiamata,100))
nrow(filt_ups_0_chiamata)
# 5.708

filt_ups_abbandono_or_0_chiamata <- filter(info_target_tot, (info_target_tot$sum_flg_abbandono >= 1 | info_target_tot$somma_secondi_chiamate == 0))
View(head(filt_ups_abbandono_or_0_chiamata,100))
nrow(filt_ups_abbandono_or_0_chiamata)
# 8.155 # 9.616

createOrReplaceTempView(filt_ups_abbandono_or_0_chiamata, "filt_ups_abbandono_or_0_chiamata")
info_target_liv_digit_17k <- sql("select livello_digitalizzazione, count(distinct COD_CLIENTE_CIFRATO) as count
                             from filt_ups_abbandono_or_0_chiamata
                             group by livello_digitalizzazione")
View(head(info_target_liv_digit_17k,100))

createOrReplaceTempView(filt_ups_abbandono_or_0_chiamata, "filt_ups_abbandono_or_0_chiamata")
info_target_week_FY_17k <- sql("select week_FY, count(distinct COD_CLIENTE_CIFRATO) as count
                             from filt_ups_abbandono_or_0_chiamata
                             group by week_FY")
View(head(info_target_week_FY_17k,100))


################################################################################################################################################################

## Analisi su week 7

crm_ups_w7 <- read.parquet(path_lista_crm_ups_w7)
View(head(crm_ups_w7,100))
nrow(crm_ups_w7)
# 447.483

## filtro i clienti target (quelli che effettuano ups digital) impattati
crm_ups_digit <- filter(crm_ups_w7, isNotNull(crm_ups_w7$external_id))
nrow(crm_ups_digit)
# 21.448

valdist_clienti <- distinct(select(crm_ups_digit, "external_id"))
nrow(valdist_clienti)
# 9.859

createOrReplaceTempView(crm_ups_digit, "crm_ups_digit")
distr_liv_digit_10k <- sql("select livello_digitalizzazione, count(distinct external_id) as count
                           from crm_ups_digit
                           group by livello_digitalizzazione")
View(head(distr_liv_digit_10k,100))


## filtro per periodo temporale: ups digital nella settimana successiva al disservizio
crm_ups_target <- filter(crm_ups_digit, crm_ups_digit$data_dt <= crm_ups_digit$dat_1_week_post_call)
View(head(crm_ups_target,100))
nrow(crm_ups_target)
# 16.690
crm_ups_target_1 <- filter(crm_ups_target, crm_ups_target$data_dt >= crm_ups_target$date_time_dt)
View(head(crm_ups_target_1,100))
nrow(crm_ups_target_1)
# 8.225


valdist_clienti <- distinct(select(crm_ups_target_1, "external_id"))
nrow(valdist_clienti)
# 4.323

createOrReplaceTempView(crm_ups_target_1, "crm_ups_target_1")
info_target_liv_digit_4k <- sql("select livello_digitalizzazione, count(distinct external_id) as count
                                 from crm_ups_target_1
                                 group by livello_digitalizzazione")
View(head(info_target_liv_digit_4k,100))


createOrReplaceTempView(crm_ups_target_1, "crm_ups_target_1")
info_target_tot <- sql("select COD_CLIENTE_CIFRATO, 
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
                       max(dat_1_week_post_call) as max_dat_1_week_post_call,
                       max(livello_digitalizzazione) as livello_digitalizzazione
                       from crm_ups_target_1
                       group by COD_CLIENTE_CIFRATO")
View(head(info_target_tot,100))
nrow(info_target_tot)
# 4.323


filt_ups_abbandono <- filter(info_target_tot, info_target_tot$sum_flg_abbandono >= 1)
View(head(filt_ups_abbandono,100))
nrow(filt_ups_abbandono)
# 2.756

filt_ups_0_chiamata <- filter(info_target_tot, info_target_tot$somma_secondi_chiamate == 0)
View(head(filt_ups_0_chiamata,100))
nrow(filt_ups_0_chiamata)
# 2.420

filt_ups_abbandono_or_0_chiamata <- filter(info_target_tot, (info_target_tot$sum_flg_abbandono >= 1 | info_target_tot$somma_secondi_chiamate == 0))
View(head(filt_ups_abbandono_or_0_chiamata,100))
nrow(filt_ups_abbandono_or_0_chiamata)
# 3.055

createOrReplaceTempView(filt_ups_abbandono_or_0_chiamata, "filt_ups_abbandono_or_0_chiamata")
info_target_liv_digit_3k <- sql("select livello_digitalizzazione, count(distinct COD_CLIENTE_CIFRATO) as count
                                 from filt_ups_abbandono_or_0_chiamata
                                 group by livello_digitalizzazione")
View(head(info_target_liv_digit_3k,100))



################################################################################################################################################################

## Analisi su week 8

crm_ups_w8 <- read.parquet(path_lista_crm_ups_w8)
View(head(crm_ups_w8,100))
nrow(crm_ups_w8)
# 398.513

## filtro i clienti target (quelli che effettuano ups digital) impattati
crm_ups_digit <- filter(crm_ups_w8, isNotNull(crm_ups_w8$external_id))
nrow(crm_ups_digit)
# 14.867

valdist_clienti <- distinct(select(crm_ups_digit, "external_id"))
nrow(valdist_clienti)
# 8.036

createOrReplaceTempView(crm_ups_digit, "crm_ups_digit")
distr_liv_digit_8k <- sql("select livello_digitalizzazione, count(distinct external_id) as count
                           from crm_ups_digit
                           group by livello_digitalizzazione")
View(head(distr_liv_digit_8k,100))


## filtro per periodo temporale: ups digital nella settimana successiva al disservizio
crm_ups_target <- filter(crm_ups_digit, crm_ups_digit$data_dt <= crm_ups_digit$dat_1_week_post_call)
View(head(crm_ups_target,100))
nrow(crm_ups_target)
# 12.624
crm_ups_target_1 <- filter(crm_ups_target, crm_ups_target$data_dt >= crm_ups_target$date_time_dt)
View(head(crm_ups_target_1,100))
nrow(crm_ups_target_1)
# 4.925


valdist_clienti <- distinct(select(crm_ups_target_1, "external_id"))
nrow(valdist_clienti)
# 3.062

createOrReplaceTempView(crm_ups_target_1, "crm_ups_target_1")
info_target_liv_digit_3k <- sql("select livello_digitalizzazione, count(distinct external_id) as count
                                from crm_ups_target_1
                                group by livello_digitalizzazione")
View(head(info_target_liv_digit_3k,100))


createOrReplaceTempView(crm_ups_target_1, "crm_ups_target_1")
info_target_tot <- sql("select COD_CLIENTE_CIFRATO, 
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
                       max(dat_1_week_post_call) as max_dat_1_week_post_call,
                       max(livello_digitalizzazione) as livello_digitalizzazione
                       from crm_ups_target_1
                       group by COD_CLIENTE_CIFRATO")
View(head(info_target_tot,100))
nrow(info_target_tot)
# 3.062


filt_ups_abbandono <- filter(info_target_tot, info_target_tot$sum_flg_abbandono >= 1)
View(head(filt_ups_abbandono,100))
nrow(filt_ups_abbandono)
# 1.139

filt_ups_0_chiamata <- filter(info_target_tot, info_target_tot$somma_secondi_chiamate == 0)
View(head(filt_ups_0_chiamata,100))
nrow(filt_ups_0_chiamata)
# 1.051

filt_ups_abbandono_or_0_chiamata <- filter(info_target_tot, (info_target_tot$sum_flg_abbandono >= 1 | info_target_tot$somma_secondi_chiamate == 0))
View(head(filt_ups_abbandono_or_0_chiamata,100))
nrow(filt_ups_abbandono_or_0_chiamata)
# 1.377

createOrReplaceTempView(filt_ups_abbandono_or_0_chiamata, "filt_ups_abbandono_or_0_chiamata")
info_target_liv_digit_1k <- sql("select livello_digitalizzazione, count(distinct COD_CLIENTE_CIFRATO) as count
                                from filt_ups_abbandono_or_0_chiamata
                                group by livello_digitalizzazione")
View(head(info_target_liv_digit_1k,100))



## all up&down grade ##########################################################################################################################################

path_upselling <- "/user/stefano.mazzucca/up_post_disservizio_crm/20180917_UPSELLING.csv"
path_kpi_digitalizzazione <- "/user/silvia/digitalizzazione_parziale_navigazione_201810"


up_down <- read.df(path_upselling, source = "csv",  header = "true", delimiter = ";")
View(head(up_down,100))
nrow(up_down)
# 811.305

# createOrReplaceTempView(up_down, "up_down")
# des_canale_contatto <- sql("select DES_CANALE_CONTATTO, count(COD_CLIENTE_CIFRATO)
#                            from up_down
#                            group by DES_CANALE_CONTATTO")
# View(head(des_canale_contatto,100))

up_down <- withColumn(up_down, "data_ups_dt", cast(cast(unix_timestamp(up_down$DAT_ATVZ_UPSELLING, "dd/MM/yyyy"), "timestamp"), "date"))
up_down_1 <- filter(up_down, up_down$data_ups_dt >= '2018-07-02')
ver_date <- summarize(up_down_1, min_data = min(up_down_1$data_ups_dt), max_data = max(up_down_1$data_ups_dt))
View(head(ver_date,100))
# min_data      max_data
# 2018-07-02    2018-09-12

ups_bi <- filter(up_down_1, "DES_CANALE_CONTATTO in ('MOBILE', 'WEB', 'SELFCARE', 'IVR SELFCARE', 'IVR-SELFCARE') and DES_PREMIUM_PACK_UP NOT like 'Undefined'") 
# 'CHAT', 'EMAIL', 'WEBMAIL'
View(head(ups_bi,100))
nrow(ups_bi)
# 115.712

ups_bi_no_digital <- filter(up_down_1, "DES_CANALE_CONTATTO NOT in ('MOBILE', 'WEB', 'SELFCARE', 'IVR SELFCARE', 'IVR-SELFCARE') and DES_PREMIUM_PACK_UP NOT like 'Undefined'") 
View(head(ups_bi_no_digital,100))
nrow(ups_bi_no_digital)
# 266.750
filt_ups_tel <- filter(ups_bi_no_digital, ups_bi_no_digital$DES_CANALE_CONTATTO == 'TELEFONO')
nrow(filt_ups_tel)
# 184.565



kpi_digit <- read.parquet(path_kpi_digitalizzazione)
View(head(kpi_digit,100))
nrow(kpi_digit)
# 4.506.444


## join ups_bi_digital con kpi_digit
createOrReplaceTempView(ups_bi, "ups_bi")
createOrReplaceTempView(kpi_digit, "kpi_digit")

ups_bi_digital <- sql("select distinct t1.*, t2.livello_digitalizzazione
                      from ups_bi t1
                      left join kpi_digit t2
                      on t1.COD_CLIENTE_CIFRATO = t2.skyid")
View(head(ups_bi_digital,100))
nrow(ups_bi_digital)
# 115.712

createOrReplaceTempView(ups_bi_digital, "ups_bi_digital")
distr_liv_digit <- sql("select livello_digitalizzazione, count(COD_CLIENTE_CIFRATO) as count
                       from ups_bi_digital
                       group by livello_digitalizzazione")
distr_liv_digit <- arrange(distr_liv_digit, asc(distr_liv_digit$livello_digitalizzazione))
View(head(distr_liv_digit,100))


## join ups_bi_NO_digital con kpi_digit
createOrReplaceTempView(ups_bi_no_digital, "ups_bi_no_digital")
createOrReplaceTempView(kpi_digit, "kpi_digit")

ups_bi_no_digital_1 <- sql("select distinct t1.*, t2.livello_digitalizzazione
                           from ups_bi_no_digital t1
                           left join kpi_digit t2
                           on t1.COD_CLIENTE_CIFRATO = t2.skyid")
View(head(ups_bi_no_digital_1,100))
nrow(ups_bi_no_digital_1)
# 266.750

createOrReplaceTempView(ups_bi_no_digital_1, "ups_bi_no_digital_1")
distr_liv_digit <- sql("select livello_digitalizzazione, count(COD_CLIENTE_CIFRATO) as count
                       from ups_bi_no_digital_1
                       group by livello_digitalizzazione")
distr_liv_digit <- arrange(distr_liv_digit, asc(distr_liv_digit$livello_digitalizzazione))
View(head(distr_liv_digit,100))


## join ups_bi_tel_inbound con kpi_digit
createOrReplaceTempView(filt_ups_tel, "filt_ups_tel")
createOrReplaceTempView(kpi_digit, "kpi_digit")

ups_bi_tel <- sql("select distinct t1.*, t2.livello_digitalizzazione
                  from filt_ups_tel t1
                  left join kpi_digit t2
                  on t1.COD_CLIENTE_CIFRATO = t2.skyid")
View(head(ups_bi_tel,100))
nrow(ups_bi_tel)
# 184.565

createOrReplaceTempView(ups_bi_tel, "ups_bi_tel")
distr_liv_digit <- sql("select livello_digitalizzazione, count(COD_CLIENTE_CIFRATO) as count
                       from ups_bi_tel
                       group by livello_digitalizzazione")
distr_liv_digit <- arrange(distr_liv_digit, asc(distr_liv_digit$livello_digitalizzazione))
View(head(distr_liv_digit,100))


######################################################################################################

# join tra clienti tot che hanno effettuato upselling non digitale e clienti che hanno chiamato inbound
ups_bi_no_digital_2 <- withColumnRenamed(ups_bi_no_digital_1, "COD_CLIENTE_CIFRATO", "COD_CLIENTE_CIFRATO_2")

createOrReplaceTempView(inbound_tot_6, "inbound_tot_6")
createOrReplaceTempView(ups_bi_no_digital_2, "ups_bi_no_digital_2")
crm_ups_nd_tot <- sql("select distinct t1.*, t2.*
                       from inbound_tot_6 t1
                       left join ups_bi_no_digital_2 t2
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO_2 ")
View(head(crm_ups_nd_tot,100))
nrow(crm_ups_nd_tot)
# 2.903.949 (vs 2.855.235 del crm)

write.parquet(crm_ups_nd_tot, "/user/stefano.mazzucca/up_post_disservizio_crm/dataset_crm_ups_nd_digital_tot.parquet", mode = "overwrite")

## Analisi 

crm_ups_nd_tot <- read.parquet("/user/stefano.mazzucca/up_post_disservizio_crm/dataset_crm_ups_nd_digital_tot.parquet")
View(head(crm_ups_nd_tot,100))
nrow(crm_ups_nd_tot)
# 2.903.949

## filtro i clienti target (quelli che effettuano ups no digital) impattati
crm_ups_nd <- filter(crm_ups_nd_tot, isNotNull(crm_ups_nd_tot$COD_CLIENTE_CIFRATO_2))
nrow(crm_ups_nd)
# 643.729

valdist_clienti <- distinct(select(crm_ups_nd, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti)
# 168.638

createOrReplaceTempView(crm_ups_nd, "crm_ups_nd")
distr_liv_digit_168k <- sql("select livello_digitalizzazione, count(distinct COD_CLIENTE_CIFRATO) as count
                           from crm_ups_nd
                           group by livello_digitalizzazione")
View(head(distr_liv_digit_168k,100))


## filtro per periodo temporale: ups digital nella settimana successiva al disservizio
crm_ups_nd_target <- filter(crm_ups_nd, crm_ups_nd$data_ups_dt <= crm_ups_nd$dat_1_week_post_call)
View(head(crm_ups_nd_target,100))
nrow(crm_ups_nd_target)
# 459.931
crm_ups_nd_target_1 <- filter(crm_ups_nd_target, crm_ups_nd_target$data_ups_dt >= crm_ups_nd_target$date_time_dt)
View(head(crm_ups_nd_target_1,100))
nrow(crm_ups_nd_target_1)
# 285.125


valdist_clienti <- distinct(select(crm_ups_nd_target_1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti)
# 132.714

createOrReplaceTempView(crm_ups_nd_target_1, "crm_ups_nd_target_1")
info_target_liv_digit_132k <- sql("select livello_digitalizzazione, count(distinct COD_CLIENTE_CIFRATO) as count
                                 from crm_ups_nd_target_1
                                 group by livello_digitalizzazione")
View(head(info_target_liv_digit_132k,100))


createOrReplaceTempView(crm_ups_nd_target_1, "crm_ups_nd_target_1")
info_target_nd_tot <- sql("select COD_CLIENTE_CIFRATO, 
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
                       max(data_ups_dt) as data_ups,
                       last(DES_CANALE_CONTATTO) as canale_ups,
                       min(dat_1_week_post_call) as min_dat_1_week_post_call,
                       max(dat_1_week_post_call) as max_dat_1_week_post_call,
                       max(livello_digitalizzazione) as livello_digitalizzazione,
                       last(week_FY) as week_FY
                       from crm_ups_nd_target_1
                       group by COD_CLIENTE_CIFRATO")
View(head(info_target_nd_tot,100))
nrow(info_target_nd_tot)
# 132.714


filt_ups_abbandono_or_0_chiamata <- filter(info_target_nd_tot, (info_target_nd_tot$sum_flg_abbandono >= 1 | info_target_nd_tot$somma_secondi_chiamate == 0))
View(head(filt_ups_abbandono_or_0_chiamata,100))
nrow(filt_ups_abbandono_or_0_chiamata)
# 38.394

createOrReplaceTempView(filt_ups_abbandono_or_0_chiamata, "filt_ups_abbandono_or_0_chiamata")
info_target_liv_digit_38k <- sql("select livello_digitalizzazione, count(distinct COD_CLIENTE_CIFRATO) as count
                                 from filt_ups_abbandono_or_0_chiamata
                                 group by livello_digitalizzazione")
View(head(info_target_liv_digit_38k,100))

createOrReplaceTempView(filt_ups_abbandono_or_0_chiamata, "filt_ups_abbandono_or_0_chiamata")
info_target_week_FY_38k <- sql("select week_FY, count(distinct COD_CLIENTE_CIFRATO) as count
                               from filt_ups_abbandono_or_0_chiamata
                               group by week_FY")
View(head(info_target_week_FY_38k,100))


#################################################################################################################################################################



#chiudi sessione
sparkR.stop()
