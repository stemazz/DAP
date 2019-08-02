
## up_post_disserv_crm_3

## Analisi sui dati di Celesete Maisto (CRM)



source("connection_R.R")
options(scipen = 10000)



path_inbound_call <- "/user/stefano.mazzucca/up_post_disservizio_crm/QUERY_FOR_NO_02.csv"

path_upselling <- "/user/stefano.mazzucca/up_post_disservizio_crm/20181106_2_estrazione upselling fy18_19.txt"

path_kpi_contatto <- "/user/stefano.mazzucca/up_post_disservizio_crm/cb_COMPO_CONT_GIU18.csv"


#################################################################################################################################################################
#################################################################################################################################################################
## Call inbound ##
#################################################################################################################################################################
#################################################################################################################################################################


inb_call <- read.df(path_inbound_call, source = "csv", header = "true", delimiter = ";")
View(head(inb_call,100))
nrow(inb_call)
# 6.729.804


inb_call_1 <- withColumn(inb_call, "data_chiamata_dt", cast(cast(unix_timestamp(inb_call$GIORNO_START_TIME_QUEUE, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'),'date'))
# View(head(inb_call_1,100))

# filt <- filter(inb_call_1, inb_call_1$data_chiamata_dt != '1900-01-01')
# summarize_dat <- summarize(filt, min_data = min(filt$data_chiamata_dt), max_data = max(filt$data_chiamata_dt))
# View(head(summarize_dat,100))
# # min_data     max_data
# # 2018-07-01   2018-09-30

inb_call_range_temp <- filter(inb_call_1, inb_call_1$data_chiamata_dt >= '2018-07-02' & inb_call_1$data_chiamata_dt <= '2018-09-30')
inb_call_range_temp$week <- ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-09-24', 'W13', 
                        ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-09-17', 'W12', 
                               ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-09-10', 'W11', 
                                      ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-09-03', 'W10',
                                             ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-08-27', 'W9',
                                                    ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-08-20', 'W8',
                                                           ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-08-13', 'W7',
                                                                  ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-08-06', 'W6',
                                                                         ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-07-30', 'W5',
                                                                                ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-07-23', 'W4',
                                                                                       ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-07-16', 'W3',
                                                                                              ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-07-09', 'W2',
                                                                                                     ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-07-02', 'W1',
                                                                                                            'undefined')))))))))))))
View(head(inb_call_range_temp,100))
nrow(inb_call_range_temp)
# 2.813.723


## Telefonate entrate in CODA OPERATORE
inb_call_coda <- filter(inb_call_range_temp, inb_call_range_temp$NUM_OFFERTE == 1)
View(head(inb_call_coda,100))
nrow(inb_call_coda)
# 2.799.893

## Telefonate NON gestite ( = abbandonate)
inb_call_abband <- filter(inb_call_coda, inb_call_coda$NUM_GESTITE == 0)
View(head(inb_call_abband,100))
nrow(inb_call_abband)
# 534.737 (vs 668.147 dati BI ma su range temporali diversi)


## FOCUS W7
inb_call_abband_w7 <- filter(inb_call_abband, inb_call_abband$week == 'W7')
nrow(inb_call_abband_w7)
# 137.559 (vs 190.682 dati BI ma su range temporali diversi)
unici_extid_w7_abb <- distinct(select(inb_call_abband_w7, "COD_CLIENTE"))
nrow(unici_extid_w7_abb)
# 80.004 (vs 112.469 dati BI ma su range temporali diversi)



## Elaborazione chiamate abbandonate (VAI a "JOIN con le call")
valdist_utenti <- distinct(select(inb_call_abband, "COD_CONTRATTO"))
nrow(valdist_utenti)
# 272.959

createOrReplaceTempView(inb_call_abband, "inb_call_abband")
gb_week <- sql("select week, count(*) as count
               from inb_call_abband
               group by week")
gb_week <- arrange(gb_week, asc(gb_week$week))
View(head(gb_week,100))




#############################################################sto###cazzo#########################################################################################
#################################################################################################################################################################
## Upselling ##
#################################################################################################################################################################
#################################################################################################################################################################

ups_tot <- read.df(path_upselling, source = "csv", header = "true", delimiter = "$")
ups_tot_1 <- withColumn(ups_tot, "dat_attivazione_dt", cast(cast(unix_timestamp(ups_tot$dat_attivazione, 'dd/MM/yyyy'), 'timestamp'), 'date'))
View(head(ups_tot_1,100))
nrow(ups_tot_1)
# 5.293.699

# createOrReplaceTempView(ups_tot_1, "ups_tot_1")
# gb_canale <- sql("select des_canale_contatto, count(*) as count
#                  from ups_tot_1
#                  group by des_canale_contatto")
# gb_canale <- arrange(gb_canale, desc(gb_canale$count))
# View(head(gb_canale,100))
# # des_canale_contatto      count
# # Undefined                3108925
# # TELEFONO                 1534074
# # WEB                      214358
# # WEBMAIL                  187633
# # MOBILE                   148296
# # CHAT                     50853
# # IVR-SELFCARE             27367
# # SKY SERVICE              7578
# # INTERNO                  4813
# # STB                      2433
# # AR                       1920
# # SMS                      1332
# # SKY CENTER               1221
# # FAX                      1213
# # SELFCARE                 1145

ups_tot_2 <- withColumn(ups_tot_1, "dat_prima_attivazione_dt", cast(cast(unix_timestamp(ups_tot_1$dat_prima_attivazione, 'yyyy-MM-dd HH:mm:ss.000000'), 'timestamp'), 'date'))
ups_tot_3 <- withColumn(ups_tot_2, "gg_sky", datediff(ups_tot_2$dat_attivazione_dt, ups_tot_2$dat_prima_attivazione_dt))
ups_tot_3$tenure_custom <- ifelse(ups_tot_3$gg_sky > 3650, '5.>10',
                                  ifelse(ups_tot_3$gg_sky <= 3650 & ups_tot_3$gg_sky > 2190, '4.6-10',
                                        ifelse(ups_tot_3$gg_sky <= 2190 & ups_tot_3$gg_sky > 1095, '3.3-6',
                                               ifelse(ups_tot_3$gg_sky <= 1095 & ups_tot_3$gg_sky > 365, '2.1-3',
                                                       ifelse(ups_tot_3$gg_sky <= 365, '1.0-1', NA)))))
ups_tot_4 <- withColumn(ups_tot_3, "eta", cast(ups_tot_3$eta_dat, "integer"))
ups_tot_5 <- withColumn(ups_tot_4, "eta", ifelse(ups_tot_4$eta > 18 & ups_tot_4$eta <= 25, '18-25', 
                                       ifelse(ups_tot_4$eta > 25 & ups_tot_4$eta <= 35, "26-35",
                                              ifelse(ups_tot_4$eta > 35  & ups_tot_4$eta <= 45, "36-45",
                                                     ifelse(ups_tot_4$eta > 45 & ups_tot_4$eta <= 55, "46-55",
                                                            ifelse(ups_tot_4$eta > 55 & ups_tot_4$eta <= 65, "55-65", "over_65"))))))

ups_tot_6 <- withColumn(ups_tot_5, "flg_sky_q", ifelse(ups_tot_5$flg_sky_q == 'Y', 1, 0))
ups_tot_7 <- withColumn(ups_tot_6, "flg_sky_q_def", ifelse(ups_tot_6$flg_sky_q + ups_tot_6$flg_sky_q_plus >= 1, 1, 0))
# ups_tot_7 <- fillna(ups_tot_7, '0')
ups_tot_8 <- withColumn(ups_tot_7, "flg_skygo_def", ifelse(ups_tot_7$flg_skygo + ups_tot_7$flg_skygo_plus_attivo >= 1, 1, 0))

ups_tot_9 <- filter(ups_tot_8, ups_tot_8$flg_upgrade == 1)
ups_tot_10 <- filter(ups_tot_9, ups_tot_9$des_mod_srv == 'SKY SATELLITE' | ups_tot_9$des_mod_srv == 'SKY VIA FIBRA' | ups_tot_9$des_mod_srv == 'SKY IPTV TELECOM')
View(head(ups_tot_10,100))
nrow(ups_tot_10)
# 2.172.718

# summarize_dat <- summarize(ups_tot_10, min_data = min(ups_tot_10$dat_attivazione_dt), max_data = max(ups_tot_10$dat_attivazione_dt))
# View(head(summarize_dat,100))
# # min_data     max_data
# # 2017-07-01   2018-11-02


## JOIN con il kpi_contatto ## ###################################################################################################################################

kpi_c <- read.df(path_kpi_contatto, source = "csv", header = "true", delimiter = ";")
View(head(kpi_c,100))
nrow(kpi_c)
# 3.858.471

# valdist <- distinct(select(kpi_c, "COD_CONTRATTO"))
# nrow(valdist)
# # 3.858.471

createOrReplaceTempView(kpi_c, "kpi_c")
createOrReplaceTempView(ups_tot_10, "ups_tot_10")

ups_tot_11 <- sql("select distinct t1.*, t2.comp_contatto_jul as kpi_contatti
                            from ups_tot_10 t1
                            left join kpi_c t2
                            on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(ups_tot_11,100))
nrow(ups_tot_11)
# 2.172.718

# ver <- filter(ups_tot_11, "kpi_contatti IS NULL")
# nrow(ver)
# # 389.550

createOrReplaceTempView(filt, "filt")
gb <- sql("select des_canale_contatto, count(*) as count
          from filt
          group by des_canale_contatto")
gb <- arrange (gb, desc(gb$count))
View(head(gb,100))


##################################################################################################################################################################

## JOIN con le call ## ###########################################################################################################################################

createOrReplaceTempView(inb_call_abband, "inb_call_abband")
createOrReplaceTempView(ups_tot_11, "ups_tot_11")

base_call_ups_abband <- sql("select distinct t1.COD_CONTRATTO
                            from inb_call_abband t1
                            inner join ups_tot_11 t2
                            on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(base_call_ups_abband,100))
nrow(base_call_ups_abband)
# 101.006  (# 253.973)

createOrReplaceTempView(base_call_ups_abband, "base_call_ups_abband")
createOrReplaceTempView(ups_tot_11, "ups_tot_11")
ups_call_abband <- sql("select distinct t2.*
                       from base_call_ups_abband t1
                       left join ups_tot_11 t2
                       on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(ups_call_abband,100))
nrow(ups_call_abband)
# 146.500

prova_kpi_c <- summarize(groupBy(ups_call_abband, ups_call_abband$kpi_contatti), count = count(ups_call_abband$cod_cliente_cifrato))
View(head(prova_kpi_c,100))

createOrReplaceTempView(ups_call_abband, "ups_call_abband")
gb_utente <- sql("select cod_contratto, count(*) as count
                 from ups_call_abband
                 group by cod_contratto")
gb_utente <- arrange(gb_utente, desc(gb_utente$count))
View(head(gb_utente,100))



write.df(repartition( ups_call_abband, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_call_abband.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


###################################################################################################################################################################
###################################################################################################################################################################
## AGG. post 12/12/2018 ##


inb_call_ivr_1 <- withColumn(inb_call, "data_chiamata_dt", cast(cast(unix_timestamp(inb_call$DATA_STARTIME_IVR, 'yyyy-MM-dd HH:mm:ss'), 'timestamp'),'date'))
View(head(inb_call_ivr_1,100))
nrow(inb_call_ivr_1)
# 6.729.804

inb_call_range_temp <- filter(inb_call_ivr_1, inb_call_ivr_1$data_chiamata_dt >= '2018-07-02' & inb_call_ivr_1$data_chiamata_dt <= '2018-09-30')
nrow(inb_call_range_temp)
# 2.694.195
inb_call_range_temp$week <- ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-09-24', 'W13', 
                                   ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-09-17', 'W12', 
                                          ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-09-10', 'W11', 
                                                 ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-09-03', 'W10',
                                                        ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-08-27', 'W9',
                                                               ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-08-20', 'W8',
                                                                      ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-08-13', 'W7',
                                                                             ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-08-06', 'W6',
                                                                                    ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-07-30', 'W5',
                                                                                           ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-07-23', 'W4',
                                                                                                  ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-07-16', 'W3',
                                                                                                         ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-07-09', 'W2',
                                                                                                                ifelse(inb_call_range_temp$data_chiamata_dt >= '2018-07-02', 'W1',
                                                                                                                       'undefined')))))))))))))
View(head(inb_call_range_temp,100))
nrow(inb_call_range_temp)
# 2.694.195

## Telefonata entrate in IVR + CODA OPERATORE
# inb_call_ivr_coda <- filter(inb_call_range_temp, "(NUM_GESTITE = 0 OR NUM_GESTITE is NULL) ") #and data_chiamata_dt is NOT NULL")
# View(head(inb_call_ivr_coda,100))
# nrow(inb_call_ivr_coda)
# # 679.807

## Telefonata entrate in IVR 
inb_call_ivr <- filter(inb_call_range_temp, "(NUM_OFFERTE = 0 OR NUM_OFFERTE is NULL)")
View(head(inb_call_ivr,100))
nrow(inb_call_ivr)
# 232.922 # (solo NUM_OFFERTE = 0 -> 12.071) 


## JOIN con le call ##

createOrReplaceTempView(inb_call_ivr, "inb_call_ivr")
createOrReplaceTempView(ups_tot_11, "ups_tot_11")

base_call_ups_ivr <- sql("select distinct t1.COD_CONTRATTO, 
                            from inb_call_ivr t1
                            inner join ups_tot_11 t2
                            on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(base_call_ups_ivr,100))
nrow(base_call_ups_ivr)
# 3.212 # (90.678 non ?? possibile che siano di meno!!)

createOrReplaceTempView(base_call_ups_ivr, "base_call_ups_ivr")
createOrReplaceTempView(ups_tot_11, "ups_tot_11")
ups_call_abband <- sql("select distinct t2.*, t1.
                       from base_call_ups_ivr t1
                       left join ups_tot_11 t2
                       on t1.COD_CONTRATTO = t2.COD_CONTRATTO")
View(head(ups_call_abband,100))
nrow(ups_call_abband)
# 4.809 # (131.661)

prova_kpi_c <- summarize(groupBy(ups_call_abband, ups_call_abband$kpi_contatti), count = count(ups_call_abband$cod_cliente_cifrato))
View(head(prova_kpi_c,100))

createOrReplaceTempView(inb_call_ivr, "inb_call_ivr")
gb_utente <- sql("select COD_CONTRATTO, count(*) as count
                 from inb_call_ivr
                  where scelta_IVR like 'COMMERCIALE'
                 group by COD_CONTRATTO")
gb_utente <- arrange(gb_utente, desc(gb_utente$count))
View(head(gb_utente,100))



write.df(repartition( ups_call_abband, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_call_IVR_abband.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



###################################################################################################################################################################
###################################################################################################################################################################


##################################################################################################################################################################

ups_tot_11_ups <- filter(ups_tot_11, ups_tot_11$flg_upgrade == 1)

socio_demo_ups <- agg(
  groupBy(ups_tot_11_ups, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag",
          "tenure_custom", "eta", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  # visite
  id = countDistinct(ups_tot_11_ups$cod_cliente_cifrato))
View(head(socio_demo_ups,100))

write.df(repartition( socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_tot_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



## filtro sui soli UPS DIGITAL (tot) ##
filt_ups_dig <- filter(ups_tot_11, "(des_canale_contatto LIKE 'WEB' or des_canale_contatto LIKE 'WEBMAIL' or des_canale_contatto LIKE 'MOBILE' or 
                            des_canale_contatto LIKE 'CHAT' or des_canale_contatto LIKE 'IVR-SELFCARE' or des_canale_contatto LIKE 'STB') ") 
filt_ups_dig <- filter(filt_ups_dig, filt_ups_dig$flg_upgrade == 1)
# filt_ups_dig <- filter(filt_ups_dig, filt_ups_dig$dat_attivazione_dt >= '2018-07-02' & filt_ups_dig$dat_attivazione_dt <= '2018-09-30')

ups_dig_socio_demo_ups <- agg(
  groupBy(filt_ups_dig, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag", 
          "tenure_custom", "eta", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  id = countDistinct(filt_ups_dig$cod_cliente_cifrato))
View(head(ups_dig_socio_demo_ups,100))

write.df(repartition( ups_dig_socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_dig_tot_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



## filtro sui soli UPS INBOUND TEL (tot) ##
filt_ups_call_inb <- filter(ups_tot_11, "des_canale_contatto LIKE 'TELEFONO' and des_tipo_contatto LIKE 'INBOUND' ") 
filt_ups_call_inb <- filter(filt_ups_call_inb, filt_ups_call_inb$flg_upgrade == 1)
# filt_ups_call_inb <- filter(filt_ups_call_inb, filt_ups_call_inb$dat_attivazione_dt >= '2018-07-02' & filt_ups_call_inb$dat_attivazione_dt <= '2018-09-30')

ups_inb_socio_demo_ups <- agg(
  groupBy(filt_ups_call_inb, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag", 
          "tenure_custom", "eta", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  id = countDistinct(filt_ups_call_inb$cod_cliente_cifrato))
View(head(ups_inb_socio_demo_ups,100))

write.df(repartition( ups_inb_socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_inb_tot_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




## Q1 FY19 #######################################################################################################################################################

## Digital ##
filt <- filter(filt_ups_dig, filt_ups_dig$dat_attivazione_dt >= '2018-07-02' & filt_ups_dig$dat_attivazione_dt <= '2018-09-30')
filt$week_ups <- ifelse(filt$dat_attivazione_dt >= '2018-09-24', 'W13', 
                        ifelse(filt$dat_attivazione_dt >= '2018-09-17', 'W12', 
                               ifelse(filt$dat_attivazione_dt >= '2018-09-10', 'W11', 
                                      ifelse(filt$dat_attivazione_dt >= '2018-09-03', 'W10',
                                             ifelse(filt$dat_attivazione_dt >= '2018-08-27', 'W9',
                                                    ifelse(filt$dat_attivazione_dt >= '2018-08-20', 'W8',
                                                           ifelse(filt$dat_attivazione_dt >= '2018-08-13', 'W7',
                                                                  ifelse(filt$dat_attivazione_dt >= '2018-08-06', 'W6',
                                                                         ifelse(filt$dat_attivazione_dt >= '2018-07-30', 'W5',
                                                                                ifelse(filt$dat_attivazione_dt >= '2018-07-23', 'W4',
                                                                                       ifelse(filt$dat_attivazione_dt >= '2018-07-16', 'W3',
                                                                                              ifelse(filt$dat_attivazione_dt >= '2018-07-09', 'W2',
                                                                                                     ifelse(filt$dat_attivazione_dt >= '2018-07-02', 'W1',
                                                                                                                   'undefined')))))))))))))
View(head(filt,100))
nrow(filt)
# 154.266

ups_socio_demo_ups <- agg(
  groupBy(filt, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag", 
          "tenure_custom", "eta", "week_ups", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  id = countDistinct(filt$cod_cliente_cifrato))
View(head(ups_socio_demo_ups,100))

write.df(repartition( ups_socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_dig_Q1FY19_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



## Telefono inbound ##
filt <- filter(filt_ups_call_inb, filt_ups_call_inb$dat_attivazione_dt >= '2018-07-02' & filt_ups_call_inb$dat_attivazione_dt <= '2018-09-30')
filt$week_ups <- ifelse(filt$dat_attivazione_dt >= '2018-09-24', 'W13', 
                        ifelse(filt$dat_attivazione_dt >= '2018-09-17', 'W12', 
                               ifelse(filt$dat_attivazione_dt >= '2018-09-10', 'W11', 
                                      ifelse(filt$dat_attivazione_dt >= '2018-09-03', 'W10',
                                             ifelse(filt$dat_attivazione_dt >= '2018-08-27', 'W9',
                                                    ifelse(filt$dat_attivazione_dt >= '2018-08-20', 'W8',
                                                           ifelse(filt$dat_attivazione_dt >= '2018-08-13', 'W7',
                                                                  ifelse(filt$dat_attivazione_dt >= '2018-08-06', 'W6',
                                                                         ifelse(filt$dat_attivazione_dt >= '2018-07-30', 'W5',
                                                                                ifelse(filt$dat_attivazione_dt >= '2018-07-23', 'W4',
                                                                                       ifelse(filt$dat_attivazione_dt >= '2018-07-16', 'W3',
                                                                                              ifelse(filt$dat_attivazione_dt >= '2018-07-09', 'W2',
                                                                                                     ifelse(filt$dat_attivazione_dt >= '2018-07-02', 'W1',
                                                                                                            'undefined')))))))))))))
View(head(filt,100))
nrow(filt)
# 72.002 (vs 88.4k Celeste Ambroggi)

ups_socio_demo_ups <- agg(
  groupBy(filt, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag", 
          "tenure_custom", "eta", "week_ups", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  id = countDistinct(filt$cod_cliente_cifrato))
View(head(ups_socio_demo_ups,100))

write.df(repartition( ups_socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_inb_Q1FY19_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



##################################################################################################################################################################


## Q1 FY18 #######################################################################################################################################################

## Digital ##
filt <- filter(filt_ups_dig, filt_ups_dig$dat_attivazione_dt >= '2017-07-03' & filt_ups_dig$dat_attivazione_dt <= '2017-10-01')
filt$week_ups <- ifelse(filt$dat_attivazione_dt >= '2017-09-25', 'W13', 
                        ifelse(filt$dat_attivazione_dt >= '2017-09-18', 'W12', 
                               ifelse(filt$dat_attivazione_dt >= '2017-09-11', 'W11', 
                                      ifelse(filt$dat_attivazione_dt >= '2017-09-04', 'W10',
                                             ifelse(filt$dat_attivazione_dt >= '2017-08-28', 'W9',
                                                    ifelse(filt$dat_attivazione_dt >= '2017-08-21', 'W8',
                                                           ifelse(filt$dat_attivazione_dt >= '2017-08-14', 'W7',
                                                                  ifelse(filt$dat_attivazione_dt >= '2017-08-07', 'W6',
                                                                         ifelse(filt$dat_attivazione_dt >= '2017-07-31', 'W5',
                                                                                ifelse(filt$dat_attivazione_dt >= '2017-07-24', 'W4',
                                                                                       ifelse(filt$dat_attivazione_dt >= '2017-07-17', 'W3',
                                                                                              ifelse(filt$dat_attivazione_dt >= '2017-07-10', 'W2',
                                                                                                     ifelse(filt$dat_attivazione_dt >= '2017-07-03', 'W1',
                                                                                                            'undefined')))))))))))))
View(head(filt,100))
nrow(filt) 
# 86.079 (vs 61k dati di Celeste)

ups_socio_demo_ups <- agg(
  groupBy(filt, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag", 
          "tenure_custom", "eta", "week_ups", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  id = countDistinct(filt$cod_cliente_cifrato))
View(head(ups_socio_demo_ups,100))

write.df(repartition( ups_socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_dig_Q1FY18_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



## Telefono inbound ##
filt <- filter(filt_ups_call_inb, filt_ups_call_inb$dat_attivazione_dt >= '2017-07-03' & filt_ups_call_inb$dat_attivazione_dt <= '2017-10-01')
filt$week_ups <- ifelse(filt$dat_attivazione_dt >= '2017-09-25', 'W13', 
                        ifelse(filt$dat_attivazione_dt >= '2017-09-18', 'W12', 
                               ifelse(filt$dat_attivazione_dt >= '2017-09-11', 'W11', 
                                      ifelse(filt$dat_attivazione_dt >= '2017-09-04', 'W10',
                                             ifelse(filt$dat_attivazione_dt >= '2017-08-28', 'W9',
                                                    ifelse(filt$dat_attivazione_dt >= '2017-08-21', 'W8',
                                                           ifelse(filt$dat_attivazione_dt >= '2017-08-14', 'W7',
                                                                  ifelse(filt$dat_attivazione_dt >= '2017-08-07', 'W6',
                                                                         ifelse(filt$dat_attivazione_dt >= '2017-07-31', 'W5',
                                                                                ifelse(filt$dat_attivazione_dt >= '2017-07-24', 'W4',
                                                                                       ifelse(filt$dat_attivazione_dt >= '2017-07-17', 'W3',
                                                                                              ifelse(filt$dat_attivazione_dt >= '2017-07-10', 'W2',
                                                                                                     ifelse(filt$dat_attivazione_dt >= '2017-07-03', 'W1',
                                                                                                            'undefined')))))))))))))
View(head(filt,100))
nrow(filt)
# 77.969 (vs 79k dati di Celeste)

ups_socio_demo_ups <- agg(
  groupBy(filt, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag", 
          "tenure_custom", "eta", "week_ups", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  id = countDistinct(filt$cod_cliente_cifrato))
View(head(ups_socio_demo_ups,100))

write.df(repartition( ups_socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_inb_Q1FY18_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



##################################################################################################################################################################


## Q4 FY18 #######################################################################################################################################################

## Digital ##
filt <- filter(filt_ups_dig, filt_ups_dig$dat_attivazione_dt >= '2018-04-02' & filt_ups_dig$dat_attivazione_dt <= '2018-07-01')
filt$week_ups <- ifelse(filt$dat_attivazione_dt >= '2018-06-25', 'W52', 
                        ifelse(filt$dat_attivazione_dt >= '2018-06-18', 'W51', 
                               ifelse(filt$dat_attivazione_dt >= '2018-06-11', 'W50', 
                                      ifelse(filt$dat_attivazione_dt >= '2018-06-04', 'W49',
                                             ifelse(filt$dat_attivazione_dt >= '2018-05-28', 'W48',
                                                    ifelse(filt$dat_attivazione_dt >= '2018-05-21', 'W47',
                                                           ifelse(filt$dat_attivazione_dt >= '2018-05-14', 'W46',
                                                                  ifelse(filt$dat_attivazione_dt >= '2018-05-07', 'W45',
                                                                         ifelse(filt$dat_attivazione_dt >= '2018-04-30', 'W44',
                                                                                ifelse(filt$dat_attivazione_dt >= '2018-04-23', 'W43',
                                                                                       ifelse(filt$dat_attivazione_dt >= '2018-04-16', 'W42',
                                                                                              ifelse(filt$dat_attivazione_dt >= '2018-04-09', 'W41',
                                                                                                     ifelse(filt$dat_attivazione_dt >= '2018-04-02', 'W40',
                                                                                                            'undefined')))))))))))))
View(head(filt,100))
nrow(filt) 
# 34.430 (vs 32k dati di Celeste)

ups_socio_demo_ups <- agg(
  groupBy(filt, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag", 
          "tenure_custom", "eta", "week_ups", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  id = countDistinct(filt$cod_cliente_cifrato))
View(head(ups_socio_demo_ups,100))

write.df(repartition( ups_socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_dig_Q4FY18_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



## Telefono inbound ##
filt <- filter(filt_ups_call_inb, filt_ups_call_inb$dat_attivazione_dt >= '2018-04-02' & filt_ups_call_inb$dat_attivazione_dt <= '2018-07-01')
filt$week_ups <- ifelse(filt$dat_attivazione_dt >= '2018-06-25', 'W52', 
                        ifelse(filt$dat_attivazione_dt >= '2018-06-18', 'W51', 
                               ifelse(filt$dat_attivazione_dt >= '2018-06-11', 'W50', 
                                      ifelse(filt$dat_attivazione_dt >= '2018-06-04', 'W49',
                                             ifelse(filt$dat_attivazione_dt >= '2018-05-28', 'W48',
                                                    ifelse(filt$dat_attivazione_dt >= '2018-05-21', 'W47',
                                                           ifelse(filt$dat_attivazione_dt >= '2018-05-14', 'W46',
                                                                  ifelse(filt$dat_attivazione_dt >= '2018-05-07', 'W45',
                                                                         ifelse(filt$dat_attivazione_dt >= '2018-04-30', 'W44',
                                                                                ifelse(filt$dat_attivazione_dt >= '2018-04-23', 'W43',
                                                                                       ifelse(filt$dat_attivazione_dt >= '2018-04-16', 'W42',
                                                                                              ifelse(filt$dat_attivazione_dt >= '2018-04-09', 'W41',
                                                                                                     ifelse(filt$dat_attivazione_dt >= '2018-04-02', 'W40',
                                                                                                            'undefined')))))))))))))
View(head(filt,100))
nrow(filt) 
# 32.940 (vs 39k dati di Celeste)

ups_socio_demo_ups <- agg(
  groupBy(filt, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag", 
          "tenure_custom", "eta", "week_ups", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  id = countDistinct(filt$cod_cliente_cifrato))
View(head(ups_socio_demo_ups,100))

write.df(repartition( ups_socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_inb_Q4FY18_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



##################################################################################################################################################################


## Q2 FY19 #######################################################################################################################################################

## Digital ##
filt <- filter(filt_ups_dig, filt_ups_dig$dat_attivazione_dt >= '2018-10-01' & filt_ups_dig$dat_attivazione_dt <= '2018-11-04')
filt$week_ups <- ifelse(filt$dat_attivazione_dt >= '2018-11-05', 'W19',
                        ifelse(filt$dat_attivazione_dt >= '2018-10-29', 'W18',
                               ifelse(filt$dat_attivazione_dt >= '2018-10-22', 'W17',
                                      ifelse(filt$dat_attivazione_dt >= '2018-10-15', 'W16',
                                             ifelse(filt$dat_attivazione_dt >= '2018-10-08', 'W15',
                                                    ifelse(filt$dat_attivazione_dt >= '2018-10-01', 'W14',
                                                           'undefined'))))))
View(head(filt,100))
nrow(filt) 
# 26.345

ups_socio_demo_ups <- agg(
  groupBy(filt, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag", 
          "tenure_custom", "eta", "week_ups", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  id = countDistinct(filt$cod_cliente_cifrato))
View(head(ups_socio_demo_ups,100))

write.df(repartition( ups_socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_dig_Q2FY19_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



## Telefono inbound ##
filt <- filter(filt_ups_call_inb, filt_ups_call_inb$dat_attivazione_dt >= '2018-10-01' & filt_ups_call_inb$dat_attivazione_dt <= '2018-11-04')
filt$week_ups <- ifelse(filt$dat_attivazione_dt >= '2018-11-05', 'W19',
                        ifelse(filt$dat_attivazione_dt >= '2018-10-29', 'W18',
                               ifelse(filt$dat_attivazione_dt >= '2018-10-22', 'W17',
                                      ifelse(filt$dat_attivazione_dt >= '2018-10-15', 'W16',
                                             ifelse(filt$dat_attivazione_dt >= '2018-10-08', 'W15',
                                                    ifelse(filt$dat_attivazione_dt >= '2018-10-01', 'W14',
                                                           'undefined'))))))
View(head(filt,100))
nrow(filt) 
# 22.604

ups_socio_demo_ups <- agg(
  groupBy(filt, "des_mod_srv", "des_canale_contatto", "des_tipo_contatto", "des_area_nielsen", "des_tipo_pag", 
          "tenure_custom", "eta", "week_ups", "flg_skygo_def", "flg_sky_q_def", "flg_mv", "flg_vod_attivo", "flg_mysky_hd", "kpi_contatti"),
  id = countDistinct(filt$cod_cliente_cifrato))
View(head(ups_socio_demo_ups,100))

write.df(repartition( ups_socio_demo_ups, 1), path = "/user/stefano.mazzucca/up_post_disservizio_crm/ups_inb_Q2FY19_socio_demo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



##################################################################################################################################################################





#chiudi sessione
sparkR.stop()
