
#apri sessione
source("connection_R.R")
options(scipen = 1000)


## Ricerca di info disservizi WSC pre 13 agosto 2018


## Scarico navigaizoni #####################################################################################################################################

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")

skyitdev <- select(skyitdev_df, "external_id_post_evar",
                   "post_channel",
                   "post_pagename",
                   "post_visid_high",
                   "post_visid_low",
                   "post_visid_concatenated",
                   "date_time",
                   "page_url_post_evar","page_url_post_prop",
                   "secondo_livello_post_prop",
                   "terzo_livello_post_prop",
                   "visit_num",
                   "hit_source",
                   "exclude_hit",
                   "external_id_con_NL_post_evar",
                   "post_campaign",
                   "va_closer_id",
                   "canale_corporate_post_prop"
)

## FILTRO DATE
skyitdev <- withColumn(skyitdev, "date_time_dt", cast(skyitdev$date_time, "date"))
skyitdev <- withColumn(skyitdev, "date_time_ts", cast(skyitdev$date_time, "timestamp"))

skyitdev <- filter(skyitdev, skyitdev$date_time_dt >= '2018-01-01') 
skyitdev <- filter(skyitdev, skyitdev$date_time_dt <= '2018-08-31') 

## Scarico navigazioni
navigazioni <- filter(skyitdev, "post_channel = 'corporate' or post_channel = 'Guidatv'")

write.parquet(navigazioni, "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_corporate_gen_ago_2018.parquet", mode = "overwrite")



navigazioni <- read.parquet("/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_corporate_gen_ago_2018.parquet")
View(head(navigazioni,100))
nrow(navigazioni)
# 243.256.417                 # 176.128.479 (01/01/2018 - 31/07/2018)
# ver_dat <- summarize(navigazioni, min_data = min(navigazioni$date_time_ts), max_data = max(navigazioni$date_time_ts))
# View(head(ver_dat))
# #	2018-01-01 00:00:01     2018-08-31 23:59:59
nav_mal_wsc <- filter(navigazioni, "post_pagename LIKE 'sky:it:pagina_di_cortesia' ")

write.parquet(nav_mal_wsc, "/user/stefano.mazzucca/mal_wsc/scarico_mal_nav_wsc_gen_ago_2018.parquet", mode = "overwrite")



nav_mal_wsc <- read.parquet("/user/stefano.mazzucca/mal_wsc/scarico_mal_nav_wsc_gen_ago_2018.parquet")
View(head(nav_mal_wsc,100))
nrow(nav_mal_wsc)
# 1.255.954                  # 373.293 (01/01/2018 - 31/07/2018)


## Count utenti impattati da disservizi WSC ###########################################################################################################

nav_20_26_agosto <- filter(nav_mal_wsc, "date_time_dt >= '2018-08-20' and date_time_dt <= '2018-08-26'")
createOrReplaceTempView(nav_20_26_agosto, "nav_20_26_agosto")

nav_13_19_agosto <- filter(nav_mal_wsc, "date_time_dt >= '2018-08-13' and date_time_dt <= '2018-08-19'")
createOrReplaceTempView(nav_13_19_agosto, "nav_13_19_agosto")

nav_6_12_agosto <- filter(nav_mal_wsc, "date_time_dt >= '2018-08-06' and date_time_dt <= '2018-08-12'")
createOrReplaceTempView(nav_6_12_agosto, "nav_6_12_agosto")

nav_30_5_agosto <- filter(nav_mal_wsc, "date_time_dt >= '2018-07-30' and date_time_dt <= '2018-08-05'")
createOrReplaceTempView(nav_30_5_agosto, "nav_30_5_agosto")

nav_23_29_luglio <- filter(nav_mal_wsc, "date_time_dt >= '2018-07-23' and date_time_dt <= '2018-07-29'")
createOrReplaceTempView(nav_23_29_luglio, "nav_23_29_luglio")

nav_16_22_luglio <- filter(nav_mal_wsc, "date_time_dt >= '2018-07-16' and date_time_dt <= '2018-07-22'")
createOrReplaceTempView(nav_16_22_luglio, "nav_16_22_luglio")

nav_9_15_luglio <- filter(nav_mal_wsc, "date_time_dt >= '2018-07-09' and date_time_dt <= '2018-07-15'")
createOrReplaceTempView(nav_9_15_luglio, "nav_9_15_luglio")

nav_2_8_luglio <- filter(nav_mal_wsc, "date_time_dt >= '2018-07-02' and date_time_dt <= '2018-07-08'")
createOrReplaceTempView(nav_2_8_luglio, "nav_2_8_luglio")

nav_7_13_maggio <- filter(nav_mal_wsc, "date_time_dt >= '2018-05-07' and date_time_dt <= '2018-05-13'")
createOrReplaceTempView(nav_7_13_maggio, "nav_7_13_maggio")

nav_23_29_aprile <- filter(nav_mal_wsc, "date_time_dt >= '2018-04-23' and date_time_dt <= '2018-04-29'")
createOrReplaceTempView(nav_23_29_aprile, "nav_23_29_aprile")

nav_5_11_febbraio <- filter(nav_mal_wsc, "date_time_dt >= '2018-02-05' and date_time_dt <= '2018-02-11'")
createOrReplaceTempView(nav_5_11_febbraio, "nav_5_11_febbraio")

nav_8_14_gennaio <- filter(nav_mal_wsc, "date_time_dt >= '2018-01-08' and date_time_dt <= '2018-01-14'")
createOrReplaceTempView(nav_8_14_gennaio, "nav_8_14_gennaio")

nav_1_7_gennaio <- filter(nav_mal_wsc, "date_time_dt >= '2018-01-01' and date_time_dt <= '2018-01-07'")
createOrReplaceTempView(nav_1_7_gennaio, "nav_1_7_gennaio")



nav_mal_wsc_count <- sql("select external_id_post_evar, 
                                  count(post_visid_concatenated) as count_hit,
                                  min(date_time_ts) as primo_tentativo,
                                  max(date_time_ts) as ultimo_tentativo
                         from nav_30_5_agosto
                         group by external_id_post_evar ")
nav_mal_wsc_count <- arrange(nav_mal_wsc_count, desc(nav_mal_wsc_count$count_hit))
View(head(nav_mal_wsc_count,100))
nrow(nav_mal_wsc_count)
# 40.432    20_26_agosto WEEK 8
# 177.227   13_19_agosto WEEK 7
# 12.359    6_12_agosto
# 17.116    30_5_agosto
# 9.947     23_29_luglio
# 7.948     16_22_luglio
# 10.702    9_15_luglio
# 4.815     2_8_luglio
# 8.384     7_13_maggio
# 5.191     23_29_aprile
# 5.252     5_11_febbraio
# 5.957     8_14_gennaio
# 6.303     1_7_gennaio

createOrReplaceTempView(nav_mal_wsc_count, "nav_mal_wsc_count")
mean_median_count <- sql("select mean(count_hit) as media_count, 
                                percentile_approx(count_hit, 0.50) as mediana_count
                  from nav_mal_wsc_count")
View(head(mean_median_count))
# 2.508137  1       20_26_agosto WEEK 8
# 3.799652  2       13_19_agosto WEEK 7
# 2.537018  1       6_12_agosto
# 2.698995  1       30_5_agosto
# 2.321705  1       23_29_luglio
# 2.362984  1       16_22_luglio
# 2.27294   1       9_15_luglio
# 2.511319  1       2_8_luglio
# 1.803793  1       7_13_maggio
# 1.796764  1       23_29_aprile
# 2.112719  1       5_11_febbraio
# 1.96844   1       8_14_gennaio
# 1.974615  1       1_7_gennaio





## Count utenti sul TOT #########################################################################################################################################

nav_wsc <- filter(navigazioni, "terzo_livello_post_prop LIKE '%faidate%'")
nrow(nav_wsc)
# 44.113.373


createOrReplaceTempView(nav_mal_wsc, "nav_mal_wsc")
nav_mal_wsc_count <- sql("select external_id_post_evar, 
                                  count(post_visid_concatenated) as count_hit,
                                  min(date_time_ts) as primo_tentativo,
                                  max(date_time_ts) as ultimo_tentativo
                         from nav_mal_wsc
                         group by external_id_post_evar ")
nav_mal_wsc_count <- arrange(nav_mal_wsc_count, desc(nav_mal_wsc_count$count_hit))
View(head(nav_mal_wsc_count,100))
nrow(nav_mal_wsc_count)
# 363.679       ## utenti impattati dal disservizio

createOrReplaceTempView(nav_wsc, "nav_wsc")
nav_wsc_count <- sql("select external_id_post_evar, 
                         count(post_visid_concatenated) as count_hit,
                         min(date_time_ts) as primo_tentativo,
                         max(date_time_ts) as ultimo_tentativo
                         from nav_wsc
                         group by external_id_post_evar ")
nav_wsc_count <- arrange(nav_wsc_count, desc(nav_wsc_count$count_hit))
View(head(nav_wsc_count,100))
nrow(nav_wsc_count)
# 2.041.497     ## utenti che accedono al WSC


## calcolo % di disservizi sul totale: 
percentuale_utenti_impattatti_su_tot_accessi_wsc <- nrow(nav_mal_wsc_count) / nrow(nav_wsc_count)
# 0.1781433



createOrReplaceTempView(nav_mal_wsc_count, "nav_mal_wsc_count")
createOrReplaceTempView(nav_wsc, "nav_wsc")
nav_mal_wsc_rawdata <- sql("select distinct t1.*, t2.secondo_livello_post_prop, t2.date_time_ts
                       from nav_mal_wsc_count t1
                       left join nav_wsc t2
                       on t1.external_id_post_evar = t2.external_id_post_evar ")
View(head(nav_mal_wsc_rawdata,100))
nrow(nav_mal_wsc_rawdata)
# 13.502.124          ## 5.131.568 (01/01/2018 - 31/07/2018)
nav_mal_wsc_rawdata$flg_wsc_post_mal <- ifelse(nav_mal_wsc_rawdata$date_time_ts >= nav_mal_wsc_rawdata$primo_tentativo, 1, 0)
createOrReplaceTempView(nav_mal_wsc_rawdata, "nav_mal_wsc_rawdata")
nav_mal_wsc_def <- sql("select distinct external_id_post_evar, last(primo_tentativo) as primo_tentativo, 
                                  last(ultimo_tentativo) as ultimo_tentativo, 
                                  -- sum(count_hit) as count_hit, 
                                  max(flg_wsc_post_mal) as flg_wsc_post_mal
                         from nav_mal_wsc_rawdata
                         group by external_id_post_evar ")

createOrReplaceTempView(nav_mal_wsc_count, "nav_mal_wsc_count")
createOrReplaceTempView(nav_mal_wsc_def, "nav_mal_wsc_def")
nav_mal_wsc_def_1 <- sql("select distinct t2.*, t1.count_hit
                         from nav_mal_wsc_count t1
                         inner join nav_mal_wsc_def t2
                         on t1.external_id_post_evar = t2.external_id_post_evar")
nav_mal_wsc_def_1 <- arrange(nav_mal_wsc_def_1, desc(nav_mal_wsc_def_1$count_hit))
View(head(nav_mal_wsc_def_1,100))
nrow(nav_mal_wsc_def_1)
# 363.678          ## utenti impattati dal disservizio con flag di accesso al wsc post_disservizio


createOrReplaceTempView(nav_mal_wsc_def_1, "nav_mal_wsc_def_1")
ver_count_flg <- sql("select flg_wsc_post_mal, count(external_id_post_evar) as count
                 from nav_mal_wsc_def_1
                 group by flg_wsc_post_mal")
View(head(ver_count_flg,100))
# flg_wsc_post_mal  count
# 0                 103.480
# 1                 260.198





################################################################################################################################################################
## Focus su Week 7, Week 8 e Week 9
################################################################################################################################################################

nav_mal_wsc <- read.parquet("/user/stefano.mazzucca/mal_wsc/scarico_mal_nav_wsc_gen_ago_2018.parquet")
View(head(nav_mal_wsc,100))
nrow(nav_mal_wsc)
# 1.255.954 

nav_mal_wsc_w7 <- filter(nav_mal_wsc, nav_mal_wsc$date_time_dt >= '2018-08-13' & nav_mal_wsc$date_time_dt <= '2018-08-19')
View(head(nav_mal_wsc_w7,100))
nrow(nav_mal_wsc_w7)
# 673.401

nav_mal_wsc_w8 <- filter(nav_mal_wsc, nav_mal_wsc$date_time_dt >= '2018-08-20' & nav_mal_wsc$date_time_dt <= '2018-08-26')
View(head(nav_mal_wsc_w8,100))
nrow(nav_mal_wsc_w8)
# 101.409

nav_mal_wsc_w9 <- read.parquet("/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_malfunzionamento_wsc_20180903.parquet")
View(head(nav_mal_wsc_w9,100))
nrow(nav_mal_wsc_w9)
# 50.914

## unisco week 7, 8 e 9: 
nav_mal_wsc_target <- rbind(nav_mal_wsc_w7, nav_mal_wsc_w8, nav_mal_wsc_w9)
View(head(nav_mal_wsc_target,100))
nrow(nav_mal_wsc_target)
# 825.724

# valdist_extid <-distinct(select(nav_mal_wsc_target, "external_id_post_evar"))
# nrow(valdist_extid)
# # 226.404

nav_mal_wsc_target_1 <- withColumn(nav_mal_wsc_target, "weekday", date_format(nav_mal_wsc_target$date_time_dt, 'EEEE'))
nav_mal_wsc_target_2 <- withColumn(nav_mal_wsc_target_1, "orario_disservizio", hour(nav_mal_wsc_target_1$date_time_ts))
View(head(nav_mal_wsc_target_2,100))

# createOrReplaceTempView(nav_mal_wsc_target_2, "nav_mal_wsc_target_2")
# nav_mal_wsc_target_3 <- sql("select external_id_post_evar, date_time_dt, weekday, 
#                                     count(post_visid_concatenated) as count_hit,
#                                     case when orario_disservizio LIKE '0' then 1 else NULL end as fascia_h_0_1, 
#                                     case when orario_disservizio LIKE '1' then 1 else NULL end as fascia_h_1_2, 
#                                     case when orario_disservizio LIKE '2' then 1 else NULL end as fascia_h_2_3, 
#                                     case when orario_disservizio LIKE '3' then 1 else NULL end as fascia_h_3_4, 
#                                     case when orario_disservizio LIKE '4' then 1 else NULL end as fascia_h_4_5, 
#                                     case when orario_disservizio LIKE '5' then 1 else NULL end as fascia_h_5_6, 
#                                     case when orario_disservizio LIKE '6' then 1 else NULL end as fascia_h_6_7, 
#                                     case when orario_disservizio LIKE '7' then 1 else NULL end as fascia_h_7_8, 
#                                     case when orario_disservizio LIKE '8' then 1 else NULL end as fascia_h_8_9, 
#                                     case when orario_disservizio LIKE '9' then 1 else NULL end as fascia_h_9_10, 
#                                     case when orario_disservizio LIKE '10' then 1 else NULL end as fascia_h_10_11, 
#                                     case when orario_disservizio LIKE '11' then 1 else NULL end as fascia_h_11_12, 
#                                     case when orario_disservizio LIKE '12' then 1 else NULL end as fascia_h_12_13, 
#                                     case when orario_disservizio LIKE '13' then 1 else NULL end as fascia_h_13_14, 
#                                     case when orario_disservizio LIKE '14' then 1 else NULL end as fascia_h_14_15, 
#                                     case when orario_disservizio LIKE '15' then 1 else NULL end as fascia_h_15_16, 
#                                     case when orario_disservizio LIKE '16' then 1 else NULL end as fascia_h_16_17,
#                                     case when orario_disservizio LIKE '17' then 1 else NULL end as fascia_h_17_18, 
#                                     case when orario_disservizio LIKE '18' then 1 else NULL end as fascia_h_18_19, 
#                                     case when orario_disservizio LIKE '19' then 1 else NULL end as fascia_h_19_20, 
#                                     case when orario_disservizio LIKE '20' then 1 else NULL end as fascia_h_20_21, 
#                                     case when orario_disservizio LIKE '21' then 1 else NULL end as fascia_h_21_22, 
#                                     case when orario_disservizio LIKE '22' then 1 else NULL end as fascia_h_22_23, 
#                                     case when orario_disservizio LIKE '23' then 1 else NULL end as fascia_h_23_24
#                             from nav_mal_wsc_target_2
#                             group by external_id_post_evar, date_time_dt, orario_disservizio, weekday")
# nav_mal_wsc_target_4 <- filter(nav_mal_wsc_target_3, "external_id_post_evar is NOT NULL")
# nav_mal_wsc_target_5 <- arrange(nav_mal_wsc_target_4, asc(nav_mal_wsc_target_4$external_id_post_evar), asc(nav_mal_wsc_target_4$date_time_dt))
# View(head(nav_mal_wsc_target_5,100))
# nrow(nav_mal_wsc_target_5)
# # 371.847

## OPPURE ##
createOrReplaceTempView(nav_mal_wsc_target_2, "nav_mal_wsc_target_2")
nav_mal_wsc_target_3 <- sql("select external_id_post_evar, date_time_dt, weekday, 
                                    case when orario_disservizio LIKE '0' then 'fascia_0_1'   
                                      when orario_disservizio LIKE '1' then 'fascia_1_2' 
                                      when orario_disservizio LIKE '2' then 'fascia_2_3' 
                                      when orario_disservizio LIKE '3' then 'fascia_3_4' 
                                      when orario_disservizio LIKE '4' then 'fascia_4_5' 
                                      when orario_disservizio LIKE '5' then 'fascia_5_6' 
                                      when orario_disservizio LIKE '6' then 'fascia_6_7' 
                                      when orario_disservizio LIKE '7' then 'fascia_7_8' 
                                      when orario_disservizio LIKE '8' then 'fascia_8_9' 
                                      when orario_disservizio LIKE '9' then 'fascia_9_10' 
                                      when orario_disservizio LIKE '10' then 'fascia_10_11' 
                                      when orario_disservizio LIKE '11' then 'fascia_11_12' 
                                      when orario_disservizio LIKE '12' then 'fascia_12_13' 
                                      when orario_disservizio LIKE '13' then 'fascia_13_14' 
                                      when orario_disservizio LIKE '14' then 'fascia_14_15' 
                                      when orario_disservizio LIKE '15' then 'fascia_15_16' 
                                      when orario_disservizio LIKE '16' then 'fascia_16_17' 
                                      when orario_disservizio LIKE '17' then 'fascia_17_18' 
                                      when orario_disservizio LIKE '18' then 'fascia_18_19' 
                                      when orario_disservizio LIKE '19' then 'fascia_19_20' 
                                      when orario_disservizio LIKE '20' then 'fascia_20_21' 
                                      when orario_disservizio LIKE '21' then 'fascia_21_22' 
                                      when orario_disservizio LIKE '22' then 'fascia_22_23' 
                                      when orario_disservizio LIKE '23' then 'fascia_23_24' else NULL end as fascia_oraria_disservizio,
                                    count(post_visid_concatenated) as count_hit
                            from nav_mal_wsc_target_2
                            group by external_id_post_evar, date_time_dt, orario_disservizio, weekday")
nav_mal_wsc_target_4 <- filter(nav_mal_wsc_target_3, "external_id_post_evar is NOT NULL")
nav_mal_wsc_target_5 <- arrange(nav_mal_wsc_target_4, asc(nav_mal_wsc_target_4$external_id_post_evar), asc(nav_mal_wsc_target_4$date_time_dt))
View(head(nav_mal_wsc_target_5,100))
nrow(nav_mal_wsc_target_5)
# 371.847

nav_mal_wsc_target_5$week <- ifelse(nav_mal_wsc_target_5$date_time_dt >= '2018-08-13' & nav_mal_wsc_target_5$date_time_dt <= '2018-08-19', "week 7", 
                                        ifelse(nav_mal_wsc_target_5$date_time_dt >= '2018-08-20' & nav_mal_wsc_target_5$date_time_dt <= '2018-08-26', "week 8",
                                               "week 9"))
View(head(nav_mal_wsc_target_5,100))
nrow(nav_mal_wsc_target_5)
# 371.847

nav_mal_wsc_target_6 <- select(nav_mal_wsc_target_5, "external_id_post_evar", "week", "date_time_dt", "weekday", "fascia_oraria_disservizio", "count_hit")
View(head(nav_mal_wsc_target_6,100))


write.df(repartition( nav_mal_wsc_target_6, 1), path = "/user/stefano.mazzucca/mal_wsc/info_utenti_malfunzionamento_wsc_fasce_orarie_week_7_8_9.csv", 
         "csv", sep=";", mode = "overwrite", header=TRUE)







#chiudi sessione
sparkR.stop()
