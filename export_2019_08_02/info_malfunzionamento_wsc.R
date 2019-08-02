
## Estrazione info MALFUNZIONAMENTO WSC


#apri sessione
source("connection_R.R")
options(scipen = 1000)


data_inizio_analisi <- as.Date('2018-10-22') # inserire il lunedi della settimana precedente all'analisi
data_fine_analisi <- as.Date('2018-10-29')

path_scarico_navigazioni <- "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_corporate_20181029.parquet"
path_nav_mal_wsc <- "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_malfunzionamento_wsc_20181029.parquet" 
path_info_csv <- "/user/stefano.mazzucca/mal_wsc/info_utenti_malfunzionamento_wsc_20181029.csv"
path_info_def_csv <- "/user/stefano.mazzucca/mal_wsc/info_utenti_malfunzionamento_wsc_20181029_def.csv"

path_scarico_nav_app <- "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_app_20181029.parquet"

## N.B.: Aggiornato al 20 Agosto 2018!!
path_diz_coo <- "/user/stefano.mazzucca/info_dazn/diz_cookie.parquet" 

path_kpi_sito <- "/user/stefano.mazzucca/mal_wsc/kpi_navigazioni_sito_20181029.parquet"
path_kpi_app <- "/user/stefano.mazzucca/mal_wsc/kpi_navigazioni_app_20181029.parquet"
path_kpi_tot <- "/user/stefano.mazzucca/mal_wsc/kpi_navigazioni_tot_20181029.parquet"
path_kpi_nav_csv <- "/user/stefano.mazzucca/mal_wsc/kpi_finali_navigazioni_tot_20181029.csv"


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

skyitdev <- filter(skyitdev, skyitdev$date_time_dt >= data_inizio_analisi) # & skyitdev$date_time_dt < data_fine_analisi) 

# test_date <- summarize(skyitdev, min_data = min(skyitdev$date_time_dt), max_data = max(skyitdev$date_time_dt)
# View(head(test_date,100))

## Scarico navigazioni
navigazioni <- filter(skyitdev, "post_channel = 'corporate' or post_channel = 'Guidatv'")


write.parquet(navigazioni, path_scarico_navigazioni, mode = "overwrite")
# "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_corporate_20180903.parquet"   <- 2018_09_03
# "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_corporate_20180827.parquet"   <- 2018_08_27


navigazioni <- read.parquet(path_scarico_navigazioni)
View(head(navigazioni,100))
nrow(navigazioni)
# 7.685.881
ver_dat <- summarize(navigazioni, min_data = min(navigazioni$date_time_ts), max_data = max(navigazioni$date_time_ts))
View(head(ver_dat))
#	2018-10-22    2018-10-28 23:59:59
nav_mal_wsc <- filter(navigazioni, "post_pagename LIKE 'sky:it:pagina_di_cortesia' ")


write.parquet(nav_mal_wsc, path_nav_mal_wsc, mode = "overwrite")
# "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_malfunzionamento_wsc_20180903.parquet"    <- 2018_09_03
# "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_malfunzionamento_wsc_20180827.parquet"    <- 2018_08_27
# "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_malfunzionamento_wsc.parquet"             <- 2018_08_20 



## Per calcolare gli external_id riconsociuti impattati dal malfunizonamento di WSC: ###############################################################

nav_mal_wsc <- read.parquet(path_nav_mal_wsc)
View(head(nav_mal_wsc,100))
nrow(nav_mal_wsc)
# 17.908     <- 2018_10_29
# 46.305     <- 2018_10_22
# 21.066     <- 2018_10_15
# 21.399     <- 2018_10_08
# 31.628     <- 2018_10_01
# 34.143     <- 2018_09_24
# 26.061     <- 2018_09_17
# 24.555     <- 2018_09_10
# 50.914     <- 2018_09_03 
# 101.409    <- 2018_08_27
# 683.066    <- 2018_08_20

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
# 9.879     <- 2018_10_29
# 21.981    <- 2018_10_22
# 10.144    <- 2018_10_15
# 10.996    <- 2018_10_08
# 17.354    <- 2018_10_01
# 17.931    <- 2018_09_24
# 12.083    <- 2018_09_17
# 11.580    <- 2018_09_10
# 24.516    <- 2018_09_03
# 40.432    <- 2018_08_27
# 179.605   <- 2018_08_20

verifica_date <- summarize(nav_mal_wsc_count, min_data = min(nav_mal_wsc_count$primo_tentativo), max_data = max(nav_mal_wsc_count$ultimo_tentativo))
View(head(verifica_date))
# min_data              max_data
# 2018-10-22 00:00:16   2018-10-28 23:59:58


createOrReplaceTempView(nav_mal_wsc_count, "nav_mal_wsc_count")
mean_median_count <- sql("select mean(count_hit) as media_count, 
                                percentile_approx(count_hit, 0.50) as mediana_count
                  from nav_mal_wsc_count")
View(head(mean_median_count))
# media_count     mediana_count
# 1.812734        1               <- 2018_10_29
# 2.106592        1               <- 2018_10_22
# 2.076696        1               <- 2018_10_15
# 1.946071        1               <- 2018_10_08
# 1.822519        1               <- 2018_10_01
# 1.904133        1               <- 2018_09_24
# 2.156832        1               <- 2018_09_17
# 2.120466        1               <- 2018_09_10
# 2.076766        1               <- 2018_09_03
# 2.509033        1               <- 2018_08_27 
# 3.803157        2               <- 2018_08_20



## Per calcolare i cookies NON riconsociuti on top alla lista degli external_id "danneggiati": ###############################################################

createOrReplaceTempView(nav_mal_wsc, "nav_mal_wsc")
nav_mal_wsc_count_coo <- sql("select external_id_post_evar, post_visid_concatenated,
                                  count(post_visid_concatenated) as count_hit,
                                  min(date_time_ts) as primo_tentativo,
                                  max(date_time_ts) as ultimo_tentativo
                         from nav_mal_wsc
                         group by external_id_post_evar, post_visid_concatenated")
nav_mal_wsc_count_coo <- arrange(nav_mal_wsc_count_coo, desc(nav_mal_wsc_count_coo$count_hit))
View(head(nav_mal_wsc_count_coo,100))
nrow(nav_mal_wsc_count_coo)
# 11.275                            <- 2018_10_29
# 24.557                            <- 2018_10_22
# 11.564                            <- 2018_10_15
# 12.530                            <- 2018_10_08
# 19.426                            <- 2018_10_01
# 20.315                            <- 2018_09_24
# 14.270                            <- 2018_09_17
# 13.653                            <- 2018_09_10
# 28.255                            <- 2018_09_03
# 47.047 considerando i cookies     <- 2018_08_27
# 219.932 considerando i cookies    <- 2018_08_20

filt <- filter(nav_mal_wsc_count_coo, "external_id_post_evar is NULL or
               external_id_post_evar LIKE 'null' or
               external_id_post_evar LIKE 'n-a' ")
View(head(filt,100))
nrow(filt)
# 903 cookies non riconosciuti    <- 2018_10_29
# 1.215 cookies non riconosciuti    <- 2018_10_22
# 813   cookies non riconosciuti    <- 2018_10_15
# 905   cookies non riconosciuti    <- 2018_10_08
# 1.176 cookies non riconosciuti    <- 2018_10_01
# 1.363 cookies non riconosciuti    <- 2018_09_24
# 1.276 cookies non riconosciuti    <- 2018_09_17
# 1.244 cookies non riconosciuti    <- 2018_09_10
# 2.095 cookies non riconosciuti    <- 2018_09_03
# 2.882 cookies non riconosciuti    <- 2018_08_27
# 7.832 cookie non riconosciuti     <- 2018_08_20




write.df(repartition( nav_mal_wsc_count, 1), path = path_info_csv, "csv", sep=";", mode = "overwrite", header=TRUE)
# "/user/stefano.mazzucca/mal_wsc/info_utenti_malfunzionamento_wsc_20180903.csv"    <- 2019_09_03
# "/user/stefano.mazzucca/mal_wsc/info_utenti_malfunzionamento_wsc_20180827.csv"    <- 2018_08_27
# "/user/stefano.mazzucca/mal_wsc/info_utenti_malfunzionamento_wsc.csv"             <- 2018_08_20




##############################################################################################################################################################
## Elaborazioni per differenziare gli utenti "impattati" che sono riusciti successivamente a loggarsi: #######################################################
##############################################################################################################################################################

navigazioni <- read.parquet(path_scarico_navigazioni)
View(head(navigazioni,100))
nrow(navigazioni)
# 7.685.881
nav_wsc <- filter(navigazioni, "terzo_livello_post_prop LIKE '%faidate%'")
nrow(nav_wsc)
# 1.393.311


nav_mal_wsc <- read.parquet(path_nav_mal_wsc)
View(head(nav_mal_wsc,100))
nrow(nav_mal_wsc)
# 17.908
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
# 9.879        ## utenti impattati dal disservizio

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
# 216.029       ## utenti che accedono a WSC

createOrReplaceTempView(nav_mal_wsc_count, "nav_mal_wsc_count")
createOrReplaceTempView(nav_wsc, "nav_wsc")
nav_mal_wsc_rawdata <- sql("select distinct t1.*, t2.secondo_livello_post_prop, t2.date_time_ts
                       from nav_mal_wsc_count t1
                       left join nav_wsc t2
                       on t1.external_id_post_evar = t2.external_id_post_evar ")
View(head(nav_mal_wsc_rawdata,100))
nrow(nav_mal_wsc_rawdata)
# 70.983

# nav_mal_wsc_def$flg_wsc_mal <- ifelse(nav_mal_wsc_def$date_time_ts <= nav_mal_wsc_def$primo_tentativo, "accesso_pre_mal", 
#                                            ifelse(nav_mal_wsc_def$date_time_ts >= nav_mal_wsc_def$primo_tentativo & 
#                                                   nav_mal_wsc_def$date_time_ts <= nav_mal_wsc_def$ultimo_tentativo, "accesso_intra_mal", 
#                                                   ifelse(nav_mal_wsc_def$date_time_ts >= nav_mal_wsc_def$ultimo_tentativo, "accesso_post_mal", "NO_accesso")))
# View(head(nav_mal_wsc_def,1000))

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
# 9.878          ## utenti impattati dal disservizio con flag di accesso al wsc post_disservizio


createOrReplaceTempView(nav_mal_wsc_def_1, "nav_mal_wsc_def_1")
ver_count_flg <- sql("select flg_wsc_post_mal, count(external_id_post_evar) as count
                 from nav_mal_wsc_def_1
                 group by flg_wsc_post_mal")
View(head(ver_count_flg,100))
# flg_wsc_post_mal  count
# 0                 5264
# 1                 4614


write.df(repartition( nav_mal_wsc_def_1, 1), path = path_info_def_csv, "csv", sep=";", mode = "overwrite", header=TRUE)
# "/user/stefano.mazzucca/mal_wsc/info_utenti_malfunzionamento_wsc_def_20180903.csv"    <- 2018_09_03
# "/user/stefano.mazzucca/mal_wsc/info_utenti_malfunzionamento_wsc_def_20180827.csv"    <- 2018_08_27 



## FOCUS orari disservizio WSC:


nav_mal_wsc_orario <- withColumn(nav_mal_wsc, "weekday", date_format(nav_mal_wsc$date_time_dt, 'EEEE'))
nav_mal_wsc_orario <- withColumn(nav_mal_wsc_orario, "orario_disservizio", hour(nav_mal_wsc_orario$date_time_ts))
View(head(nav_mal_wsc_orario,100))
nrow(nav_mal_wsc_orario)
# 46.305

createOrReplaceTempView(nav_mal_wsc_orario, "nav_mal_wsc_orario")
nav_mal_wsc_orario <- sql("select external_id_post_evar, date_time_dt, weekday, 
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
                            from nav_mal_wsc_orario
                            group by external_id_post_evar, date_time_dt, orario_disservizio, weekday")
nav_mal_wsc_orario <- filter(nav_mal_wsc_orario, "external_id_post_evar is NOT NULL")
nav_mal_wsc_orario <- arrange(nav_mal_wsc_orario, asc(nav_mal_wsc_orario$external_id_post_evar), asc(nav_mal_wsc_orario$date_time_dt))
View(head(nav_mal_wsc_orario,100))
nrow(nav_mal_wsc_orario)
# 27.769

write.df(repartition( nav_mal_wsc_orario, 1), path = "/user/stefano.mazzucca/mal_wsc/export_fasce_orarie_20181021.csv", 
         "csv", sep=";", mode = "overwrite", header=TRUE)





## Se avessi inserito "flg_wsc_mal" con le 4 opzioni "no", "pre", "intra" o "post_accesso_wsc": #################################################################

# count_accessi <- summarize(groupBy(nav_mal_wsc_def_1, "flg_wsc_mal"), count = count(nav_mal_wsc_def_1$external_id_post_evar))
# View(head(count_accessi,100))
# count_extid <- summarize(groupBy(nav_mal_wsc_def_1, "external_id_post_evar"), count = count(nav_mal_wsc_def_1$external_id_post_evar))
# count_extid <- arrange(count_extid, desc(count_extid$count))
# View(head(count_extid,1000))
# count_count_extid <- summarize(groupBy(count_extid, "count"), count = count(count_extid$external_id_post_evar))
# View(head(count_count_extid))
# 
# #### Calcolo sovrapposizone NO_accesso + accesso_pre_mal:
# filt_no_accessi <- filter(nav_mal_wsc_def_1, "flg_wsc_mal LIKE 'NO_accesso'")
# nrow(filt_no_accessi)
# # 13.180
# filt_accesso_pre_mal <- filter(nav_mal_wsc_def_1, "flg_wsc_mal LIKE 'accesso_pre_mal'")
# nrow(filt_accesso_pre_mal)
# # 17.672
# filt_accesso_intra_mal <- filter(nav_mal_wsc_def_1, "flg_wsc_mal LIKE 'accesso_intra_mal'")
# nrow(filt_accesso_intra_mal)
# # 3.137
# filt_accesso_post_mal <- filter(nav_mal_wsc_def_1, "flg_wsc_mal LIKE 'accesso_post_mal'")
# nrow(filt_accesso_post_mal)
# # 18.559
# 
# createOrReplaceTempView(filt_no_accessi, "filt_no_accessi")
# createOrReplaceTempView(filt_accesso_pre_mal, "filt_accesso_pre_mal")
# join_no_pre <- sql("select distinct t1.external_id_post_evar
#                    from filt_no_accessi t1
#                    inner join filt_accesso_pre_mal t2
#                    on t1.external_id_post_evar = t2.external_id_post_evar")
# nrow(join_no_pre)
# # 0
# 
# createOrReplaceTempView(filt_no_accessi, "filt_no_accessi")
# createOrReplaceTempView(filt_accesso_intra_mal, "filt_accesso_intra_mal")
# join_no_intra <- sql("select distinct t1.external_id_post_evar
#                    from filt_no_accessi t1
#                    inner join filt_accesso_intra_mal t2
#                    on t1.external_id_post_evar = t2.external_id_post_evar")
# nrow(join_no_intra)
# # 0
# 
# createOrReplaceTempView(filt_no_accessi, "filt_no_accessi")
# createOrReplaceTempView(filt_accesso_post_mal, "filt_accesso_post_mal")
# join_no_post <- sql("select distinct t1.external_id_post_evar
#                    from filt_no_accessi t1
#                    inner join filt_accesso_post_mal t2
#                    on t1.external_id_post_evar = t2.external_id_post_evar")
# nrow(join_no_post)
# # 0
# 
# createOrReplaceTempView(filt_accesso_pre_mal, "filt_accesso_pre_mal")
# createOrReplaceTempView(filt_accesso_intra_mal, "filt_accesso_intra_mal")
# join_pre_intra <- sql("select distinct t1.external_id_post_evar
#                    from filt_accesso_pre_mal t1
#                    inner join filt_accesso_intra_mal t2
#                    on t1.external_id_post_evar = t2.external_id_post_evar")
# nrow(join_pre_intra)
# # 1.874
# 
# createOrReplaceTempView(filt_accesso_intra_mal, "filt_accesso_intra_mal")
# createOrReplaceTempView(filt_accesso_post_mal, "filt_accesso_post_mal")
# join_intra_post <- sql("select distinct t1.external_id_post_evar
#                    from filt_accesso_intra_mal t1
#                    inner join filt_accesso_post_mal t2
#                    on t1.external_id_post_evar = t2.external_id_post_evar")
# nrow(join_intra_post)
# # 1.884
# 
# createOrReplaceTempView(filt_accesso_pre_mal, "filt_accesso_pre_mal")
# createOrReplaceTempView(filt_accesso_post_mal, "filt_accesso_post_mal")
# join_pre_post <- sql("select distinct t1.external_id_post_evar
#                    from filt_accesso_pre_mal t1
#                    inner join filt_accesso_post_mal t2
#                    on t1.external_id_post_evar = t2.external_id_post_evar")
# nrow(join_pre_post)
# # 9.525




################################################################################################################################################################
################################################################################################################################################################
## Descrittive sulle navigazioni ##
################################################################################################################################################################
################################################################################################################################################################

## Sito corporate

# "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_dal13agosto.parquet"          <- scarico del 2018_08_20
# "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_corporate_20180827.parquet"   <- scarico del 2018_08_27
navigazioni_sito <- read.parquet(path_scarico_navigazioni)
View(head(navigazioni_sito,100))
nrow(navigazioni_sito)
# 14.374.617


## App WSC

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod/table=hitdata/hitdata.parquet")
skyappwsc_1 <- select(skyappwsc, "external_id_post_evar",
                      "post_visid_high",
                      "post_visid_low",
                      "post_visid_concatenated",
                      "visit_num",
                      "date_time",
                      "post_channel", 
                      "page_name_post_evar",
                      "page_url_post_prop",
                      "site_section",
                      "hit_source",
                      "exclude_hit"
)
skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_ts", cast(skyappwsc_2$date_time, "timestamp"))
skyappwsc_4 <- filter(skyappwsc_3, "external_id_post_evar is not NULL")

# filtro temporale
skyappwsc_5 <- filter(skyappwsc_4, skyappwsc_4$date_time_dt >= data_inizio_analisi)


write.parquet(skyappwsc_5, path_scarico_nav_app)
# "/user/stefano.mazzucca/mal_wsc/scarico_navigazioni_app_dal13agosto.parquet"    <- scarico navigazioni app 2018_08_20



## Recap + elaborazioni ##

navigazioni_sito <- read.parquet(path_scarico_navigazioni)
View(head(navigazioni_sito,100))
nrow(navigazioni_sito)
# 193.691.778

navigazioni_app <- read.parquet(path_scarico_nav_app)
View(head(navigazioni_app,100))
nrow(navigazioni_app)
# 8.919.076

diz <- read.parquet(path_diz_coo)
View(head(diz,100))
nrow(diz)
# 364.729.241

# unique_id_sito <- distinct(select(navigazioni_sito, "external_id_post_evar"))
# nrow(unique_id_sito)
# # 1.074.718


source("Utils.R")


navigazioni_sito_1 <- filter(navigazioni_sito, "post_channel = 'corporate' or post_channel = 'Guidatv'")

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
navigazioni_sito_2 <- remove_too_short(navigazioni_sito_1, 5)

# KPI SITO ----------------------------------------------------------------
navigazioni_sito_3 <- withColumn(navigazioni_sito_2, "TARGET_dt", lit(data_fine_analisi))
navigazioni_sito_4 <- group_kpi_sito(navigazioni_sito_3)

write.parquet(navigazioni_sito_4, path_kpi_sito)
# "/user/stefano.mazzucca/mal_wsc/kpi_navigazioni_sito_dal13agosto.parquet"   <- kpi_sito 2018_08_20

kpi_navigazioni_sito <- read.parquet(path_kpi_sito)
View(head(kpi_navigazioni_sito,100))
nrow(kpi_navigazioni_sito)
# 786.940


navigazioni_app <- remove_too_short(navigazioni_app, 3)

# KPI APP -----------------------------------------------------------------
navigazioni_app_1 <- withColumn(navigazioni_app, "TARGET_dt", lit(data_fine_analisi))
navigazioni_app_2 <- group_kpi_app(navigazioni_app_1)

write.parquet(navigazioni_app_2, path_kpi_app)
# "/user/stefano.mazzucca/mal_wsc/kpi_navigazioni_app_dal13agosto.parquet"    <- kpi_app 2018_08_20

kpi_navigazioni_app <- read.parquet(path_kpi_app)
View(head(kpi_navigazioni_app,100))
nrow(kpi_navigazioni_app)
# 367.731


# MERGE KPI ---------------------------------------------------------------
kpi_navigazioni_sito <- rename_with_list(kpi_navigazioni_sito, "_sito")
kpi_navigazioni_sito <- withColumnRenamed(kpi_navigazioni_sito, "external_id_post_evar_sito", "external_id_post_evar")
kpi_navigazioni_app <- rename_with_list(kpi_navigazioni_app, "_app")
kpi_navigazioni_app <- withColumnRenamed(kpi_navigazioni_app, "external_id_post_evar_app", "external_id_post_evar")

kpi_navigazioni <- join_external_id_post_evar(kpi_navigazioni_sito, kpi_navigazioni_app)
kpi_navigazioni_2 <- melt_kpi(kpi_navigazioni)


write.parquet(kpi_navigazioni_2, path_kpi_tot, mode = "overwrite")
# "/user/stefano.mazzucca/mal_wsc/kpi_navigazioni_tot_dal13agosto.parquet"    <- kpi_tot_navigazioni 2018_08_20


kpi_navigazioni <- read.parquet(path_kpi_tot)
View(head(kpi_navigazioni,100))
nrow(kpi_navigazioni)
# 786.940

extid_mal_wsc <- filter(nav_mal_wsc_count, "external_id_post_evar is not NULL")
extid_mal_wsc <- distinct(select(extid_mal_wsc, "external_id_post_evar"))
nrow(extid_mal_wsc)
# 179.604


## Join tra gli ext_id impattati dal malfunzionamento e le navigazioni:

createOrReplaceTempView(extid_mal_wsc, "extid_mal_wsc")
createOrReplaceTempView(kpi_navigazioni, "kpi_navigazioni")

nav_extid_mal_wsc <- sql("select distinct t1.*
                         from kpi_navigazioni t1
                         inner join extid_mal_wsc t2
                         on t1.external_id_post_evar = t2.external_id_post_evar")
View(head(nav_extid_mal_wsc,100))
nrow(nav_extid_mal_wsc)
# 179.604

nav_extid_mal_wsc <- select(nav_extid_mal_wsc, retain)

nav_extid_mal_wsc <- withColumnRenamed(nav_extid_mal_wsc, "visite_Fatture_sito", "visite_Fatture_sitoWSC")
nav_extid_mal_wsc <- withColumnRenamed(nav_extid_mal_wsc, "visite_FaiDaTeExtra_sito", "visite_Extra_sitoWSC")
nav_extid_mal_wsc <- withColumnRenamed(nav_extid_mal_wsc, "visite_ArricchisciAbbonamento_sito", "visite_ArricchisciAbbonamento_sitoWSC")
nav_extid_mal_wsc <- withColumnRenamed(nav_extid_mal_wsc, "visite_Gestione_sito", "visite_Gestione_sitoWSC")
nav_extid_mal_wsc <- withColumnRenamed(nav_extid_mal_wsc, "visite_Contatti_sito", "visite_AssistenzaContatta_sito")

nav_extid_mal_wsc$visite_Trasloco_sito <- NULL
nav_extid_mal_wsc$visite_TrasparenzaTariffaria_sito <- NULL
nav_extid_mal_wsc$visite_TrovaSkyService_sito <- NULL
nav_extid_mal_wsc$visite_SearchDisdetta_sito <- NULL
nav_extid_mal_wsc$visite_SkyExpert_sito <- NULL
nav_extid_mal_wsc$visite_Trigger_sito <- NULL


nav_extid_mal_wsc$flg_fatture_sitowsc <- ifelse(nav_extid_mal_wsc$visite_Fatture_sitoWSC >= 1, 1, 0)
nav_extid_mal_wsc$visite_Fatture_sitoWSC <- NULL

nav_extid_mal_wsc$flg_extra_sitowsc <- ifelse(nav_extid_mal_wsc$visite_Extra_sitoWSC >= 1, 1, 0)
nav_extid_mal_wsc$visite_Extra_sitoWSC <- NULL

nav_extid_mal_wsc$flg_ArricchisciAbbonamento_sitowsc <- ifelse(nav_extid_mal_wsc$visite_ArricchisciAbbonamento_sitoWSC >= 1, 1, 0)
nav_extid_mal_wsc$visite_ArricchisciAbbonamento_sitoWSC <- NULL

nav_extid_mal_wsc$flg_gestione_sitowsc <- ifelse(nav_extid_mal_wsc$visite_Gestione_sitoWSC >= 1, 1, 0)
nav_extid_mal_wsc$visite_Gestione_sitoWSC <- NULL

nav_extid_mal_wsc$flg_AssistenzaContatta_sito <- ifelse(nav_extid_mal_wsc$visite_AssistenzaContatta_sito >= 1, 1, 0)
nav_extid_mal_wsc$visite_AssistenzaContatta_sito <- NULL

nav_extid_mal_wsc$flg_comunicazione_sito <- ifelse(nav_extid_mal_wsc$visite_Comunicazione_sito >= 1, 1, 0)
nav_extid_mal_wsc$visite_Comunicazione_sito <- NULL

nav_extid_mal_wsc$flg_assistenza_sito <- ifelse(nav_extid_mal_wsc$visite_Assistenza_sito >= 1, 1, 0)
nav_extid_mal_wsc$visite_Assistenza_sito <- NULL

nav_extid_mal_wsc$flg_GuidaTv_sito <- ifelse(nav_extid_mal_wsc$visite_GuidaTv_sito >= 1, 1, 0)
nav_extid_mal_wsc$visite_GuidaTv_sito <- NULL

nav_extid_mal_wsc$flg_PaginaDiServizio_sito <- ifelse(nav_extid_mal_wsc$visite_PaginaDiServizio_sito >= 1, 1, 0)
nav_extid_mal_wsc$visite_PaginaDiServizio_sito <- NULL

nav_extid_mal_wsc$flg_extra_sito <- ifelse(nav_extid_mal_wsc$visite_Extra_sito >= 1, 1, 0)
nav_extid_mal_wsc$visite_Extra_sito <- NULL

nav_extid_mal_wsc$flg_tecnologia_sito <- ifelse(nav_extid_mal_wsc$visite_Tecnologia_sito >= 1, 1, 0)
nav_extid_mal_wsc$visite_Tecnologia_sito <- NULL

nav_extid_mal_wsc$flg_PacchettiOfferte_sito <- ifelse(nav_extid_mal_wsc$visite_PacchettiOfferte_sito >= 1, 1, 0)
nav_extid_mal_wsc$visite_PacchettiOfferte_sito <- NULL

nav_extid_mal_wsc$flg_InfoDisdetta_sito <- ifelse(nav_extid_mal_wsc$visite_InfoDisdetta_sito >= 1, 1, 0)
nav_extid_mal_wsc$visite_InfoDisdetta_sito <- NULL

nav_extid_mal_wsc$flg_visite_app <- ifelse(nav_extid_mal_wsc$visite_totali_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_totali_app <- NULL

nav_extid_mal_wsc$flg_fatture_app <- ifelse(nav_extid_mal_wsc$visite_fatture_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_fatture_app <- NULL

nav_extid_mal_wsc$flg_extra_app <- ifelse(nav_extid_mal_wsc$visite_extra_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_extra_app <- NULL

nav_extid_mal_wsc$flg_arricchisci_abbonamento_app <- ifelse(nav_extid_mal_wsc$visite_arricchisci_abbonamento_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_arricchisci_abbonamento_app <- NULL

nav_extid_mal_wsc$flg_stato_abbonamento_app <- ifelse(nav_extid_mal_wsc$visite_stato_abbonamento_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_stato_abbonamento_app <- NULL

nav_extid_mal_wsc$flg_contatta_sky_app <- ifelse(nav_extid_mal_wsc$visite_contatta_sky_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_contatta_sky_app <- NULL

nav_extid_mal_wsc$flg_assistenza_app <- ifelse(nav_extid_mal_wsc$visite_assistenza_e_supporto_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_assistenza_e_supporto_app <- NULL

nav_extid_mal_wsc$flg_comunicazioni_app <- ifelse(nav_extid_mal_wsc$visite_comunicazioni_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_comunicazioni_app <- NULL

nav_extid_mal_wsc$flg_gestisci_servizi_app <- ifelse(nav_extid_mal_wsc$visite_gestisci_servizi_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_gestisci_servizi_app <- NULL

nav_extid_mal_wsc$flg_mio_abbonamento_app <- ifelse(nav_extid_mal_wsc$visite_mio_abbonamento_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_mio_abbonamento_app <- NULL

nav_extid_mal_wsc$flg_info_app <- ifelse(nav_extid_mal_wsc$visite_info_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_info_app <- NULL

nav_extid_mal_wsc$flg_indoDAZN_sito <- ifelse(nav_extid_mal_wsc$visite_indoDAZN_sito >= 1, 1, 0)
nav_extid_mal_wsc$visite_indoDAZN_sito <- NULL

nav_extid_mal_wsc$flg_infoDAZN_app <- ifelse(nav_extid_mal_wsc$visite_InfoDAZN_app >= 1, 1, 0)
nav_extid_mal_wsc$visite_InfoDAZN_app <- NULL

nav_extid_mal_wsc$visite_totali_sito <- NULL
nav_extid_mal_wsc$visite_totali <- NULL
nav_extid_mal_wsc$visite_fatture <- NULL
nav_extid_mal_wsc$visite_extra <- NULL
nav_extid_mal_wsc$visite_arricchisciAbbonamento <- NULL
nav_extid_mal_wsc$visite_gestione <- NULL
nav_extid_mal_wsc$visite_contatti <- NULL
nav_extid_mal_wsc$visite_comunicazione <- NULL
nav_extid_mal_wsc$visite_assistenza <- NULL


View(head(nav_extid_mal_wsc,100))
nrow(nav_extid_mal_wsc)
# 179.604


write.df(repartition( nav_extid_mal_wsc, 1), path = path_kpi_nav_csv, "csv", sep=";", mode = "overwrite", header=TRUE)
# "/user/stefano.mazzucca/mal_wsc/kpi_finali_navigazioni_tot_dal13agosto.csv"     <- kpi_nav_csv 2018_08_20


#chiudi sessione
sparkR.stop()
