
# UPSELLING richiesta Marco Storti
# Stima potenziale chat e CMN per upselling 
# Ananlisi sui mesi APRILE - MAGGIO - GIUGNO - LUGLIO - AGOSTO - SETTEMBRE 2017

#apri sessione
source("connection_R.R")
options(scipen = 1000)


################
# mov_up_down ---> COD_CONTRATTO in chiaro
# rich_up_down --> COD_CONTRATTO cifrato
# skyit_wsc -----> COD_CLIENTE cifrato
################
# TABELLA DI CONVERSIONE:
# hdfs:///STAGE/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2_dap_obfuscation.parquet



#################################################################################################################################################################################
#################################################################################################################################################################################

## UPSELLING dal canale digitale:

path <- "/user/stefano.mazzucca/wsc_ordini_up_apr_set_2017.csv" 
up_wsc <- read.df(path, source = "csv", header = "true", delimiter = ";")

createOrReplaceTempView(up_wsc, "up_wsc")

up_wsc_1 <- sql("select data, ext_id, ordini_netti
                from up_wsc
                where ordini_netti != 0")

up_wsc_2 <- withColumn(up_wsc_1, "data_2", cast(unix_timestamp(up_wsc_1$data, 'dd/MM/yyyy'), 'timestamp'))
up_wsc_3 <- withColumn(up_wsc_2, "date", cast(up_wsc_2$data_2, 'date'))
up_wsc_4 <- select(up_wsc_3, "ext_id", "ordini_netti", "date")
up_wsc_5 <- withColumn(up_wsc_4, "canale", lit("wsc"))

View(head(up_wsc_5,100))
nrow(up_wsc_5)
# 49.652



path <- "/user/stefano.mazzucca/app_ordini_up_apr_set_2017.csv" 
up_app <- read.df(path, source = "csv", header = "true", delimiter = "\t")

createOrReplaceTempView(up_app, "up_app")

up_app_1 <- sql("select data, ext_id, ordini_netti
                from up_app
                where ordini_netti != 0")

up_app_2 <- withColumn(up_app_1, "data_2", cast(unix_timestamp(up_app_1$data, 'dd/MM/yyyy'), 'timestamp'))
up_app_3 <- withColumn(up_app_2, "date", cast(up_app_2$data_2, 'date'))
up_app_4 <- select(up_app_3, "ext_id", "ordini_netti", "date")
up_app_5 <- withColumn(up_app_4, "canale", lit("app"))

View(head(up_app_5,100))
nrow(up_app_5)
# 37.242



up_digital <- rbind(up_wsc_5, up_app_5)

createOrReplaceTempView(up_digital, "up_digital")

up_digital_1 <- sql("select *
                    from up_digital
                    order by ext_id, date")
View(head(up_digital_1,1000))
nrow(up_digital_1)
# 86.894

createOrReplaceTempView(up_digital_1, "up_digital_1")

up_digital_2 <- sql("select *
                    from up_digital_1
                    where ext_id is not NULL
                    order by ext_id, date")
View(head(up_digital_2,1000))
nrow(up_digital_2)
# 86.894

up_digital_3 <- filter(up_digital_2, "ordini_netti > 0")
View(head(up_digital_3,100))
nrow(up_digital_3)
# 86.890

valdist_cl_up_digital <- distinct(select(up_digital_3, "ext_id"))
nrow(valdist_cl_up_digital)
# 79.854
valdist_cl_date_up_digital <- distinct(select(up_digital_3, "ext_id", "date"))
nrow(valdist_cl_date_up_digital)
# 86.683
valdist_cl_date_canale_up_digital <- distinct(select(up_digital_3, "ext_id", "date", "canale"))
nrow(valdist_cl_date_canale_up_digital)
# 86.890 -->> significa che alcuni utenti (86.890-86.683 = 207) fanno upselling su 2 canali diversi (wsc e app)


write.df(repartition( up_digital_3, 1), path = "/user/stefano.mazzucca/upselling_digital_apr_sett_2017.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



#################################################################################################################################################################################
#################################################################################################################################################################################

# Visite al sito WSC di Sky
skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")

skyit_wsc_v1 <- select(skyitdev_df, "external_id_post_evar", "post_visid_high", "post_visid_low", "post_visid_concatenated", "date_time", "visit_num",
                       "secondo_livello_post_evar", "terzo_livello_post_evar")
skyit_wsc_v2 <- withColumn(skyit_wsc_v1, "ts", cast(skyit_wsc_v1$date_time, "timestamp"))
skyit_wsc_v3 <- withColumn(skyit_wsc_v2, "date", cast(skyit_wsc_v2$date_time, "date"))
skyit_wsc_v3 <- filter(skyit_wsc_v3,"ts >= '2017-04-01 00:00:00' and ts <= '2017-10-01 00:00:00' and external_id_post_evar is not NULL")
skyit_wsc_v4 <- filter(skyit_wsc_v3,"
                       terzo_livello_post_evar like '%faidate home%' or
                       terzo_livello_post_evar like '%faidate gestisci dati servizi%' or
                       terzo_livello_post_evar like '%faidate arricchisci abbonamento%' or
                       terzo_livello_post_evar like '%faidate fatture pagamenti%' or
                       terzo_livello_post_evar like '%faidate webtracking%' or
                       terzo_livello_post_evar like '%faidate extra%' ") 
View(head(skyit_wsc_v4,100))


write.parquet(skyit_wsc_v4,"/user/stefano.mazzucca/scarico_wsc_apr_set_2017.parquet")



#################################################################################################################################################################################

# Visite all'APP WSC di Sky
skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")

skyappwsc_2 <- withColumn(skyappwsc, "ts", cast(skyappwsc$date_time, "timestamp"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date", cast(skyappwsc$date_time, "date"))
skyappwsc_4 <- filter(skyappwsc_3,"ts >= '2017-04-01 00:00:00' and ts <= '2017-10-01 00:00:00' ")
nrow(skyappwsc_4)
# 78.367.858
skyappwsc_5 <- select(skyappwsc_4,
                      "external_id_post_evar",
                      "site_section",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "post_pagename",
                      "date_time",
                      "ts",
                      "date",
                      "visit_num")
nrow(skyappwsc_5)
# 78.367.858
skyappwsc_6 <- filter(skyappwsc_5,"
                      site_section like '%arricchisci abbonamento%' or
                      site_section like '%dati e fatture%' or
                      site_section like '%il mio abbonamento%' or
                      site_section like '%sky extra%' or
                      site_section like '%comunicazioni%' or
                      site_section like '%assistenza%' or
                      site_section like '%widget%' or
                      site_section like '%impostazioni%' or
                      site_section like '%i miei dati%' or
                      page_name_post_evar like '%assistenza:home%' or
                      page_name_post_evar like '%assistenza:contatta%' or
                      page_name_post_evar like '%assistenza:conosci%' or
                      page_name_post_evar like '%assistenza:ricerca%' or
                      page_name_post_evar like '%assistenza:gestisci%' or
                      page_name_post_evar like '%assistenza:risolvi%' or
                      page_name_post_evar like '%widget:dispositivi%' or
                      page_name_post_evar like '%widget:ultimafattura%' or
                      page_name_post_evar like '%widget:contatta sky%' or
                      page_name_post_evar like '%widget:gestisci%'
                      ")
View(head(skyappwsc_6,1000))
nrow(skyappwsc_6)
# 27.352.826


write.parquet(skyappwsc_6,"/user/stefano.mazzucca/scarico_app_apr_set_2017.parquet")



#################################################################################################################################################################################
#################################################################################################################################################################################

# Richieste di UP e DDOWNGRADE
rich_up_down <- read.parquet("/STAGE/CMDM/DettaglioRichiesteUpDowngradeReali/full/vs_rich_up_downgrade_SHOT_2.parquet")
View(head(rich_up_down,1000))
printSchema(rich_up_down)

# root
# |-- COD_CONTRATTO: string (nullable = true)

# |-- DAT_RICHIESTA: string (nullable = true)
# |-- DES_PACCHETTO_DA: string (nullable = true)
# |-- DES_PACCHETTO_A: string (nullable = true)
# |-- DAT_SCHEDULAZIONE_RICHIESTA: string (nullable = true)
# |-- FLG_UPG_PACK: string (nullable = true)
# |-- FLG_DWG_PACK: string (nullable = true)

# |-- FLG_UPGRADE_CALCIO: string (nullable = true)
# |-- FLG_UPGRADE_SPORT: string (nullable = true)
# |-- FLG_UPGRADE_CINEMA: string (nullable = true)
# |-- FLG_DOWNGRADE_CALCIO: string (nullable = true)
# |-- FLG_DOWNGRADE_SPORT: string (nullable = true)
# |-- FLG_DOWNGRADE_CINEMA: string (nullable = true)
# |-- NUM_UPG_PACK: string (nullable = true)
# |-- NUM_DWG_PACK: string (nullable = true)

# |-- FLG_UPG_HD: string (nullable = true)
# |-- FLG_DWG_HD: string (nullable = true)
# |-- FLG_UPG_SKYTV: string (nullable = true)
# |-- FLG_DWG_SKYTV: string (nullable = true)
# |-- FLG_UPG_SKYFAMIGLIA: string (nullable = true)
# |-- FLG_DWG_SKYFAMIGLIA: string (nullable = true)


rich_up_down_1 <- select(rich_up_down, "COD_CONTRATTO", "DAT_RICHIESTA", "FLG_UPGRADE_CALCIO", "FLG_UPGRADE_SPORT", "FLG_UPGRADE_CINEMA", 
                         "FLG_UPG_HD", "FLG_UPG_SKYFAMIGLIA", "DES_PACCHETTO_DA", "DES_PACCHETTO_A")
rich_up_down_2 <- withColumn(rich_up_down_1, "date", cast(cast(unix_timestamp(rich_up_down_1$DAT_RICHIESTA, 'dd/MM/yyyy'), 'timestamp'), 'date'))
nrow(rich_up_down_2)
# 17.440.445

rich_up_down_3 <- filter(rich_up_down_2, "date >= '2017-04-01' and date <= '2017-10-07'")
View(head(rich_up_down_3, 1000))
nrow(rich_up_down_3)
# 985.984

# Selezione dei soli UPGRADE di pacchetti premium
rich_up_down_4 <- filter(rich_up_down_3, "FLG_UPGRADE_CALCIO = 1 or
                         FLG_UPGRADE_SPORT = 1 or
                         FLG_UPGRADE_CINEMA = 1 or
                         FLG_UPG_HD = 1 or
                         FLG_UPG_SKYFAMIGLIA = 1")
View(head(rich_up_down_4,1000))
nrow(rich_up_down_4)
# 577.626



#################################################################################################################################################################################

# Movimenti/Attivazioni di UP e DOWN GRADE
mov_up_down <- read.parquet("/STAGE/CMDM/DettaglioAttivazioniUpDowngrade/full/vs_attiv_up_downgrade_SHOT_2.parquet")
View(head(mov_up_down,1000))
printSchema(mov_up_down)

# root
# |-- COD_CONTRATTO: string (nullable = true)
# |-- DAT_EVENTO: string (nullable = true)
# |-- FLG_UPGRADE_GENERICO: string (nullable = true)
# |-- FLG_UPGRADE_CALCIO: string (nullable = true)
# |-- FLG_UPGRADE_SPORT: string (nullable = true)
# |-- FLG_UPGRADE_CINEMA: string (nullable = true)
# |-- FLG_DOWNGRADE_GENERICO: string (nullable = true)
# |-- FLG_DOWNGRADE_CALCIO: string (nullable = true)
# |-- FLG_DOWNGRADE_SPORT: string (nullable = true)
# |-- FLG_DOWNGRADE_CINEMA: string (nullable = true)
# |-- DES_PACCHETTO_DA: string (nullable = true)
# |-- DES_PACCHETTO_A: string (nullable = true)

# |-- FLG_UPG_HD: string (nullable = true)
# |-- FLG_DWG_HD: string (nullable = true)
# |-- FLG_UPG_SKYTV: string (nullable = true)
# |-- FLG_DWG_SKYTV: string (nullable = true)
# |-- FLG_UPG_SKYFAMIGLIA: string (nullable = true)
# |-- FLG_DWG_SKYFAMIGLIA: string (nullable = true)


mov_up_down_1 <- select(mov_up_down, "COD_CONTRATTO", "DAT_EVENTO", "FLG_UPGRADE_CALCIO", "FLG_UPGRADE_SPORT", "FLG_UPGRADE_CINEMA", 
                        "FLG_UPG_HD", "FLG_UPG_SKYFAMIGLIA", "DES_PACCHETTO_DA", "DES_PACCHETTO_A")
mov_up_down_2 <- withColumn(mov_up_down_1, "date", cast(cast(unix_timestamp(mov_up_down_1$DAT_EVENTO, 'dd/MM/yyyy'), 'timestamp'), 'date'))
nrow(mov_up_down_2)
# 19.723.578

mov_up_down_3 <- filter(mov_up_down_2, "date >= '2017-04-01' and date <= '2017-10-07'")
View(head(mov_up_down_3, 1000))
nrow(mov_up_down_3)
# 919.507

# Selezione dei soli UPGRADE di pacchetti premium
mov_up_down_4 <- filter(mov_up_down_3, "FLG_UPGRADE_CALCIO = 1 or
                        FLG_UPGRADE_SPORT = 1 or
                        FLG_UPGRADE_CINEMA = 1 or
                        FLG_UPG_HD = 1 or
                        FLG_UPG_SKYFAMIGLIA = 1")
View(head(mov_up_down_4,1000))
nrow(mov_up_down_4)
# 591.674 



######################
# 985.984 richieste di movimento pacchetto
# 919.507 attivazioni di movimento pacchetto
# -> caduta di circa il 6,7 %
######################

######################
# 577.626 richieste di upselling
# 591.674 attivazioni di upselling
# -> "caduta" (in realt?? aumentano) di circa il 2,4 %
######################



################################################################################################################################################################################

## Conversione dei COD_CONTRATTO in COD_CLIENTE

tab_conv <- read.parquet("hdfs:///OBF/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2.parquet")
View(head(tab_conv,100))


createOrReplaceTempView(tab_conv, "tab_conv")
createOrReplaceTempView(rich_up_down_4, "rich_up_down_4")
createOrReplaceTempView(mov_up_down_4, "mov_up_down_4")

rich_up_down_5 <- sql("select t1.*, t2.COD_CLIENTE_CIFRATO
                      from rich_up_down_4 t1
                      inner join tab_conv t2
                        on t1.COD_CONTRATTO = t2.COD_CONTRATTO_CIFRATO
                      order by t2.COD_CLIENTE_CIFRATO")
View(head(rich_up_down_5,100))
nrow(rich_up_down_5) 
# 577.626 (Ceck OK)
rich_up_down_5_ver <- filter(rich_up_down_5, "COD_CLIENTE_CIFRATO is NULL")
nrow(rich_up_down_5_ver)
# 0

valdist_cl_rich <- distinct(select(rich_up_down_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_rich)
# 521.983
valdist_cl_date_rich <- distinct(select(rich_up_down_5, "COD_CLIENTE_CIFRATO", "date"))
nrow(valdist_cl_date_rich)
# 576.557


write.parquet(rich_up_down_5, "/user/stefano.mazzucca/upselling_rich_apr_sett_2017.parquet")



mov_up_down_5 <- sql("select t1.*, t2.COD_CLIENTE_CIFRATO
                     from mov_up_down_4 t1
                     inner join tab_conv t2
                     on t1.COD_CONTRATTO = t2.COD_CONTRATTO
                     order by t2.COD_CLIENTE_CIFRATO")
View(head(mov_up_down_5,100))
nrow(mov_up_down_5)
# 591.674 (Ceck OK)
mov_up_down_5_ver <- filter(mov_up_down_5, "COD_CLIENTE_CIFRATO is NULL")
nrow(mov_up_down_5_ver)
# 0

valdist_cl_mov <- distinct(select(mov_up_down_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_mov)
# 533.708
valdist_cl_date_mov <- distinct(select(mov_up_down_5, "COD_CLIENTE_CIFRATO", "date"))
nrow(valdist_cl_date_mov)
# 590.713 (961 in meno)

prova <- distinct(select(mov_up_down_5, "COD_CONTRATTO", "date"))
nrow(prova)
# 591.674


write.parquet(mov_up_down_5, "/user/stefano.mazzucca/upselling_mov_apr_sett_2017.parquet")



################################################################################################################################################################################
################################################################################################################################################################################

## Verifica se utilizzare i dati di rich_up oppure di mov_up

createOrReplaceTempView(up_digital_3, "up_digital_3")
createOrReplaceTempView(rich_up_down_5, "rich_up_down_5")
createOrReplaceTempView(mov_up_down_5, "mov_up_down_5")

ver_1 <- sql("select t1.ext_id
             from up_digital_3 t1
             inner join rich_up_down_5 t2
             on t1.ext_id = t2.COD_CLIENTE_CIFRATO and t1.date = t2.date
             order by t1.ext_id")
View(head(ver_1,100))
nrow(ver_1)
# 77.602 con "date" nella condizione di join


ver_2 <- sql("select t1.ext_id
             from up_digital_3 t1
             inner join mov_up_down_5 t2
             on t1.ext_id = t2.COD_CLIENTE_CIFRATO and t1.date = t2.date")
View(head(ver_2,100))
nrow(ver_2)
# 81.300 con "date" nella condizione di join

######################
# 79.854 utenti distinti di upselling DIGITALE
# 86.683 utenti+data distinti di upselling DIGITALE
######################




################################################################################################################################################################################
################################################################################################################################################################################
################################################################################################################################################################################
################################################################################################################################################################################

# Elaborazioni e sviluppo

skywsc <- read.parquet("/user/stefano.mazzucca/scarico_wsc_apr_set_2017.parquet")
View(head(skywsc,100))
nrow(skywsc)
# 35.198.602

skyappwsc <- read.parquet("/user/stefano.mazzucca/scarico_app_apr_set_2017.parquet")
View(head(skyappwsc,100))
nrow(skyappwsc)
# 27.352.826

path <- "/user/stefano.mazzucca/upselling_digital_apr_sett_2017.csv"
up_digital <- read.df(path, source = "csv", header = "true", delimiter = ";")
View(head(up_digital,100))
nrow(up_digital)
# 86.890
valdist_clienti_up_digital <- distinct(select(up_digital, "ext_id"))
nrow(valdist_clienti_up_digital)
# 79.854

up_totali <- read.parquet("/user/stefano.mazzucca/upselling_mov_apr_sett_2017.parquet")
View(head(up_totali,100))
nrow(up_totali)
# 591.674
valdist_clienti_up_totali <- distinct(select(up_totali, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_up_totali)
# 533.708
valdist_contratti_up_totali <- distinct(select(up_totali, "COD_CONTRATTO"))
nrow(valdist_contratti_up_totali)
# 536.300
######################
# 591.674 movimenti di UPSELLING totali
# 533.708 cod_cliente registrati
# 536.300 cod_contratto registrati
######################


## Tabellone degli upselling totali

createOrReplaceTempView(up_totali, "up_totali")
createOrReplaceTempView(up_digital, "up_digital")

upselling <- sql("select t1.COD_CLIENTE_CIFRATO, 
                         t1.date as data, 
                         t2.canale as canale_up_digital,
                         t1.FLG_UPGRADE_CALCIO,
                         t1.FLG_UPGRADE_SPORT,
                         t1.FLG_UPGRADE_CINEMA,
                         t1.FLG_UPG_HD,
                         t1.FLG_UPG_SKYFAMIGLIA,
                         t1.DES_PACCHETTO_DA,
                         t1.DES_PACCHETTO_A,
                         t2.ordini_netti,
                         case when (t2.ext_id is NULL) then 0
                          else 1 end as flg_canale_digital,
                 case when (FLG_UPGRADE_CALCIO > 0 OR FLG_UPGRADE_SPORT > 0 OR FLG_UPGRADE_CINEMA > 0) then 1
                  else 0 end as flg_up_pack_premium,
                 case when (FLG_UPG_HD > 0 OR FLG_UPG_SKYFAMIGLIA > 0) then 1
                  else 0 end as flg_up_hd_famiglia
                         from up_totali t1
                         left join up_digital t2
                          on t1.COD_CLIENTE_CIFRATO = t2.ext_id and t1.date = t2.date
                         order by t1.COD_CLIENTE_CIFRATO, t1.date")
View(head(upselling,1000))
nrow(upselling)
# 591.861 con "date" come condizione di join (vs 591.674 movimenti totali)

ver_upselling_digital <- filter(upselling, "flg_canale_digital = 1")
nrow(ver_upselling_digital)
# 81.300 

createOrReplaceTempView(ver_upselling_digital, "ver_upselling_digital")
count_flg_premium <- sql("select sum(flg_up_pack_premium) as count_flg_premium,
                                 sum(flg_up_hd_famiglia) as count_flg_hd_famiglia
                         from ver_upselling_digital")
View(head(count_flg_premium,100))


ver_upselling_no_digital <- filter(upselling, "flg_canale_digital = 0")
nrow(ver_upselling_no_digital)
# 510.561 (86% degli upselling totali sono NO DIGITAL)

createOrReplaceTempView(ver_upselling_no_digital, "ver_upselling_no_digital")
count_flg_premium_2 <- sql("select sum(flg_up_pack_premium) as count_flg_premium,
                                 sum(flg_up_hd_famiglia) as count_flg_hd_famiglia
                         from ver_upselling_no_digital")
View(head(count_flg_premium_2,100))

######################
# 86.890 movimenti di UPSELLING digitali estratti da Adobe
# 81.300 movimenti "trovati" UPSELLING digitali dopo aver incrociati i dati con il CMDM
######################



################################################################################################################################################################################
################################################################################################################################################################################

## Prendo le navigazioni degli utenti che hanno fatto upselling (nella sezione "promozioni")
### Sito WSC

skywsc_1 <- filter(skywsc, "terzo_livello_post_evar LIKE '%faidate arricchisci abbonamento%'")
skywsc_2 <- withColumn(skywsc_1, "seven_days_later_date", date_add(skywsc_1$date, 7))

View(head(skywsc_2,100))
nrow(skywsc_2)
# 10.381.024


createOrReplaceTempView(skywsc_2, "skywsc_2")
createOrReplaceTempView(upselling, "upselling")

target_wsc <- sql("select t2.COD_CLIENTE_CIFRATO,  
                          min(t1.date) as data_navigazione, 
                          min(t1.seven_days_later_date) as 7_days_after_nav, 
                          count(t1.date) as n_visit,
                          t2.ordini_netti as ordini_up_digital,
                          t2.data as data_upselling, 
                          t2.FLG_UPGRADE_CALCIO,
                          t2.FLG_UPGRADE_SPORT,
                          t2.FLG_UPGRADE_CINEMA,
                          t2.FLG_UPG_HD,
                          t2.FLG_UPG_SKYFAMIGLIA,
                          t2.DES_PACCHETTO_DA,
                          t2.DES_PACCHETTO_A,
                          t2.flg_canale_digital
                  from skywsc_2 t1
                  inner join upselling t2
                    on t1.external_id_post_evar = t2.COD_CLIENTE_CIFRATO and t2.data >= t1.date and t2.data <= t1.seven_days_later_date
                  group by t2.COD_CLIENTE_CIFRATO, 
                            t2.ordini_netti,
                            t2.data,
                            t2.FLG_UPGRADE_CALCIO,
                            t2.FLG_UPGRADE_SPORT,
                            t2.FLG_UPGRADE_CINEMA,
                            t2.FLG_UPG_HD,
                            t2.FLG_UPG_SKYFAMIGLIA,
                            t2.DES_PACCHETTO_DA,
                            t2.DES_PACCHETTO_A,
                            t2.flg_canale_digital
                  order by t2.COD_CLIENTE_CIFRATO, data_upselling")
View(head(target_wsc,1000))
nrow(target_wsc)
# 99.151 con il group by
# *n_visit = numero di visualizzazioni delle pagine relative alla sezione "promozioni" della wsc del SITO

## N.B. Forse meglio NON inserire la data stessa della navigazione, soprattutto se DIGITAL perch?? avrebbe potuto passare da li facendo la procedura di upselling

target_wsc <- withColumn(target_wsc, "canale_nav", lit("wsc"))

valdist_cl_target_wsc <- distinct(select(target_wsc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_target_wsc)
# 91.748

valdist_cl_data_target_wsc <- distinct(select(target_wsc, "COD_CLIENTE_CIFRATO", "data_upselling"))
nrow(valdist_cl_data_target_wsc)
# 99.045 (106 in meno)


target_wsc_no_digital <- filter(target_wsc, "flg_canale_digital = 0")
nrow(target_wsc_no_digital)
# 48.525

valdist_cl_target_wsc_no_digital <- distinct(select(target_wsc_no_digital, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_target_wsc_no_digital)
# 46.536



################################################################################################################################################################################

## Prendo le navigazioni degli utenti che hanno fatto upselling (nella sezione "promozioni")
### APP WSC

skyappwsc_1 <- filter(skyappwsc, "site_section LIKE '%arricchisci abbonamento%'")
skyappwsc_2 <- withColumn(skyappwsc_1, "seven_days_later_date", date_add(skyappwsc_1$date,7))

View(head(skyappwsc_2,100))
nrow(skyappwsc_2)
# 8.238.842


createOrReplaceTempView(skyappwsc_2, "skyappwsc_2")
createOrReplaceTempView(upselling, "upselling")

target_app <- sql("select t2.COD_CLIENTE_CIFRATO,  
                          min(t1.date) as data_navigazione, 
                          min(t1.seven_days_later_date) as 7_days_after_nav, 
                          count(t1.date) as n_visit,
                          t2.ordini_netti as ordini_up_digital,
                          t2.data as data_upselling, 
                          t2.FLG_UPGRADE_CALCIO,
                          t2.FLG_UPGRADE_SPORT,
                          t2.FLG_UPGRADE_CINEMA,
                          t2.FLG_UPG_HD,
                          t2.FLG_UPG_SKYFAMIGLIA,
                          t2.DES_PACCHETTO_DA,
                          t2.DES_PACCHETTO_A,
                          t2.flg_canale_digital
                  from skyappwsc_2 t1
                  inner join upselling t2
                    on t1.external_id_post_evar = t2.COD_CLIENTE_CIFRATO and t2.data >= t1.date and t2.data <= t1.seven_days_later_date
                  group by t2.COD_CLIENTE_CIFRATO,
                            t2.ordini_netti,
                            t2.data,
                            t2.FLG_UPGRADE_CALCIO,
                            t2.FLG_UPGRADE_SPORT,
                            t2.FLG_UPGRADE_CINEMA,
                            t2.FLG_UPG_HD,
                            t2.FLG_UPG_SKYFAMIGLIA,
                            t2.DES_PACCHETTO_DA,
                            t2.DES_PACCHETTO_A,
                            t2.flg_canale_digital
                  order by t2.COD_CLIENTE_CIFRATO, data_upselling")
View(head(target_app,1000))
nrow(target_app)
# 75.139 con il group by
# *n_visit = numero di visualizzazioni delle pagine relative alla sezione "promozioni" della wsc dell'APP

## N.B. Forse meglio NON inserire la data stessa della navigazione, soprattutto se DIGITAL perch?? avrebbe potuto passare da li facendo la procedura di upselling

target_app <- withColumn(target_app, "canale_nav", lit("app"))

valdist_cl_target_app <- distinct(select(target_app, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_target_app)
# 67.811

valdist_cl_data_target_app <- distinct(select(target_app, "COD_CLIENTE_CIFRATO", "data_upselling"))
nrow(valdist_cl_data_target_app)
# 75.057 (82 in meno)


target_app_no_digital <- filter(target_app, "flg_canale_digital = 0")
nrow(target_app_no_digital)
# 34.179

valdist_cl_target_app_no_digital <- distinct(select(target_app_no_digital, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_target_app_no_digital)
# 32.408



################################################################################################################################################################################

## Unisco le navigazioni su WSC trovate pre-7-giorni dall'UPSELLING

target_tot <- rbind(target_wsc, target_app)
View(head(target_tot,1000))
nrow(target_tot)
# 174.290

target_tot_arrange <- arrange(target_tot, target_tot$COD_CLIENTE_CIFRATO)
View(head(target_tot_arrange,1000))


createOrReplaceTempView(target_tot, "target_tot")

target_tot_1 <- sql("select COD_CLIENTE_CIFRATO, data_upselling,
                            min(data_navigazione) as data_navigazione,
                            min(7_days_after_nav) as 7_days_after_nav,
                            sum(n_visit) as n_visit,
                            case when sum(flg_canale_digital) > 0 then 1
                              else 0 end as flg_canale_up_digital,
                            sum(ordini_up_digital) as n_ordini_up_digital_Adobe,
                            max(FLG_UPGRADE_CALCIO) as FLG_UPGRADE_CALCIO,
                            max(FLG_UPGRADE_SPORT) as FLG_UPGRADE_SPORT,
                            max(FLG_UPGRADE_CINEMA) as FLG_UPGRADE_CINEMA,
                            max(FLG_UPG_HD) as FLG_UPG_HD,
                            max(FLG_UPG_SKYFAMIGLIA) as FLG_UPG_SKYFAMIGLIA
                    from target_tot
                    group by COD_CLIENTE_CIFRATO, data_upselling
                    order by COD_CLIENTE_CIFRATO, data_upselling")
#canale_nav,
#DES_PACCHETTO_DA),
#DES_PACCHETTO_A,
View(head(target_tot_1,1000))
nrow(target_tot_1)
# 153.918

# target_tot_1 <- sql("select COD_CLIENTE_CIFRATO, data_upselling, 
#                             min(data_navigazione) as data_navigazione,
#                             min(7_days_after_nav) as 7_days_after_nav,
#                             sum(n_visit) as n_visit,
#                             case when sum(flg_canale_digital) > 0 then 1
#                               else 0 end as flg_canale_up_digital,
#                             sum(ordini_up_digital) as n_ordini_up_digital_Adobe,
#                             FLG_UPGRADE_CALCIO, 
#                             FLG_UPGRADE_SPORT, 
#                             FLG_UPGRADE_CINEMA, 
#                             FLG_UPG_HD, 
#                             FLG_UPG_SKYFAMIGLIA
#                     from target_tot
#                     group by COD_CLIENTE_CIFRATO, data_upselling, FLG_UPGRADE_CALCIO, FLG_UPGRADE_SPORT, FLG_UPGRADE_CINEMA, FLG_UPG_HD, FLG_UPG_SKYFAMIGLIA
#                     order by COD_CLIENTE_CIFRATO, data_upselling")
# #canale_nav,
# #DES_PACCHETTO_DA),
# #DES_PACCHETTO_A,
# View(head(target_tot_1,1000))
# nrow(target_tot_1)
# # 153.970

createOrReplaceTempView(target_tot_1, "target_tot_1")

target_tot_2 <- sql("select *,
                          (FLG_UPGRADE_CALCIO + FLG_UPGRADE_SPORT + FLG_UPGRADE_CINEMA + FLG_UPG_HD + FLG_UPG_SKYFAMIGLIA) as n_ordini_up_cmdm,
                          case when (FLG_UPGRADE_CALCIO > 0 OR FLG_UPGRADE_SPORT > 0 OR FLG_UPGRADE_CINEMA > 0) then 1
                            else 0 end as flg_up_pack_premium,
                          case when (FLG_UPG_HD > 0 OR FLG_UPG_SKYFAMIGLIA > 0) then 1
                            else 0 end as flg_up_hd_famiglia
                    from target_tot_1
                    order by COD_CLIENTE_CIFRATO, data_upselling")
View(head(target_tot_2,1000))
nrow(target_tot_2)
# 153.918


valdist_cl_target_tot <- distinct(select(target_tot_2, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_target_tot)
# 139.867


tartget_tot_no_digital <- filter(target_tot_2, "flg_canale_up_digital = 0")
View(head(tartget_tot_no_digital,1000))
nrow(tartget_tot_no_digital)
# 73.362

createOrReplaceTempView(tartget_tot_no_digital, "tartget_tot_no_digital")
count_flg_premium_tot <- sql("select sum(flg_up_pack_premium) as count_flg_premium,
                                     sum(flg_up_hd_famiglia) as count_flg_hd_famiglia
                             from tartget_tot_no_digital")
View(head(count_flg_premium_tot,100))

valdist_cl_tartget_tot_no_digital <- distinct(select(tartget_tot_no_digital, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_tartget_tot_no_digital)
# 69.845

valdist_cl_data_tartget_tot_no_digital <- distinct(select(tartget_tot_no_digital, "COD_CLIENTE_CIFRATO", "data_navigazione"))
nrow(valdist_cl_data_tartget_tot_no_digital)
# 72.034



tartget_tot_digital <- filter(target_tot_2, "flg_canale_up_digital = 1")
View(head(tartget_tot_digital,1000))
nrow(tartget_tot_digital)
# 80.556

createOrReplaceTempView(tartget_tot_digital, "tartget_tot_digital")
count_flg_premium_tot_2 <- sql("select sum(flg_up_pack_premium) as count_flg_premium,
                                     sum(flg_up_hd_famiglia) as count_flg_hd_famiglia
                             from tartget_tot_digital")
View(head(count_flg_premium_tot_2,100))




#chiudi sessione
sparkR.stop()
