
# CJ_PDISC_vers10
# Tabellone navigazione utenti pdisc su sito pubblico, wsc, APP


#apri sessione
source("connection_R.R")
options(scipen = 1000)



######### Recupero il target PDISC da cui partire per analizzare le navigazioni degli utenti


base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
View(head(base_pdisc,100))
nrow(base_pdisc)
# 37.976

valdist_base <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_base)
# 37.753


base_pdisc_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc.parquet")
View(head(base_pdisc_coo,100))
nrow(base_pdisc_coo)
# 161.766
ver_clienti <- distinct(select(base_pdisc_coo, "COD_CLIENTE_CIFRATO"))
nrow(ver_clienti)
# 24.340 clienti pdisc on line (su device)

  

createOrReplaceTempView(base_pdisc, "base_pdisc")
createOrReplaceTempView(base_pdisc_coo, "base_pdisc_coo")

base_pdisc_coo_2 <- sql("select t1.*, t2.post_visid_concatenated
                        from base_pdisc t1
                        left join base_pdisc_coo t2
                          on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
View(head(base_pdisc_coo_2,100))
nrow(base_pdisc_coo_2)
# 176.511




#############################################################################################################################################################################
#############################################################################################################################################################################

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")

skyitdev_df_1 <- select(skyitdev_df,
                        "external_id_post_evar",    
                        "post_visid_high", "post_visid_low","post_visid_concatenated","date_time","visit_num",
                        "page_url_post_evar", "page_url_post_prop", "download_name_post_prop", 
                        "secondo_livello_post_evar","terzo_livello_post_evar",
                        "hit_source","exclude_hit")

skyitdev_df_2 <- withColumn(skyitdev_df_1, "ts", cast(skyitdev_df_1$date_time, "timestamp"))
skyitdev_df_3 <- filter(skyitdev_df_2,"ts>='2017-03-01 00:00:00'")
skyitdev_df_4 <- filter(skyitdev_df_3,"ts<='2018-01-01 00:00:00'")



#############################################################################################################################################################################
### Tabellone sito pubblico - no wsc
#############################################################################################################################################################################

tab_sito_pubb_1 <- filter(skyitdev_df_4,"terzo_livello_post_evar NOT LIKE '%faidate%' ")

tab_sito_pubb_2 <- withColumn(tab_sito_pubb_1,"concatena_chiave",
                                 concat(tab_sito_pubb_1$post_visid_concatenated, tab_sito_pubb_1$visit_num))

tab_sito_pubb_3 <- distinct(select(tab_sito_pubb_2,"external_id_post_evar","post_visid_concatenated","visit_num", "ts","concatena_chiave","hit_source","exclude_hit", 
                                      "page_url_post_evar", "page_url_post_prop", "download_name_post_prop", "secondo_livello_post_evar","terzo_livello_post_evar"))


#join
createOrReplaceTempView(tab_sito_pubb_3,"tab_sito_pubb_3")
createOrReplaceTempView(base_pdisc_coo_2,"base_pdisc_coo_2")

join_sito_pub_1 <- sql("select distinct t2.COD_CLIENTE_CIFRATO, t2.post_visid_concatenated, t1.concatena_chiave, t1.visit_num, t1.ts,
                                t2.data_ingr_pdisc_formdate, t2.digitalizzazione_bin, 
                                t1.page_url_post_evar, t1.page_url_post_prop, t1.download_name_post_prop, t1.secondo_livello_post_evar, t1.terzo_livello_post_evar, 
                                t1.hit_source, t1.exclude_hit
                              from tab_sito_pubb_3 t1 
                              inner join base_pdisc_coo_2 t2
                                on t1.post_visid_concatenated = t2.post_visid_concatenated") 

join_sito_pub_2 <- filter(join_sito_pub_1,"ts <= data_ingr_pdisc_formdate")



write.parquet(join_sito_pub_2,"/user/stefano.mazzucca/CJ_PDISC_tab_sito_pub.parquet")

CJ_PDISC_tab_sito_pub <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_tab_sito_pub.parquet")
View(head(CJ_PDISC_tab_sito_pub,100))
nrow(CJ_PDISC_tab_sito_pub)
# 1.926.447

ver_valdist_utenti_tot <- distinct(select(CJ_PDISC_tab_sito_pub, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_utenti_tot)
# 21.110



filter_sito_pubb <- filter(CJ_PDISC_tab_sito_pub,
                  "secondo_livello_post_evar like '%guidatv%' or
                   secondo_livello_post_evar like '%Guidatv%' or
                   secondo_livello_post_evar like '%pagine di servizio%' or
                   secondo_livello_post_evar like '%assistenza%' or
                   secondo_livello_post_evar like '%fai da te%' or
                   secondo_livello_post_evar like '%extra%' or
                   secondo_livello_post_evar like '%tecnologia%' or
                   secondo_livello_post_evar like '%pacchetti-offerte%' or
                   terzo_livello_post_evar   like '%conosci%' or
                   terzo_livello_post_evar   like '%gestisci%' or
                   terzo_livello_post_evar   like '%contatta%' or 
                   terzo_livello_post_evar   like '%home%' or
                   terzo_livello_post_evar   like '%ricerca%' or
                   terzo_livello_post_evar   like '%risolvi%' or
                   page_url_post_evar like '%/pacchetti-offerte/%'")

View(head(filter_sito_pubb,100))
nrow(filter_sito_pubb)
# 983.955

ver_valdist_utenti <- distinct(select(filter_sito_pubb, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_utenti)
# 20.547 utenti che navigano sul sito pubblico (vs 37.753 dei pdisc totali)




## per il calcolo del numero di visite:

createOrReplaceTempView(filter_sito_pubb,"filter_sito_pubb")
#createOrReplaceTempView(CJ_PDISC_tab_sito_pub,"CJ_PDISC_tab_sito_pub")

num_visite_sito_pub_count <- sql("select COD_CLIENTE_CIFRATO,post_visid_concatenated,
                                  count(distinct concatena_chiave) as count_visite
                                 from filter_sito_pubb
                                 where hit_source = 1 AND exclude_hit = 0
                                 group by COD_CLIENTE_CIFRATO,post_visid_concatenated")
View(head(num_visite_sito_pub_count,100))
nrow(num_visite_sito_pub_count)
# 63.802
# 70.323

createOrReplaceTempView(num_visite_sito_pub_count,"num_visite_sito_pub_count")
num_visite_sito_pub_count_2 <- sql("select COD_CLIENTE_CIFRATO,
                                  sum(count_visite) as tot_visite
                                 from num_visite_sito_pub_count
                                 group by COD_CLIENTE_CIFRATO")
View(head(num_visite_sito_pub_count_2,100))
nrow(num_visite_sito_pub_count_2)
# 20.547 utenti che hanno fatto almeno una visita al sito pubblico di Sky
# 21.110



#############################################################################################################################################################################
### Tabellone al WSC
#############################################################################################################################################################################

tab_wsc_1 <- filter(skyitdev_df_4,"terzo_livello_post_evar LIKE '%faidate%' ")

tab_wsc_2 <- withColumn(tab_wsc_1,"concatena_chiave",concat(tab_wsc_1$post_visid_concatenated, tab_wsc_1$visit_num))

tab_wsc_3 <- distinct(select(tab_wsc_2,"external_id_post_evar", "post_visid_concatenated", "visit_num", "ts", "concatena_chiave", "hit_source", "exclude_hit",
                             "page_url_post_evar", "page_url_post_prop", "download_name_post_prop", "secondo_livello_post_evar","terzo_livello_post_evar"))

tab_wsc_4 <- filter(tab_wsc_3,"external_id_post_evar is not NULL")


#join
createOrReplaceTempView(tab_wsc_4,"tab_wsc_4")
createOrReplaceTempView(base_pdisc_coo_2,"base_pdisc_coo_2")

join_wsc_1 <- sql("select distinct t2.COD_CLIENTE_CIFRATO, t2.post_visid_concatenated, t1.concatena_chiave, t1.visit_num, t1.ts,
                                t2.data_ingr_pdisc_formdate, t2.digitalizzazione_bin, 
                                t1.page_url_post_evar, t1.page_url_post_prop, t1.download_name_post_prop, t1.secondo_livello_post_evar, t1.terzo_livello_post_evar, 
                                t1.hit_source, t1.exclude_hit
                        from tab_wsc_4 t1 
                        inner join base_pdisc_coo_2 t2
                          on t1.post_visid_concatenated = t2.post_visid_concatenated
                          and t1.ts <= t2.data_ingr_pdisc_formdate")



write.parquet(join_wsc_1,"/user/stefano.mazzucca/CJ_PDISC_tab_wsc.parquet")

CJ_PDISC_tab_wsc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_tab_wsc.parquet")
View(head(CJ_PDISC_tab_wsc,100))
nrow(CJ_PDISC_tab_wsc)
# 582.479


filter_wsc <- filter(CJ_PDISC_tab_wsc,"
                    terzo_livello_post_evar   like '%faidate gestisci dati servizi%' or
                    terzo_livello_post_evar   like '%faidate arricchisci abbonamento%' or
                    terzo_livello_post_evar   like '%faidate fatture pagamenti%' or
                    terzo_livello_post_evar   like '%faidate webtracking%' or
                    terzo_livello_post_evar   like '%faidate extra%' or
                    page_url_post_evar like '%abbonamento.sky.it/skyportal/faces/faiDaTe/subscription%'
                   ")

View(head(filter_wsc,100))
nrow(filter_wsc)
# 424.839



## per il calcolo del numero di visite:

createOrReplaceTempView(filter_wsc,"filter_wsc")
num_visite_wsc_count <- sql("select COD_CLIENTE_CIFRATO,post_visid_concatenated,
                                  count(distinct concatena_chiave) as count_visite
                                 from filter_wsc
                                 where hit_source = 1 AND exclude_hit = 0
                                 group by COD_CLIENTE_CIFRATO,post_visid_concatenated")
View(head(num_visite_wsc_count,100))
nrow(num_visite_wsc_count)
# 39.800

createOrReplaceTempView(num_visite_wsc_count,"num_visite_wsc_count")
num_visite_wsc_count_2 <- sql("select COD_CLIENTE_CIFRATO,
                                  sum(count_visite) as tot_visite
                                 from num_visite_wsc_count
                                 group by COD_CLIENTE_CIFRATO")
View(head(num_visite_wsc_count_2,100))
nrow(num_visite_wsc_count_2)
# 16.633 utenti che hanno fatto almeno una visita al WSC di Sky



#############################################################################################################################################################################
### Tabellone APP
#############################################################################################################################################################################

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")

skyappwsc_2 <- withColumn(skyappwsc, "ts", cast(skyappwsc$date_time, "timestamp"))
# test_1 <- withColumn(skyappwsc_2, "date", cast(skyappwsc_2$ts, "date"))
# test_2 <- distinct(select(skyappwsc,"date"))
# write.df(repartition(test_2, 1),path = "/user/stefano.mazzucca/export_date_app.csv", "csv", sep=";", mode = "overwrite", header=TRUE)

skyappwsc_3 <- filter(skyappwsc_2,"ts>='2017-03-01 00:00:00'")
skyappwsc_4 <- filter(skyappwsc_3,"ts<='2018-01-01 00:00:00' ")

# test_3 <- withColumn(skyappwsc_4, "date", cast(skyappwsc_3$ts, "date"))
# test_4 <- distinct(select(test_3,"date"))
# skyappwsc_5 <- arrange(skyappwsc_4,desc(skyappwsc_4$ts))
# sel_skyappwsc_5 <- distinct(select(skyappwsc_5, "ts"))
# write.df(repartition(sel_skyappwsc_5, 1),path = "/user/stefano.mazzucca/export_date_app_3.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
# 
# View(head(skyappwsc_5,1000))
# 
# 
# write.df(repartition(test_2, 1),path = "/user/stefano.mazzucca/export_date_app_2.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


skyappwsc_4 <- select(skyappwsc_3,"external_id_post_evar", "site_section", "page_name_post_evar", "page_name_post_prop", "post_pagename",
                                    "ts", "visit_num", "post_visid_concatenated", "hit_source", "exclude_hit")

skyappwsc_5 <- withColumn(skyappwsc_4,"concatena_chiave",concat(skyappwsc_4$post_visid_concatenated,skyappwsc_4$visit_num))

skyappwsc_6 <- filter(skyappwsc_5,"external_id_post_evar is not NULL")

createOrReplaceTempView(skyappwsc_6,"skyappwsc_6")
# test <- sql("select min(ts), max(ts)
#             from skyappwsc_6")
# View(head(test,100))

#join
createOrReplaceTempView(skyappwsc_6,"skyappwsc_6")
createOrReplaceTempView(base_pdisc_coo_2,"base_pdisc_coo_2")

join_app_1 <- sql("select distinct t2.COD_CLIENTE_CIFRATO, t2.post_visid_concatenated, t1.concatena_chiave, t1.visit_num, t1.ts,
                              t1.site_section, t1.page_name_post_evar, t1.page_name_post_prop, t1.post_pagename,
                              t2.data_ingr_pdisc_formdate, t2.digitalizzazione_bin, t1.hit_source, t1.exclude_hit
                        from skyappwsc_6 t1 
                        inner join base_pdisc_coo_2 t2
                          on t1.external_id_post_evar = t2.COD_CLIENTE_CIFRATO 
                          and t1.ts <= t2.data_ingr_pdisc_formdate")


write.parquet(join_app_1  ,"/user/stefano.mazzucca/CJ_PDISC_tab_app.parquet")

CJ_PDISC_tab_app <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_tab_app.parquet")
View(head(CJ_PDISC_tab_app,100))
nrow(CJ_PDISC_tab_app)
# 7.434.135 # 9.225.281

ver_valdist_utenti_app_tot <- distinct(select(CJ_PDISC_tab_app, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_utenti_app_tot)
# 9.498 utenti che utlizzano l'APP di Sky



filter_app <- filter(CJ_PDISC_tab_app,"
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
                      page_name_post_evar like '%widget:gestisci%' or
                      page_name_post_evar='arricchisci abbonamento:home'
                      ")

View(head(filter_app,100))
nrow(filter_app)
# 2.844.660

ver_valdist_utenti_app <- distinct(select(filter_app, "COD_CLIENTE_CIFRATO"))
nrow(ver_valdist_utenti_app)
# 8.985 utenti che utilizzano il WSC APP di Sky


createOrReplaceTempView(CJ_PDISC_tab_app,"CJ_PDISC_tab_app")
createOrReplaceTempView(base_pdisc_coo,"base_pdisc_coo")

ver_join_app <- sql("select distinct t1.COD_CLIENTE_CIFRATO
                    from base_pdisc_coo t1
                    inner join CJ_PDISC_tab_app t2
                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
nrow(ver_join_app)
# 9.073 (vs 9.498 che utilizzano l'APP)

####### Verifica ########################################################################################################

createOrReplaceTempView(ver_valdist_utenti_app_tot, "ver_valdist_utenti_app_tot")
createOrReplaceTempView(ver_join_app, "ver_join_app")

missing_from_device <- sql("select t2.COD_CLIENTE_CIFRATO
                           from ver_valdist_utenti_app_tot t1
                           left join ver_join_app t2
                            on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO 
                            and t1.COD_CLIENTE_CIFRATO is NULL")
nrow(missing_from_device)
# 9073
#########################################################################################################################


## per il calcolo del numero di visite:

createOrReplaceTempView(filter_app,"filter_app")
num_visite_app_count <- sql("select COD_CLIENTE_CIFRATO,post_visid_concatenated,
                                  count(distinct concatena_chiave) as count_visite
                                 from filter_app
                                 where hit_source = 1 AND exclude_hit = 0
                                 group by COD_CLIENTE_CIFRATO,post_visid_concatenated")
View(head(num_visite_app_count,100))
nrow(num_visite_app_count)
# 75.958

createOrReplaceTempView(num_visite_app_count,"num_visite_app_count")
num_visite_app_count_2 <- sql("select COD_CLIENTE_CIFRATO,
                                  sum(count_visite) as tot_visite
                                 from num_visite_app_count
                                 group by COD_CLIENTE_CIFRATO")
View(head(num_visite_app_count_2,100))
nrow(num_visite_app_count_2)
# 8.985 utenti che hanno fatto almeno una visita sull'APP di Sky




#chiudi sessione
sparkR.stop()

