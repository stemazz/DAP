
# CJ_PDISC_vers08
# count_download_disdetta con confronto su clienti totali che visitano la pagina


#apri sessione
source("connection_R.R")
options(scipen = 1000)



##### Ricostruisco la base pdisc con le chiavi + cookies

######### Recupero le chiavi

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
# skyitdev_df_1 <- filter(skyitdev_df,"external_id_post_evar is not NULL")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated", "date_time",
                        "visit_num", "page_url_post_evar", "page_url_post_prop", "download_name_post_prop", "hit_source","exclude_hit")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated", "date_time",
                                 "visit_num", "page_url_post_evar", "page_url_post_prop", "download_name_post_prop", "hit_source","exclude_hit"))

skyitdev_df_4 <- withColumn(skyitdev_df_3, "date", cast(skyitdev_df_3$date_time, "date"))
skyitdev_df_5 <- withColumn(skyitdev_df_4, "ts", cast(skyitdev_df_4$date_time, "timestamp"))

skyitdev_df_6 <- filter(skyitdev_df_5, "ts >= '2017-10-01 00:00:00' and ts <= '2017-12-31 00:00:00'") ## filtro temporale Ott-Nov-Dic

skyitdev_df_7 <- filter(skyitdev_df_6, "page_url_post_evar like '%/assistenza/info-disdetta%' or  page_url_post_prop LIKE '%moduli%' or download_name_post_prop LIKE '%modulo_disdetta%'")
cache(skyitdev_df_7)


#### Salvataggio ####

write.parquet(skyitdev_df_7,"/user/stefano.mazzucca/CJ_PDISC_visite_tot_pag_disdetta_2.parquet")

CJ_PDISC_visite_tot_pag_disdetta <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_visite_tot_pag_disdetta_2.parquet")
View(head(CJ_PDISC_visite_tot_pag_disdetta,100))
nrow(CJ_PDISC_visite_tot_pag_disdetta)
# 1.433.892








############### join con pagina disdetta


## Preparo la base_pdisc_coo a cui aggancio anche il giorno_ingr_pdisc

base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
View(head(base_pdisc,100))
nrow(base_pdisc)
# 37.976

valdist_base <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_base)
# 37.753

base_pdisc_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc.parquet")


createOrReplaceTempView(base_pdisc, "base_pdisc")
createOrReplaceTempView(base_pdisc_coo, "base_pdisc_coo")

base_pdisc_coo_2 <- sql("select t1.*, t2.external_id_post_evar, t2.post_visid_concatenated
                        from base_pdisc t1
                        left join base_pdisc_coo t2
                          on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
View(head(base_pdisc_coo_2,100))
nrow(base_pdisc_coo_2)
# 176.511





# creo la chiave per contare le visite
CJ_PDISC_visite_tot_pag_disdetta <- withColumn(CJ_PDISC_visite_tot_pag_disdetta,"concatena_chiave",
                              concat(CJ_PDISC_visite_tot_pag_disdetta$post_visid_concatenated, CJ_PDISC_visite_tot_pag_disdetta$visit_num))


createOrReplaceTempView(CJ_PDISC_visite_tot_pag_disdetta,"CJ_PDISC_visite_tot_pag_disdetta")
createOrReplaceTempView(base_pdisc_coo_2,"base_pdisc_coo_2")

join_base_pag_disdetta <- sql("select t1.COD_CLIENTE_CIFRATO, t1.post_visid_concatenated, t2.concatena_chiave, t1.data_ingr_pdisc_formdate, t1.digitalizzazione_bin, 
                                t2.visit_num, t2.page_url_post_evar, t2.page_url_post_prop, t2.download_name_post_prop,
                                t2.hit_source, t2.exclude_hit,
                                t2.ts as data_ora_vis_pag
                              from base_pdisc_coo_2 t1 
                              inner join CJ_PDISC_visite_tot_pag_disdetta t2
                                on t1.post_visid_concatenated = t2.post_visid_concatenated")



#### Salvataggio ####

write.parquet(join_base_pag_disdetta,"/user/stefano.mazzucca//CJ_PDISC_visite_pdisc_pag_disdetta_2.parquet")

CJ_PDISC_visite_pdisc_pag_disdetta <- read.parquet("/user/stefano.mazzucca//CJ_PDISC_visite_pdisc_pag_disdetta_2.parquet")
View(head(CJ_PDISC_visite_pdisc_pag_disdetta,100))
nrow(CJ_PDISC_visite_pdisc_pag_disdetta)
# 59.654





############################################################################################################################################################################
## Analisi ##################################################################################################################################################################
############################################################################################################################################################################


#CJ_PDISC_visite_pdisc_pag_disdetta <- read.parquet("/user/stefano.mazzucca//CJ_PDISC_visite_pdisc_pag_disdetta_2.parquet")
#CJ_PDISC_visite_tot_pag_disdetta <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_visite_tot_pag_disdetta_2.parquet")



createOrReplaceTempView(CJ_PDISC_visite_tot_pag_disdetta,"CJ_PDISC_visite_tot_pag_disdetta")

count_distr_tot <- sql("select post_visid_concatenated,  
                          min(ts) as min_data,
                          max(ts) as max_data,
                          count(distinct(concatena_chiave)) as count_distinct_visit,
                          count(concatena_chiave) as count_visit,
                          count(download_name_post_prop) as dwnl,
                          count(distinct(download_name_post_prop)) as valdist_dwnl
                      from CJ_PDISC_visite_tot_pag_disdetta
                      group by post_visid_concatenated
                      order by post_visid_concatenated")

View(head(count_distr_tot,100))
nrow(count_distr_tot)
# 414.948


#write.df(repartition(count_distr_tot, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_distr_tot.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




CJ_PDISC_visite_pdisc_pag_disdetta_2 <- withColumn(CJ_PDISC_visite_pdisc_pag_disdetta, "data_vis_pag", cast(CJ_PDISC_visite_pdisc_pag_disdetta$data_ora_vis_pag, "date"))
CJ_PDISC_visite_pdisc_pag_disdetta_3 <- withColumn(CJ_PDISC_visite_pdisc_pag_disdetta_2, "days_from_pdisc", 
                                                   datediff(CJ_PDISC_visite_pdisc_pag_disdetta_2$data_ingr_pdisc_formdate, CJ_PDISC_visite_pdisc_pag_disdetta_2$data_vis_pag))
CJ_PDISC_visite_pdisc_pag_disdetta_3 <- filter(CJ_PDISC_visite_pdisc_pag_disdetta_3, "data_vis_pag <= data_ingr_pdisc_formdate")


createOrReplaceTempView(CJ_PDISC_visite_pdisc_pag_disdetta_3,"CJ_PDISC_visite_pdisc_pag_disdetta_3")

count_distr_pdisc <- sql("select external_id_post_evar, 
                      min(data_ora_vis_pag) as min_data,
                      max(data_ora_vis_pag) as max_data,
                      count(distinct(concatena_chiave)) as count_distinct_visit,
                      count(concatena_chiave) as count_visit,
                      count(download_name_post_prop) as dwnl,
                      count(distinct(download_name_post_prop)) as valdist_dwnl
                   from CJ_PDISC_visite_pdisc_pag_disdetta_3
                   group by external_id_post_evar
                   order by external_id_post_evar")

View(head(count_distr_pdisc,100))
nrow(count_distr_pdisc)
# 8.954


#write.df(repartition(count_distr_pdisc, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_distr_pdisc.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




#######################################################################################################################################
# COUNT visite (intese come cookie+visit_num):
#######################################################################################################################################




createOrReplaceTempView(CJ_PDISC_visite_tot_pag_disdetta,"CJ_PDISC_visite_tot_pag_disdetta")
count_visit_tot <- sql("select concatena_chiave, 
                                  count(*) as count_visite,
                                  min(ts) as min_data,
                                  max(ts) as max_data,
                                  count(download_name_post_prop) as count_dwnl,
                                  count(distinct(download_name_post_prop)) as count_valdist_dwnl
                                 from CJ_PDISC_visite_tot_pag_disdetta
                                 where hit_source = 1 and exclude_hit = 0
                                 group by concatena_chiave")
View(head(count_visit_tot,100))
nrow(count_visit_tot)
# 503.565

###########################



createOrReplaceTempView(CJ_PDISC_visite_pdisc_pag_disdetta_3,"CJ_PDISC_visite_pdisc_pag_disdetta_3")
count_visit_pdisc <- sql("select external_id_post_evar, concatena_chiave,
                                  count(*) as count_visite,
                                  min(data_ora_vis_pag) as min_data,
                                  max(data_ora_vis_pag) as max_data,
                                  count(download_name_post_prop) as count_dwnl,
                                  count(distinct(download_name_post_prop)) as count_valdist_dwnl,
                                  last(days_from_pdisc) as last_day_from_pdisc
                                 from CJ_PDISC_visite_pdisc_pag_disdetta_3
                                 where hit_source = 1 AND exclude_hit = 0
                                 group by external_id_post_evar, concatena_chiave")
View(head(count_visit_pdisc,100))
nrow(count_visit_pdisc)
# 14.929 -> 15.079 (con ext_id + concatena_chiave)

createOrReplaceTempView(count_visit_pdisc,"count_visit_pdisc")
count_visit_pdisc_2 <- sql("select external_id_post_evar,
                                  sum(count_visite) as tot_visite,
                                  sum(count_dwnl) as tot_dwnl,
                                  min(last_day_from_pdisc) as day_from_pdisc
                                 from count_visit_pdisc
                                 group by external_id_post_evar")

View(head(count_visit_pdisc_2,100))
nrow(count_visit_pdisc_2)
# 8.954


#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################



# Aggiungo l'informazione weekday:

CJ_PDISC_visite_tot_pag_disdetta_2 <- withColumn(CJ_PDISC_visite_tot_pag_disdetta, "weekday", date_format(CJ_PDISC_visite_tot_pag_disdetta$date, 'EEEE'))
View(head(CJ_PDISC_visite_tot_pag_disdetta_2,1000))

CJ_PDISC_visite_pdisc_pag_disdetta_4 <- withColumn(CJ_PDISC_visite_pdisc_pag_disdetta_3, "weekday", date_format(CJ_PDISC_visite_pdisc_pag_disdetta$data_ora_vis_pag, 'EEEE'))
View(head(CJ_PDISC_visite_pdisc_pag_disdetta_2,1000))




## Distribuzione giorni della settimana

####### Su tot_visite

createOrReplaceTempView(CJ_PDISC_visite_tot_pag_disdetta_2,"CJ_PDISC_visite_tot_pag_disdetta_2")

distr_tot_weekday <- sql("select weekday, count(weekday)
                     from CJ_PDISC_visite_tot_pag_disdetta_2
                     group by weekday")
View(head(distr_tot_weekday,100))


#write.df(repartition(distr_tot_weekday, 1),path = "/user/stefano.mazzucca/CJ_PDISC_distr_tot_weekday.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




######## Su visite dei "soli" pdisc

createOrReplaceTempView(CJ_PDISC_visite_pdisc_pag_disdetta_4,"CJ_PDISC_visite_pdisc_pag_disdetta_4")

distr_pdisc_weekday <- sql("select weekday, count(weekday)
                     from CJ_PDISC_visite_pdisc_pag_disdetta_4
                     group by weekday")
View(head(distr_pdisc_weekday,100))


#write.df(repartition(distr_pdisc_weekday, 1),path = "/user/stefano.mazzucca/CJ_PDISC_distr_pdisc_weekday.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





#######################################################################################################################
## Filtro sui DOWNLOAD effettuati dei MODULI DISDETTA ################################################################
#######################################################################################################################

CJ_PDISC_dwnl_mod_tot <- filter(CJ_PDISC_visite_tot_pag_disdetta_2, "download_name_post_prop is NOT NULL")
nrow(CJ_PDISC_dwnl_mod_tot)
# 174.626

filt_utenti_dwnl_mod_tot <- filter(CJ_PDISC_dwnl_mod_tot, "external_id_post_evar is NULL")
nrow(filt_utenti_dwnl_mod_tot)
# 112.185 download di moduli effettuati da utenti non riconosciuti da external_ID



CJ_PDISC_dwnl_mod_pdisc <- filter(CJ_PDISC_visite_pdisc_pag_disdetta_4, "download_name_post_prop is NOT NULL")
nrow(CJ_PDISC_dwnl_mod_pdisc)
# 10.707



## count dei dwnload sul TOT e sulla pdisc "trovata"


createOrReplaceTempView(CJ_PDISC_dwnl_mod_tot,"CJ_PDISC_dwnl_mod_tot")

count_tot_dwnl <- sql("select post_visid_concatenated, ts, visit_num,
                   min(ts) as min_data,
                   max(ts) as max_data,
                   count(download_name_post_prop) as count_dwnl,
                   count(distinct(download_name_post_prop)) as count_valdist_dwnl
                   from CJ_PDISC_dwnl_mod_tot
                   group by post_visid_concatenated, ts, visit_num
                   order by post_visid_concatenated")

View(head(count_tot_dwnl,100))
nrow(count_tot_dwnl)
# 174.583 download distinti di moduli disdetta da TUTTI gli utenti che hanno navigato sulla pagina disdetta



#write.df(repartition(count_tot_dwnl, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_tot_dwnl.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





createOrReplaceTempView(CJ_PDISC_dwnl_mod_pdisc,"CJ_PDISC_dwnl_mod_pdisc")

count_pdisc_dwnl <- sql("select external_id_post_evar, data_ora_vis_pag, visit_num, days_from_pdisc,
                   min(data_ora_vis_pag) as min_data,
                   max(data_ora_vis_pag) as max_data,
                   count(download_name_post_prop) as count_dwnl,
                   count(distinct(download_name_post_prop)) as count_valdist_dwnl
                   from CJ_PDISC_dwnl_mod_pdisc
                   group by external_id_post_evar, data_ora_vis_pag, visit_num, days_from_pdisc
                   order by external_id_post_evar")

View(head(count_pdisc_dwnl,100))
nrow(count_pdisc_dwnl)
# 10.645 download distinti di moduli disdetta dai clienti PDISC



#write.df(repartition(count_pdisc_dwnl, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_pdisc_dwnl.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



# count dwnl sul days_from_pdisc

distr_tot_dwnl <- summarize(groupBy(count_tot_dwnl, count_tot_dwnl$days_from_pdisc), 
                                         count_data_dwnl = count(count_tot_dwnl$days_from_pdisc))
View(head(distr_tot_dwnl,100))



#write.df(repartition(distr_tot_dwnl, 1),path = "/user/stefano.mazzucca/CJ_PDISC_distr_tot_dwnl.csv", "csv", sep=";", mode = "overwrite", header=TRUE)















#chiudi sessione
sparkR.stop()

