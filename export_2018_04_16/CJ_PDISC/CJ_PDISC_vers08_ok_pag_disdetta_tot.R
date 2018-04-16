
# CJ_PDISC_vers08_def
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


CJ_PDISC_visite_pdisc_pag_disdetta_2 <- withColumn(CJ_PDISC_visite_pdisc_pag_disdetta, "data_vis_pag", cast(CJ_PDISC_visite_pdisc_pag_disdetta$data_ora_vis_pag, "date"))
CJ_PDISC_visite_pdisc_pag_disdetta_3 <- withColumn(CJ_PDISC_visite_pdisc_pag_disdetta_2, "days_from_pdisc", 
                                                   datediff(CJ_PDISC_visite_pdisc_pag_disdetta_2$data_ingr_pdisc_formdate, CJ_PDISC_visite_pdisc_pag_disdetta_2$data_vis_pag))
CJ_PDISC_visite_pdisc_pag_disdetta_3 <- filter(CJ_PDISC_visite_pdisc_pag_disdetta_3, "data_vis_pag <= data_ingr_pdisc_formdate")





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


#export

write.df(repartition(count_visit_tot, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_visit_tot.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



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



#export

write.df(repartition(count_visit_pdisc_2, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_visit_pdisc.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################

#######################################################################################################################################
# COUNT download moduli disdetta:
#######################################################################################################################################

count_dwnl_tot <- filter(count_visit_tot, "count_dwnl != 0")
View(head(count_dwnl_tot,100))
nrow(count_dwnl_tot)
# 119.139

#export

write.df(repartition(count_dwnl_tot, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_dwnl_tot.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



count_dwnl_pdisc <- filter(count_visit_pdisc_2, "tot_dwnl != 0")
View(head(count_dwnl_pdisc,100))
nrow(count_dwnl_pdisc)
# 5.520

#export

write.df(repartition(count_dwnl_pdisc, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_dwnl_pdisc.csv", "csv", sep=";", mode = "overwrite", header=TRUE)







################################################################################################################################################################################
## Export totale ##############################################################################################################################################################
################################################################################################################################################################################


CJ_PDISC_visite_tot_pag_disdetta <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_visite_tot_pag_disdetta_2.parquet")
View(head(CJ_PDISC_visite_tot_pag_disdetta,100))
nrow(CJ_PDISC_visite_tot_pag_disdetta)
# 1.433.892

# Aggiungo l'informazione weekday:
CJ_PDISC_visite_tot_pag_disdetta_2 <- withColumn(CJ_PDISC_visite_tot_pag_disdetta, "weekday", date_format(CJ_PDISC_visite_tot_pag_disdetta$ts, 'EEEE'))
View(head(CJ_PDISC_visite_tot_pag_disdetta_2,1000))

# Aggiungo l'informazione oraria
CJ_PDISC_visite_tot_pag_disdetta_3 <- withColumn(CJ_PDISC_visite_tot_pag_disdetta_2, "orario_dwnl", hour(CJ_PDISC_visite_tot_pag_disdetta_2$ts))
View(head(CJ_PDISC_visite_tot_pag_disdetta_3,100))


createOrReplaceTempView(CJ_PDISC_visite_tot_pag_disdetta_3,"CJ_PDISC_visite_tot_pag_disdetta_3")

CJ_PDISC_visite_tot_pag_disdetta_4 <- sql("select *,
                                                  case when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                                                      and orario_dwnl >= 0 and orario_dwnl <= 8 then 'casa'
                                                  when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                                                      and orario_dwnl >= 9 and orario_dwnl <= 18 then 'ufficio'
                                                  when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                                                      and orario_dwnl >= 19 and orario_dwnl <= 23 then 'casa'
                                                  else 'casa' end as flg_dwnl_casa_ufficio
                                               from CJ_PDISC_visite_tot_pag_disdetta_3")
View(head(CJ_PDISC_visite_tot_pag_disdetta_4,100))
nrow(CJ_PDISC_visite_tot_pag_disdetta_4)
# 1.433.892


createOrReplaceTempView(CJ_PDISC_visite_tot_pag_disdetta_4,"CJ_PDISC_visite_tot_pag_disdetta_4")
distr_tot <- sql("select weekday, flg_dwnl_casa_ufficio, 
                      count(external_id_post_evar) as num_vis
                   from CJ_PDISC_visite_tot_pag_disdetta_4
                   group by weekday, flg_dwnl_casa_ufficio")
View(head(distr_tot,100))


write.df(repartition(distr_tot, 1),path = "/user/stefano.mazzucca/CJ_PDISC_distr_tot.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


###########################

filt_dwnl_tot <- filter(CJ_PDISC_visite_tot_pag_disdetta_4, "download_name_post_prop is NOT NULL")
nrow(filt_dwnl_tot)
# 174.626

createOrReplaceTempView(filt_dwnl_tot,"filt_dwnl_tot")
distr_dwnl_tot <- sql("select weekday, flg_dwnl_casa_ufficio, 
                      count(external_id_post_evar) as num_dwnl
                   from filt_dwnl_tot
                   group by weekday, flg_dwnl_casa_ufficio")
View(head(distr_dwnl_tot,100))


write.df(repartition(distr_dwnl_tot, 1),path = "/user/stefano.mazzucca/CJ_PDISC_distr_dwnl_tot.csv", "csv", sep=";", mode = "overwrite", header=TRUE)






#chiudi sessione
sparkR.stop()

