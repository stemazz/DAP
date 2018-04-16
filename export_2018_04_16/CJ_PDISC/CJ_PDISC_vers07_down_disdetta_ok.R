
# CJ_PDISC_vers07 
# count_download_disdetta


#apri sessione
source("connection_R.R")
options(scipen = 1000)


# # Recupero la base PDISC
# 
# base_pdisc_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_pdisc_coo.parquet")
# View(head(base_pdisc_coo))
# nrow(base_pdisc_coo)
# # 176.511
# 
# ver_clienti <- distinct(select(base_pdisc_coo, "COD_CLIENTE_CIFRATO"))
# nrow(ver_clienti)
# # 37.753
# ver_clienti_ <- summarize(groupBy(base_pdisc_coo, base_pdisc_coo$COD_CLIENTE_CIFRATO, base_pdisc_coo$post_visid_concatenated), count = count(base_pdisc_coo$COD_CLIENTE_CIFRATO))
# nrow(ver_clienti_)
# # 175.179
# ver_clienti_2 <- filter(base_pdisc_coo, "post_visid_concatenated is NULL")
# nrow(ver_clienti_2)
# # 13.469
# ver_clienti_2_dist <- distinct(select(ver_clienti_2, "COD_CLIENTE_CIFRATO"))
# nrow(ver_clienti_2_dist)
# # 13.413


##### Ricostruisco la base pdisc con le chiavi + cookies

######### Recupero le chiavi

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
# skyitdev_df_1 <- filter(skyitdev_df,"external_id_post_evar is not NULL")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated"))


## Recupero il target PDISC su cui fare le analisi

base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
View(head(base_pdisc,100))
nrow(base_pdisc)
# 37.976

valdist_base <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_base)
# 37.753

createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
createOrReplaceTempView(valdist_base,"valdist_base")

recupera_cookies <- sql("select *
                       from valdist_base  t1
                       left join skyitdev_df_3 t2
                       on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
View(head(recupera_cookies,100))
nrow(recupera_cookies)


# Salvataggio delle chiavi

write.parquet(recupera_cookies,"/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc_2.parquet") ##NON ?? servito perch?? (in questo caso) andava bene la precedente base.









###########################################################################################################################################################################
## Pagina disdetta + download moduli disdetta #############################################################################################################################
###########################################################################################################################################################################



# reportsuite
skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
#names(skyitdev_df)


skyit_v2 <- select(skyitdev_df,
                        "external_id_post_evar", "post_visid_high", "post_visid_low", "post_visid_concatenated", "visit_num",
                        "date_time",
                        "page_url_post_evar",
                        "page_url_post_prop",
                        "download_name_post_prop")

# skyitdev_df_3 <- filter(skyitdev_df_2,"external_id_post_evar is not NULL and stack_tracking_code_post_evar is not NULL")

skyit_v4 <- withColumn(skyit_v2, "ts", cast(skyit_v2$date_time, "timestamp"))
skyit_v5 <- withColumn(skyit_v4, "date", cast(skyit_v4$date_time, "date"))
# skyit_v6 <- filter(skyit_v5,"ts>='2017-03-01 00:00:00' and  ts<='2018-01-01 00:00:00' ")

cache(skyit_v5)
#View(head(skyit_v5,200))


skyit_v6 <- filter(skyit_v5, "page_url_post_evar like '%/assistenza/info-disdetta%' or  page_url_post_prop LIKE '%moduli%' or download_name_post_prop LIKE '%modulo_disdetta%'")
View(head(skyit_v6,100))



#################################################################################################################
#################################################################################################################
#### join base con le visite alla pagina di disdetta e download moduli
#################################################################################################################
#################################################################################################################

createOrReplaceTempView(skyit_v6, "skyit_v6")
createOrReplaceTempView(base_pdisc_coo, "base_pdisc_coo")


join <- sql("select t1.COD_CONTRATTO, t1.data_ingr_pdisc_formdate, t1.DES_ULTIMA_CAUSALE_CESSAZIONE, t1.DAT_RICH_CESSAZIONE_CNTR, t1.DES_PACCHETTO_POSS_FROM, 
                t1.DES_AREA_NIELSEN_FRU, t1.cl_tenure, t1.digitalizzazione_bin, t1.COD_CLIENTE_CIFRATO, t1.post_visid_concatenated,
                t2.visit_num, t2.ts, t2.date, t2.page_url_post_evar, t2.page_url_post_prop, t2.download_name_post_prop
            from base_pdisc_coo t1
            inner join skyit_v6 t2
            on t1.post_visid_concatenated = t2.post_visid_concatenated and t2.date <= t1.data_ingr_pdisc_formdate and t2.date >= '2017-03-01'")


write.parquet(join, "/user/stefano.mazzucca/CJ_PDISC_scarico_pag_disdetta_moduli.parquet")


# leggi tabella
CJ_PDISC_scarico_pag_disdetta_moduli <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_pag_disdetta_moduli.parquet")

View(head(CJ_PDISC_scarico_pag_disdetta_moduli,100))
nrow(CJ_PDISC_scarico_pag_disdetta_moduli) 
# 79.908

valdist_pag_disdetta_moduli <- distinct(select(CJ_PDISC_scarico_pag_disdetta_moduli,"COD_CLIENTE_CIFRATO"))
nrow(valdist_pag_disdetta_moduli)
# 11.022


# Aggiungo l'informazione weekday:
CJ_PDISC_scarico_pag_disdetta_moduli <- withColumn(CJ_PDISC_scarico_pag_disdetta_moduli, "weekday", date_format(CJ_PDISC_scarico_pag_disdetta_moduli$ts, 'EEEE'))
View(head(CJ_PDISC_scarico_pag_disdetta_moduli,1000))





############################################################################################################################################################################
## Analisi ##################################################################################################################################################################
############################################################################################################################################################################


createOrReplaceTempView(CJ_PDISC_scarico_pag_disdetta_moduli,"CJ_PDISC_scarico_pag_disdetta_moduli")

count_distr <- sql("select COD_CLIENTE_CIFRATO, data_ingr_pdisc_formdate, 
                      min(date) as min_data,
                      max(date) as max_data,
                      count(distinct(visit_num)) as count_visit,
                      count(download_name_post_prop) as dwnl,
                      count(distinct(download_name_post_prop)) as valdist_dwnl
                   from CJ_PDISC_scarico_pag_disdetta_moduli
                   group by COD_CLIENTE_CIFRATO, data_ingr_pdisc_formdate
                   order by COD_CLIENTE_CIFRATO")

View(head(count_distr,100))
nrow(count_distr)
# 11.022 -> 11.035 (con data_ingr_pdisc) -> 18.520 (con date = data evento)



count_partition_dwnl_mod <- sql("select distinct COD_CLIENTE_CIFRATO, download_name_post_prop,
                                  count(download_name_post_prop) over(partition by COD_CLIENTE_CIFRATO) as tipologia_mod_dwnl,
                                  count(download_name_post_prop) as count_dwnl_effettuati
                                from CJ_PDISC_scarico_pag_disdetta_moduli 
                                where download_name_post_prop is not null
                                group by COD_CLIENTE_CIFRATO, download_name_post_prop")
View(head(count_partition_dwnl_mod,100))
nrow(count_partition_dwnl_mod)
# 9.512


distr_mod_dwnl <- sql("select distinct download_name_post_prop, count(download_name_post_prop)
                      from CJ_PDISC_scarico_pag_disdetta_moduli
                      where download_name_post_prop is not null
                      group by download_name_post_prop")
View(head(distr_mod_dwnl,100))
nrow(distr_mod_dwnl)
# 34



## Distribuzioni temporali ##########################################################################################################################################

#### Data prima visita

distr_min_data <- withColumn(count_distr, "days_from_1_vis", datediff(count_distr$data_ingr_pdisc_formdate, count_distr$min_data))

View(head(distr_min_data,100))
nrow(distr_min_data)
# 11.035


min_data <- select(distr_min_data, "days_from_1_vis")
View(head(min_data,100))
nrow(min_data)
# 11.035
min_data_1 <- summarize(groupBy(min_data, min_data$days_from_1_vis), 
                        count_min_data = count(min_data$days_from_1_vis))
View(head(min_data_1,100))

#write.df(repartition(min_data, 1),path = "/user/stefano.mazzucca/CJ_PDISC_distr_prima_visita_pag_disdetta.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
write.df(repartition(min_data_1, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_prima_visita_pag_disdetta.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





#### Data ultima visita

distr_max_data <- withColumn(count_distr, "days_from_last_vis", datediff(count_distr$data_ingr_pdisc_formdate,count_distr$max_data))

View(head(distr_max_data,100))
nrow(distr_max_data)
# 11.035


max_data <- select(distr_max_data, "days_from_last_vis")
View(head(max_data,100))
nrow(max_data)
# 11.035
max_data_1 <- summarize(groupBy(max_data, max_data$days_from_last_vis), 
                        count_max_data = count(max_data$days_from_last_vis))
View(head(max_data_1,100))

#write.df(repartition(max_data, 1),path = "/user/stefano.mazzucca/CJ_PDISC_distr_ultima_visita_pag_disdetta.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
write.df(repartition(max_data_1, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_last_visita_pag_disdetta.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





#### Data download modulo disdetta

distr_data_dwnl_1 <- filter(CJ_PDISC_scarico_pag_disdetta_moduli, "download_name_post_prop is NOT NULL")
nrow(distr_data_dwnl_1)
# 14.582



distr_data_last_dwnl <- summarize(groupBy(distr_data_dwnl_1, distr_data_dwnl_1$COD_CLIENTE_CIFRATO), 
                               days_from_last_dwnl_mod = datediff(last(distr_data_dwnl_1$data_ingr_pdisc_formdate), last(distr_data_dwnl_1$date)))
View(head(distr_data_last_dwnl,100))
nrow(distr_data_last_dwnl)
# 6.698

last_data_dwnl <- select(distr_data_last_dwnl, "days_from_last_dwnl_mod")
View(head(last_data_dwnl,100))
nrow(last_data_dwnl)
# 6.698
last_data_dwnl_1 <- summarize(groupBy(last_data_dwnl, last_data_dwnl$days_from_last_dwnl_mod), 
                        count_last_data_dwnl = count(last_data_dwnl$days_from_last_dwnl_mod))
View(head(last_data_dwnl_1,100))

write.df(repartition(last_data_dwnl_1, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_last_dwnl_mod_disdetta.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




distr_data_dwnl <- withColumn(distr_data_dwnl_1, "days_from_dwnl_mod", datediff(distr_data_dwnl_1$data_ingr_pdisc_formdate, distr_data_dwnl_1$date))
View(head(distr_data_dwnl_prova2,100))
nrow(distr_data_dwnl_prova2)
# 14.582

data_dwnl <- select(distr_data_dwnl, "days_from_dwnl_mod")
View(head(data_dwnl,100))
nrow(data_dwnl)
# 14.582
data_dwnl_1 <- summarize(groupBy(data_dwnl, data_dwnl$days_from_dwnl_mod), 
                              count_data_dwnl = count(data_dwnl$days_from_dwnl_mod))
View(head(data_dwnl_1,100))

write.df(repartition(data_dwnl_1, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_dwnl_mod_disdetta.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




###################################################################################################################################################################
## Ricerca dell'orario di download del modulo disdetta #############################################################################################################
###################################################################################################################################################################


#names(CJ_PDISC_scarico_pag_disdetta_moduli)

filt_orari_dwnl <- filter(CJ_PDISC_scarico_pag_disdetta_moduli, "download_name_post_prop is NOT NULL")

View(head(filt_orari_dwnl,1000))
nrow(filt_orari_dwnl)
# 14.582

filt_orari_dwnl_2 <- select(filt_orari_dwnl, "COD_CLIENTE_CIFRATO", "post_visid_concatenated", "visit_num", "data_ingr_pdisc_formdate", 
                                                "digitalizzazione_bin", "ts", "date", "weekday", "download_name_post_prop")
View(head(filt_orari_dwnl_2,100))
nrow(filt_orari_dwnl_2)
# 14.582 

filt_orari_dwnl_3 <- withColumn(filt_orari_dwnl_2, "orario_dwnl", hour(filt_orari_dwnl_2$ts))
View(head(filt_orari_dwnl_3,100))

createOrReplaceTempView(filt_orari_dwnl_3,"filt_orari_dwnl_3")

filt_orari_dwnl_4 <- sql("select *,
                            case when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                                and orario_dwnl >= 0 and orario_dwnl <= 8 then 'casa'
                            when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                                and orario_dwnl >= 9 and orario_dwnl <= 18 then 'ufficio'
                            when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                                and orario_dwnl >= 19 and orario_dwnl <= 23 then 'casa'
                            else 'casa' end as flg_dwnl_casa_ufficio
                         from filt_orari_dwnl_3")
View(head(filt_orari_dwnl_4,100))
nrow(filt_orari_dwnl_4)
# 14.582


## Distribuzione flg_casa_ufficio

createOrReplaceTempView(filt_orari_dwnl_4,"filt_orari_dwnl_4")

distr_flg_casa_ufficio <- sql("select flg_dwnl_casa_ufficio, count(flg_dwnl_casa_ufficio)
                              from filt_orari_dwnl_4
                              group by flg_dwnl_casa_ufficio")
View(head(distr_flg_casa_ufficio,100))


#######################################################################
filt_casa <- filter(filt_orari_dwnl_4, "flg_dwnl_casa_ufficio = 'casa'")
View(head(filt_casa,100))
nrow(filt_casa)
# 6.046

filt_casa_distr_data_dwnl <- withColumn(filt_casa, "days_from_dwnl_mod", datediff(filt_casa$data_ingr_pdisc_formdate, filt_casa$date))
View(head(filt_casa_distr_data_dwnl,100))

filt_casa_distr_data_dwnl_1 <- summarize(groupBy(filt_casa_distr_data_dwnl, filt_casa_distr_data_dwnl$days_from_dwnl_mod), 
                         count_data_dwnl = count(filt_casa_distr_data_dwnl$days_from_dwnl_mod))
View(head(filt_casa_distr_data_dwnl_1,100))

write.df(repartition(filt_casa_distr_data_dwnl_1, 1),path = "/user/stefano.mazzucca/CJ_PDISC_filt_casa_count_dwnl.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


#############################################################################
filt_ufficio <- filter(filt_orari_dwnl_4, "flg_dwnl_casa_ufficio = 'ufficio'")
View(head(filt_ufficio,100))
nrow(filt_ufficio)
# 8.536

filt_ufficio_distr_data_dwnl <- withColumn(filt_ufficio, "days_from_dwnl_mod", datediff(filt_ufficio$data_ingr_pdisc_formdate, filt_ufficio$date))
View(head(filt_ufficio_distr_data_dwnl,100))

filt_ufficio_distr_data_dwnl_1 <- summarize(groupBy(filt_ufficio_distr_data_dwnl, filt_ufficio_distr_data_dwnl$days_from_dwnl_mod), 
                                         count_data_dwnl = count(filt_ufficio_distr_data_dwnl$days_from_dwnl_mod))
View(head(filt_ufficio_distr_data_dwnl_1,100))

write.df(repartition(filt_ufficio_distr_data_dwnl_1, 1),path = "/user/stefano.mazzucca/CJ_PDISC_filt_ufficio_count_dwnl.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





## Distribuzione last_download_orario

createOrReplaceTempView(filt_orari_dwnl_4, "filt_orari_dwnl_4")

filt_last_orario_dwnl <- sql("select COD_CLIENTE_CIFRATO, last(flg_dwnl_casa_ufficio) as flg_last_dwnl
                             from filt_orari_dwnl_4
                             group by COD_CLIENTE_CIFRATO")
View(head(filt_last_orario_dwnl,100))
nrow(filt_last_orario_dwnl)
# 6.698

createOrReplaceTempView(filt_last_orario_dwnl, "filt_last_orario_dwnl")
  
filt_last_orario_dwnl_2 <- sql("select flg_last_dwnl, count(flg_last_dwnl)
                               from filt_last_orario_dwnl
                               group by flg_last_dwnl")
View(head(filt_last_orario_dwnl_2,100))
  



## Distribuzione giorni della settimana

createOrReplaceTempView(filt_orari_dwnl_4,"filt_orari_dwnl_4")

distr_weekday <- sql("select weekday, count(weekday)
                     from filt_orari_dwnl_4
                     group by weekday")
View(head(distr_weekday,100))





## Distribuzione count_visite alla pagine

createOrReplaceTempView(CJ_PDISC_scarico_pag_disdetta_moduli,"CJ_PDISC_scarico_pag_disdetta_moduli")

count_distr_visite <- sql("select COD_CLIENTE_CIFRATO, data_ingr_pdisc_formdate, date,
                            count(distinct(visit_num)) as count_visit
                            from CJ_PDISC_scarico_pag_disdetta_moduli
                          group by COD_CLIENTE_CIFRATO, data_ingr_pdisc_formdate, date
                          order by COD_CLIENTE_CIFRATO")
View(head(count_distr_visite,100))
nrow(count_distr_visite)
# 18.520

count_distr_visite_2 <- withColumn(count_distr_visite, "days_from_visit", datediff(count_distr_visite$data_ingr_pdisc_formdate, count_distr_visite$date))
View(head(count_distr_visite_2,100))
nrow(count_distr_visite_2)
# 18.520

count_distr_visite_3 <- select(count_distr_visite_2, "count_visit", "days_from_visit")
View(head(count_distr_visite_3,100))

createOrReplaceTempView(count_distr_visite_3,"count_distr_visite_3")

count_distr_visite_4 <- sql("select days_from_visit, sum(count_visit)
                            from count_distr_visite_3
                            group by days_from_visit")
View(head(count_distr_visite_4,100))
nrow(count_distr_visite_4)



## Distribuzione count_visit per cliente

createOrReplaceTempView(CJ_PDISC_scarico_pag_disdetta_moduli,"CJ_PDISC_scarico_pag_disdetta_moduli")

count_visit_cliente <- sql("select COD_CLIENTE_CIFRATO, 
                              count(distinct(visit_num)) as count_visit
                           from CJ_PDISC_scarico_pag_disdetta_moduli
                           group by COD_CLIENTE_CIFRATO
                           order by COD_CLIENTE_CIFRATO")
View(head(count_visit_cliente,100))
nrow(count_visit_cliente)
# 11.022

write.df(repartition(count_visit_cliente, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_visit_cliente.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




## Match tra i clienti che visualizzano la pagina disdetta e il loro livello di digitalizzazione 

View(head(base_pdisc_coo,100))
View(head(valdist_pag_disdetta_moduli,100))
nrow(valdist_pag_disdetta_moduli)
# 11.022

createOrReplaceTempView(base_pdisc_coo,"base_pdisc_coo")

match_1 <- sql("select COD_CLIENTE_CIFRATO, digitalizzazione_bin
               from base_pdisc_coo
               group by COD_CLIENTE_CIFRATO, digitalizzazione_bin")
View(head(match_1,100))
nrow(match_1)
# 37.753

createOrReplaceTempView(match_1, "match_1")
createOrReplaceTempView(valdist_pag_disdetta_moduli, "valdist_pag_disdetta_moduli")

match_2 <- sql("select t1.COD_CLIENTE_CIFRATO, t1.digitalizzazione_bin
               from match_1 t1
               inner join valdist_pag_disdetta_moduli t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(match_2,100))
nrow(match_2)
# 11.022

createOrReplaceTempView(match_2,"match_2")

distr_match <- sql("select digitalizzazione_bin, count(COD_CLIENTE_CIFRATO)
                   from match_2
                   group by digitalizzazione_bin")
View(head(distr_match,100))






################################################################################################################################################################################
## Export totale pdisc ##############################################################################################################################################################
################################################################################################################################################################################


CJ_PDISC_scarico_pag_disdetta_moduli <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_scarico_pag_disdetta_moduli.parquet")

# Aggiungo l'informazione weekday:
CJ_PDISC_scarico_pag_disdetta_moduli_2 <- withColumn(CJ_PDISC_scarico_pag_disdetta_moduli, "weekday", date_format(CJ_PDISC_scarico_pag_disdetta_moduli$ts, 'EEEE'))
View(head(CJ_PDISC_scarico_pag_disdetta_moduli_2,1000))

# Aggiungo l'informazione oraria
CJ_PDISC_scarico_pag_disdetta_moduli_3 <- withColumn(CJ_PDISC_scarico_pag_disdetta_moduli_2, "orario_dwnl", hour(CJ_PDISC_scarico_pag_disdetta_moduli_2$ts))
View(head(CJ_PDISC_scarico_pag_disdetta_moduli_3,100))


createOrReplaceTempView(CJ_PDISC_scarico_pag_disdetta_moduli_3,"CJ_PDISC_scarico_pag_disdetta_moduli_3")

CJ_PDISC_scarico_pag_disdetta_moduli_4 <- sql("select *,
                                                  case when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                                                      and orario_dwnl >= 0 and orario_dwnl <= 8 then 'casa'
                                                  when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                                                      and orario_dwnl >= 9 and orario_dwnl <= 18 then 'ufficio'
                                                  when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                                                      and orario_dwnl >= 19 and orario_dwnl <= 23 then 'casa'
                                                  else 'casa' end as flg_dwnl_casa_ufficio
                                               from CJ_PDISC_scarico_pag_disdetta_moduli_3")
View(head(CJ_PDISC_scarico_pag_disdetta_moduli_4,100))
nrow(CJ_PDISC_scarico_pag_disdetta_moduli_4)
# 79.908


createOrReplaceTempView(CJ_PDISC_scarico_pag_disdetta_moduli_4,"CJ_PDISC_scarico_pag_disdetta_moduli_4")
distr_pdisc <- sql("select weekday, flg_dwnl_casa_ufficio, 
                      count(COD_CLIENTE_CIFRATO) as num_vis
                   from CJ_PDISC_scarico_pag_disdetta_moduli_4
                   group by weekday, flg_dwnl_casa_ufficio")
View(head(distr_pdisc,100))


write.df(repartition(distr_pdisc, 1),path = "/user/stefano.mazzucca/CJ_PDISC_distr_pdisc.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



###########################

filt_dwnl_pdisc <- filter(CJ_PDISC_scarico_pag_disdetta_moduli_4, "download_name_post_prop is NOT NULL")
nrow(filt_dwnl_pdisc)
# 14.582

createOrReplaceTempView(filt_dwnl_pdisc,"filt_dwnl_pdisc")
distr_dwnl_pdisc <- sql("select weekday, flg_dwnl_casa_ufficio, 
                      count(COD_CLIENTE_CIFRATO) as num_dwnl
                   from filt_dwnl_pdisc
                   group by weekday, flg_dwnl_casa_ufficio")
View(head(distr_dwnl_pdisc,100))


write.df(repartition(distr_dwnl_pdisc, 1),path = "/user/stefano.mazzucca/CJ_PDISC_distr_dwnl_pdisc.csv", "csv", sep=";", mode = "overwrite", header=TRUE)










#chiudi sessione
sparkR.stop()

