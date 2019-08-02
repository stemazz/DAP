
# CJ_PDISC_vers13
# count_viz_pag_assitenza_e_fatture


#apri sessione
source("connection_R.R")
options(scipen = 1000)


CJ_PDISC_visite_seq <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav.parquet")
View(head(CJ_PDISC_visite_seq,100))
nrow(CJ_PDISC_visite_seq)
# 4.253.420 # 4.951.720


CJ_PDISC_visite_seq_1 <- withColumn(CJ_PDISC_visite_seq, "one_month_before_pdisc", date_sub(CJ_PDISC_visite_seq$data_ingr_pdisc_formdate, 31))

CJ_PDISC_visite_seq_2 <- filter(CJ_PDISC_visite_seq_1, "sequenza LIKE '%assistenza%' or sequenza LIKE '%fatture%' or sequenza LIKE '%fattura%'")
View(head(CJ_PDISC_visite_seq_2,1000))
nrow(CJ_PDISC_visite_seq_2)
# 1.930.237 # 2.299.094

# prova <- distinct(select(CJ_PDISC_visite_seq_2, "sequenza"))
# View(head(prova,100))

CJ_PDISC_visite_seq_2$flg_31gg <- ifelse(CJ_PDISC_visite_seq_2$giorno_visualizzaz <= CJ_PDISC_visite_seq_2$one_month_before_pdisc, 0, 1)
View(head(CJ_PDISC_visite_seq_2,1000))

CJ_PDISC_visite_seq_2 <- withColumn(CJ_PDISC_visite_seq_2, "start", lit("2017-03-01"))
CJ_PDISC_visite_seq_3 <- withColumn(CJ_PDISC_visite_seq_2, "start", cast(CJ_PDISC_visite_seq_2$start, 'date'))

CJ_PDISC_visite_seq_4 <- withColumn(CJ_PDISC_visite_seq_3, "n_mesi", datediff(CJ_PDISC_visite_seq_3$one_month_before_pdisc, CJ_PDISC_visite_seq_3$start)/30)
View(head(CJ_PDISC_visite_seq_4,100))
nrow(CJ_PDISC_visite_seq_4)
# 1.930.237 # 2.299.094

createOrReplaceTempView(CJ_PDISC_visite_seq_4,"CJ_PDISC_visite_seq_4")


### Visite alla pagina ASSISTENZA 

CJ_PDISC_visite_seq_assistenza <- sql("select COD_CLIENTE_CIFRATO, flg_31gg, n_mesi, 
                                            count(*) as tot_viz_pag_assistenza
                                       from CJ_PDISC_visite_seq_4
                                       where sequenza LIKE '%assistenza%' and sequenza NOT LIKE '%assistenza_fattura_elettronica%'
                                       group by COD_CLIENTE_CIFRATO, flg_31gg, n_mesi")
View(head(CJ_PDISC_visite_seq_assistenza,100))

CJ_PDISC_visite_seq_assistenza$count_visite_pag_assistenza_ok <- ifelse(CJ_PDISC_visite_seq_assistenza$flg_31gg == 0, 
                                                                          CJ_PDISC_visite_seq_assistenza$tot_viz_pag_assistenza/CJ_PDISC_visite_seq_assistenza$n_mesi, 
                                                                          CJ_PDISC_visite_seq_assistenza$tot_viz_pag_assistenza)

CJ_PDISC_visite_seq_assistenza_1 <- arrange(CJ_PDISC_visite_seq_assistenza, CJ_PDISC_visite_seq_assistenza$COD_CLIENTE_CIFRATO)
View(head(CJ_PDISC_visite_seq_assistenza_1,100))

###############
#prova <- reshape(CJ_PDISC_visite_seq_assistenza_1, idvar = "COD_CLIENTE_CIFRATO", timevar = "flg_31gg", direction = "wide", sep = "_")
#############

CJ_PDISC_visite_seq_assistenza_flg1 <- filter(CJ_PDISC_visite_seq_assistenza_1, "flg_31gg == 1")
CJ_PDISC_visite_seq_assistenza_flg0 <- filter(CJ_PDISC_visite_seq_assistenza_1, "flg_31gg == 0")


tot_clienti <- distinct(union(distinct(select(CJ_PDISC_visite_seq_assistenza_flg1, "COD_CLIENTE_CIFRATO")), 
                              distinct(select(CJ_PDISC_visite_seq_assistenza_flg0, "COD_CLIENTE_CIFRATO"))))
nrow(tot_clienti)
# 19.420 <<<<-----------------------------------------------------------------------------------------------------------------------------------------------
# 19.717

createOrReplaceTempView(CJ_PDISC_visite_seq_assistenza_flg1,"CJ_PDISC_visite_seq_assistenza_flg1")
createOrReplaceTempView(CJ_PDISC_visite_seq_assistenza_flg0,"CJ_PDISC_visite_seq_assistenza_flg0")
createOrReplaceTempView(tot_clienti,"tot_clienti")

CJ_PDISC_visite_seq_assistenza_2 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, 
                                                          t1.count_visite_pag_assistenza_ok as viz_post_1_mese
                                        from tot_clienti t0
                                        left join CJ_PDISC_visite_seq_assistenza_flg1 t1
                                          on t0.COD_CLIENTE_CIFRATO = t1.COD_CLIENTE_CIFRATO
                                        ")
nrow(CJ_PDISC_visite_seq_assistenza_2) # 19.423 # 19.721
createOrReplaceTempView(CJ_PDISC_visite_seq_assistenza_2, "CJ_PDISC_visite_seq_assistenza_2")
CJ_PDISC_visite_seq_assistenza_3 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, t0.viz_post_1_mese,
                                                          t2.count_visite_pag_assistenza_ok as viz_pre_1_mese
                                        from CJ_PDISC_visite_seq_assistenza_2 t0
                                        left join CJ_PDISC_visite_seq_assistenza_flg0 t2
                                          on t0.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                                        ")
View(head(CJ_PDISC_visite_seq_assistenza_3,1000))
nrow(CJ_PDISC_visite_seq_assistenza_3)
# 19.451 (vs 19.420 clienti totali) # 19.750
ver <- filter(CJ_PDISC_visite_seq_assistenza_3, "COD_CLIENTE_CIFRATO is NULL")
nrow(ver) # 0
ver_2 <- filter(CJ_PDISC_visite_seq_assistenza_3, "viz_post_1_mese is NULL and viz_pre_1_mese is NULL")
nrow(ver_2) # 0


write.df(repartition( CJ_PDISC_visite_seq_assistenza_3, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_viz_assistenza.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



####################################
### Visite alla pagina delle FATTURE  

CJ_PDISC_visite_seq_fatture <- sql("select COD_CLIENTE_CIFRATO, flg_31gg, n_mesi, 
                                      count(*) as tot_viz_fatture
                                      from CJ_PDISC_visite_seq_4
                                      where sequenza LIKE '%fatture%' or sequenza LIKE '%fattura%'
                                      group by COD_CLIENTE_CIFRATO, flg_31gg, n_mesi")
View(head(CJ_PDISC_visite_seq_fatture,100))

CJ_PDISC_visite_seq_fatture$count_visite_fatture_ok <- ifelse(CJ_PDISC_visite_seq_fatture$flg_31gg == 0, 
                                                              CJ_PDISC_visite_seq_fatture$tot_viz_fatture/CJ_PDISC_visite_seq_fatture$n_mesi, 
                                                              CJ_PDISC_visite_seq_fatture$tot_viz_fatture)

CJ_PDISC_visite_seq_fatture_1 <- arrange(CJ_PDISC_visite_seq_fatture, CJ_PDISC_visite_seq_fatture$COD_CLIENTE_CIFRATO)
View(head(CJ_PDISC_visite_seq_fatture_1,100))

CJ_PDISC_visite_seq_fatture_flg1 <- filter(CJ_PDISC_visite_seq_fatture_1, "flg_31gg == 1")
CJ_PDISC_visite_seq_fatture_flg0 <- filter(CJ_PDISC_visite_seq_fatture_1, "flg_31gg == 0")


tot_clienti <- distinct(union(distinct(select(CJ_PDISC_visite_seq_fatture_flg1, "COD_CLIENTE_CIFRATO")), 
                              distinct(select(CJ_PDISC_visite_seq_fatture_flg0, "COD_CLIENTE_CIFRATO"))))
nrow(tot_clienti)
# 11.860 <<<<-----------------------------------------------------------------------------------------------------------------------------------------------
# 12.191

createOrReplaceTempView(CJ_PDISC_visite_seq_fatture_flg1,"CJ_PDISC_visite_seq_fatture_flg1")
createOrReplaceTempView(CJ_PDISC_visite_seq_fatture_flg0,"CJ_PDISC_visite_seq_fatture_flg0")
createOrReplaceTempView(tot_clienti,"tot_clienti")

CJ_PDISC_visite_seq_fatture_2 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, 
                                                          count_visite_fatture_ok as viz_post_1_mese
                                        from tot_clienti t0
                                        left join CJ_PDISC_visite_seq_fatture_flg1 t1
                                          on t0.COD_CLIENTE_CIFRATO = t1.COD_CLIENTE_CIFRATO
                                        ")
nrow(CJ_PDISC_visite_seq_fatture_2) # 11.861 # 12.192
createOrReplaceTempView(CJ_PDISC_visite_seq_fatture_2, "CJ_PDISC_visite_seq_fatture_2")
CJ_PDISC_visite_seq_fatture_3 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, t0.viz_post_1_mese,
                                                          t2.count_visite_fatture_ok as viz_pre_1_mese
                                        from CJ_PDISC_visite_seq_fatture_2 t0
                                        left join CJ_PDISC_visite_seq_fatture_flg0 t2
                                          on t0.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                                        ")
View(head(CJ_PDISC_visite_seq_fatture_3,100))
nrow(CJ_PDISC_visite_seq_fatture_3)
# 11.875 (vs 11.860 clienti totali) # 12.206
ver <- filter(CJ_PDISC_visite_seq_fatture_3, "COD_CLIENTE_CIFRATO is NULL")
nrow(ver) # 0
ver_2 <- filter(CJ_PDISC_visite_seq_fatture_3, "viz_post_1_mese is NULL and viz_pre_1_mese is NULL")
nrow(ver_2) # 0


write.df(repartition( CJ_PDISC_visite_seq_fatture_3, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_viz_fatture.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





##############################################################################################################################################################################
## Analisi   #################################################################################################################################################################
##############################################################################################################################################################################

# utenti attivi nella navigazione nei 31gg precedenti al PDISC ###############################################################################################################


CJ_PDISC_visite_seq <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav.parquet")

CJ_PDISC_visite_seq_1 <- withColumn(CJ_PDISC_visite_seq, "one_month_before_pdisc", date_sub(CJ_PDISC_visite_seq$data_ingr_pdisc_formdate, 31))


base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
nrow(base_pdisc)
# 37.976

valdist_base <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_base)
# 37.753


base_pdisc_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc.parquet")
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
nrow(base_pdisc_coo_2)
# 176.511


createOrReplaceTempView(CJ_PDISC_visite_seq_4, "CJ_PDISC_visite_seq_4")
createOrReplaceTempView(base_pdisc_coo_2, "base_pdisc_coo_2")

utenti_attivi_31gg <- sql("select distinct t1.COD_CLIENTE_CIFRATO
                          from base_pdisc_coo_2 t1
                          inner join CJ_PDISC_visite_seq_4 t2
                            on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t2.giorno_visualizzaz >= t2.one_month_before_pdisc 
                            and t2.giorno_visualizzaz <= t2.data_ingr_pdisc_formdate ")
View(head(utenti_attivi_31gg,100))
nrow(utenti_attivi_31gg)
# 14.222 utenti attivi nei 31gg precedenti al PDISC ##########################################################################################################################
# 13.860

createOrReplaceTempView(CJ_PDISC_visite_seq_1, "CJ_PDISC_visite_seq_4")
createOrReplaceTempView(base_pdisc_coo_2, "base_pdisc_coo_2")

utenti_attivi_sito_pub_31gg <- sql("select distinct t1.COD_CLIENTE_CIFRATO
                                    from base_pdisc_coo_2 t1
                                    inner join CJ_PDISC_visite_seq_4 t2
                                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t2.giorno_visualizzaz >= t2.one_month_before_pdisc 
                                      and t2.giorno_visualizzaz <= t2.data_ingr_pdisc_formdate and t2.canale LIKE '%sito_pubblico%'")
View(head(utenti_attivi_sito_pub_31gg,100))
nrow(utenti_attivi_sito_pub_31gg)
# 13.750 utenti attivi negli ultimi 31gg che navigano sul sito pubblico ######################################################################################################
# 13.750

createOrReplaceTempView(CJ_PDISC_visite_seq_1, "CJ_PDISC_visite_seq_4")
createOrReplaceTempView(base_pdisc_coo_2, "base_pdisc_coo_2")

utenti_attivi_sito_wsc_31gg <- sql("select distinct t1.COD_CLIENTE_CIFRATO
                                    from base_pdisc_coo_2 t1
                                    inner join CJ_PDISC_visite_seq_4 t2
                                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t2.giorno_visualizzaz >= t2.one_month_before_pdisc 
                                      and t2.giorno_visualizzaz <= t2.data_ingr_pdisc_formdate and t2.canale LIKE '%sito_wsc%'")
View(head(utenti_attivi_sito_wsc_31gg,100))
nrow(utenti_attivi_sito_wsc_31gg)
# 8.850 utenti attivi negli ultimi 31gg che navigano su WSC #################################################################################################################
# 8.850

createOrReplaceTempView(CJ_PDISC_visite_seq_1, "CJ_PDISC_visite_seq_1")
createOrReplaceTempView(base_pdisc_coo_2, "base_pdisc_coo_2")

utenti_attivi_app_31gg <- sql("select distinct t1.COD_CLIENTE_CIFRATO
                                    from base_pdisc_coo_2 t1
                                    inner join CJ_PDISC_visite_seq_1 t2
                                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t2.giorno_visualizzaz >= t2.one_month_before_pdisc 
                                      and t2.giorno_visualizzaz <= t2.data_ingr_pdisc_formdate and t2.canale LIKE '%app%'")
View(head(utenti_attivi_app_31gg,100))
nrow(utenti_attivi_app_31gg)
# 536 utenti attivi negli ultimi 31gg che navigano su APP ###################################################################################################################
# 5.007

##############################################################################################################################################################################


prova <- sql("select distinct t1.COD_CLIENTE_CIFRATO
              from base_pdisc_coo_2 t1
              inner join CJ_PDISC_visite_seq_1 t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                and t2.giorno_visualizzaz >= '2017-11-01' and t2.canale LIKE '%app%'")
# t2.giorno_visualizzaz >= t2.one_month_before_pdisc 
nrow(prova)

# 8985 senza filtro data
# 3137 maggiore di 2017-10-01
# 2243 maggiore di 2017-10-15
# 1462 maggiore di 2017-10-21
# 674 maggiore di 2017-10-28
# 355 maggiore di 2017-10-30
# 165 maggiore di 2017-10-31
# 0 maggiore di 2017-11-01 bah??????????????????????????????????????????????????

createOrReplaceTempView(CJ_PDISC_visite_seq_4,"CJ_PDISC_visite_seq_4")
prova_2 <- sql("select distinct COD_CLIENTE_CIFRATO
               from CJ_PDISC_visite_seq_4
               where canale LIKE '%app%' and flg_31gg == 1")
nrow(prova_2)
# giorno_visualizzaz >= one_month_before_pdisc

a <- distinct(select(CJ_PDISC_visite_seq_4, "canale"))
View(a)


prova_4 <- subset(CJ_PDISC_visite_seq_4, CJ_PDISC_visite_seq_4$canale == 'app' )
nrow(prova_4) # 1.526.914
createOrReplaceTempView(prova_4,"prova_4")
prova_5 <- sql("select distinct COD_CLIENTE_CIFRATO
                from prova_4 
                where flg_31gg == 1")
nrow(prova_5)
# 481



skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")

skyappwsc_2 <- withColumn(skyappwsc, "ts", cast(skyappwsc$date_time, "timestamp"))

# skyappwsc_3 <- filter(skyappwsc_2,"ts>='2017-03-01 00:00:00' and  ts<='2018-01-01 00:00:00' ")
# 
# skyappwsc_4 <- select(skyappwsc_3,"external_id_v0", "site_section", "page_name_post_evar", "page_name_post_prop", "post_pagename",
#                       "ts", "visit_num", "post_visid_concatenated", "hit_source", "exclude_hit")
# 
# skyappwsc_5 <- withColumn(skyappwsc_4,"concatena_chiave",concat(skyappwsc_4$post_visid_concatenated,skyappwsc_4$visit_num))
# 
# skyappwsc_6 <- filter(skyappwsc_5,"external_id_v0 is not NULL")

createOrReplaceTempView(skyappwsc_2, "skyappwsc_2")
prova_3 <- sql("select max(ts), min(ts)
               from skyappwsc_2")
View(head(prova_3))
  







##############################################################################################################################################################################
## Verifica sul nuovo scarico di Vale ########################################################################################################################################
##############################################################################################################################################################################


nuovo_scarico_skyitdev <- read.parquet("/user/valentina/CJ_PDISC_scarico_pag_sito.parquet")
View(head(nuovo_scarico_skyitdev,100))
nrow(nuovo_scarico_skyitdev)
# 4.492.336




clienti_valdist_nuovo <- distinct(select(nuovo_scarico_skyitdev, "COD_CLIENTE_CIFRATO"))
nrow(clienti_valdist_nuovo)
# 22.045




valdist_clienti_tot_vecchio <- distinct(select(CJ_PDISC_tab_sito_pub, "COD_CLIENTE_CIFRATO")) 
nrow(valdist_clienti_tot_vecchio)
# 21.110

valdist_clienti_tot_vecchio_2 <- distinct(select(CJ_PDISC_tab_wsc, "COD_CLIENTE_CIFRATO")) 
nrow(valdist_clienti_tot_vecchio_2)
# 17.153

createOrReplaceTempView(valdist_clienti_tot_vecchio, "valdist_clienti_tot_vecchio")
createOrReplaceTempView(valdist_clienti_tot_vecchio_2, "valdist_clienti_tot_vecchio_2")

join <- sql("select distinct t1.COD_CLIENTE_CIFRATO
            from valdist_clienti_tot_vecchio t1
            full outer join valdist_clienti_tot_vecchio_2 t2
            on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
nrow(join)
# 21.111









CJ_PDISC_visite_seq <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav.parquet")
valdist_clienti_tot <- distinct(select(CJ_PDISC_visite_seq, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_tot)
# 21.859


createOrReplaceTempView(valdist_clienti_tot, "valdist_clienti_tot")
createOrReplaceTempView(clienti_valdist_nuovo, "clienti_valdist_nuovo")

test <- sql("select distinct t1.COD_CLIENTE_CIFRATO
            from valdist_clienti_tot t1
            inner join clienti_valdist_nuovo t2 
             on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
nrow(test)
# 20.952











##############################################################################################################################################################################
##############################################################################################################################################################################
##############################################################################################################################################################################







#chiudi sessione
sparkR.stop()
