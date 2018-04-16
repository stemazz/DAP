
# CJ_PDISC_totale_4
# elaborazioni per estrarre KPI di navigazione dei clienti PDISC

#apri sessione
source("connection_R.R")
options(scipen = 1000)


nav_sky_tot <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_completo_20180406.parquet")
View(head(nav_sky_tot,100))
nrow(nav_sky_tot)
# 2.162.983

valdist_clienti <- distinct(select(nav_sky_tot, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti)
# 22.435

flg0 <- filter(nav_sky_tot, "flg_31gg = 0")
valdist_clienti_flg0 <- distinct(select(flg0, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_flg0)
# 18.184
flg1 <- filter(nav_sky_tot, "flg_31gg = 1")
valdist_clienti_flg1 <- distinct(select(flg1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_flg1)
# 18.358


################################################################################################################################################################################
################################################################################################################################################################################
## Elaborazione per ACCESSI (non per pagine visitate), ACCESSI = VISITE
## FOCUS su PAGINE IMPORTANTI (variabili importanti del modello)

nav_sky_tot_2 <- withColumn(nav_sky_tot, "start", lit("2017-03-01"))
nav_sky_tot_3 <- withColumn(nav_sky_tot_2, "start", cast(nav_sky_tot_2$start, 'date'))
nav_sky_tot_4 <- withColumn(nav_sky_tot_3, "n_mesi", datediff(nav_sky_tot_3$one_month_before_pdisc, nav_sky_tot_3$start)/30)

View(head(nav_sky_tot_4,100))
nrow(nav_sky_tot_4)
# 2.162.983


#1# FAIDATE_FATTURE_PAGAMENTI
#2# ASSISTENZA_MODULI_CONTRATTUALI
#3# APP_ASSISTENZA_CONTATTA
#4# ASSISTENZA_CONTATTA
#5# APP_I_MIEI_DATI
#6# FAIDATE_GESTISCI_DATI_SERVIZI
#7# ASSISTENZA_SKY_EXPERT
#8# ASSISTENZA_TROVA_SKY_SERVICE
#9# APP_ASSISTENZA_RICERCA
#10# ASSISTENZA_RICERCA
#A# HOMEPAGE_SKY
#B# FAIDATE_HOME


################################################################################################################################################################################
################################################################################################################################################################################
## FAIDATE_FATTURE_PAGAMENTI

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%faidate_fatture_pagamenti%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 102.197

valdist_clienti_filt_fatture <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_fatture)
# 10.156

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_wsc_fatture_pagamenti
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 34.759 (10.156 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 36.919

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 6.150


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_wsc_fatture_pagamenti
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 6.150 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_wsc_fatture_pagamenti.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 65.278

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 7.289


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_wsc_fatture_pagamenti
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 7.289

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_wsc_fatture_pagamenti.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_wsc_fatture_pagamenti,
                                t2.n_visit_POST_wsc_fatture_pagamenti,
                                ((t2.n_visit_POST_wsc_fatture_pagamenti/t1.n_visit_PRE_wsc_fatture_pagamenti)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 10.156


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_wsc_fatture_pagamenti.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




################################################################################################################################################################################
################################################################################################################################################################################
## ASSISTENZA_MODULI_CONTRATTUALI

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%assistenza_moduli_contrattuali%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 19.094

valdist_clienti_filt_ass_mod <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_ass_mod)
# 4.914

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_assist_mod
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 7.213 (4.914 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 11.216

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 3.216


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_assist_mod
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 3.216 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_assist_mod.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 7.878

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 2.284


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_assist_mod
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 2.284

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_assist_mod.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_assist_mod,
                                t2.n_visit_POST_assist_mod,
                                ((t2.n_visit_POST_assist_mod/t1.n_visit_PRE_assist_mod)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 4.914


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_assist_mod.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




################################################################################################################################################################################
################################################################################################################################################################################
## APP_ASSISTENZA_CONTATTA

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%app_assistenza_contatta%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 12.924

valdist_clienti_filt_app_assist_contat <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_app_assist_contat)
# 3.016

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_app_assist_contat
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 6.430 (3.016 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 5.256

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 1.538


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_app_assist_contat
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 1.538 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_app_assist_contat.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 7.668

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 2.048


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_app_assist_contat
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 2.048

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_app_assist_contat.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_app_assist_contat,
                                t2.n_visit_POST_app_assist_contat,
                                ((t2.n_visit_POST_app_assist_contat/t1.n_visit_PRE_app_assist_contat)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 3.016


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_app_assist_contat.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





################################################################################################################################################################################
################################################################################################################################################################################
## ASSISTENZA_CONTATTA

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%assistenza_contatta%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 82.324

valdist_clienti_filt_assist_contat <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_assist_contat)
# 10.062

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_assist_contat
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 25.792 (10.062 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 32.500

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 5.691


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_assist_contat
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 5.691 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_assist_contat.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 49.824

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 6.980


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_assist_contat
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 6.980

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_assist_contat.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_assist_contat,
                                t2.n_visit_POST_assist_contat,
                                ((t2.n_visit_POST_assist_contat/t1.n_visit_PRE_assist_contat)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 10.062


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_assist_contat.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





################################################################################################################################################################################
################################################################################################################################################################################
## APP_I_MIEI_DATI

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%app_i_miei_dati%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 9.090

valdist_clienti_filt_app_dati <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_app_dati)
# 3.471

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_app_dati
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 5.488 (3.471 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 3.078

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 1.420


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_app_dati
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 1.420 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_app_miei_dati.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 6.012

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 2.550


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_app_dati
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 2.550

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_app_miei_dati.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_app_dati,
                                t2.n_visit_POST_app_dati,
                                ((t2.n_visit_POST_app_dati/t1.n_visit_PRE_app_dati)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 3.471


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_app_miei_dati.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





################################################################################################################################################################################
################################################################################################################################################################################
## FAIDATE_GESTISCI_DATI_SERVIZI

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%faidate_gestisci_dati_servizi%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 115.204

valdist_clienti_filt_wsc_gest_serv <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_wsc_gest_serv)
# 12.806

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_wsc_gest_serv
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 38.069 (12.806 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 41.299

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 7.561


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                            count(canale) as n_visit_POST_wsc_gest_serv
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 7.561 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_wsc_gest_serv.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 73.905

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 9.646


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_wsc_gest_serv
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 9.646

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_wsc_gest_serv.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_wsc_gest_serv,
                                t2.n_visit_POST_wsc_gest_serv,
                                ((t2.n_visit_POST_wsc_gest_serv/t1.n_visit_PRE_wsc_gest_serv)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 12.806


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_wsc_gest_serv.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





################################################################################################################################################################################
################################################################################################################################################################################
## ASSISTENZA_SKY_EXPERT

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%assistenza_sky_expert%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 385

valdist_clienti_filt_assist_sky <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_assist_sky)
# 248

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_assist_sky
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 300 (248 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 114

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 79


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                            count(canale) as n_visit_POST_assist_sky
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 79 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_assist_sky.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 271

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 173


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_assist_sky 
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 173
# --------------------------->>>>>>>>> VALUTARE SE NON METTERE LA NORMALIZZAZIONE PER MESE! <<<<<<<<----------------------------------------------------------------

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_assist_sky.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_assist_sky,
                                t2.n_visit_POST_assist_sky,
                                ((t2.n_visit_POST_assist_sky/t1.n_visit_PRE_assist_sky)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 248


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_assist_sky.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





################################################################################################################################################################################
################################################################################################################################################################################
## ASSISTENZA_TROVA_SKY_SERVICE

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%assistenza_trova_sky_service%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 4.207

valdist_clienti_filt_assist_trova_serv <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_assist_trova_serv)
# 1.668

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_assist_trova_serv_sky
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 2.471 (1.668 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 1.499

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 685


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_assist_trova_serv_sky
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 685 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_assist_trova_serv_sky.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 2.708

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 1.063


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_assist_trova_serv_sky 
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 1.063
# --------------------------->>>>>>>>> VALUTARE SE NON METTERE LA NORMALIZZAZIONE PER MESE! <<<<<<<<----------------------------------------------------------------

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_assist_trova_serv_sky.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_assist_trova_serv_sky,
                                t2.n_visit_POST_assist_trova_serv_sky,
                                ((t2.n_visit_POST_assist_trova_serv_sky/t1.n_visit_PRE_assist_trova_serv_sky)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 1.668


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_assist_trova_serv_sky.csv", "csv", sep=";", mode = "overwrite", header=TRUE)






################################################################################################################################################################################
################################################################################################################################################################################
## APP_ASSISTENZA_RICERCA

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%app_assistenza_ricerca%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 3.386

valdist_clienti_filt_app_assist_ricerca <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_app_assist_ricerca)
# 1.406

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_app_assist_ricerca
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 1.962 (1.406 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 1.523

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 666


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_app_assist_ricerca
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 666 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_app_assist_ricerca.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 1.863

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 872


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_app_assist_ricerca
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 872

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_app_assist_ricerca.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_app_assist_ricerca,
                                t2.n_visit_POST_app_assist_ricerca,
                                ((t2.n_visit_POST_app_assist_ricerca/t1.n_visit_PRE_app_assist_ricerca)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 1.406


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_app_assist_ricerca.csv", "csv", sep=";", mode = "overwrite", header=TRUE)







################################################################################################################################################################################
################################################################################################################################################################################
## ASSISTENZA_RICERCA

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%assistenza_ricerca%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 12.869

valdist_clienti_filt_assist_ricerca <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_assist_ricerca)
# 4.711

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_assist_ricerca
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 7.024 (4.711 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 5.353

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 2.290


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_assist_ricerca
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 2.290 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_assist_ricerca.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 7.516

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 2.954


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_assist_ricerca
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 2.954

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_assist_ricerca.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_assist_ricerca,
                                t2.n_visit_POST_assist_ricerca,
                                ((t2.n_visit_POST_assist_ricerca/t1.n_visit_PRE_assist_ricerca)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 4.711


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_assist_ricerca.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





################################################################################################################################################################################
################################################################################################################################################################################
## FAIDATE_FATTURE_PAGAMENTI

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%faidate_fatture_pagamenti%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 102.197

valdist_clienti_filt_fatture <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_fatture)
# 10.156

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_wsc_fatture_pagamenti
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 34.759 (10.156 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 36.919

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 6.150


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_wsc_fatture_pagamenti
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 6.150 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_wsc_fatture_pagamenti.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 65.278

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 7.289


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                         count(canale)/last(n_mesi) as n_visit_PRE_wsc_fatture_pagamenti
                         from nav_sky_tot_5_pre
                         group by COD_CLIENTE_CIFRATO
                         order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 7.289

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_wsc_fatture_pagamenti.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                     case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                     when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                     else NULL end as cod_cliente_cifrato,
                     t1.n_visit_PRE_wsc_fatture_pagamenti,
                     t2.n_visit_POST_wsc_fatture_pagamenti,
                     ((t2.n_visit_POST_wsc_fatture_pagamenti/t1.n_visit_PRE_wsc_fatture_pagamenti)-1) as tasso_incremento
                     from nav_sky_tot_6_pre t1
                     full outer join nav_sky_tot_6_post t2
                     on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 10.156


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_wsc_fatture_pagamenti.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




################################################################################################################################################################################
################################################################################################################################################################################
## HOMEPAGE_SKY

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%homepage_sky%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 172.596

valdist_clienti_filt_home_sky <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_home_sky)
# 12.179

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_home_sky
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 39.244 (12.179 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 147.402

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 7.191


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_home_sky
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 7.191 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_home_sky.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 25.194

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 7.304


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_home_sky
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 7.304

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_home_sky.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_home_sky,
                                t2.n_visit_POST_home_sky,
                                ((t2.n_visit_POST_home_sky/t1.n_visit_PRE_home_sky)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 12.179


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_home_sky.csv", "csv", sep=";", mode = "overwrite", header=TRUE)






################################################################################################################################################################################
################################################################################################################################################################################
## FAIDATE_HOME

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%faidate_home%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 169.099

valdist_clienti_filt_wsc_home <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_wsc_home)
# 14.127

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_wsc_home
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 65.748 (14.127 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 58.470

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 9.105


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_wsc_home
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 9.105 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_wsc_home.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 110.629

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 11.117


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_wsc_home
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 11.117

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_wsc_home.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_wsc_home,
                                t2.n_visit_POST_wsc_home,
                                ((t2.n_visit_POST_wsc_home/t1.n_visit_PRE_wsc_home)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 14.127


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_wsc_home.csv", "csv", sep=";", mode = "overwrite", header=TRUE)






################################################################################################################################################################################
################################################################################################################################################################################
################################################################################################################################################################################
################################################################################################################################################################################

## Pagine/Variabile in aggiunta dopoa aver fatto rigirare il modello con "assistenza_conosci_disdetta"

## ASSISTENZA_INFO_DISDETTA%
## ASSISTENZA_CONOSCI_DISDETTA (= http://www.sky.it/assistenza/conosci/informazioni-sull-abbonamento/termini-e-condizioni/come-posso-dare-la-disdetta-del-mio-contratto.html)


################################################################################################################################################################################
################################################################################################################################################################################
## ASSISTENZA_INFO_DISDETTA%

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%assistenza_info_disdetta%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 58.852

valdist_clienti_filt_assist_info_disdetta <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_assist_info_disdetta)
# 9.791

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_assist_info_disdetta
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 17.949 (9.791 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 40.508

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 8.030


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_assist_info_disdetta
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 8.030 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_assist_info_disdetta.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 18.344

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 3.473


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_assist_info_disdetta
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 3.473

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_assist_info_disdetta.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_assist_info_disdetta,
                                t2.n_visit_POST_assist_info_disdetta,
                                ((t2.n_visit_POST_assist_info_disdetta/t1.n_visit_PRE_assist_info_disdetta)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 9.791


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_assist_info_disdetta.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





################################################################################################################################################################################
################################################################################################################################################################################
## ASSISTENZA_CONOSCI_DISDETTA (= http://www.sky.it/assistenza/conosci/informazioni-sull-abbonamento/termini-e-condizioni/come-posso-dare-la-disdetta-del-mio-contratto.html)

nav_sky_tot_5 <- filter(nav_sky_tot_4, "sequenza LIKE '%assistenza_conosci_disdetta%'")
View(head(nav_sky_tot_5,1000))
nrow(nav_sky_tot_5)
# 7.863

valdist_clienti_filt_assist_conosci_disdetta <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt_assist_conosci_disdetta)
# 3.941

createOrReplaceTempView(nav_sky_tot_5, "nav_sky_tot_5")

nav_sky_tot_6 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit_assist_conosci_disdetta
                     from nav_sky_tot_5
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 5.205 (3.941 clienti che visitano questa pagina)



## FIltro su ultimo mese ############################################################################################################################################

nav_sky_tot_5_post <- filter(nav_sky_tot_5, "flg_31gg = 1")
nrow(nav_sky_tot_5_post)
# 5.507

valdist_clienti_post <- distinct(select(nav_sky_tot_5_post, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_post)
# 2.933


createOrReplaceTempView(nav_sky_tot_5_post, "nav_sky_tot_5_post")

nav_sky_tot_6_post <- sql("select COD_CLIENTE_CIFRATO, 
                          count(canale) as n_visit_POST_assist_conosci_disdetta
                          from nav_sky_tot_5_post
                          group by COD_CLIENTE_CIFRATO
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_post,100))
nrow(nav_sky_tot_6_post)
# 2.933 


# write.df(repartition( nav_sky_tot_6_post, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_POST_flg31gg_assist_conosci_disdetta.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_5_pre <- filter(nav_sky_tot_5, "flg_31gg = 0")
nrow(nav_sky_tot_5_pre)
# 2.356

valdist_clienti_pre <- distinct(select(nav_sky_tot_5_pre, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pre)
# 1.301


createOrReplaceTempView(nav_sky_tot_5_pre, "nav_sky_tot_5_pre")

nav_sky_tot_6_pre <- sql("select COD_CLIENTE_CIFRATO, 
                                count(canale)/last(n_mesi) as n_visit_PRE_assist_conosci_disdetta
                              from nav_sky_tot_5_pre
                              group by COD_CLIENTE_CIFRATO
                              order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6_pre,100))
nrow(nav_sky_tot_6_pre)
# 1.301

# write.df(repartition( nav_sky_tot_6_pre, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_PRE_flg31gg_assist_conosci_disdetta.csv", 
#          "csv", sep=";", mode = "overwrite", header=TRUE)




createOrReplaceTempView(nav_sky_tot_6_pre, "nav_sky_tot_6_pre")
createOrReplaceTempView(nav_sky_tot_6_post, "nav_sky_tot_6_post")

nav_sky_tot_7 <- sql("select distinct 
                              case when t1.COD_CLIENTE_CIFRATO is NOT NULL then t1.COD_CLIENTE_CIFRATO
                              when t1.COD_CLIENTE_CIFRATO is NULL then t2.COD_CLIENTE_CIFRATO
                              else NULL end as cod_cliente_cifrato,
                                t1.n_visit_PRE_assist_conosci_disdetta,
                                t2.n_visit_POST_assist_conosci_disdetta,
                                ((t2.n_visit_POST_assist_conosci_disdetta/t1.n_visit_PRE_assist_conosci_disdetta)-1) as tasso_incremento
                              from nav_sky_tot_6_pre t1
                              full outer join nav_sky_tot_6_post t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_7,100))
nrow(nav_sky_tot_7)
# 3.941


write.df(repartition( nav_sky_tot_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_kpi_nav_assist_conosci_disdetta.csv", "csv", sep=";", mode = "overwrite", header=TRUE)















#chiudi sessione
sparkR.stop()
