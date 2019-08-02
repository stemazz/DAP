
# CJ_PDISC_totale
# elaborazioni + modello predittivo

#apri sessione
source("connection_R.R")
options(scipen = 1000)


nav_sky_tot <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_completo.parquet")
View(head(nav_sky_tot,100))
nrow(nav_sky_tot)
# 2.162.983

valdist_clienti <- distinct(select(nav_sky_tot, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti)
# 22.435


################################################################################################################################################################################
################################################################################################################################################################################
## Elaborazione per PAGINE VISITATE

createOrReplaceTempView(nav_sky_tot, "nav_sky_tot")
nav_sky_tot_2 <- sql("select COD_CLIENTE_CIFRATO, 
                                last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                                last(one_month_before_pdisc) as meno_1_mese_pdisc,
                                canale,
                                count(canale) as n_visit
                      from nav_sky_tot
                      group by COD_CLIENTE_CIFRATO, canale
                      order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_2,100))
nrow(nav_sky_tot_2)
# 47.344 (22.435 clienti)


nav_1 <- filter(nav_sky_tot_2, "canale = 'sito_pubblico'")
nrow(nav_1)
# 21.032

nav_2 <- filter(nav_sky_tot_2, "canale = 'sito_wsc'")
nrow(nav_2)
# 16.287

nav_3 <- filter(nav_sky_tot_2, "canale = 'app'")
nrow(nav_3)
# 10.025


createOrReplaceTempView(nav_1, "nav_1")
createOrReplaceTempView(nav_2, "nav_2")
createOrReplaceTempView(nav_3, "nav_3")
createOrReplaceTempView(nav_sky_tot, "nav_sky_tot")

nav_sky_tot_3 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, t0.data_ingr_pdisc_formdate, t0.one_month_before_pdisc, 
                              t1.n_visit as n_visit_pub, t2.n_visit as n_visit_wsc, t3.n_visit as n_visit_app
                     from nav_sky_tot t0 
                     left join nav_1 t1
                      on t0.COD_CLIENTE_CIFRATO = t1.COD_CLIENTE_CIFRATO
                     left join nav_2 t2
                      on t0.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                     left join nav_3 t3
                      on t0.COD_CLIENTE_CIFRATO = t3.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_3,100))
nrow(nav_sky_tot_3)
# 22.435

# ver_visit <- filter(nav_sky_tot_3, "n_visit_pub is NULL AND n_visit_wsc is NULL AND n_visit_app is NULL")
# nrow(ver_visit)
# # 0


write.df(repartition( nav_sky_tot_3, 1), path = "/user/stefano.mazzucca/CJ_PDISC_export_kpi_nav.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## FIltro su ultimo mese

nav_sky_tot_filt <- filter(nav_sky_tot, "flg_31gg = 1")
nrow(nav_sky_tot_filt)
# 811.371

valdist_clienti_filt <- distinct(select(nav_sky_tot_filt, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt)
# 18.358


createOrReplaceTempView(nav_sky_tot_filt, "nav_sky_tot_filt")
nav_sky_tot_filt_2 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit
                     from nav_sky_tot_filt
                     group by COD_CLIENTE_CIFRATO, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_filt_2,100))
nrow(nav_sky_tot_filt_2)
# 33.078 (18.358 clienti)


nav_1_filt <- filter(nav_sky_tot_filt_2, "canale = 'sito_pubblico'")
nrow(nav_1_filt)
# 16.110

nav_2_filt <- filter(nav_sky_tot_filt_2, "canale = 'sito_wsc'")
nrow(nav_2_filt)
# 10.611

nav_3_filt <- filter(nav_sky_tot_filt_2, "canale = 'app'")
nrow(nav_3_filt)
# 6.357


createOrReplaceTempView(nav_1_filt, "nav_1_filt")
createOrReplaceTempView(nav_2_filt, "nav_2_filt")
createOrReplaceTempView(nav_3_filt, "nav_3_filt")
createOrReplaceTempView(nav_sky_tot_filt, "nav_sky_tot_filt")

nav_sky_tot_filt_3 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, t0.data_ingr_pdisc_formdate, t0.one_month_before_pdisc, 
                             t1.n_visit as n_visit_pub, t2.n_visit as n_visit_wsc, t3.n_visit as n_visit_app
                          from nav_sky_tot_filt t0 
                          left join nav_1_filt t1
                           on t0.COD_CLIENTE_CIFRATO = t1.COD_CLIENTE_CIFRATO
                          left join nav_2_filt t2
                           on t0.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                          left join nav_3_filt t3
                           on t0.COD_CLIENTE_CIFRATO = t3.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_filt_3,100))
nrow(nav_sky_tot_filt_3)
# 18.384

# ver_visit <- filter(nav_sky_tot_filt_3, "n_visit_pub is NULL AND n_visit_wsc is NULL AND n_visit_app is NULL")
# nrow(ver_visit)
# # 0
# 
# ver_2 <- distinct(select(nav_sky_tot_filt_3, "COD_CLIENTE_CIFRATO"))
# nrow(ver_2)
# # 18.358


write.df(repartition( nav_sky_tot_filt_3, 1), path = "/user/stefano.mazzucca/CJ_PDISC_export_kpi_nav_31gg.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


################################################################################################################################################################################

# nav_sky_tot_4 <- SparkR::summarize(groupBy(nav_sky_tot_2, nav_sky_tot_2$COD_CLIENTE_CIFRATO), sum_visit = sum(nav_sky_tot_2$n_visit))
# View(head(nav_sky_tot_4,100))
# nrow(nav_sky_tot_4)
# # 22.435
# 
# 
# createOrReplaceTempView(nav_sky_tot_2, "nav_sky_tot_2")
# createOrReplaceTempView(nav_sky_tot_4, "nav_sky_tot_4")
# 
# nav_sky_tot_5 <- sql("select t1.*, t2.sum_visit
#                      from nav_sky_tot_2 t1
#                      left join nav_sky_tot_4 t2
#                       on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# View(head(nav_sky_tot_5,100))
# nrow(nav_sky_tot_5)
# # 47.344

################################################################################################################################################################################



################################################################################################################################################################################
################################################################################################################################################################################
## Elaborazione per ACCESSI (non per pagine visitate), ACCESSI = VISITE

nav_sky_tot_2 <- withColumn(nav_sky_tot, "start", lit("2017-03-01"))
nav_sky_tot_3 <- withColumn(nav_sky_tot_2, "start", cast(nav_sky_tot_2$start, 'date'))
nav_sky_tot_4 <- withColumn(nav_sky_tot_3, "n_mesi", datediff(nav_sky_tot_3$one_month_before_pdisc, nav_sky_tot_3$start)/30)

View(head(nav_sky_tot_4,100))
nrow(nav_sky_tot_4)
# 2.162.983


createOrReplaceTempView(nav_sky_tot_4, "nav_sky_tot_4")

# nav_sky_tot_5 <- sql("select COD_CLIENTE_CIFRATO, flg_31gg, n_mesi,
#                                 count(*) as sum_visit
#                       from nav_sky_tot_4
#                       group by COD_CLIENTE_CIFRATO, flg_31gg, n_mesi")
# View(head(nav_sky_tot_5,100))

nav_sky_tot_5 <- sql("select COD_CLIENTE_CIFRATO, 
                                last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                                last(one_month_before_pdisc) as meno_1_mese_pdisc,
                                canale,
                                count(canale) as n_visit
                      from nav_sky_tot_4
                      group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                      order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_5,100))
nrow(nav_sky_tot_5)
# 369.267 (22.435 clienti)

# valdist_cl <- distinct(select(nav_sky_tot_5, "COD_CLIENTE_CIFRATO"))
# nrow(valdist_cl)
# # 22.435


nav_1 <- filter(nav_sky_tot_5, "canale = 'sito_pubblico'")
nrow(nav_1)
# 173.916
nav_1 <- summarize(groupBy(nav_1, nav_1$COD_CLIENTE_CIFRATO), count_accessi = count(nav_1$n_visit))
View(head(nav_1,100))
nrow(nav_1)
# 21.032

nav_2 <- filter(nav_sky_tot_5, "canale = 'sito_wsc'")
nrow(nav_2)
# 91.056
nav_2 <- summarize(groupBy(nav_2, nav_2$COD_CLIENTE_CIFRATO), count_accessi = count(nav_2$n_visit))
View(head(nav_2,100))
nrow(nav_2)
# 16.287

nav_3 <- filter(nav_sky_tot_5, "canale = 'app'")
nrow(nav_3)
# 104.295
nav_3 <- summarize(groupBy(nav_3, nav_3$COD_CLIENTE_CIFRATO), count_accessi = count(nav_3$n_visit))
View(head(nav_3,100))
nrow(nav_3)
# 10.025


createOrReplaceTempView(nav_1, "nav_1")
createOrReplaceTempView(nav_2, "nav_2")
createOrReplaceTempView(nav_3, "nav_3")
createOrReplaceTempView(nav_sky_tot_4, "nav_sky_tot_4")

nav_sky_tot_6 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, t0.data_ingr_pdisc_formdate, t0.one_month_before_pdisc, 
                              t1.count_accessi as n_accessi_pub, t2.count_accessi as n_accessi_wsc, t3.count_accessi as n_accessi_app
                     from nav_sky_tot_4 t0 
                     left join nav_1 t1
                      on t0.COD_CLIENTE_CIFRATO = t1.COD_CLIENTE_CIFRATO
                     left join nav_2 t2
                      on t0.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                     left join nav_3 t3
                      on t0.COD_CLIENTE_CIFRATO = t3.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_6,100))
nrow(nav_sky_tot_6)
# 22.468 (vs 22.435 clienti)

ver_visit <- filter(nav_sky_tot_6, "n_accessi_pub is NULL AND n_accessi_wsc is NULL AND n_accessi_app is NULL")
nrow(ver_visit)
# 0


write.df(repartition( nav_sky_tot_6, 1), path = "/user/stefano.mazzucca/CJ_PDISC_export_kpi_nav_tot_accessi.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## FIltro su ultimo mese

nav_sky_tot_4_filt <- filter(nav_sky_tot_4, "flg_31gg = 1")
nrow(nav_sky_tot_4_filt)
# 811.371

valdist_clienti_filt <- distinct(select(nav_sky_tot_4_filt, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filt)
# 18.358


createOrReplaceTempView(nav_sky_tot_4_filt, "nav_sky_tot_4_filt")

nav_sky_tot_filt_5 <- sql("select COD_CLIENTE_CIFRATO, 
                     last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                     last(one_month_before_pdisc) as meno_1_mese_pdisc,
                     canale,
                     count(canale) as n_visit
                     from nav_sky_tot_4_filt
                     group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_filt_5,100))
nrow(nav_sky_tot_filt_5)
# 128.331 (22.435 clienti)

# valdist_cl <- distinct(select(nav_sky_tot_filt_5, "COD_CLIENTE_CIFRATO"))
# nrow(valdist_cl)
# # 18.358


nav_1 <- filter(nav_sky_tot_filt_5, "canale = 'sito_pubblico'")
nrow(nav_1)
# 67.441
nav_1 <- summarize(groupBy(nav_1, nav_1$COD_CLIENTE_CIFRATO), count_accessi = count(nav_1$n_visit))
View(head(nav_1,100))
nrow(nav_1)
# 16.110

nav_2 <- filter(nav_sky_tot_filt_5, "canale = 'sito_wsc'")
nrow(nav_2)
# 29.231
nav_2 <- summarize(groupBy(nav_2, nav_2$COD_CLIENTE_CIFRATO), count_accessi = count(nav_2$n_visit))
View(head(nav_2,100))
nrow(nav_2)
# 10.611

nav_3 <- filter(nav_sky_tot_filt_5, "canale = 'app'")
nrow(nav_3)
# 31.659
nav_3 <- summarize(groupBy(nav_3, nav_3$COD_CLIENTE_CIFRATO), count_accessi = count(nav_3$n_visit))
View(head(nav_3,100))
nrow(nav_3)
# 6.357


createOrReplaceTempView(nav_1, "nav_1")
createOrReplaceTempView(nav_2, "nav_2")
createOrReplaceTempView(nav_3, "nav_3")
createOrReplaceTempView(nav_sky_tot_4_filt, "nav_sky_tot_4_filt")

nav_sky_tot_filt_6 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, t0.data_ingr_pdisc_formdate, t0.one_month_before_pdisc, 
                              t1.count_accessi as n_accessi_ultimi31gg_pub, t2.count_accessi as n_accessi_ultimi31gg_wsc, t3.count_accessi as n_accessi_ultimi31gg_app
                     from nav_sky_tot_4_filt t0 
                     left join nav_1 t1
                      on t0.COD_CLIENTE_CIFRATO = t1.COD_CLIENTE_CIFRATO
                     left join nav_2 t2
                      on t0.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                     left join nav_3 t3
                      on t0.COD_CLIENTE_CIFRATO = t3.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_filt_6,100))
nrow(nav_sky_tot_filt_6)
# 18.384 (vs 18.358 clienti)

ver_visit <- filter(nav_sky_tot_filt_6, "n_accessi_ultimi31gg_pub is NULL AND n_accessi_ultimi31gg_wsc is NULL AND n_accessi_ultimi31gg_app is NULL")
nrow(ver_visit)
# 0


write.df(repartition( nav_sky_tot_filt_6, 1), path = "/user/stefano.mazzucca/CJ_PDISC_export_kpi_nav_flg31gg_accessi.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
## Filtro Mar17 fino ai 31gg precedenti al PDISC

nav_sky_tot_4_antifilt <- filter(nav_sky_tot_4, "flg_31gg = 0")
nrow(nav_sky_tot_4_antifilt)
# 1.351.612

valdist_clienti_antifilt <- distinct(select(nav_sky_tot_4_antifilt, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_antifilt)
# 18.184


createOrReplaceTempView(nav_sky_tot_4_antifilt, "nav_sky_tot_4_antifilt")

nav_sky_tot_antifilt_5 <- sql("select COD_CLIENTE_CIFRATO, 
                          last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                          last(one_month_before_pdisc) as meno_1_mese_pdisc,
                          canale,
                          count(canale) as n_visit
                          from nav_sky_tot_4_antifilt
                          group by COD_CLIENTE_CIFRATO, concatena_chiave, canale
                          order by COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_antifilt_5,100))
nrow(nav_sky_tot_antifilt_5)
# 240.962 (18.184 clienti)

# valdist_cl <- distinct(select(nav_sky_tot_antifilt_5, "COD_CLIENTE_CIFRATO"))
# nrow(valdist_cl)
# # 18.184


nav_1 <- filter(nav_sky_tot_antifilt_5, "canale = 'sito_pubblico'")
nrow(nav_1)
# 106.490
nav_1 <- summarize(groupBy(nav_1, nav_1$COD_CLIENTE_CIFRATO), count_accessi = count(nav_1$n_visit))
View(head(nav_1,100))
nrow(nav_1)
# 16.399

nav_2 <- filter(nav_sky_tot_antifilt_5, "canale = 'sito_wsc'")
nrow(nav_2)
# 61.828
nav_2 <- summarize(groupBy(nav_2, nav_2$COD_CLIENTE_CIFRATO), count_accessi = count(nav_2$n_visit))
View(head(nav_2,100))
nrow(nav_2)
# 13.482

nav_3 <- filter(nav_sky_tot_antifilt_5, "canale = 'app'")
nrow(nav_3)
# 72.644
nav_3 <- summarize(groupBy(nav_3, nav_3$COD_CLIENTE_CIFRATO), count_accessi = count(nav_3$n_visit))
View(head(nav_3,100))
nrow(nav_3)
# 8.862


createOrReplaceTempView(nav_1, "nav_1")
createOrReplaceTempView(nav_2, "nav_2")
createOrReplaceTempView(nav_3, "nav_3")
createOrReplaceTempView(nav_sky_tot_4_antifilt, "nav_sky_tot_4_antifilt")

nav_sky_tot_antifilt_6 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, t0.data_ingr_pdisc_formdate, t0.one_month_before_pdisc, 
                              t1.count_accessi as n_accessi_pre31gg_pub, t2.count_accessi as n_accessi_pre31gg_wsc, t3.count_accessi as n_accessi_pre31gg_app
                     from nav_sky_tot_4_antifilt t0 
                     left join nav_1 t1
                      on t0.COD_CLIENTE_CIFRATO = t1.COD_CLIENTE_CIFRATO
                     left join nav_2 t2
                      on t0.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                     left join nav_3 t3
                      on t0.COD_CLIENTE_CIFRATO = t3.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_antifilt_6,100))
nrow(nav_sky_tot_antifilt_6)
# 18.213 (vs 18.184 clienti)

ver_visit <- filter(nav_sky_tot_antifilt_6, "n_accessi_pre31gg_pub is NULL AND n_accessi_pre31gg_wsc is NULL AND n_accessi_pre31gg_app is NULL")
nrow(ver_visit)
# 0


createOrReplaceTempView(nav_sky_tot_antifilt_6, "nav_sky_tot_antifilt_6")

nav_sky_tot_antifilt_7 <- sql("select distinct t1.COD_CLIENTE_CIFRATO, 
                                      t1.data_ingr_pdisc_formdate,
                                      t1.one_month_before_pdisc,
                                      (t1.n_accessi_pre31gg_pub / t2.n_mesi) as n_accessi_mensili_pre31gg_pub,
                                      (t1.n_accessi_pre31gg_wsc / t2.n_mesi) as n_accessi_mensili_pre31gg_wsc,
                                      (t1.n_accessi_pre31gg_app / t2.n_mesi) as n_accessi_mensili_pre31gg_app
                              from nav_sky_tot_antifilt_6 t1
                              inner join nav_sky_tot_4_antifilt t2
                                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
View(head(nav_sky_tot_antifilt_7,100))
nrow(nav_sky_tot_antifilt_7)
# 18.271


write.df(repartition( nav_sky_tot_antifilt_7, 1), path = "/user/stefano.mazzucca/CJ_PDISC_export_kpi_nav_pre31gg_accessi.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



################################################################################################################################################################################
################################################################################################################################################################################
## Modellino predittivo per estrarre info sulle pagne importanti
################################################################################################################################################################################
################################################################################################################################################################################

# nav_sky_tot <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_completo.parquet")
# View(head(nav_sky_tot,100))
# nrow(nav_sky_tot)
# # 2.162.983

nav_sky_tot <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_completo_20180406.parquet")
View(head(nav_sky_tot,100))
nrow(nav_sky_tot)
# 2.162.983

valdist_pag <- distinct(select(nav_sky_tot, "sequenza"))
View(head(valdist_pag,100))
nrow(valdist_pag)
# 76

nav_sky_2 <- filter(nav_sky_tot, "sequenza NOT LIKE '%skyatlantic_guidatv%'")
nrow(nav_sky_2)
# 2.162.982



nav_sky_3 <- withColumn(nav_sky_2, "appoggio", lit("A"))
nav_sky_4 <- withColumn(nav_sky_3, "concatena_chiave_2", concat_ws("-", nav_sky_3$appoggio, nav_sky_3$concatena_chiave))
nav_sky_4$appoggio <- NULL
View(head(nav_sky_4,100))
nrow(nav_sky_4)
# 2.162.982


## -->> Elaborazione in Knime "one to many"
write.df(repartition( nav_sky_4, 1), path = "/user/stefano.mazzucca/CJ_PDISC_export_nav_20180406.csv", "csv", sep=";", mode = "overwrite", header=TRUE)














#chiudi sessione
sparkR.stop()
