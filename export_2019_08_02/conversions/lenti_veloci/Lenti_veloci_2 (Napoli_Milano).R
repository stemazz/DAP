
## Lenti-veloci (2) NAPOLI e MILANO
## Navigazione organica su Adobe



#apri sessione
source("connection_R.R")
options(scipen = 1000)


nav_tot <- read.parquet("/user/valentina/estrai_lista_IBRIDI_scarico_naigazione.parquet")
View(head(nav_tot,100))
nrow(nav_tot)
# 1.012.441
nav_organica <- filter(nav_tot, "post_campaign is NULL and canale_corporate_post_prop is NOT NULL")
View(head(nav_organica,100))
nrow(nav_organica)
# 328.315

lista_sezioni <- distinct(select(nav_organica, "canale_corporate_post_prop"))
View(head(lista_sezioni,1000))

nav_organica_1 <- filter(nav_organica, "canale_corporate_post_prop LIKE 'acquista' or 
                         canale_corporate_post_prop LIKE 'aol' or 
                         canale_corporate_post_prop LIKE 'pacchetti-offerte'")
nrow(nav_organica_1)
# 47.089
createOrReplaceTempView(nav_organica_1, "nav_organica_1")
nav_organica_2 <- sql("select COD_CLIENTE_CIFRATO, max_Data_stipula_dt,
                            count(COD_CLIENTE_CIFRATO) as num_tot_page_views,
                            sum(case when canale_corporate_post_prop = 'acquista' then 1 else 0 end) as num_views_ACQUISTA,
                            sum(case when canale_corporate_post_prop = 'aol' then 1 else 0 end) as num_views_AOL,
                            sum(case when canale_corporate_post_prop = 'pacchetti-offerte' then 1 else 0 end) as num_views_PACCHETTI_OFFERTE,
                            min(date_time_dt) as min_date_organic,
                            max(date_time_dt) as max_date_organic
                     from nav_organica_1
                     group by COD_CLIENTE_CIFRATO, max_Data_stipula_dt
                     order by COD_CLIENTE_CIFRATO")
View(head(nav_organica_2,100))
nrow(nav_organica_2)
# 3.229


## Riprendo le interazioni ADV (click e impression) e integro le 2 fonti di informazioni #######################################################################

skyid_tot <- read.parquet("/user/stefano.mazzucca/lenti_veloci_skyid_finale.parquet")
View(head(skyid_tot,100))
nrow(skyid_tot)
# 28.041

createOrReplaceTempView(skyid_tot, "skyid_tot")
createOrReplaceTempView(nav_organica_2, "nav_organica_2")
skyid_completo <- sql("select distinct t1.*, t2.min_date_organic, t2.max_date_organic
                      from skyid_tot t1
                      left join nav_organica_2 t2
                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t1.data_stipula_dt = t2.max_Data_stipula_dt")
View(head(skyid_completo,100))
nrow(skyid_completo)
# 28.041

skyid_completo_1 <- select(skyid_completo, "COD_CLIENTE_CIFRATO", "data_stipula_dt", "DAT_PRIMA_ATTIVAZIONE",
                           "min_data_interactions", "max_data_interactions", "min_date_organic", "max_date_organic")

createOrReplaceTempView(skyid_completo_1, "skyid_completo_1")
skyid_completo_2 <- sql("select *, 
                             case when min_data_interactions <= min_date_organic then min_data_interactions
                              else min_date_organic end as min_data_provv,
                             case when max_data_interactions >= max_date_organic then max_data_interactions
                              else max_date_organic end as max_data_provv
                        from skyid_completo_1")
View(head(skyid_completo_2,100))
nrow(skyid_completo_2)
# 28.041

skyid_completo_2$min_data_provv2 <- ifelse(isNull(skyid_completo_2$min_data_provv) == TRUE & isNull(skyid_completo_2$min_data_interactions) == TRUE,
                                                        skyid_completo_2$min_date_organic, skyid_completo_2$min_data_provv)
skyid_completo_2$min_data <- ifelse(isNull(skyid_completo_2$min_data_provv2) == TRUE & isNull(skyid_completo_2$min_date_organic) == TRUE,
                                                 skyid_completo_2$min_data_interactions, skyid_completo_2$min_data_provv2)

skyid_completo_2$max_data_provv2 <- ifelse(isNull(skyid_completo_2$max_data_provv) == TRUE & isNull(skyid_completo_2$max_data_interactions) == TRUE, 
                                                        skyid_completo_2$max_date_organic, skyid_completo_2$max_data_provv)
skyid_completo_2$max_data <- ifelse(isNull(skyid_completo_2$max_data_provv2) == TRUE & isNull(skyid_completo_2$max_date_organic) == TRUE, 
                                                 skyid_completo_2$max_data_interactions, skyid_completo_2$max_data_provv2)

skyid_completo_2$min_data_provv <- NULL
skyid_completo_2$min_data_provv2 <- NULL
skyid_completo_2$max_data_provv <- NULL
skyid_completo_2$max_data_provv2 <- NULL

View(head(skyid_completo_2,100))
nrow(skyid_completo_2)
# 28.041


write.parquet(skyid_completo_2, "/user/stefano.mazzucca/lenti_veloci_skyid_completo_finale.parquet")
write.df(repartition( skyid_completo_2, 1), path = "/user/stefano.mazzucca/lenti_veloci_skyid_completo_finale.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



###########################################################################################################################################################
## Per ogni COD_CLIENTE ricavo le frequenze di visita delle sezioni corporate navigate in maniera ORGANICA nelle ultime 2 settimane prima della conversione

skyid_completo <- read.df("/user/stefano.mazzucca/lenti_veloci_skyid_completo_finale.csv", source = "csv", header = "true", delimiter = ";")
View(head(skyid_completo,100))
nrow(skyid_completo)
# 28.041

createOrReplaceTempView(skyid_completo, "skyid_completo")
skyid_completo_1 <- sql("SELECT *, 
                                datediff(data_stipula_dt, min_data) as diff_gg 
                        from skyid_completo")
View(head(skyid_completo_1,100))


## classificazione per definire le fasce di "velocita'", da applicare alla base con tutti i movimenti (organico+ad-form)
createOrReplaceTempView(skyid_completo_1,"skyid_completo_1")
skyid_completo_2 <- sql("select *,
                              case when diff_gg <= 7 then '1_<=7gg'
                              when diff_gg <= 14 then '2_<=14gg'
                              when diff_gg <= 21 then '3_<=21gg'
                              when diff_gg <= 28 then '4_<=28gg'
                              when diff_gg <= 60 then '5_<=60gg'
                              when diff_gg > 60 then '6_>60gg'
                              else Null end as FASCIA_P2C
                        from skyid_completo_1")
View(head(skyid_completo_2,100))
nrow(skyid_completo_2)
# 28.041

skyid_completo_2$profilo <- ifelse(skyid_completo_2$diff_gg <= 28, 'veloce', 'lento')
skyid_completo_2$profilo <- ifelse(isNull(skyid_completo_2$FASCIA_P2C) == TRUE, NULL, skyid_completo_2$profilo)
View(head(skyid_completo_2,100))


write.parquet(skyid_completo_2, "/user/stefano.mazzucca/lenti_veloci_skyid_completo_finale_info.parquet")
write.df(repartition( skyid_completo_2, 1), path = "/user/stefano.mazzucca/lenti_veloci_skyid_completo_finale_info.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



info_finali <- read.parquet("/user/stefano.mazzucca/lenti_veloci_skyid_completo_finale_info.parquet")
View(head(info_finali,100))
nrow(info_finali)
# 28.041

info_finali_ibridi <- filter(info_finali, "profilo is NOT null")
View(head(info_finali_ibridi,100))
nrow(info_finali_ibridi)
# 9.730


## Rianggancio le info iniziali
conversions_target_filter <- read.parquet("/user/stefano.mazzucca/conversion_target_filtered.parquet")
View(head(conversions_target_filter,100))
nrow(conversions_target_filter)
# 28.505

createOrReplaceTempView(info_finali, "info_finali")
createOrReplaceTempView(conversions_target_filter, "conversions_target_filter")
info_finali_1 <- sql("select distinct t1.*, t2.FASCIA_P2C, t2.profilo
                     from conversions_target_filter  t1
                     inner join info_finali t2
                     on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and 
                      t1.data_stipula_dt = t2.data_stipula_dt and
                      t1.DAT_PRIMA_ATTIVAZIONE = t2.DAT_PRIMA_ATTIVAZIONE")
View(head(info_finali_1,100))
nrow(info_finali_1)
# 28.124
## ATTENZIONE !! vs 28.041 clienti.. ci sono doppioni!


write.df(repartition( info_finali_1, 1), path = "/user/stefano.mazzucca/lenti_veloci_def.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




#chiudi sessione
sparkR.stop()
