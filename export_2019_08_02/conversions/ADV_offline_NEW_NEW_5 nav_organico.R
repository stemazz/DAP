
## Impatto ADV su vendite offline NEW 5

#apri sessione
source("connection_R.R")
options(scipen = 1000)




skyid_tot <- read.parquet("/user/stefano.mazzucca/p2c_nd_base_completa_2.parquet")
View(head(skyid_tot,100))
nrow(skyid_tot)
# 128.829


## Analisi sugli OUTBOUND ##############################################################

outb <- filter(skyid_tot, "DES_CATEGORIA_CANALE = 'TELESELLING ESTERNO' ")
View(head(outb,100))
nrow(outb)
# 21.450

outb_no_dig_ibridi <- filter(skyid_tot, "DES_CATEGORIA_CANALE = 'TELESELLING ESTERNO' and flg_profilo = 'no_digital_ibridi' ")
View(head(outb_no_dig_ibridi,100))
nrow(outb_no_dig_ibridi)
# 6.452


## Di questi VERIFICARE se ricevono chiamata OUTBOUND prima o dopo le loro navigazioni!!!!


##########################################################################################

nav_tot <- read.parquet("/user/valentina/IMPATTO_ADV_scarico_naigazione.parquet")
View(head(nav_tot,100))
nrow(nav_tot)
# 4.236.648

# post_campaign: ha persistenza di 7gg (trackingcode)
# va_closer_id: ha persistenza legata alla singola visita, non rimane nel tempo

nav_tot_1 <- filter(nav_tot, "post_campaign is NULL and canale_corporate_post_prop is NOT NULL")
View(head(nav_tot_1,100))
nrow(nav_tot_1)
# 1.305.064


###########################################################################################################################################################
## Per ogni COD_CLIENTE ricavo le frequenze di visita delle sezioni corporate navigate in maniera ORGANICA ################################################
###########################################################################################################################################################

lista_sezioni <- distinct(select(nav_tot_1, "canale_corporate_post_prop"))
View(head(lista_sezioni,1000))


createOrReplaceTempView(nav_tot_1, "nav_tot_1")

tot_organic <- sql("select COD_CLIENTE_CIFRATO, max_Data_stipula_dt,
                            count(COD_CLIENTE_CIFRATO) as num_tot_page_views,
                            sum(case when canale_corporate_post_prop = 'aol' then 1 else 0 end) as num_views_AOL,
                            sum(case when canale_corporate_post_prop = 'pacchetti-offerte' then 1 else 0 end) as num_views_PACCHETTI_OFFERTE
                   from nav_tot_1
                   group by COD_CLIENTE_CIFRATO, max_Data_stipula_dt
                   order by COD_CLIENTE_CIFRATO")
View(head(tot_organic,100))
nrow(tot_organic)
# 21.148


# tot_organic_2 <- sql("select COD_CLIENTE_CIFRATO, max_Data_stipula_dt, canale_corporate_post_prop,
#                             count(COD_CLIENTE_CIFRATO) as num_tot_page_views
#                    from nav_tot_1
#                    group by COD_CLIENTE_CIFRATO, max_Data_stipula_dt, canale_corporate_post_prop
#                    order by COD_CLIENTE_CIFRATO")
# View(head(tot_organic_2,100))


createOrReplaceTempView(tot_organic, "tot_organic")
createOrReplaceTempView(skyid_tot, "skyid_tot")

tot_organic_info <- sql("select distinct  t1.*, t2.flg_profilo
                        from tot_organic t1
                        left join skyid_tot t2
                          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t1.max_Data_stipula_dt = t2.data_stipula_dt")
View(head(tot_organic_info,100))
nrow(tot_organic_info)
# 21.189


## Quanti utenti di profilo diverso becco?

createOrReplaceTempView(tot_organic_info, "tot_organic_info")

profili_organic <- sql("select flg_profilo, count(COD_CLIENTE_CIFRATO) as num_profilo
                       from tot_organic_info
                       group by flg_profilo")
View(head(profili_organic,100))
# flg_profilo             num_profilo
# no_digital_ibridi         12139
# no_digital_altri_canali   4081
# digital                   4969



###########################################################################################################################################################
## Per ogni COD_CLIENTE ricavo le frequenze di visita delle sezioni corporate navigate in maniera ORGANICA nelle ultime 2 settimane prima della conversione
###########################################################################################################################################################

nav_tot_2 <- withColumn(nav_tot_1, "days_to_conv", datediff(nav_tot_1$max_Data_stipula_dt, nav_tot_1$date_time_dt))
View(head(nav_tot_2,100))
nrow(nav_tot_2)
# 1.305.064 (Ceck OK)

nav_tot_3 <- filter(nav_tot_2, "days_to_conv <= 14")
View(head(nav_tot_3,100))
nrow(nav_tot_3)
# 290.905


createOrReplaceTempView(nav_tot_3, "nav_tot_3")

tot_organic_last_15gg <- sql("select COD_CLIENTE_CIFRATO, max_Data_stipula_dt,
                                      count(COD_CLIENTE_CIFRATO) as num_tot_page_views,
                                      sum(case when canale_corporate_post_prop = 'aol' then 1 else 0 end) as num_views_AOL,
                                      sum(case when canale_corporate_post_prop = 'pacchetti-offerte' then 1 else 0 end) as num_views_PACCHETTI_OFFERTE
                             from nav_tot_3
                             group by COD_CLIENTE_CIFRATO, max_Data_stipula_dt
                             order by COD_CLIENTE_CIFRATO")
View(head(tot_organic_last_15gg,100))
nrow(tot_organic_last_15gg)
# 13.278


createOrReplaceTempView(tot_organic_last_15gg, "tot_organic_last_15gg")
createOrReplaceTempView(skyid_tot, "skyid_tot")

tot_organic_last_15gg_info <- sql("select distinct  t1.*, t2.flg_profilo
                        from tot_organic_last_15gg t1
                        left join skyid_tot t2
                          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t1.max_Data_stipula_dt = t2.data_stipula_dt")
View(head(tot_organic_last_15gg_info,100))
nrow(tot_organic_last_15gg_info)
# 13.310


## Quanti utenti di profilo diverso becco?

createOrReplaceTempView(tot_organic_last_15gg_info, "tot_organic_last_15gg_info")

profili_organic_last_15gg <- sql("select flg_profilo, count(COD_CLIENTE_CIFRATO) as num_profilo
                       from tot_organic_last_15gg_info
                       group by flg_profilo")
View(head(profili_organic_last_15gg,100))
# flg_profilo              num_profilo
# no_digital_ibridi           7471
# no_digital_altri_canali     2023
# digital                     3816


profili_organic_sez_last_15gg <- sql("select flg_profilo, 
                                              count(COD_CLIENTE_CIFRATO) as utenti_in_AOL_15gg
                                     from tot_organic_last_15gg_info
                                     where num_views_AOL > 0
                                     group by flg_profilo")
View(head(profili_organic_sez_last_15gg,100))
# flg_profilo             utenti_in_AOL_15gg
# no_digital_ibridi          1797
# no_digital_altri_canali    371
# digital                    2460



#chiudi sessione
sparkR.stop()
