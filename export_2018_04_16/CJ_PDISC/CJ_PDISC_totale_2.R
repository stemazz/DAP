
# CJ_PDISC_totale_2
# elaborazioni su sequenze navigazione

#apri sessione
source("connection_R.R")
options(scipen = 1000)



# CJ_PDISC_visite_seq <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav.parquet")
# View(head(CJ_PDISC_visite_seq,100))
# nrow(CJ_PDISC_visite_seq)
# # 2.162.983

CJ_PDISC_visite_seq <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_20180406.parquet")
View(head(CJ_PDISC_visite_seq,100))
nrow(CJ_PDISC_visite_seq)
# 2.162.983


CJ_PDISC_visite_seq_1 <- withColumn(CJ_PDISC_visite_seq, "one_month_before_pdisc", date_sub(CJ_PDISC_visite_seq$data_ingr_pdisc_formdate, 31))

CJ_PDISC_visite_seq_1$flg_31gg <- ifelse(CJ_PDISC_visite_seq_1$date_time_dt <= CJ_PDISC_visite_seq_1$one_month_before_pdisc, 0, 1)
View(head(CJ_PDISC_visite_seq_1,100))
nrow(CJ_PDISC_visite_seq_1)
# 2.162.983


################################################################################################################################################################################
## Recupero di 2387 clienti 
################################################################################################################################################################################

filt_2 <- read.df("/user/stefano.mazzucca/export_clienti_post_pdisc", source = "csv", header = TRUE)

createOrReplaceTempView(filt_2, "filt_2")
createOrReplaceTempView(CJ_PDISC_visite_seq_1, "CJ_PDISC_visite_seq_1")

join_post_pdisc <- sql("select distinct t1.COD_CLIENTE_CIFRATO
                       from filt_2 t1 
                       inner join CJ_PDISC_visite_seq_1 t2
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
nrow(join_post_pdisc)
# 2.243 <<--- Attenzione! Si "becchano" 144 clienti in meno perch?? evidentemente il filtro delle apgine successive (assistenza, fatture, etc.) riduce i clienti intercettati.

join_post_pdisc_2 <- sql("select t2.*
                       from filt_2 t1 
                       inner join CJ_PDISC_visite_seq_1 t2
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
nrow(join_post_pdisc_2)
# 128.710
View(head(join_post_pdisc_2,1000))

join_post_pdisc_3 <- withColumn(join_post_pdisc_2, "sequenza_2", lit("homepage_sky"))
View(head(join_post_pdisc_3,1000))

join_post_pdisc_3$sequenza <- NULL

join_post_pdisc_4 <- withColumnRenamed(join_post_pdisc_3, "sequenza_2", "sequenza")
View(head(join_post_pdisc_4,1000))
nrow(join_post_pdisc_4)
# 128.710

join_post_pdisc_5 <- withColumn(join_post_pdisc_4, "canale_2", lit("sito_pubblico"))
View(head(join_post_pdisc_5,1000))

join_post_pdisc_5$canale <- NULL

join_post_pdisc_6 <- withColumnRenamed(join_post_pdisc_5, "canale_2", "canale")
View(head(join_post_pdisc_6,1000))
nrow(join_post_pdisc_6)
# 128.710

join_post_pdisc_7 <- withColumn(join_post_pdisc_6, "date_time_dt_2", join_post_pdisc_6$one_month_before_pdisc)
View(head(join_post_pdisc_7,1000))
join_post_pdisc_7$date_time_dt <- NULL
join_post_pdisc_8 <- withColumnRenamed(join_post_pdisc_7, "date_time_dt_2", "date_time_dt")

join_post_pdisc_9 <- withColumn(join_post_pdisc_8, "date_time_ts_2", join_post_pdisc_8$one_month_before_pdisc)
join_post_pdisc_10 <- withColumn(join_post_pdisc_9, "date_time_ts_3", cast(unix_timestamp(join_post_pdisc_9$date_time_ts_2, 'yyyy-MM-dd HH:mm:ss'), "timestamp"))
join_post_pdisc_10$date_time_ts <- NULL
join_post_pdisc_10$date_time_ts_2 <- NULL
join_post_pdisc_11 <- withColumnRenamed(join_post_pdisc_10, "date_time_ts_3", "date_time_ts")
View(head(join_post_pdisc_10,100))
join_post_pdisc_11$flg_31gg <- lit(1)

View(head(join_post_pdisc_11,1000))
nrow(join_post_pdisc_11)
# 128.710


################################################################################################################################################################################
################################################################################################################################################################################

## Sistemo 

cod_flg_post_pdisc <- distinct(select(join_post_pdisc_11, "COD_CLIENTE_CIFRATO"))
nrow(cod_flg_post_pdisc) # 2.243
cod_flg_post_pdisc_2 <- distinct(select(CJ_PDISC_visite_seq_1, "COD_CLIENTE_CIFRATO"))
nrow(cod_flg_post_pdisc_2) # 22.435

createOrReplaceTempView(cod_flg_post_pdisc, "cod_flg_post_pdisc")
createOrReplaceTempView(cod_flg_post_pdisc_2, "cod_flg_post_pdisc_2")

subg_1 <- sql("select t1.*,
                case when (t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO) then 1
                else 0 end as flg_post_pdisc
              from cod_flg_post_pdisc_2 t1
              left join cod_flg_post_pdisc t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
nrow(subg_1)
# 22.435
View(head(subg_1,100))

ver_1 <- filter(subg_1, "flg_post_pdisc = 1")
nrow(ver_1)
# 2.243


createOrReplaceTempView(CJ_PDISC_visite_seq_1, "CJ_PDISC_visite_seq_1")
createOrReplaceTempView(subg_1, "subg_1")

subg_2 <- sql("select t1.*, t2.flg_post_pdisc
              from CJ_PDISC_visite_seq_1 t1
              left join subg_1 t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
nrow(subg_2)
# 2.162.983
View(head(subg_2,100))

subg_3 <- filter(subg_2, "flg_post_pdisc = 0")
nrow(subg_3)
# 2.034.273
# 2.162.983 - 2.034.273 = 128.710 (ceck ok!)

subg_3$flg_post_pdisc <- NULL
join_post_pdisc_12 <- select(join_post_pdisc_11, "COD_CLIENTE_CIFRATO", "post_visid_concatenated", "visit_num", "concatena_chiave", "date_time_dt", "date_time_ts",
                            "data_ingr_pdisc_formdate", "hit_source", "exclude_hit", "sequenza", "download_name_post_prop", "canale",              
                            "one_month_before_pdisc", "flg_31gg")


final_dataset <- rbind(subg_3, join_post_pdisc_12)
nrow(final_dataset)
# 2.162.983



write.parquet(final_dataset,"/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_completo_20180406.parquet")


################################################################################################################################################################################
################################################################################################################################################################################



## Seleziono solo le navigaizoni PRE INGRESSO PDISC

CJ_PDISC_visite_seq_2 <- filter(final_dataset, "date_time_dt <= data_ingr_pdisc_formdate")
# prova <- filter(CJ_PDISC_visite_seq_2, "date_time_dt >= one_month_before_pdisc")
# prova_2 <- filter(prova, "flg_31gg = 0")
View(head(CJ_PDISC_visite_seq_2,1000))
nrow(CJ_PDISC_visite_seq_2)
# 1.918.439 --> # 1.961.521 (dopo le correzioni!)

## Converto dei parametri per l'elaborazione successiva

CJ_PDISC_visite_seq_3 <- withColumn(CJ_PDISC_visite_seq_2, "appoggio", lit("A"))
CJ_PDISC_visite_seq_4 <- withColumn(CJ_PDISC_visite_seq_3, "concatena_chiave_2", concat_ws("-", CJ_PDISC_visite_seq_3$appoggio, CJ_PDISC_visite_seq_3$concatena_chiave))
CJ_PDISC_visite_seq_4$appoggio <- NULL
View(head(CJ_PDISC_visite_seq_4,100))
nrow(CJ_PDISC_visite_seq_4)
# 1.961.521



# export 
write.df(repartition( CJ_PDISC_visite_seq_4, 1),path = "/user/stefano.mazzucca/CJ_PDISC_seq_nav_tot_20180406.csv", "csv", sep=";", mode = "overwrite", header=TRUE)










createOrReplaceTempView(CJ_PDISC_visite_seq_2, "CJ_PDISC_visite_seq_2")

CJ_PDISC_visite_seq_uniq <- sql("select concatena_chiave,
                                        sequenza, 
                                        last(date_time_ts) as ts,
                                        download_name_post_prop,
                                        canale,
                                        last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
                                        last(one_month_before_pdisc) as meno_1_mese_pdisc,
                                        last(COD_CLIENTE_CIFRATO) as external_id
                                from CJ_PDISC_visite_seq_2
                                group by concatena_chiave, sequenza, canale, download_name_post_prop
                                order by concatena_chiave")
View(head(CJ_PDISC_visite_seq_uniq,100))
nrow(CJ_PDISC_visite_seq_uniq)
# 741.892 --> # 727.343 (dopo le correzioni!)

valdist_clienti <- distinct(select(CJ_PDISC_visite_seq_uniq, "external_id"))
nrow(valdist_clienti)
# 22.426 utenti che navigano PRE pdisc (vs 23.094 totali cha navigano da Marzo a Dicembre 2017)


# export 
write.df(repartition( CJ_PDISC_visite_seq_uniq, 1),path = "/user/stefano.mazzucca/CJ_PDISC_seq_nav_uniq.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





# [1] "COD_CLIENTE_CIFRATO"      "post_visid_concatenated"  "visit_num"                "concatena_chiave"         "date_time_dt"             "date_time_ts"            
# [7] "data_ingr_pdisc_formdate" "hit_source"               "exclude_hit"              "sequenza"                 "download_name_post_prop"  "canale"                  
# [13] "one_month_before_pdisc"   "flg_31gg" 



# prova <- sql("select concatena_chiave,
#                       sequenza, 
#                                 last(date_time_ts) as ts,
#                       download_name_post_prop,
#                       canale,
#                                 last(data_ingr_pdisc_formdate) as data_ingr_pdisc,
#                                 last(one_month_before_pdisc) as meno_1_mese_pdisc,
#                                 last(COD_CLIENTE_CIFRATO) as external_id
#                                 from CJ_PDISC_visite_seq_1
#                                 group by concatena_chiave, sequenza, canale, download_name_post_prop
#                                 order by concatena_chiave")
# View(head(prova,100))
# nrow(prova)
# # 732.851 (concatena_chiave, sequenza)
# # 732.851 (concatena_chiave, sequenza, canale)
# # 750.091 (concatena_chiave, sequenza, canale, download_name_post_prop)

 


pdisc_path <- "/user/stefano.mazzucca/CJ_PDISC_seq_nav_uniq_reshaped_2.csv"
reshaped <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ",")
View(head(reshaped,1000))
nrow(reshaped)
# 272.288


reshaped_flg1 <- filter(reshaped, "flg_31gg = 1")
View(head(reshaped_flg1,100))
nrow(reshaped_flg1)
# 77.137

ver_clienti <- distinct(select(reshaped_flg1, "COD_CLIENTE_CIFRATO"))
nrow(ver_clienti)
# 18.338

reshaped_2 <- withColumn(reshaped, "dt", cast(reshaped$date_time_dt, "date"))
reshaped_3 <- withColumn(reshaped_2, "date_pdisc", cast(reshaped$data_ingr_pdisc, "date"))
reshaped_4 <- withColumn(reshaped_3, "date_meno_1_mese_pdisc", cast(reshaped$one_month_before_pdisc, "date"))

View(head(reshaped_4, 100))


ver_clienti_2 <- filter(reshaped_4, "dt >= date_meno_1_mese_pdisc")
nrow(ver_clienti_2)
# 78.213




a <- distinct(select(CJ_PDISC_visite_seq_4, "COD_CLIENTE_CIFRATO"))
nrow(a) # 22.435

filt_a <- filter(CJ_PDISC_visite_seq_4, "flg_31gg = 1")
filt_aa <- distinct(select(filt_a, "COD_CLIENTE_CIFRATO"))
nrow(filt_aa) # 18.338









# spark.fpGrowth(reshaped)






#chiudi sessione
sparkR.stop()
