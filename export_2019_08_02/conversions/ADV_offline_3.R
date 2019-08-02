
## Impatto ADV su vendite offline 3


## Comprendere i canali di conversione e la differenzazione tra Try&By e nuove attivazioni normali
## -> join con: conversioni tot Dic 17 - Feb 18
##              /user/silvia/estrazione_nuoviattivati_Nov2017Mar2018.csv



#apri sessione
source("connection_R.R")
options(scipen = 1000)


p2c_no_digital <- read.parquet("/user/stefano.mazzucca/p2c_no_digital_by_day.parquet")
View(head(p2c_no_digital,100))
nrow(p2c_no_digital)
# 31.794

p2c_no_digital_wd <- withColumn(p2c_no_digital, "weekday_attiv_cntr", date_format(p2c_no_digital$data_prima_attiv_cntr_dt, 'EEEE'))
View(head(p2c_no_digital_wd,100))
nrow(p2c_no_digital_wd)
# 31.794

createOrReplaceTempView(p2c_no_digital_wd, "p2c_no_digital_wd")
p2c_no_digital_wd_1 <- sql("select *,
                              case when days_p2c_first_interaction <= 7 then '<7'
                              when days_p2c_first_interaction > 7 and days_p2c_first_interaction <= 14 then '<14'
                              when days_p2c_first_interaction > 14 and days_p2c_first_interaction <= 21 then '<21'
                              when days_p2c_first_interaction > 21 and days_p2c_first_interaction <= 28 then '<28'
                              when days_p2c_first_interaction > 28 and days_p2c_first_interaction <= 60 then '<60'
                              else '>60' end as bin_temp_conv
                           from p2c_no_digital_wd")
View(head(p2c_no_digital_wd_1,100))
nrow(p2c_no_digital_wd_1)
# 31.794

p2c_no_digital_wd_1$week_FY <- ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-02-26', 'W35', 
                                      ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-02-19', 'W34',
                                             ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-02-12', 'W33',
                                                    ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-02-05', 'W32',
                                                           ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-01-29', 'W31',
                                                                  ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-01-22', 'W30',
                                                                         ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-01-15', 'W29',
                                                                                ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-01-08', 'W28',
                                                                                       ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2018-01-01', 'W27',
                                                                                              ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2017-12-25', 'W26',
                                                                                                     ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2017-12-18', 'W25',
                                                                                                            ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2017-12-11', 'W24',
                                                                                                                   ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2017-12-04', 'W23',
                                                                                                                          ifelse(p2c_no_digital_wd_1$data_prima_attiv_cntr_dt >= '2017-12-01', 'W22', 
                                                                                                                                 'undefined'))))))))))))))

View(head(p2c_no_digital_wd_1,100))
nrow(p2c_no_digital_wd_1)
# 31.794



## File estrazione Nov17-Mar18

estrazioni_tot <- read.df("/user/silvia/estrazione_nuoviattivati_Nov2017Mar2018.csv", source = "csv", header = "true", delimiter = ";")
View(head(estrazioni_tot,100))
nrow(estrazioni_tot)
# 200.477

estrazioni_tot_1 <- withColumn(estrazioni_tot, "dat_prima_attiv_contr_dt", cast(cast(unix_timestamp(estrazioni_tot$DAT_PRIMA_ATTIV_CNTR, 'dd/MM/yyyy HH:mm'), 'timestamp'), 'date'))
View(head(estrazioni_tot_1,100))
nrow(estrazioni_tot_1)
# 200.477

estrazioni_tot_2 <- filter(estrazioni_tot_1, "dat_prima_attiv_contr_dt >= '2017-12-01' and dat_prima_attiv_contr_dt <= '2018-02-28'")
nrow(estrazioni_tot_2)
# 120.504
estrazioni_tot_3 <- distinct(select(estrazioni_tot_2, "COD_CLIENTE_CIFRATO", "dat_prima_attiv_contr_dt", "FLG_TBY", "DES_CANALE_VENDITA"))
nrow(estrazioni_tot_3)
# 119.474
estrazioni_tot_4 <- summarize(groupBy(estrazioni_tot_3, "COD_CLIENTE_CIFRATO", "dat_prima_attiv_contr_dt"), 
                              FLG_TBY_ok = first(estrazioni_tot_3$FLG_TBY), 
                              DES_CANALE_VENDITA_ok = first(estrazioni_tot_3$DES_CANALE_VENDITA))
nrow(estrazioni_tot_4)
# 119.428
estrazioni_tot_4 <- withColumnRenamed(estrazioni_tot_4, "FLG_TBY_ok", "FLG_TBY")
estrazioni_tot_4 <- withColumnRenamed(estrazioni_tot_4, "DES_CANALE_VENDITA_ok", "DES_CANALE_VENDITA")


#################################################################################################################################################################################
valdist_tby <- distinct(select(estrazioni_tot_1, "FLG_TBY"))
View(head(valdist_tby,100))

tby_test <- filter(estrazioni_tot_1, "(FLG_TBY = 0 or FLG_TBY = 1) and dat_prima_attiv_contr_dt >= '2017-12-01' and dat_prima_attiv_contr_dt <= '2018-02-28'")
View(head(tby_test,1000))
nrow(tby_test)
# 23.437

# ## Verifico che gli utenti TBY non ci siano 2 volte nel file totale dell'estrazione
# estrazioni_tot_2 <- filter(estrazioni_tot_1, "dat_prima_attiv_contr_dt >= '2017-12-01' and dat_prima_attiv_contr_dt <= '2018-02-28'")
# nrow(estrazioni_tot_2)
# # 120.504
# 
# createOrReplaceTempView(tby_test, "tby_test")
# createOrReplaceTempView(estrazioni_tot_2, "estrazioni_tot_2")
# 
# verifica <- sql("select t1.COD_CLIENTE_CIFRATO, t1.dat_prima_attiv_contr_dt, t2.dat_prima_attiv_contr_dt as data_file_estrazione
#                 from tby_test t1
#                 inner join estrazioni_tot_2 t2
#                   on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
# View(head(verifica,100))
# nrow(verifica)
# # 23.935
# 
# valdist_skyid <- distinct(select(verifica, "COD_CLIENTE_CIFRATO", "dat_prima_attiv_contr_dt", "data_file_estrazione"))
# nrow(valdist_skyid)
# # 23.894 (41 "doppioni)
# 
# createOrReplaceTempView(valdist_skyid, "valdist_skyid")
# createOrReplaceTempView(verifica, "verifica")
# verifica_2 <- sql("select *
#                   from verifica
#                   where dat_prima_attiv_contr_dt <> data_file_estrazione")
# View(head(verifica_2,100))
# nrow(verifica_2)
# # 461

#################################################################################################################################################################################

## JOIN per mappare i canali di converisone alla nostra base

createOrReplaceTempView(p2c_no_digital_wd_1, "p2c_no_digital_wd_1")
createOrReplaceTempView(estrazioni_tot_3, "estrazioni_tot_3")

p2c_no_digital_completo <- sql("select t1.*, t2.FLG_TBY, t2.DES_CANALE_VENDITA
                               from p2c_no_digital_wd_1 t1
                               left join estrazioni_tot_3 t2
                                  on t1.skyid = t2.COD_CLIENTE_CIFRATO and t1.data_prima_attiv_cntr_dt = t2.dat_prima_attiv_contr_dt ")
View(head(p2c_no_digital_completo,100))
nrow(p2c_no_digital_completo)
# 31.815 (21 "doppioni")
# 31.794 (Ceck OK)


## Ricerca dei "doppioni" ##

p2c_no_digital_completo_1 <- summarize(groupBy(p2c_no_digital_completo, "skyid", "data_prima_attiv_cntr_dt"), count = count(p2c_no_digital_completo$skyid))
p2c_no_digital_completo_2 <- arrange(p2c_no_digital_completo_1, desc(p2c_no_digital_completo_1$count))
View(head(p2c_no_digital_completo_2,100))
nrow(p2c_no_digital_completo_2)
# 31.794

filtro_doppioni <- filter(p2c_no_digital_completo_2, "count = 1")
nrow(filtro_doppioni)
# 31.773


## JOIN con chiave unica (senza doppioni) skyid + data_prima_attiv_cntr

createOrReplaceTempView(p2c_no_digital_completo, "p2c_no_digital_completo")
createOrReplaceTempView(filtro_doppioni, "filtro_doppioni")

p2c_no_digital_completo_def <- sql("select t1.*
                                   from p2c_no_digital_completo t1
                                   right join filtro_doppioni t2
                                    on t1.skyid = t2.skyid and t1.data_prima_attiv_cntr_dt = t2.data_prima_attiv_cntr_dt")
View(head(p2c_no_digital_completo_def,100))
nrow(p2c_no_digital_completo_def)
# 31.773


createOrReplaceTempView(p2c_no_digital_completo_def, "p2c_no_digital_completo_def")

p2c_no_digital_completo_def_1 <- sql("select *, 
                                case when num_tot_interactions < 10 then '<10'
                                when num_tot_interactions < 20 then '<20' 
                                when num_tot_interactions < 30 then '<30' 
                                when num_tot_interactions < 40 then '<40' 
                                when num_tot_interactions < 50 then '<50' 
                                when num_tot_interactions < 100 then '<100' 
                                when num_tot_interactions < 200 then '<200' 
                                when num_tot_interactions < 300 then '<300'
                                when num_tot_interactions < 400 then '<400' 
                                when num_tot_interactions < 500 then '<500' 
                                when num_tot_interactions < 1000 then '<1000' 
                                else '>1000' end as bin_interactions
                         from p2c_no_digital_completo_def")
View(head(p2c_no_digital_completo_def_1,100))
nrow(p2c_no_digital_completo_def_1)
# 31.773




write.parquet(p2c_no_digital_completo_def_1, "/user/stefano.mazzucca/p2c_no_digital_def.parquet")
write.df(repartition( p2c_no_digital_completo_def_1, 1), path = "/user/stefano.mazzucca/p2c_no_digital_def.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




distr_tby <- summarize(groupBy(p2c_no_digital_completo_def_1, "FLG_TBY"), count = count(p2c_no_digital_completo_def_1$skyid))
View(head(distr_tby,100))
# FLG_TBY   count
# 0         4.593
# NA        25.536
# 1         1.644


distr_channel <- summarize(groupBy(p2c_no_digital_completo_def_1, "DES_CANALE_VENDITA"), count = count(p2c_no_digital_completo_def_1$skyid))
View(head(distr_channel,100))
# DES_CANALE_VENDITA  count
# Other               3760
# SKY CENTER          7399
# IPTV                366
# TELESELLING INTERNO 7705
# SKY SERVICE         6714
# TELESELLING ESTERNO 5829


distr_interazioni <- summarize(groupBy(p2c_no_digital_completo_def_1, "bin_interactions"), count = count(p2c_no_digital_completo_def_1$skyid))
View(head(distr_interazioni,100))






## VERIFICHE sui file DAILY SALES #############################################################################################################################



bi_file <- read.df("/user/silvia/conversioni_nov17_mar18.csv", source = "csv", header = "true", delimiter = ";")
View(head(bi_file,100))
nrow(bi_file)
# 203.060

bi_file_1 <- withColumn(bi_file, "dat_prima_attiv_dt", cast(cast(unix_timestamp(bi_file$dat_prima_attivazione, 'dd/MM/yyyy'), 'timestamp'), 'date'))
View(head(bi_file_1,100))

bi_file_2 <- filter(bi_file_1, "dat_prima_attiv_dt >= '2017-12-04' and dat_prima_attiv_dt <= '2018-02-25' and des_cls_utenza_sales = 'RESIDENTIAL'")
nrow(bi_file_2)
# 109.662



mi_file <- read.df("/user/silvia/estrazione_nuoviattivati_Nov2017Mar2018.csv", source = "csv", header = "true", delimiter = ";")
View(head(mi_file,100))
nrow(mi_file)
# 200.477

mi_file_1 <- withColumn(mi_file, "dat_prima_attiv_cntr_dt", cast(cast(unix_timestamp(mi_file$DAT_PRIMA_ATTIV_CNTR, 'dd/MM/yyyy'), 'timestamp'), 'date'))
View(head(mi_file_1,100))

mi_file_2 <- filter(mi_file_1, "dat_prima_attiv_cntr_dt >= '2017-12-04' and dat_prima_attiv_cntr_dt <= '2018-02-25' and DES_TIPO_CONTRATTO = 'RESIDENZIALE'")
nrow(mi_file_2)
# 111.931




campagne <- read.df("/user/silvia/campaign_attribute.csv", source = "csv", header = "true", delimiter = ";")
View(head(campagne, 100))
nrow(campagne)
# 2.534


no_special <- read.df("/user/silvia/metadata_promo.csv", source = "csv", header = "true", delimiter = ";")
View(head(no_special,100))

no_special_1 <- filter(no_special, "`Macro Type L1` not like '%Special%'")
nrow(no_special_1) # ok!




createOrReplaceTempView(bi_file_2, "bi_file_2")
createOrReplaceTempView(mi_file_2, "mi_file_2")
#createOrReplaceTempView(campagne, "campagne")
createOrReplaceTempView(no_special_1, "no_special_1")

bi_join_no_special <- sql("select t1.*, t2.*
                          from bi_file_2 t1
                          inner join no_special_1 t2
                          on t1.num_cod_vis = t2.Cod ")
View(head(bi_join_no_special,100))
nrow(bi_join_no_special)
# 89.933


mi_join_no_special <- sql("select t1.*, t2.*
                          from mi_file_2 t1
                          inner join no_special_1 t2
                          on t1.num_cod_vis_vendita = t2.Cod ")
View(head(mi_join_no_special,100))
nrow(mi_join_no_special)
# 90.856



## Levo il codice subentro

mi_join_no_special_1 <- filter(mi_join_no_special, "num_cod_vis_vendita NOT LIKE '5462'")
nrow(mi_join_no_special_1)
# 89.040





p2c_no_digital_completo_def <- read.parquet("/user/stefano.mazzucca/p2c_no_digital_def.parquet")
#p2c_no_digital_completo_def_2 <- arrange(p2c_no_digital_completo_def, "skyid")
View(head(p2c_no_digital_completo_def,100))
nrow(p2c_no_digital_completo_def)
# 31.773


createOrReplaceTempView(p2c_no_digital_completo_def, "p2c_no_digital_completo_def")
createOrReplaceTempView(mi_join_no_special_1, "mi_join_no_special_1")

p2c_no_digital_NO_subentro <- sql("select *
                                  from mi_join_no_special_1 t1
                                  inner join p2c_no_digital_completo_def t2
                                  on t1.COD_CLIENTE_CIFRATO = t2.skyid and t1.dat_prima_attiv_cntr_dt = t2.data_prima_attiv_cntr_dt")
View(p2c_no_digital_NO_subentro,100)
nrow(p2c_no_digital_NO_subentro)
# 28.995

valdist_ext_dat <- distinct(select(p2c_no_digital_NO_subentro, "skyid", "dat_prima_attiv_cntr_dt"))
nrow(valdist_ext_dat)
# 28.931















#chiudi sessione
sparkR.stop()
