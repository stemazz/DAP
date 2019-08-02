
## Impatto ADV su vendite offline 1


# /user/silvia/click_filter_cookie_conversioni_day
# /user/silvia/impression_filter_cookie_conversioni_day
# /user/silvia/cookieid_skyid_conversioni (questo e' il dizionario)
# /user/silvia/cookieid_skyid_conversioni_v2 (questo e' il dizionario da usare con data di attivazione)



#apri sessione
source("connection_R.R")
options(scipen = 1000)


imp_coo_conv_day <- read.parquet("/user/silvia/impression_filter_cookie_conversioni_day")
View(head(imp_coo_conv_day,100))
nrow(imp_coo_conv_day)
# 1.622.651
imp_coo_conv_day_1 <- withColumn(imp_coo_conv_day, "data_nav", cast(imp_coo_conv_day$yyyymmdd, 'string'))
imp_coo_conv_day_2 <- withColumn(imp_coo_conv_day_1, "data_nav_dt", cast(cast(unix_timestamp(imp_coo_conv_day_1$data_nav, 'yyyyMMdd'), 'timestamp'), 'date'))
View(head(imp_coo_conv_day_2,100))
nrow(imp_coo_conv_day_2)
# 1.622.651


clic_coo_conv_day <- read.parquet("/user/silvia/click_filter_cookie_conversioni_day")
View(head(clic_coo_conv_day,100))
nrow(clic_coo_conv_day)
# 37.943
clic_coo_conv_day_1 <- withColumn(clic_coo_conv_day, "data_nav", cast(clic_coo_conv_day$yyyymmdd, 'string'))
clic_coo_conv_day_2 <- withColumn(clic_coo_conv_day_1, "data_nav_dt", cast(cast(unix_timestamp(clic_coo_conv_day_1$data_nav, 'yyyyMMdd'), 'timestamp'), 'date'))
View(head(clic_coo_conv_day_2,100))
nrow(clic_coo_conv_day_2)
# 37.943


diz_skyid_coo <- read.parquet("/user/silvia/cookieid_skyid_conversioni_v2")
View(head(diz_skyid_coo,100))
nrow(diz_skyid_coo)
# 426.055
diz_skyid_coo_1 <- withColumn(diz_skyid_coo,"data_prima_attiv_cntr_dt", cast(cast(unix_timestamp(diz_skyid_coo$DAT_PRIMA_ATTIV_CNTR, 'dd/MM/yyyy'), 'timestamp'), 'date'))
View(head(diz_skyid_coo_1,100))
nrow(diz_skyid_coo_1)
# 426.055


## Elaborazioni #################################################################################################################################################################

createOrReplaceTempView(imp_coo_conv_day_2, "imp_coo_conv_day_2")
createOrReplaceTempView(diz_skyid_coo_1, "diz_skyid_coo_1")

imp_skyid_conv_day <- sql("select t2.skyid, t1.cookieid , t1.`device-name` , t1.`os-name` , t1.`browser-name`, t1.num_imps, t1.data_nav_dt, t2.data_prima_attiv_cntr_dt
                          from imp_coo_conv_day_2 t1
                          inner join diz_skyid_coo_1 t2
                          on t1.cookieid = t2.cookieid and 
                              t1.`device-name` = t2.`device-name` and 
                              t1.`os-name` = t2.`os-name` and
                              t1.`browser-name` = t2.`browser-name` and 
                              t1.data_nav_dt <= t2.data_prima_attiv_cntr_dt")
View(head(imp_skyid_conv_day,100))
nrow(imp_skyid_conv_day)
# 2.281.802

# imp_skyid_conv_day_1 <- summarize(groupBy(imp_skyid_conv_day, "skyid", "`device-name`", "`os-name`", "`browser-name`", "data_prima_attiv_cntr_dt"), 
#                                   prima_data_nav = min(imp_skyid_conv_day$data_nav_dt), ultima_data_nav = max(imp_skyid_conv_day$data_nav_dt), 
#                                   num_imps_tot = sum(imp_skyid_conv_day$num_imps))
# View(head(imp_skyid_conv_day_1,100))
# nrow(imp_skyid_conv_day_1)
# # 45.267
# ## verifica sui skyid 
# valdist_skyid <- distinct(select(imp_skyid_conv_day_1, "skyid"))
# nrow(valdist_skyid)
# # 31.622

imp_skyid_conv_day_2 <- summarize(groupBy(imp_skyid_conv_day, "skyid", "data_prima_attiv_cntr_dt"), 
                                  prima_data_nav = min(imp_skyid_conv_day$data_nav_dt), ultima_data_nav = max(imp_skyid_conv_day$data_nav_dt), 
                                  num_imps_tot = sum(imp_skyid_conv_day$num_imps))
View(head(imp_skyid_conv_day_2,100))
nrow(imp_skyid_conv_day_2)
# 31.794
## verifica sui skyid 
valdist_skyid_imp <- distinct(select(imp_skyid_conv_day_2, "skyid"))
nrow(valdist_skyid_imp)
# 31.622 (vs. 31.794 --> 172 "doppioni", con doppia conversione)




createOrReplaceTempView(clic_coo_conv_day_2, "clic_coo_conv_day_2")
createOrReplaceTempView(diz_skyid_coo_1, "diz_skyid_coo_1")

clic_skyid_conv_day <- sql("select t2.skyid, t1.cookieid , t1.`device-name` , t1.`os-name` , t1.`browser-name`, t1.num_imps as num_clic, t1.data_nav_dt, t2.data_prima_attiv_cntr_dt
                          from clic_coo_conv_day_2 t1
                          inner join diz_skyid_coo_1 t2
                          on t1.cookieid = t2.cookieid and 
                              t1.`device-name` = t2.`device-name` and 
                              t1.`os-name` = t2.`os-name` and
                              t1.`browser-name` = t2.`browser-name` and 
                              t1.data_nav_dt <= t2.data_prima_attiv_cntr_dt")
View(head(clic_skyid_conv_day,100))
nrow(clic_skyid_conv_day)
# 40.070

clic_skyid_conv_day_2 <- summarize(groupBy(clic_skyid_conv_day, "skyid", "data_prima_attiv_cntr_dt"), 
                                  prima_data_nav = min(clic_skyid_conv_day$data_nav_dt), ultima_data_nav = max(clic_skyid_conv_day$data_nav_dt), 
                                  num_clic_tot = sum(clic_skyid_conv_day$num_clic))
View(head(clic_skyid_conv_day_2,100))
nrow(clic_skyid_conv_day_2)
# 10.385
## verifica sui skyid 
valdist_skyid_clic <- distinct(select(clic_skyid_conv_day_2, "skyid"))
nrow(valdist_skyid_clic)
# 10.335 (vs. 10.385 --> 50 "doppioni", con doppia conversione)


## Verifica sovrapposizione skyid su impression e click ##

valdist_skyid_tot <- union(valdist_skyid_imp, valdist_skyid_clic)
valdist_skyid_tot_2 <- distinct(select(valdist_skyid_tot, "skyid"))
nrow(valdist_skyid_tot_2)
# 31.622 (significa che tutti gli skyid "intercettati" nei click sono contenuti anche nelle ipression)

###########################################################


## Modifico il formato delle date ##
clic_skyid_conv_day_3 <- withColumn(clic_skyid_conv_day_2, "prima_data_nav_str", cast(clic_skyid_conv_day_2$prima_data_nav, "string"))
clic_skyid_conv_day_4 <- withColumn(clic_skyid_conv_day_3, "ultima_data_nav_str", cast(clic_skyid_conv_day_3$ultima_data_nav, "string"))
View(head(clic_skyid_conv_day_4,100))
printSchema(clic_skyid_conv_day_4)

#####################################


createOrReplaceTempView(imp_skyid_conv_day_2, "imp_skyid_conv_day_2")
createOrReplaceTempView(clic_skyid_conv_day_2, "clic_skyid_conv_day_2")

skyid_tot_conv_day <- sql("select t1.skyid, t1.data_prima_attiv_cntr_dt, 
                                  t1.prima_data_nav as imp_prima_data_nav, t2.prima_data_nav as clic_prima_data_nav,
                                  t1.ultima_data_nav as imp_ultima_data_nav, t2.ultima_data_nav as clic_ultima_data_nav,
                                  t1.num_imps_tot, t2.num_clic_tot
                          from imp_skyid_conv_day_2 t1
                          left join clic_skyid_conv_day_2 t2
                          on t1.skyid = t2.skyid")

View(head(skyid_tot_conv_day,1000))
nrow(skyid_tot_conv_day)
# 31.924
## Con la join anche su: "and t1.data_prima_attiv_cntr_dt = t2.data_prima_attiv_cntr_dt"
# 31.794 (Ceck OK al netto dei "doppioni", cioe' delle doppie conversioni)



# skyid_tot_conv_day_2 <- withColumn(skyid_tot_conv_day, "clic_prima_data_nav_dt", cast(cast(unix_timestamp(skyid_tot_conv_day$clic_prima_data_nav, 'yyyy-MM-dd'),"timestamp"), "date"))
# skyid_tot_conv_day_3 <- withColumn(skyid_tot_conv_day_2, "clic_ultima_data_nav_dt", cast(skyid_tot_conv_day$clic_ultima_data_nav, "date"))
# View(head(skyid_tot_conv_day_2,100))
# printSchema(skyid_tot_conv_day_3)




skyid_tot_conv_day_2 <- summarize(groupBy(skyid_tot_conv_day, "skyid", "data_prima_attiv_cntr_dt"), 
                                  imp_prima_data_nav = min(skyid_tot_conv_day$imp_prima_data_nav), 
                                  clic_prima_data_nav = min(skyid_tot_conv_day$clic_prima_data_nav),
                                  imp_ultima_data_nav = max(skyid_tot_conv_day$imp_ultima_data_nav), 
                                  clic_ultima_data_nav = max(skyid_tot_conv_day$clic_ultima_data_nav),
                                  num_imps_tot = sum(skyid_tot_conv_day$num_imps_tot),
                                  num_clic_tot = sum(skyid_tot_conv_day$num_clic_tot))
View(head(skyid_tot_conv_day_2,100))
nrow(skyid_tot_conv_day_2)
# 31.794


createOrReplaceTempView(skyid_tot_conv_day_2, "skyid_tot_conv_day_2")

skyid_tot_conv_day_3 <- sql("select *, 
                                    case when imp_prima_data_nav <= clic_prima_data_nav then imp_prima_data_nav
                                      else clic_prima_data_nav end as prima_data_interactions_provv,
                                    case when imp_ultima_data_nav >= clic_ultima_data_nav then imp_ultima_data_nav
                                      else clic_ultima_data_nav end as ultima_data_interactions_provv,
                                    (num_imps_tot + num_clic_tot) as num_tot_interactions_provv
                            from skyid_tot_conv_day_2")
#View(head(skyid_tot_conv_day_3,100))

skyid_tot_conv_day_3$prima_data_interactions <- ifelse(isNotNull(skyid_tot_conv_day_3$prima_data_interactions_provv) == TRUE, 
                                                          skyid_tot_conv_day_3$prima_data_interactions_provv, skyid_tot_conv_day_3$imp_prima_data_nav)
skyid_tot_conv_day_3$ultima_data_interactions <- ifelse(isNotNull(skyid_tot_conv_day_3$ultima_data_interactions_provv) == TRUE, 
                                                          skyid_tot_conv_day_3$ultima_data_interactions_provv, skyid_tot_conv_day_3$imp_ultima_data_nav)
skyid_tot_conv_day_3$num_tot_interactions <- ifelse(isNotNull(skyid_tot_conv_day_3$num_tot_interactions_provv) == TRUE, 
                                                          skyid_tot_conv_day_3$num_tot_interactions_provv, skyid_tot_conv_day_3$num_imps_tot)

skyid_tot_conv_day_3$prima_data_interactions_provv <- NULL
skyid_tot_conv_day_3$ultima_data_interactions_provv <- NULL
skyid_tot_conv_day_3$num_tot_interactions_provv <- NULL

View(head(skyid_tot_conv_day_3,100))
nrow(skyid_tot_conv_day_3)
# 31.794


write.parquet(skyid_tot_conv_day_3, "/user/stefano.mazzucca/conversion_no_digital_by_day.parquet")
write.df(repartition( skyid_tot_conv_day_3, 1), path = "/user/stefano.mazzucca/conversion_no_digital_by_day.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


conv_no_dig_day <- read.parquet("/user/stefano.mazzucca/conversion_no_digital_by_day.parquet")
View(head(conv_no_dig_day,100))
nrow(conv_no_dig_day)
# 31.794



#chiudi sessione
sparkR.stop()
