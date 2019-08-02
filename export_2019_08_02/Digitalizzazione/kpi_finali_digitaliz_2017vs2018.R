
## Analisi digitalizzazione 2017 e 2018


source("connection_R.R")
options(scipen = 10000)



digitalizzazione_201709_path= 'hdfs:///user/alessandro/clusterfinalscore_conApp.parquet'

digitalizzazione_201810_path= 'hdfs:///user/silvia/digitalizzazione_parziale_navigazione_201810'



dig_2017 <- read.parquet(digitalizzazione_201709_path)
View(head(dig_2017,100))
nrow(dig_2017)
# 4.325.832

createOrReplaceTempView(dig_2017, "dig_2017")
dig_2017 <- sql("select skyid,  
                        (cluster_score + (devices_prediction_score + visite_prediction_score)) as livello_digitalizzazione 
                from dig_2017")
dig_2017 <- withColumn(dig_2017, "livello_2017", ifelse(dig_2017$livello_digitalizzazione == 0, 0, 
                                                        ifelse(dig_2017$livello_digitalizzazione <=2, 1, 
                                                               ifelse(dig_2017$livello_digitalizzazione <=4, 2, 
                                                                      ifelse(dig_2017$livello_digitalizzazione <=6, 3, 
                                                                             ifelse(dig_2017$livello_digitalizzazione <=8, 4, 
                                                                                    ifelse(dig_2017$livello_digitalizzazione <=10, 5, 
                                                                                           ifelse(dig_2017$livello_digitalizzazione <=12, 6, 
                                                                                                  ifelse(dig_2017$livello_digitalizzazione <=14, 7, 
                                                                                                         ifelse(dig_2017$livello_digitalizzazione <=16, 8, 
                                                                                                                ifelse(dig_2017$livello_digitalizzazione <=18, 9, 
                                                                                                                       ifelse(dig_2017$livello_digitalizzazione <=20, 10, NULL))))))))))))
View(head(dig_2017,100))
nrow(dig_2017)
# 4.325.832



dig_2018 <- read.parquet(digitalizzazione_201810_path)
View(head(dig_2018,100))
nrow(dig_2018)
# 4.506.444

dig_2018 <- withColumnRenamed(dig_2018, "livello_digitalizzazione", "livello_2018")


createOrReplaceTempView(dig_2017, "dig_2017")
createOrReplaceTempView(dig_2018, "dig_2018")
new_skyid <- sql("select t1.*
                 from dig_2018 t1
                 left join dig_2017 t2
                 on t1.skyid = t2.skyid where t2.skyid is NULL")
View(head(new_skyid,100))
nrow(new_skyid)
# 770.331

createOrReplaceTempView(new_skyid, "new_skyid")
gb_new_skyid <- sql("select livello_2018, count(skyid) as count
                    from new_skyid
                    group by livello_2018")
View(head(gb_new_skyid,100))



#chiudi sessione
sparkR.stop()
