
## Digitalizzazione_info_join


#apri sessione
source("connection_R.R")
options(scipen = 1000)



## Info sito interno
## Associazione skyid + cluster
pred_int <- read.parquet("/user/riccardo.motta/Aggiornamento_Digitalizzazione_20180301_20180831/predictions4.parquet") #prediction
View(head(pred_int,100))
nrow(pred_int)
# 3.006.565
scores <- summarize(pred_int, max_score = max(pred_int$prediction), min_score = min(pred_int$prediction))
View(head(scores))
# max_score   min_score
# 4           0


## Info sito esterno
## Associazione skyid + cluster
pred_ext <- read.parquet("/user/stefano.mazzucca/digitalizzazione/sm_cluster_k_6_score")
View(head(pred_ext,100))
nrow(pred_ext)
# 3.785.570
scores <- summarize(pred_ext, max_score = max(pred_ext$cluster_score), min_score = min(pred_ext$cluster_score))
View(head(scores))
# max_score   min_score
# 10          2


# createOrReplaceTempView(pred_int, "pred_int")
# createOrReplaceTempView(pred_ext, "pred_ext")
# 
# inner_join <- sql("select distinct t1.label as skyid, t1.prediction as cluster_int, t2.cluster_score as cluster_ext
#                   from pred_int t1
#                   inner join pred_ext t2
#                   on t1.label = t2.skyid")
# View(head(inner_join,100))
# nrow(inner_join)
# # 2.995.904
# # 10.661 skyid di scarto...


## Tabella degli ATTIVI
cb_attivi <- read.df("/user/silvia/External_id_sept2018_advocacy.csv", source = "csv", header = "true", delimiter = ",")
View(head(cb_attivi,100))
nrow(cb_attivi)
# 4.260.581


## APP + interessi
possesso_app <- read.parquet("/user/valentina/aggiorna_ADVOCACY_20181003.parquet")
View(head(possesso_app,100))
nrow(possesso_app)
# 4.144.666
possesso_app <- select(possesso_app, possesso_app$COD_CLIENTE_CIFRATO, possesso_app$flg_app_skySport, possesso_app$flg_app_masterchef, possesso_app$flg_app_xfactor)
View(head(possesso_app,100))
nrow(possesso_app)
# 4.144.666


## Elaborazioni cluster digitalizzazione ##

## pred_int_attivi
createOrReplaceTempView(pred_int, "pred_int")
createOrReplaceTempView(cb_attivi, "cb_attivi")
pred_int_attivi <- sql("select distinct t2.COD_CLIENTE_CIFRATO as skyid, t1.prediction as cluster_int
                       from pred_int t1
                       left join cb_attivi t2
                       on t1.label = t2.COD_CLIENTE_CIFRATO")
View(head(pred_int_attivi,100))
nrow(pred_int_attivi)
# 2.685.499

valdist_cluster_int <- distinct(select(pred_int_attivi, "cluster_int"))
View(head(valdist_cluster_int,100))
# cluster_int
# 0
# 1
# 2
# 3
# 4


## pred_ext_attivi
createOrReplaceTempView(pred_ext, "pred_ext")
createOrReplaceTempView(cb_attivi, "cb_attivi")
pred_ext_attivi <- sql("select distinct t2.COD_CLIENTE_CIFRATO as skyid, t1.cluster_score as cluster_ext
                       from pred_ext t1
                       left join cb_attivi t2
                       on t1.skyid = t2.COD_CLIENTE_CIFRATO")
View(head(pred_ext_attivi,100))
nrow(pred_ext_attivi)
# 3.084.000

valdist_cluster_ext <- distinct(select(pred_ext_attivi, "cluster_ext"))
View(head(valdist_cluster_ext,100))
# cluster_ext
# 2
# 4
# 6
# 8
# 10


## join con la cb_attiva
createOrReplaceTempView(cb_attivi, "cb_attivi")
createOrReplaceTempView(pred_int_attivi, "pred_int_attivi")
cb_advocacy <- sql("select distinct t1.COD_CLIENTE_CIFRATO as skyid, t2.cluster_int
                        from cb_attivi t1
                        left join pred_int_attivi t2
                        on t1.COD_CLIENTE_CIFRATO = t2.skyid")
View(head(cb_advocacy,100))
nrow(cb_advocacy)
# 4.144.666

createOrReplaceTempView(cb_advocacy, "cb_advocacy")
createOrReplaceTempView(pred_ext_attivi, "pred_ext_attivi")
cb_advocacy_1 <- sql("select distinct t1.*, t2.cluster_ext
                          from cb_advocacy t1
                          left join pred_ext_attivi t2
                          on t1.skyid = t2.skyid")
View(head(cb_advocacy_1,100))
nrow(cb_advocacy_1)
# 4.144.666

createOrReplaceTempView(cb_advocacy_1, "cb_advocacy_1")
cb_advocacy_2 <- sql("select skyid, 
                                  case when cluster_int = 4 then 'Heavy Users Multidevices' 
                                    when cluster_int = 3 then 'Nuovi Digitali' else 0 end as cluster_nav_siti_int,
                                  case when cluster_ext = 10 then 'Smartphone Addicted' 
                                    when cluster_ext = 8 then 'True Multi-device' else 0 end as cluster_nav_siti_ext
                          from cb_advocacy_1")
View(head(cb_advocacy_2,100))
nrow(cb_advocacy_2)
# 4.144.666

## Aggiungo info possesso_app
createOrReplaceTempView(cb_advocacy_2, "cb_advocacy_2")
createOrReplaceTempView(possesso_app, "possesso_app")
cb_advocacy_3 <- sql("select distinct t1.*, t2.flg_app_skySport, t2.flg_app_masterchef, t2.flg_app_xfactor
                     from cb_advocacy_2 t1
                     left join possesso_app t2
                     on t1.skyid = t2.COD_CLIENTE_CIFRATO")
View(head(cb_advocacy_3,100))
nrow(cb_advocacy_3)
# 4.144.666


write.parquet(cb_advocacy_3, "/user/stefano.mazzucca/digitalizzazione/advocacy_info.parquet", mode = "overwrite")

info_advocacy <- read.parquet("/user/stefano.mazzucca/digitalizzazione/advocacy_info.parquet")
View(head(info_advocacy,100))
nrow(info_advocacy)
# 4.144.666



## Elaborazione interessi ##

## Interessi siti esterni (sulle impression)
interessi_ext <- read.parquet("/user/riccardo.motta/Aggiornamento_Digitalizzazione_20180301_20180831/rapporto_sky_siti_esterni_imp.parquet")
View(head(interessi_ext,100))
nrow(interessi_ext)
# 51.711.416

interessi <- read.parquet("hdfs:///user/riccardo.motta/Aggiornamento_Digitalizzazione_20180301_20180831/interessi_sky_id_sport_calcio_cinema.parquet")
View(head(interessi,100))
nrow(interessi)
# 4.633.562

# filt <- filter(interessi, "bin_calcio = 1") # bin_sport # bin_cinema
# nrow(filt)
# # 1.073.804 calcio
# # 2.215.375 sport
# # 289.321 cinema


valdist_interessi_label <- distinct(select(interessi_ext, "label"))
View(head(valdist_interessi_label,1000))
valdist_interessi_Iab_label <- distinct(select(interessi_ext, "Iab_label"))
View(head(valdist_interessi_Iab_label,1000))

createOrReplaceTempView(interessi_ext, "interessi_ext")
interessi_ext_map <- sql("select *, 
                                case when label = 'World Football / Soccer' or label = 'Calcio' then 1 else 0 end as interessi_calcio, 
                                case when label in ('Boxing', 'Ice Hockey', 'Golf', 'Formula1', 'Bicycling', 'Swimming', 'NASCAR Racing', 'Motorcycles', 
                                                    'Sports', 'Olympics', 'Baseball / Softball', 'Figure Skating', 'Basket', 'Ciclismo', 'Auto Racing', 
                                                    'Running', 'Cricket', 'Rugby', 'Volleyball', 'American Football', 'Moto', 'Volley', 'Sailing / Boating', 
                                                    'Nuoto', 'Scuba Diving', 'Tennis', 'Bike', 'World Football / Soccer', 'Calcio') 
                                                    then 1 else 0 end as interessi_sport,
                                case when label in ('TV', 'Film Serietv', 'Streaming Media', 'Television & Video', 'Cinema Serietv', 'Serietv', 'Movies', 
                                                    'TV Cinema', 'Cinema', 'Cinema-SerieTv', 'Netflix') then 1 else 0 end as interessi_cinema
                         from interessi_ext")
createOrReplaceTempView(interessi_ext_map, "interessi_ext_map")
interessi_ext_map_2 <- sql("select skyid, max(interessi_calcio) as interessi_calcio, max(interessi_sport) as interessi_sport, 
                                    max(interessi_cinema) as interessi_cinema, count(skyid)
                            from interessi_ext_map
                            group by skyid")
View(head(interessi_ext_map_2,100))
nrow(interessi_ext_map_2)
# 3.133.512
# prova_calcio <- filter(interessi_ext_map_2, "interessi_calcio = 1")
# nrow(prova_calcio)
# # 1.118.027
# prova_sport <- filter(interessi_ext_map_2, "interessi_sport = 1")
# nrow(prova_sport)
# # 1.965.299     # post-calcio: 1.987.308
# prova_cinema <- filter(interessi_ext_map_2, "interessi_cinema = 1")
# nrow(prova_cinema)
# # 942.861


## combinazione degli interessi
createOrReplaceTempView(interessi, "interessi")
createOrReplaceTempView(interessi_ext_map_2, "interessi_ext_map_2")
interessi_final <- sql("select distinct t1.*, t2.skyid as skyid_2, t2.interessi_sport, t2.interessi_calcio, t2.interessi_cinema
                       from interessi t1
                       full outer join interessi_ext_map_2 t2
                       on t1.skyid = t2.skyid")
View(head(interessi_final,100))
nrow(interessi_final)
# 4.973.759

interessi_final$skyid_def <- ifelse(isNull(interessi_final$skyid), interessi_final$skyid_2, interessi_final$skyid)
interessi_final_1 <- select(interessi_final, interessi_final$skyid_def, interessi_final$bin_sport, interessi_final$bin_calcio, interessi_final$bin_cinema,
                                            interessi_final$interessi_sport, interessi_final$interessi_calcio, interessi_final$interessi_cinema)
interessi_final_2 <- withColumnRenamed(interessi_final_1, "skyid_def", "skyid")
interessi_final_3 <- fillna(interessi_final_2, 0)

createOrReplaceTempView(interessi_final_3, "interessi_final_3")
interessi_final_4 <- sql("select skyid, 
                                  case when bin_sport >= interessi_sport then bin_sport else interessi_sport end as inter_sport,
                                  case when bin_calcio >= interessi_calcio then bin_calcio else interessi_calcio end as inter_calcio,
                                  case when bin_cinema >= interessi_cinema then bin_cinema else interessi_cinema end as inter_cinema
                         from interessi_final_3")
View(head(interessi_final_4,100))
nrow(interessi_final_4)
# 4.973.759
# prova_calcio <- filter(interessi_final_4, "inter_calcio = 1")
# nrow(prova_calcio)
# # 1.146.324
# prova_sport <- filter(interessi_final_4, "inter_sport = 1")
# nrow(prova_sport)
# # 2.385.073
# prova_cinema <- filter(interessi_final_4, "inter_cinema = 1")
# nrow(prova_cinema)
# # 967.019



## Aggancio tutte le info per Advocacy ########################################################################################################################

info_advocacy <- read.parquet("/user/stefano.mazzucca/digitalizzazione/advocacy_info.parquet")
View(head(info_advocacy,100))
nrow(info_advocacy)
# 4.144.666

createOrReplaceTempView(info_advocacy, "info_advocacy")
createOrReplaceTempView(interessi_final_4, "interessi_final_4")

info_advocacy_1 <- sql("select distinct t1.*, t2.inter_sport, t2.inter_calcio, t2.inter_cinema
                       from info_advocacy t1
                       left join interessi_final_4 t2
                       on t1.skyid = t2.skyid")
View(head(info_advocacy_1,100))
nrow(info_advocacy_1)
# 4.144.666

# filt <- filter(info_advocacy_1, "inter_sport = 0 and inter_calcio = 0 and inter_cinema = 0")
# nrow(filt)
# # 2.032.563

info_advocacy_2 <- fillna(info_advocacy_1, 0)


write.parquet(info_advocacy_1, "/user/stefano.mazzucca/digitalizzazione/advocacy_info_final.parquet", mode = "overwrite")
write.df(repartition( info_advocacy, 1), path = "/user/stefano.mazzucca/digitalizzazione/advocacy_info_final.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


info_advocacy <- read.parquet("/user/stefano.mazzucca/digitalizzazione/advocacy_info_final.parquet")
View(head(info_advocacy,100))
nrow(info_advocacy)
# 4.144.666




#chiudi sessione
sparkR.stop()
