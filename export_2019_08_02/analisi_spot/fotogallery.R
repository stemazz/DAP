
## Fotogalleries [Fez]


source("connection_R.R")
options(scipen = 10000)

skyitdev <- read.parquet('/STAGE/adobe/reportsuite=skyitdev' ) # read reportsuite

# variabili utilizzate per lo scarico

skyit_1 <- select(skyitdev, "external_id_post_evar",
                  "post_visid_high",
                  "post_visid_low",
                  "post_visid_concatenated",
                  "visit_num",
                  "date_time",
                  "post_channel", 
                  "page_name_post_evar",
                  "page_url_post_evar",
                  "secondo_livello_post_prop",
                  "terzo_livello_post_prop",
                  "hit_source",
                  "exclude_hit", 
                  "post_page_event_value",
                  
                  "page_template_post_prop",
                  "page_template_post_evar",
                  
                  "hash_value_post_prop",
                  "totale_gallery_post_prop",
                  "data_prima_pubblicazione_post_prop"
)

skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date")) # cast date_time in formato date
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))  # cast date_time in formato timestamp

skyit_4 <- withColumn(skyit_3, "data_prima_pubblicazione_dt", cast(cast(unix_timestamp(skyit_3$data_prima_pubblicazione_post_prop, "dd/MM/yyyy"), "timestamp"), "date"))
# View(head(skyit_4,100))

# filtro temporale

skyit_5 <- filter(skyit_4, skyit_4$data_prima_pubblicazione_dt >= as.Date("2019-04-03"))
skyit_6 <- filter(skyit_5, skyit_5$data_prima_pubblicazione_dt <= as.Date("2019-05-12"))

skyit_7 <- filter(skyit_6, skyit_6$page_template_post_prop == 'photogallery')


write.parquet(skyit_7, "/user/stefano.mazzucca/Fotogallery/scarico_20190403_20180512.parquet", mode = "overwrite")



scarico <- read.parquet("/user/stefano.mazzucca/Fotogallery/scarico_20190403_20180512.parquet")
View(head(scarico,100))
nrow(scarico)
# 24.905.674
# prova <- summarize(scarico, max_data = max(scarico$data_prima_pubblicazione_dt), min_data = min(scarico$data_prima_pubblicazione_dt))
# View(head(prova,100))

# valdist_extid <- summarize(groupBy(scarico, scarico$external_id_post_evar), count_extid = countDistinct(scarico$external_id_post_evar))
# valdist_extid_1 <- arrange(valdist_extid, desc(valdist_extid$count_extid))
# View(head(valdist_extid_1,100))
# nrow(valdist_extid)
# # 76.188

scarico <- filter(scarico, scarico$post_channel == 'tg24')
scarico <- withColumn(scarico, "visits", concat(scarico$post_visid_concatenated, scarico$visit_num))
View(head(scarico,100))
nrow(scarico)
# 7.295.924

scarico_1 <- withColumn(scarico, "hash_value", substr(scarico$hash_value_post_prop, 3, 6))
scarico_2 <- withColumn(scarico_1, "hash_value_int", cast(scarico_1$hash_value, "integer"))
scarico_2 <- withColumn(scarico_2, "totale_gallery_int", cast(scarico_2$totale_gallery_post_prop, "integer"))
View(head(scarico_2,100))
# prova <- arrange(summarize(groupBy(scarico_2, "hash_value_int"), count = count(scarico_2$external_id_post_evar), max = max(scarico_2$hash_value_int)), "max")
# View(head(prova,100))

# valdist_visits <- summarize(groupBy(scarico_2, scarico$visits), 
#                             max_gallery = max(scarico_2$totale_gallery_post_prop), 
#                             max_hash_value = max(scarico_2$hash_value_int))
# View(head(valdist_visits,100))
# nrow(valdist_visits)
# # 1.032.125

n_fotog_2_livello <- summarize(groupBy(scarico_2, scarico_2$secondo_livello_post_prop), count = countDistinct(scarico_2$visits))
View(head(n_fotog_2_livello,100))


#################
cronaca <- filter(scarico_2, scarico_2$secondo_livello_post_prop == 'tg24-cronaca')
recap <- summarize(cronaca, max = max(cronaca$totale_gallery_int), min = min(cronaca$totale_gallery_int))
View(head(recap,100))
cronaca$fasce_lung <- ifelse(cronaca$totale_gallery_int >= 20, "(20,30]",
                             ifelse(cronaca$totale_gallery_int < 20 & cronaca$totale_gallery_int >= 14, "(14,20]", 
                             ifelse(cronaca$totale_gallery_int < 14 & cronaca$totale_gallery_int >= 10, "(10,14]", 
                             ifelse(cronaca$totale_gallery_int < 10 & cronaca$totale_gallery_int >= 8, "(8,10]", 
                             "(2.9,8]"))))
View(head(cronaca,100))
nrow(cronaca)
# 1.729.893

cronaca_gb <- summarize(groupBy(cronaca, cronaca$fasce_lung), count_gallery = countDistinct(cronaca$page_name_post_evar))
View(head(cronaca_gb,100))

createOrReplaceTempView(cronaca, "cronaca")
cronaca_gb_perc <- sql("select fasce_lung, count(visits) as count_visit_complete
                       from cronaca 
                       where totale_gallery_int == hash_value_int
                       group by fasce_lung")
View(head(cronaca_gb_perc,100))

lung_media <- sql("select page_name_post_evar, max(totale_gallery_int) as tot_gal
                  from cronaca
                  group by page_name_post_evar")
createOrReplaceTempView(lung_media,"lung_media")
lung_media_2 <- sql("select avg(tot_gal) as media
                    from lung_media")
View(head(lung_media_2,100))


# cronaca_2 <- summarize(groupBy(cronaca, cronaca$visits), max_hash_value = max(cronaca$hash_value_int), tot_fotogallery = max(cronaca$totale_gallery_int))
# View(head(cronaca_2,100))
# nrow(cronaca_2)
# # 362.998
# cronaca_final <- filter(cronaca_2, cronaca_2$tot_fotogallery == cronaca_2$max_hash_value)
# nrow(cronaca_final)
# # 70.339
# perc_completamento_fotogallery = nrow(cronaca_final)/nrow(cronaca_2)
# perc_completamento_fotogallery
# # 0.1937724



#################
intrattenimento <- filter(scarico_2, scarico_2$secondo_livello_post_prop == 'tg24-intrattenimento')
recap <- summarize(cronaca, max = max(intrattenimento$totale_gallery_int), min = min(intrattenimento$totale_gallery_int))
View(head(recap,100))
intrattenimento$fasce_lung <- ifelse(intrattenimento$totale_gallery_int >= 20, "(20,30]",
                             ifelse(intrattenimento$totale_gallery_int < 20 & intrattenimento$totale_gallery_int >= 14, "(14,20]", 
                                    ifelse(intrattenimento$totale_gallery_int < 14 & intrattenimento$totale_gallery_int >= 10, "(10,14]", 
                                           ifelse(intrattenimento$totale_gallery_int < 10 & intrattenimento$totale_gallery_int >= 8, "(8,10]", 
                                                  "(2.9,8]"))))
View(head(intrattenimento,100))

intrattenimento_gb <- summarize(groupBy(intrattenimento, intrattenimento$fasce_lung), count = countDistinct(intrattenimento$page_name_post_evar))
View(head(intrattenimento_gb,100))

createOrReplaceTempView(intrattenimento, "intrattenimento")
intrattenimento_gb_perc <- sql("select fasce_lung, count(visits) as count_visit_complete
                       from intrattenimento 
                       where totale_gallery_int == hash_value_int
                       group by fasce_lung")
View(head(intrattenimento_gb_perc,100))

lung_media <- sql("select page_name_post_evar, max(totale_gallery_int) as tot_gal
                  from intrattenimento
                  group by page_name_post_evar")
createOrReplaceTempView(lung_media,"lung_media")
lung_media_2 <- sql("select avg(tot_gal) as media
                    from lung_media")
View(head(lung_media_2,100))


# intrattenimento_2 <- summarize(groupBy(intrattenimento, intrattenimento$visits), max_hash_value = max(intrattenimento$hash_value_int), 
#                                tot_fotogallery = max(intrattenimento$totale_gallery_int))
# View(head(intrattenimento_2,100))
# nrow(intrattenimento_2)
# # 259.711
# intrattenimento_final <- filter(intrattenimento_2, intrattenimento_2$tot_fotogallery == intrattenimento_2$max_hash_value)
# nrow(intrattenimento_final)
# # 31.646
# perc_completamento_fotogallery = nrow(intrattenimento_final)/nrow(intrattenimento_2)
# perc_completamento_fotogallery
# # 0.2789079


#################
mondo <- filter(scarico_2, scarico_2$secondo_livello_post_prop == 'tg24-mondo')
recap <- summarize(mondo, max = max(mondo$totale_gallery_int), min = min(mondo$totale_gallery_int))
View(head(recap,100))
mondo$fasce_lung <- ifelse(mondo$totale_gallery_int < 52 & mondo$totale_gallery_int >= 30, "(30,51]",
                           ifelse(mondo$totale_gallery_int < 30 & mondo$totale_gallery_int >= 20, "(20,30]",
                                  ifelse(mondo$totale_gallery_int < 20 & mondo$totale_gallery_int >= 14, "(14,20]", 
                                         ifelse(mondo$totale_gallery_int < 14 & mondo$totale_gallery_int >= 10, "(10,14]", 
                                                ifelse(mondo$totale_gallery_int < 10 & mondo$totale_gallery_int >= 8, "(8,10]", 
                                                       "(2.9,8]")))))
View(head(mondo,100))

mondo_gb <- summarize(groupBy(mondo, mondo$fasce_lung), count = countDistinct(mondo$page_name_post_evar))
View(head(mondo_gb,100))


createOrReplaceTempView(mondo, "mondo")
mondo_gb_perc <- sql("select fasce_lung, count(visits) as count_visit_complete
                       from mondo 
                       where totale_gallery_int == hash_value_int
                       group by fasce_lung")
View(head(mondo_gb_perc,100))

lung_media <- sql("select page_name_post_evar, max(totale_gallery_int) as tot_gal
                  from mondo
                  group by page_name_post_evar")
createOrReplaceTempView(lung_media,"lung_media")
lung_media_2 <- sql("select avg(tot_gal) as media
                    from lung_media")
View(head(lung_media_2,100))


#################
spettacolo <- filter(scarico_2, scarico_2$secondo_livello_post_prop == 'tg24-spettacolo')
recap <- summarize(spettacolo, max = max(spettacolo$totale_gallery_int), min = min(spettacolo$totale_gallery_int))
View(head(recap,100))
spettacolo$fasce_lung <- ifelse(spettacolo$totale_gallery_int < 70 & spettacolo$totale_gallery_int >= 42, "(42,69]",
                                ifelse(spettacolo$totale_gallery_int < 42 & spettacolo$totale_gallery_int >= 30, "(30,42]",
                                       ifelse(spettacolo$totale_gallery_int < 30 & spettacolo$totale_gallery_int >= 20, "(20,30]",
                                              ifelse(spettacolo$totale_gallery_int < 20 & spettacolo$totale_gallery_int >= 14, "(14,20]", 
                                                     ifelse(spettacolo$totale_gallery_int < 14 & spettacolo$totale_gallery_int >= 10, "(10,14]", 
                                                            ifelse(spettacolo$totale_gallery_int < 10 & spettacolo$totale_gallery_int >= 8, "(8,10]", 
                                                                   "(2.9,8]"))))))
View(head(spettacolo,100))

spettacolo_gb <- summarize(groupBy(spettacolo, spettacolo$fasce_lung), count = countDistinct(spettacolo$page_name_post_evar))
View(head(spettacolo_gb,100))

createOrReplaceTempView(spettacolo, "spettacolo")
spettacolo_gb_perc <- sql("select fasce_lung, count(visits) as count_visit_complete
                       from spettacolo 
                       where totale_gallery_int == hash_value_int
                       group by fasce_lung")
View(head(spettacolo_gb_perc,100))

lung_media <- sql("select page_name_post_evar, max(totale_gallery_int) as tot_gal
                  from spettacolo
                  group by page_name_post_evar")
createOrReplaceTempView(lung_media,"lung_media")
lung_media_2 <- sql("select avg(tot_gal) as media
                    from lung_media")
View(head(lung_media_2,100))






# scarico_3 <- filter(scarico_2, scarico_2$totale_gallery_post_prop == scarico_2$hash_value_int)
# View(head(scarico_3,100))
# nrow(scarico_3)
# # 330.604






# /user/stefano.mazzucca/Fotogallery/fotogallery_2.xlsx
path_fotogallery <- "/user/stefano.mazzucca/Fotogallery/fotogallery_2.csv"


fotog <- read.df(path_fotogallery, source = "csv", header = "true", delimiter = ";")
View(head(fotog,100))

fotog_1 <- filter(fotog, fotog$`Totale gallery (prop26)`==fotog$`Hash value (prop30)` | fotog$`Hash value (prop30)`== 0)
View(head(fotog_1,100))

write.df(repartition(fotog_1, 1), "/user/stefano.mazzucca/Fotogallery/fotogallery_export.csv", "csv", header = TRUE, delimiter = ";")

# fotog_2 <- summarize(groupBy(fotog_1, fotog_1$`Secondo Livello Editoriale (prop13)`), 
#                      perc = )












#chiudi sessione
sparkR.stop()
