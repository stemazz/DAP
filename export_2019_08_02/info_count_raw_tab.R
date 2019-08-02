
## Info conteggio righe (per Silvia) tabelle IMPRESSION E TRACKING POINT

#apri sessione
source("connection_R.R")
options(scipen = 1000)


## 2018 ##########################################################################################################################################################


impression <- read.parquet('/STAGE/adform/table=Impression/Impression.parquet')

impression_2 <- withColumn(impression, "data_nav", cast(impression$yyyymmdd, 'string'))
impression_3 <- withColumn(impression_2, "data_nav_dt", cast(cast(unix_timestamp(impression_2$data_nav, 'yyyyMMdd'), 'timestamp'), 'date'))
impression_4 <- filter(impression_3, "data_nav_dt >= '2018-07-01' and data_nav_dt <= '2018-07-31'")


write.parquet(impression_4, "/user/stefano.mazzucca/info_count_raw_impression_tab.parquet")




trackingpoint <- read.parquet("/STAGE/adform/table=Trackingpoint/Trackingpoint.parquet")

trackingpoint_2 <- withColumn(trackingpoint, "data", cast(trackingpoint$yyyymmdd, 'string'))
trackingpoint_3 <- withColumn(trackingpoint_2, "data_dt", cast(cast(unix_timestamp(trackingpoint_2$data, 'yyyyMMdd'), 'timestamp'), 'date'))
trackingpoint_4 <- filter(trackingpoint_3, "data_dt >= '2018-07-01' and data_dt <= '2018-07-31'")


write.parquet(trackingpoint_4, "/user/stefano.mazzucca/info_count_raw_tp_tab.parquet")




impression_tab <- read.parquet("/user/stefano.mazzucca/info_count_raw_impression_tab.parquet")
View(head(impression_tab,100))
nrow(impression_tab)
# 559.217.865

impression_tab_1 <- withColumn(impression_tab, "flag_coo_pos", ifelse(impression_tab$CookieID >= 0, 1, 0))
View(head(impression_tab_1,100))

gp_coo <- summarize(groupBy(impression_tab_1, impression_tab_1$data_nav_dt), 
                    count_tot = count(impression_tab_1$CookieID),
                    count_tot_dist = countDistinct(impression_tab_1$CookieID),
                    count_coo_pos = sum(impression_tab_1$flag_coo_pos))

gp_coo_1 <- withColumn(gp_coo, "perc_pos", gp_coo$count_coo_pos/gp_coo$count_tot)
View(head(gp_coo_1,100))



cookie_pos <- filter(impression_tab, "CookieID NOT LIKE '-%'")
View(head(cookie_pos,100))
nrow(cookie_pos)
# 422.150.717




tp_tab <- read.parquet("/user/stefano.mazzucca/info_count_raw_tp_tab.parquet")
View(head(tp_tab,100))
nrow(tp_tab)
# 305.220.655

createOrReplaceTempView(tp_tab, "tp_tab")

tp_tab_2 <- sql("select distinct *, regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid
                   from tp_tab")

tp_tab_2 <- withColumn(tp_tab_2, "flag_coo_pos", ifelse(tp_tab_2$CookieID >= 0, 1, 0))
View(head(tp_tab_2,100))


gp_coo <- summarize(groupBy(tp_tab_2, tp_tab_2$data_dt), 
                    count_tot = count(tp_tab_2$CookieID),
                    count_tot_dist = countDistinct(tp_tab_2$CookieID),
                    count_coo_pos = sum(tp_tab_2$flag_coo_pos))

gp_coo_1 <- withColumn(gp_coo, "perc_pos", gp_coo$count_coo_pos/gp_coo$count_tot)
View(head(gp_coo_1,100))




## 2019 ##########################################################################################################################################################


impression <- read.parquet('/STAGE/adform/table=Impression/Impression.parquet')

impression_2 <- withColumn(impression, "data_nav", cast(impression$yyyymmdd, 'string'))
impression_3 <- withColumn(impression_2, "data_nav_dt", cast(cast(unix_timestamp(impression_2$data_nav, 'yyyyMMdd'), 'timestamp'), 'date'))
impression_4 <- filter(impression_3, "data_nav_dt >= '2019-06-01' and data_nav_dt <= '2019-06-30'")


write.parquet(impression_4, "/user/stefano.mazzucca/info_check_impression_coo.parquet")




trackingpoint <- read.parquet("/STAGE/adform/table=Trackingpoint/Trackingpoint.parquet")

trackingpoint_2 <- withColumn(trackingpoint, "data", cast(trackingpoint$yyyymmdd, 'string'))
trackingpoint_3 <- withColumn(trackingpoint_2, "data_dt", cast(cast(unix_timestamp(trackingpoint_2$data, 'yyyyMMdd'), 'timestamp'), 'date'))
trackingpoint_4 <- filter(trackingpoint_3, "data_dt >= '2019-06-01' and data_dt <= '2019-06-30'")


write.parquet(trackingpoint_4, "/user/stefano.mazzucca/info_check_tp_coo.parquet")




impression_tab <- read.parquet("/user/stefano.mazzucca/info_check_impression_coo.parquet")
View(head(impression_tab,100))
nrow(impression_tab)
# 309.948.456

impression_tab_1 <- withColumn(impression_tab, "flag_coo_pos", ifelse(impression_tab$CookieID >= 0, 1, 0))
View(head(impression_tab_1,100))

gp_coo_imp <- summarize(groupBy(impression_tab_1, impression_tab_1$data_nav_dt), 
                    count_tot = count(impression_tab_1$CookieID),
                    count_tot_dist = countDistinct(impression_tab_1$CookieID),
                    count_coo_pos = sum(impression_tab_1$flag_coo_pos))

gp_coo_imp_1 <- withColumn(gp_coo_imp, "perc_pos", gp_coo_imp$count_coo_pos/gp_coo_imp$count_tot)
View(head(gp_coo_imp_1,100))



cookie_pos <- filter(impression_tab, "CookieID NOT LIKE '-%'")
View(head(cookie_pos,100))
nrow(cookie_pos)
# 422.150.717




tp_tab <- read.parquet("/user/stefano.mazzucca/info_check_tp_coo.parquet")
View(head(tp_tab,100))
nrow(tp_tab)
# 195.555.953

createOrReplaceTempView(tp_tab, "tp_tab")

tp_tab_2 <- sql("select distinct *, regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid
                   from tp_tab")

tp_tab_2 <- withColumn(tp_tab_2, "flag_coo_pos", ifelse(tp_tab_2$CookieID >= 0, 1, 0))
View(head(tp_tab_2,100))


gp_coo <- summarize(groupBy(tp_tab_2, tp_tab_2$data_dt), 
                    count_tot = count(tp_tab_2$CookieID),
                    count_tot_dist = countDistinct(tp_tab_2$CookieID),
                    count_coo_pos = sum(tp_tab_2$flag_coo_pos))

gp_coo_1 <- withColumn(gp_coo, "perc_pos", gp_coo$count_coo_pos/gp_coo$count_tot)
View(head(gp_coo_1,100))






#chiudi sessione
sparkR.stop()
