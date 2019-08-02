
## Ananlisi LIFE TIME cookie Adform

source("connection_R.R")
options(scipen = 10000)



adform_tp_1 <- read.parquet('hdfs:///STAGE/adform/table=Trackingpoint/Trackingpoint.parquet')
View(head(adform_tp_1,1000))

adform_tp_2 <- withColumn(adform_tp_1, "date_time_ts", cast(adform_tp_1$Timestamp, "timestamp"))
adform_tp_3 <- withColumn(adform_tp_2, "date_time_dt", cast(adform_tp_2$date_time_ts, "date"))

createOrReplaceTempView(adform_tp_3, "adform_tp_3")
adform_tp_4 <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                            CookieID,
                            `device-name`,
                            `os-name`, 
                            `browser-name`,
                            date_time_ts,
                            date_time_dt
                    from adform_tp_3
                   where yyyymmdd >= 20181101 and yyyymmdd <= 20190131")
# View(head(adform_tp_4,100))
write.parquet(adform_tp_4, "/user/stefano.mazzucca/verifica_lifetime_cookie_adform.parquet", mode = "overwrite")


adform_tp <- read.parquet("/user/stefano.mazzucca/verifica_lifetime_cookie_adform.parquet")
View(head(adform_tp,100))
nrow(adform_tp)
# 574.830.854

adform_tp_filter <- filter(adform_tp, "CookieID <> 0")
nrow(adform_tp_filter)
# 541.089.641


verifica <- summarize(groupBy(adform_tp_filter, "CookieID", "`device-name`", "`os-name`", "`browser-name`"), 
                      count = count(adform_tp_filter$CookieID), 
                      first_ts = min(adform_tp_filter$date_time_ts),
                      last_ts = max(adform_tp_filter$date_time_ts),
                      diff_gg = datediff(max(adform_tp_filter$date_time_dt), min(adform_tp_filter$date_time_dt)))
View(head(verifica,100))
nrow(verifica)
# 41.757.526

# write.df(repartition(verifica, 1), path = "/user/stefano.mazzucca/verifica_lifetime_cookie_aggr.csv", "csv", sep=";", mode = "overwrite", header=TRUE)

count_coo <- summarize(groupBy(verifica, "diff_gg"), count_cookie = countDistinct(verifica$CookieID))
count_coo <- arrange(count_coo, asc(count_coo$diff_gg))
View(head(count_coo,100))
# diff_gg   count_cookie
# 0         26893664
# 1         1096996
# 2         549925

# 26.893.664 / 41.757.526 = 0.6440435 = 64% cookie con 0 gg di "vita"

# 10.969.96 + 26.893.664 = 27.990.660
# 27.990.660 / 41.757.526 = 0.6703141 = 67% cookie con 1 gg di "vita"




#chiudi sessione
sparkR.stop()
