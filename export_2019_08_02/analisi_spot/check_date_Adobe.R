
## Check date import ReportSuite Adobe

source("connection_R.R")
options(scipen = 10000)


##### Skyitdev #####
skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
View(head(skyitdev,100))

skyit_1 <- withColumn(skyitdev, "date_time_dt", cast(skyitdev$date_time, "date"))
skyit_2 <- filter(skyit_1, skyit_1$date_time_dt >= as.Date("2018-06-01"))

summ_date <- summarize(groupBy(skyit_2, skyit_2$date_time_dt), count_hit = count(skyit_2$post_visid_concatenated))

write.parquet(summ_date, "/user/stefano.mazzucca/check_date_Adobe/summ_skyitdev_last_year.parquet")


summ_date_skyitdev <- read.parquet("/user/stefano.mazzucca/check_date_Adobe/summ_skyitdev_last_year.parquet")
summ_date_skyitdev <- arrange(summ_date_skyitdev, asc(summ_date_skyitdev$date_time_dt))
write.df(repartition( summ_date_skyitdev, 1), "/user/stefano.mazzucca/check_date_Adobe/summ_skyitdev_last_year.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
View(head(summ_date_skyitdev,1000))
# 2019-03-05
# 2019-03-11
# 2019-03-12
# 2019-03-15
# 2019-03-17
# 2019-03-22
# 2019-03-29
# 2019-04-01
# 2019-04-10 --
# 2019-04-11
# 2019-04-13
# 2019-04-14
# 2019-04-15
# 2019-04-16
# 2019-04-18
# 2019-04-24
# 2019-04-26
# 2019-04-28
# 2019-04-29
# 2019-05-01
# 2019-05-10 --
# 2019-05-16
# 2019-05-21
# 2019-05-25


##### Skyappwsc #####
skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")

skyappwsc_1 <- withColumn(skyappwsc, "date_time_dt", cast(skyappwsc$date_time, "date"))
skyappwsc_2 <- filter(skyappwsc_1, skyappwsc_1$date_time_dt >= as.Date("2018-06-01"))

summ_date_appwsc <- summarize(groupBy(skyappwsc_2, skyappwsc_2$date_time_dt), count_hit = count(skyappwsc_2$post_visid_concatenated))

write.parquet(summ_date_appwsc, "/user/stefano.mazzucca/check_date_Adobe/summ_skyappwsc_last_year.parquet")


summ_date_appwsc <- read.parquet("/user/stefano.mazzucca/check_date_Adobe/summ_skyappwsc_last_year.parquet")
summ_date_appwsc <- arrange(summ_date_appwsc, asc(summ_date_appwsc$date_time_dt))
write.df(repartition( summ_date_appwsc, 1), "/user/stefano.mazzucca/check_date_Adobe/summ_skyappwsc_last_year.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
View(head(summ_date_appwsc,100))
# 2019-04-09
# 2019-04-22
# 2019-05-07


##### Skyappspsort #####
skyappsport <- read.parquet("/STAGE/adobe/reportsuite=skyappsport.prod")

skyappsport_1 <- withColumn(skyappsport, "date_time_dt", cast(skyappsport$date_time, "date"))
skyappsport_2 <- filter(skyappsport_1, skyappsport_1$date_time_dt >= as.Date("2018-06-01"))

summ_date_appsport <- summarize(groupBy(skyappsport_2, skyappsport_2$date_time_dt), count_hit = count(skyappsport_2$post_visid_concatenated))

write.parquet(summ_date_appsport, "/user/stefano.mazzucca/check_date_Adobe/summ_skyappsport_last_year.parquet")


summ_date_appsport <- read.parquet("/user/stefano.mazzucca/check_date_Adobe/summ_skyappsport_last_year.parquet")
summ_date_appsport <- arrange(summ_date_appsport, asc(summ_date_appsport$date_time_dt))
write.df(repartition( summ_date_appsport, 1), "/user/stefano.mazzucca/check_date_Adobe/summ_skyappsport_last_year.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
View(head(summ_date_appsport,100))
# 2019-03-05
# 2019-03-15
# 2019-03-17
# 2019-04-12



##### Skyapptg24 #####
skyapptg24 <- read.parquet("/STAGE/adobe/reportsuite=skyappskytg24.prod")

skyapptg24_1 <- withColumn(skyapptg24, "date_time_dt", cast(skyapptg24$date_time, "date"))
skyapptg24_2 <- filter(skyapptg24_1, skyapptg24_1$date_time_dt >= as.Date("2018-06-01"))

summ_date_apptg24 <- summarize(groupBy(skyapptg24_2, skyapptg24_2$date_time_dt), count_hit = count(skyapptg24_2$post_visid_concatenated))

write.parquet(summ_date_apptg24, "/user/stefano.mazzucca/check_date_Adobe/summ_skyapptg24_last_year.parquet")



summ_date_apptg24 <- read.parquet("/user/stefano.mazzucca/check_date_Adobe/summ_skyapptg24_last_year.parquet")
summ_date_apptg24 <- arrange(summ_date_apptg24, asc(summ_date_apptg24$date_time_dt))
write.df(repartition( summ_date_apptg24, 1), "/user/stefano.mazzucca/check_date_Adobe/summ_skyapptg24_last_year.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
View(head(summ_date_apptg24,100))
# 2019-04-01
# 2019-04-18
# 2019-05-01
# 2019-05-08




#chiudi sessione
sparkR.stop()
