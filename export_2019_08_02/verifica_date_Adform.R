
## Verifica dati Adform

source("connection_R.R")
options(scipen = 10000)


## Click

click <- read.parquet("/STAGE/adform/table=Click")
View(head(click,100))
printSchema(click)


click_1 <- withColumn(click, "date_time_dt", cast(cast(unix_timestamp(click$Timestamp, "yyyy-MM-dd HH:mm:ss"), "timestamp"), "date"))
click_2 <- filter(click_1, click_1$date_time_dt >= "2019-04-16" & click_1$date_time_dt <= "2019-05-09")

write.parquet(click_2, "/user/stefano.mazzucca/check_date_Adform/click_complete_buco.parquet", mode="overwrite")

daybyday <- read.parquet("/user/stefano.mazzucca/check_date_Adform/click_complete_buco.parquet")
View(head(daybyday,1000))

test_browser <- summarize(groupBy(daybyday, daybyday$`browser-name`), count = count(daybyday$`browser-language`))
View(head(test_browser,100))



check <- read.parquet("/STAGE/adform/table=Click/Click.parquet/yyyymmdd=20190418")
View(head(check,1000))
nrow(check)
# 150.000






gp_date_click <- summarize(groupBy(click_2, click_2$date_time_dt), count_hit = count(click_2$CookieID))

write.parquet(gp_date_click, "/user/stefano.mazzucca/check_date_Adform/click.parquet", mode="overwrite")

click_date <- read.parquet("/user/stefano.mazzucca/check_date_Adform/click.parquet")
click_date <- arrange(click_date, click_date$date_time_dt)
View(head(click_date,100))
nrow(click_date)
# 85






## Click

events <- read.parquet("/STAGE/adform/table=Event")
View(head(events,100))
printSchema(events)



## Impressions

imps <- read.parquet("/STAGE/adform/table=Impression")
View(head(imps,100))
printSchema(imps)


recover_imps <- read.df("/user/stefano.mazzucca/impressions_1", source = "csv", header = F, delmiter = ';')
View(head(recover_imps,100))


## Tracking Point

tp <- read.parquet("/STAGE/adform/table=Trackingpoint")
View(head(tp,100))
printSchema(tp)


recover_tp <- read.df("/user/stefano.mazzucca/trackingPoints_5", source = "csv", header = F, delimiter = ";")
View(head(recover_tp,100))




