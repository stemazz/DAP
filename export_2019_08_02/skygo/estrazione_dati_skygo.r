#	     _                        
#	    | |                       
#	 ___| | ___   _    __ _  ___  
#	/ __| |/ / | | |  / _` |/ _ \ 
#	\__ \   <| |_| | | (_| | (_) |
#	|___/_|\_\\__, |  \__, |\___/ 
#	           __/ |   __/ |      
#	          |___/   |___/       


# OPZIONI VARIE -----------------------------------------------------------

# setwd("Sky_Go_OCT18/")
source("connection_R.R")
options(scipen = 1000)


# LIBRERIE ----------------------------------------------------------------

library(magrittr)
library(stringr)


# COSTANTI ----------------------------------------------------------------

# start_date <- "2018-08-03"
# end_date <- "2018-10-03"

# PATH --------------------------------------------------------------------

#### In
trackingpoint_path <- "hdfs:///STAGE/adform/table=Trackingpoint"

#### Out


# CLEANING TRACKING POINT -------------------------------------------------

#### Import tracking point
trackingpoint <- read.parquet(trackingpoint_path)
trackingpoint$Timestamp <- cast(trackingpoint$Timestamp, "timestamp")

trackingpoint <- select(trackingpoint, c("TransactionId",
                                         "Timestamp",
                                         "CookieID",
                                         "PageURL",
                                         "DeviceTypeId",
                                         "customvars",
                                         "page",
                                         "device-name",
                                         "city",
                                         "region",
                                         "country",
                                         "browser-name",
                                         "os-name",
                                         "yyyymmdd")
)

#### Filtro per ora
trackingpoint <- filter(trackingpoint, trackingpoint$Timestamp >= "2017-10-28 18:00:00" )
trackingpoint <- filter(trackingpoint, trackingpoint$Timestamp <= "2017-10-28 20:00:00" )


#### Aggiungo Skyid a tp
trackingpoint$skyid <- regexp_extract(trackingpoint$customvars.systemvariables, '"13":"(\\w+)"', 1)

#### Tolgo nulli
createOrReplaceTempView(trackingpoint, "trackingpoint")
trackingpoint <- filter(trackingpoint, "skyid IS NOT NULL")
trackingpoint$customvars <- NULL

#### Rinomino device-name
trackingpoint <- withColumnRenamed(trackingpoint, "device-name", "devicename")

#### Tengo solo trackingpoint Sky Go
tp_skygo_list <- c("A105266:Home Page SkyGo - Post Login",
                   "A110746:PostLogin",
                   "A110761:PostLogin"
)

trackingpoint_sg <- trackingpoint[trackingpoint$page %in% tp_skygo_list,]


trackingpoint_sg$Timestamp <- cast(trackingpoint_sg$Timestamp, "timestamp")
trackingpoint_sg <- filter(trackingpoint_sg, trackingpoint_sg$yyyymmdd == "20171028")
trackingpoint_sg$hour <- hour(trackingpoint_sg$Timestamp)
trackingpoint_sg <- filter(trackingpoint_sg, trackingpoint_sg$hour == 18 | trackingpoint_sg$hour == 19)




write.parquet(trackingpoint_sg, "/user/stefano.mazzucca/skygo/export_28_ottobre_2017.parquet", mode = "overwrite")




tp_skygo <- read.parquet("/user/stefano.mazzucca/skygo/export_28_ottobre_2017.parquet")
View(head(tp_skygo,100))
nrow(tp_skygo)
# 225.774             14ott2017 juve-lazio
# 353.700 # 288.083   31ago2018 milan-roma  
# 116.357             15sett2018 napoli-fiorentina
# 167.283             25ago2018 juve-lazio
# 151.890             29sett2018 juve-napoli
# 185.510             1ott2017 milan-roma
# 505.562             28ott2017 milan-juve

valdist_extid <- distinct(select(tp_skygo, "skyid"))
nrow(valdist_extid)
# 153.477
# 195.790 # 169.387
# 74.996
# 108.098
# 93.619
# 128.177
# 167.122







#chiudi sessione
sparkR.stop()
