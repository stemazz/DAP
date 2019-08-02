
# 479e9146948c04d2c618cb07e571aee1ccfd23ff4be0f4ef



trackingpoint <- read.parquet("/STAGE/adform/table=Trackingpoint/Trackingpoint.parquet")
View(head(trackingpoint, 100))

trackingpoint_1 <- filter(trackingpoint, "IsRobot = 'No'")
trackingpoint_2 <- filter(trackingpoint_1, "IsNoRepeats = 'Yes'")
trackingpoint_3 <- filter(trackingpoint_2, "CookieID <> 0 and CookieID is NOT NULL")
trackingpoint_4 <- withColumn(trackingpoint_3, "data", cast(trackingpoint_3$yyyymmdd, 'string'))
trackingpoint_5 <- withColumn(trackingpoint_4, "data_dt", cast(cast(unix_timestamp(trackingpoint_4$data, 'yyyyMMdd'), 'timestamp'), 'date'))

createOrReplaceTempView(trackingpoint_5, "trackingpoint_5")

cookieid_skyid_adform <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                                      CookieID, VisitNumber, `device-name`,`os-name`,`browser-name`, 
                                      Timestamp, data_dt, PreviuosPageURL, PageURL
                             from trackingpoint_5
                             where customvars.systemvariables like '%13%' and CookieID <> '0' -- and 
                                    -- data_dt >= '2017-03-01' and data_dt <= '2018-05-31'
                             having (skyid <> '' and length(skyid)= 48)")

adform_extid <- filter(cookieid_skyid_adform, "skyid == '479e9146948c04d2c618cb07e571aee1ccfd23ff4be0f4ef'")

write.parquet(adform_extid, "/user/stefano.mazzucca/CJ_UP_prova_extid_strano.parquet")

adform_extid <- read.parquet("/user/stefano.mazzucca/CJ_UP_prova_extid_strano.parquet")
View(head(adform_extid,100))
nrow(adform_extid)
# 302

adform_extid_1 <- withColumn(adform_extid, "date_time_ts", cast(unix_timestamp(adform_extid$Timestamp, "yyyy-MM-dd HH:mm:ss"), "timestamp"))
#adform_extid_2 <- filter(adform_extid_1, "data_dt >= '2017-06-13'")
adform_extid_2 <- arrange(adform_extid_1, asc(adform_extid_1$data_dt))

View(head(adform_extid_2,400))









nav_sez_corporate <- read.parquet("/user/stefano.mazzucca/CJ_UP_scarico_nav_corporate.parquet")
View(head(nav_sez_corporate,100))
nrow(nav_sez_corporate)
# 47.630.594


adobe_ext_id <- filter(nav_sez_corporate, "COD_CLIENTE_CIFRATO == '479e9146948c04d2c618cb07e571aee1ccfd23ff4be0f4ef'")
adobe_ext_id_1 <- arrange(adobe_ext_id, asc(adobe_ext_id$date_time_ts))
View(head(adobe_ext_id_1,100))



adobe_ext_id_2 <- remove_too_short(adobe_ext_id_1, 10)
View(head(adobe_ext_id_2,100))







