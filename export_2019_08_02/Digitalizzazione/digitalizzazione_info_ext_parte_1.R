

## Digitalizzazione parte 1:
## impression e tracking point da navigazioni esterne ai siti Sky (adform)


#apri sessione
source("connection_R.R")
options(scipen = 1000)




## path lettura
impression_path <- 'hdfs:///STAGE/adform/table=Impression/Impression.parquet'
trackingpoint_path <- 'hdfs:///STAGE/adform/table=Trackingpoint/Trackingpoint.parquet'


## loading impression_table dataframe

impression_df <- read.parquet(impression_path)
# printSchema(impression_df)
impression_df <- filter(impression_df, "IsRobot == 'No'")
# impression_df <- filter(impression_df, "IsNoRepeats = 'Yes'")
impression_df <- filter(impression_df, "CookieID <> 0 and CookieID is NOT NULL")
# impression_df <- withColumn(impression_df, "data", cast(impression_df$yyyymmdd, 'string'))
# impression_df <- withColumn(impression_df, "data_dt", cast(cast(unix_timestamp(impression_df$data, 'yyyyMMdd'), 'timestamp'), 'date'))
createOrReplaceTempView(impression_df, "impression_df")
impression_df <- sql("select distinct regexp_extract(unloadvars.visibility, '\"visible1\":(\\\\w+)',1) AS visibility,
                                      cookieid, `device-name`, `os-name`, `browser-name`, 
                                      publisherdomain, timestamp, yyyymmdd, city, region, country
                      from impression_df
                      where CookieID <> '0' and yyyymmdd >= 20180301 and yyyymmdd <= 20180831
                      having (visibility LIKE 'true')")
cache(impression_df)
createOrReplaceTempView(impression_df, "impression")


## leggo tracking point per costruire coppie cookieid-skyid

trackingpoint_df <- read.parquet(trackingpoint_path)
# printSchema(trackingpoint_df)
trackingpoint_df <- filter(trackingpoint_df, "IsRobot = 'No'")
trackingpoint_df <- filter(trackingpoint_df, "IsNoRepeats = 'Yes'")
trackingpoint_df <- filter(trackingpoint_df, "CookieID <> 0 and CookieID is NOT NULL")
# trackingpoint_df <- withColumn(trackingpoint_df, "data", cast(trackingpoint_df$yyyymmdd, 'string'))
# trackingpoint_df <- withColumn(trackingpoint_df, "data_dt", cast(cast(unix_timestamp(trackingpoint_df$data, 'yyyyMMdd'), 'timestamp'), 'date'))
createOrReplaceTempView(trackingpoint_df, "trackingpoint")


## coppie cookieid-skyid 

cookieid_skyid_df <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                                        cookieid, `device-name`,`os-name`,`browser-name`
                         from trackingpoint
                         where customvars.systemvariables like '%13%' and CookieID <> '0' 
                         having (skyid <> '' and length(skyid)= 48)")
createOrReplaceTempView(cookieid_skyid_df, "cookieid_skyid")


lista_cookie_df <- sql("select cookieid from cookieid_skyid group by 1")
createOrReplaceTempView(lista_cookie_df, "lista_cookie")


imps_sel_col_df <- select(impression_df, 'cookieid', 'device-name','os-name','browser-name','publisherdomain','timestamp','yyyymmdd','city','region','country')
createOrReplaceTempView(imps_sel_col_df, "imps_sel_col")


skyid_imps_prep_step0_df <- sql("select * 
                                from imps_sel_col 
                                where cookieid in (select cookieid from lista_cookie)")
createOrReplaceTempView(skyid_imps_prep_step0_df, "skyid_imps_prep_step0")


skyid_imps_prep_df <- sql("select cookieid, yyyymmdd, `device-name`, `os-name`, `browser-name`, concat(`device-name`,`os-name`) as device_os, 
                                  substr(cast(yyyymmdd as varchar(8)),1,6) as mese, weekofyear(to_date(timestamp)) as week, 
                                  date_format(timestamp, 'EEEE') as weekday, hour(timestamp) as hour, 
                                  case when `device-name`='Mobile' then 1 else 0 end as flag_mobile, 
                                  case when `device-name`='Tablet' then 1 else 0 end as flag_tablet, 
                                  case when `device-name`='Desktop and Laptop' then 1 else 0 end as flag_desktop, 
                                  publisherdomain, timestamp, city, region, country 
                          from skyid_imps_prep_step0")


write.parquet(skyid_imps_prep_df, "/user/stefano.mazzucca/digitalizzazione/skyid_imps_1.parquet")


skyid_imps_prep_df <- read.parquet("/user/stefano.mazzucca/digitalizzazione/skyid_imps_1.parquet")
# View(head(skyid_imps_prep_df,100))
# nrow(skyid_imps_prep_df)
# # 250.805.069
createOrReplaceTempView(skyid_imps_prep_df, "skyid_imps_prep")


## giorno

skyid_imps_aggr_step0_df <- sql("select cookieid, `device-name`, `os-name`, `browser-name`, yyyymmdd 
                                from skyid_imps_prep 
                                group by 1,2,3,4,5")
createOrReplaceTempView(skyid_imps_aggr_step0_df, "skyid_imps_aggr_step0")
# View(head(skyid_imps_aggr_step0_df,100))


## mese

skyid_imps_aggr_step1_df <- sql("select cookieid, `device-name`, `os-name`, `browser-name`, mese 
                                from skyid_imps_prep 
                                group by 1,2,3,4,5")
createOrReplaceTempView(skyid_imps_aggr_step1_df, "skyid_imps_aggr_step1")
# View(head(skyid_imps_aggr_step1_df,100))


## week

skyid_imps_aggr_step2_df <- sql("select cookieid, `device-name`, `os-name`, `browser-name`, week 
                                from skyid_imps_prep 
                                group by 1,2,3,4,5")
createOrReplaceTempView(skyid_imps_aggr_step2_df, "skyid_imps_aggr_step2")
# View(head(skyid_imps_aggr_step2_df,100))


## imps tot e per device type

skyid_imps_aggr_step3_df <- sql("select cookieid, `device-name`, `os-name`, `browser-name`, count(*) as imps_tot, 
                                        sum(flag_mobile) as imps_mobile, sum(flag_tablet) as imps_tablet, 
                                        sum(flag_desktop) as imps_desktop 
                                from skyid_imps_prep 
                                group by 1,2,3,4")
createOrReplaceTempView(skyid_imps_aggr_step3_df, "skyid_imps_aggr_step3")
# View(head(skyid_imps_aggr_step3_df,100))


skyid_imps_aggr_step4a_df <- sql("select skyid, count (distinct(yyyymmdd)) as num_giorni 
                                 from (select skyid, cookieid, `device-name`, `os-name`, `browser-name` from cookieid_skyid where length(skyid) = 48) as a 
                                 left join (select cookieid, `device-name`, `os-name`, `browser-name`, yyyymmdd from  skyid_imps_aggr_step0) as b 
                                 on (a.cookieid = b.cookieid and a.`device-name` = b.`device-name` and 
                                    a.`os-name` = b.`os-name` and a.`browser-name` = b.`browser-name`) 
                                 group by 1")
skyid_imps_aggr_step4a_df <- fillna(skyid_imps_aggr_step4a_df, 0)
createOrReplaceTempView(skyid_imps_aggr_step4a_df, "skyid_imps_aggr_step4a")
# View(head(skyid_imps_aggr_step4a_df,100))
# nrow(skyid_imps_aggr_step4a_df)
# 


skyid_imps_aggr_step4b_df <- sql("select skyid, count (distinct mese) as num_mesi 
                                 from (select skyid, cookieid, `device-name`, `os-name`, `browser-name` from cookieid_skyid where length(skyid) = 48) as a 
                                 left join (select cookieid, `device-name`, `os-name`, `browser-name`, mese from  skyid_imps_aggr_step1) as c 
                                 on (a.cookieid = c.cookieid and a.`device-name` = c.`device-name` and a.`os-name` = c.`os-name` and 
                                      a.`browser-name`=c.`browser-name`) 
                                 group by 1")
skyid_imps_aggr_step4b_df <- fillna(skyid_imps_aggr_step4b_df, 0)
createOrReplaceTempView(skyid_imps_aggr_step4b_df, "skyid_imps_aggr_step4b")
# View(head(skyid_imps_aggr_step4b_df,100))
# nrow(skyid_imps_aggr_step4b_df)
# 


skyid_imps_aggr_step4c_df <- sql("select skyid, count (distinct week) as num_week 
                                 from (select skyid, cookieid, `device-name`, `os-name`, `browser-name` from cookieid_skyid where length(skyid) = 48) as a 
                                 left join (select cookieid, `device-name`, `os-name`, `browser-name`, week from  skyid_imps_aggr_step2) as d 
                                 on (a.cookieid = d.cookieid and a.`device-name` = d.`device-name` and a.`os-name` = d.`os-name` and 
                                      a.`browser-name` = d.`browser-name`) 
                                 group by 1")
skyid_imps_aggr_step4c_df <- fillna(skyid_imps_aggr_step4c_df, 0)
createOrReplaceTempView(skyid_imps_aggr_step4c_df, "skyid_imps_aggr_step4c")
# View(head(skyid_imps_aggr_step4c_df,100))
# nrow(skyid_imps_aggr_step4c_df)
# 


## num device step 1

skyid_imps_aggr_step5_df <- sql("select cookieid, `device-name`, `os-name`, `browser-name`, max(flag_mobile) as flag_mobile, 
                                          max(flag_tablet) as flag_tablet, max(flag_desktop) as flag_desktop 
                                from skyid_imps_prep 
                                group by 1,2,3,4")
createOrReplaceTempView(skyid_imps_aggr_step5_df, "skyid_imps_aggr_step5")
# View(head(skyid_imps_aggr_step5_df,100))


skyid_imps_aggr_step6_df <- sql("select skyid, count(distinct device_os) as num_device, 
                                        count(distinct case when flag_mobile=1 then device_os  else null end) as num_mobile, 
                                        count(distinct case when flag_tablet=1 then device_os  else null end) as num_tablet, 
                                        count(distinct case when flag_desktop=1 then device_os else null end) as num_desktop 
                                from (select skyid, cookieid, `device-name`, `os-name`, `browser-name` from cookieid_skyid where length(skyid) = 48) as a 
                                left join (select cookieid, `device-name`, `os-name`, `browser-name`, flag_mobile, flag_tablet, flag_desktop, 
                                                  concat(`device-name`,`os-name`) as device_os from  skyid_imps_aggr_step5) as b 
                                on (a.cookieid = b.cookieid and a.`device-name` = b.`device-name` and a.`os-name` = b.`os-name` and 
                                    a.`browser-name` = b.`browser-name`) 
                                group by 1")
skyid_imps_aggr_step6_df <- fillna(skyid_imps_aggr_step6_df, 0)
createOrReplaceTempView(skyid_imps_aggr_step6_df, "skyid_imps_aggr_step6")
# View(head(skyid_imps_aggr_step6_df,100))
# nrow(skyid_imps_aggr_step6_df)
# 


skyid_imps_aggr_step7_df <- sql("select skyid, sum(imps_tot) as imps_tot,sum(imps_mobile) as imps_mobile, sum(imps_tablet) as imps_tablet, 
                                        sum(imps_desktop) as imps_desktop 
                                from (select skyid, cookieid, `device-name`, `os-name`, `browser-name` from cookieid_skyid where length(skyid) = 48) as a 
                                left join (select cookieid, `device-name`, `os-name`, `browser-name`, imps_tot, imps_mobile, imps_tablet, imps_desktop 
                                          from skyid_imps_aggr_step3) as b 
                                on (a.cookieid = b.cookieid and a.`device-name` = b.`device-name` and a.`os-name` = b.`os-name` and 
                                    a.`browser-name` = b.`browser-name`) 
                                group by 1")
skyid_imps_aggr_step7_df <- fillna(skyid_imps_aggr_step7_df, 0)
createOrReplaceTempView(skyid_imps_aggr_step7_df, "skyid_imps_aggr_step7")
# View(head(skyid_imps_aggr_step7_df,100))
# nrow(skyid_imps_aggr_step7_df)
# 


skyid_imps_prep_df$Sunday <- ifelse(skyid_imps_prep_df$weekday == 'Sunday', 1, 0)
skyid_imps_prep_df$Monday <- ifelse(skyid_imps_prep_df$weekday == 'Monday', 1, 0)
skyid_imps_prep_df$Tuesday <- ifelse(skyid_imps_prep_df$weekday == 'Tuesday', 1, 0)
skyid_imps_prep_df$Wednesday <- ifelse(skyid_imps_prep_df$weekday == 'Wednesday', 1, 0)
skyid_imps_prep_df$Thursday <- ifelse(skyid_imps_prep_df$weekday == 'Thursday', 1, 0)
skyid_imps_prep_df$Friday <- ifelse(skyid_imps_prep_df$weekday == 'Friday', 1, 0)
skyid_imps_prep_df$Saturday <- ifelse(skyid_imps_prep_df$weekday == 'Saturday', 1, 0)

skyid_imps_prep_dow_df <- skyid_imps_prep_df
createOrReplaceTempView(skyid_imps_prep_dow_df, "skyid_imps_prep_dow")
# View(head(skyid_imps_prep_dow_df,100))


## weekday

skyid_imps_aggr_step8_df <- sql("select skyid, count(distinct case when Monday = 1 then yyyymmdd else null end) as num_monday,
                                        count(distinct case when Tuesday = 1 then yyyymmdd else null end) as num_tuesday, 
                                        count(distinct case when Wednesday = 1 then yyyymmdd else null end) as num_wednesday, 
                                        count(distinct case when Thursday = 1 then yyyymmdd else null end) as num_thursday, 
                                        count(distinct case when Friday = 1 then yyyymmdd else null end) as num_friday, 
                                        count(distinct case when Saturday = 1 then yyyymmdd else null end) as num_saturday, 
                                        count(distinct case when Sunday = 1 then yyyymmdd else null end) as num_sunday 
                                from (select skyid, cookieid, `device-name`, `os-name`, `browser-name` from cookieid_skyid where length(skyid) = 48) as a 
                                left join (select cookieid, `device-name`, `os-name`, `browser-name`, yyyymmdd, 
                                                  Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday from skyid_imps_prep_dow) as b 
                                on (a.cookieid = b.cookieid and a.`device-name` = b.`device-name` and a.`os-name` = b.`os-name` and 
                                    a.`browser-name` = b.`browser-name`) 
                                group by 1")
createOrReplaceTempView(skyid_imps_aggr_step8_df, "skyid_imps_aggr_step8")
# View(head(skyid_imps_aggr_step8_df,100))


## weekday imps

skyid_imps_aggr_step9_df <- sql("select skyid, sum(case when Monday = 1 then 1 else 0 end) as num_imps_monday, 
                                        sum(case when Tuesday = 1 then 1 else 0 end) as num_imps_tuesday, 
                                        sum(case when Wednesday = 1 then 1 else 0 end) as num_imps_wednesday, 
                                        sum(case when Thursday = 1 then 1 else 0 end) as num_imps_thursday, 
                                        sum(case when Friday = 1 then 1 else 0 end) as num_imps_friday, 
                                        sum(case when Saturday = 1 then 1 else 0 end) as num_imps_saturday, 
                                        sum(case when Sunday = 1 then 1 else 0 end) as num_imps_sunday 
                                from (select skyid, cookieid, `device-name`, `os-name`, `browser-name` from cookieid_skyid where length(skyid) = 48) as a 
                                left join (select cookieid, `device-name`, `os-name`, `browser-name`, yyyymmdd, 
                                                  Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday from skyid_imps_prep_dow) as b 
                                on (a.cookieid = b.cookieid and a.`device-name` = b.`device-name` and a.`os-name` = b.`os-name` and 
                                    a.`browser-name` = b.`browser-name`) 
                                group by 1")
createOrReplaceTempView(skyid_imps_aggr_step9_df, "skyid_imps_aggr_step9")
# View(head(skyid_imps_aggr_step9_df,100))


skyid_imps_prep_df$fascia_0_1 <- ifelse(skyid_imps_prep_df$hour == 0, 1, 0)
skyid_imps_prep_df$fascia_1_2 <- ifelse(skyid_imps_prep_df$hour == 1, 1, 0)
skyid_imps_prep_df$fascia_2_3 <- ifelse(skyid_imps_prep_df$hour == 2, 1, 0)
skyid_imps_prep_df$fascia_3_4 <- ifelse(skyid_imps_prep_df$hour == 3, 1, 0)
skyid_imps_prep_df$fascia_4_5 <- ifelse(skyid_imps_prep_df$hour == 4, 1, 0)
skyid_imps_prep_df$fascia_5_6 <- ifelse(skyid_imps_prep_df$hour == 5, 1, 0)
skyid_imps_prep_df$fascia_6_7 <- ifelse(skyid_imps_prep_df$hour == 6, 1, 0)
skyid_imps_prep_df$fascia_7_8 <- ifelse(skyid_imps_prep_df$hour == 7, 1, 0)
skyid_imps_prep_df$fascia_8_9 <- ifelse(skyid_imps_prep_df$hour == 8, 1, 0)
skyid_imps_prep_df$fascia_9_10 <- ifelse(skyid_imps_prep_df$hour == 9, 1, 0)
skyid_imps_prep_df$fascia_10_11 <- ifelse(skyid_imps_prep_df$hour == 10, 1, 0)
skyid_imps_prep_df$fascia_11_12 <- ifelse(skyid_imps_prep_df$hour == 11, 1, 0)
skyid_imps_prep_df$fascia_12_13 <- ifelse(skyid_imps_prep_df$hour == 12, 1, 0)
skyid_imps_prep_df$fascia_13_14 <- ifelse(skyid_imps_prep_df$hour == 13, 1, 0)
skyid_imps_prep_df$fascia_14_15 <- ifelse(skyid_imps_prep_df$hour == 14, 1, 0)
skyid_imps_prep_df$fascia_15_16 <- ifelse(skyid_imps_prep_df$hour == 15, 1, 0)
skyid_imps_prep_df$fascia_16_17 <- ifelse(skyid_imps_prep_df$hour == 16, 1, 0)
skyid_imps_prep_df$fascia_17_18 <- ifelse(skyid_imps_prep_df$hour == 17, 1, 0)
skyid_imps_prep_df$fascia_18_19 <- ifelse(skyid_imps_prep_df$hour == 18, 1, 0)
skyid_imps_prep_df$fascia_19_20 <- ifelse(skyid_imps_prep_df$hour == 19, 1, 0)
skyid_imps_prep_df$fascia_20_21 <- ifelse(skyid_imps_prep_df$hour == 20, 1, 0)
skyid_imps_prep_df$fascia_21_22 <- ifelse(skyid_imps_prep_df$hour == 21, 1, 0)
skyid_imps_prep_df$fascia_22_23 <- ifelse(skyid_imps_prep_df$hour == 22, 1, 0)
skyid_imps_prep_df$fascia_23_0 <- ifelse(skyid_imps_prep_df$hour == 23, 1, 0)

skyid_imps_prep_hour_df <- skyid_imps_prep_df
createOrReplaceTempView(skyid_imps_prep_hour_df, "skyid_imps_prep_hour")
# View(head(skyid_imps_prep_hour_df,100))


## hour of day imps

skyid_imps_aggr_step10_df <- sql("select skyid, sum(case when fascia_0_1 = 1 then 1 else 0 end) as num_imps_0_1, 
                                          sum(case when fascia_1_2 = 1 then 1 else 0 end) as num_imps_1_2, 
                                          sum(case when fascia_2_3 = 1 then 1 else 0 end) as num_imps_2_3, 
                                          sum(case when fascia_3_4 = 1 then 1 else 0 end) as num_imps_3_4, 
                                          sum(case when fascia_4_5 = 1 then 1 else 0 end) as num_imps_4_5, 
                                          sum(case when fascia_5_6 = 1 then 1 else 0 end) as num_imps_5_6, 
                                          sum(case when fascia_6_7 = 1 then 1 else 0 end) as num_imps_6_7, 
                                          sum(case when fascia_7_8 = 1 then 1 else 0 end) as num_imps_7_8, 
                                          sum(case when fascia_8_9 = 1 then 1 else 0 end) as num_imps_8_9, 
                                          sum(case when fascia_9_10 = 1 then 1 else 0 end) as num_imps_9_10, 
                                          sum(case when fascia_10_11 = 1 then 1 else 0 end) as num_imps_10_11, 
                                          sum(case when fascia_11_12 = 1 then 1 else 0 end) as num_imps_11_12, 
                                          sum(case when fascia_12_13 = 1 then 1 else 0 end) as num_imps_12_13, 
                                          sum(case when fascia_13_14 = 1 then 1 else 0 end) as num_imps_13_14, 
                                          sum(case when fascia_14_15 = 1 then 1 else 0 end) as num_imps_14_15, 
                                          sum(case when fascia_15_16 = 1 then 1 else 0 end) as num_imps_15_16, 
                                          sum(case when fascia_16_17 = 1 then 1 else 0 end) as num_imps_16_17, 
                                          sum(case when fascia_17_18 = 1 then 1 else 0 end) as num_imps_17_18, 
                                          sum(case when fascia_18_19 = 1 then 1 else 0 end) as num_imps_18_19, 
                                          sum(case when fascia_19_20 = 1 then 1 else 0 end) as num_imps_19_20, 
                                          sum(case when fascia_20_21 = 1 then 1 else 0 end) as num_imps_20_21, 
                                          sum(case when fascia_21_22 = 1 then 1 else 0 end) as num_imps_21_22, 
                                          sum(case when fascia_22_23 = 1 then 1 else 0 end) as num_imps_22_23, 
                                          sum(case when fascia_23_0 = 1 then 1 else 0 end) as num_imps_23_0 
                                 from (select skyid, cookieid, `device-name`, `os-name`, `browser-name` from cookieid_skyid where length(skyid) = 48) as a 
                                 left join (select cookieid, `device-name`, `os-name`, `browser-name`, yyyymmdd, 
                                                  fascia_0_1, fascia_1_2, fascia_2_3, fascia_3_4, fascia_4_5, fascia_5_6, fascia_6_7, fascia_7_8, 
                                                  fascia_8_9, fascia_9_10, fascia_10_11, fascia_11_12, fascia_12_13, fascia_13_14, fascia_14_15, 
                                                  fascia_15_16, fascia_16_17, fascia_17_18, fascia_18_19, fascia_19_20, fascia_20_21, fascia_21_22, 
                                                  fascia_22_23, fascia_23_0 
                                            from  skyid_imps_prep_hour) as b 
                                 on (a.cookieid = b.cookieid and a.`device-name` = b.`device-name` and a.`os-name` = b.`os-name` and 
                                    a.`browser-name` = b.`browser-name`) 
                                 group by 1")
createOrReplaceTempView(skyid_imps_aggr_step10_df, "skyid_imps_aggr_step10")
# View(head(skyid_imps_aggr_step10_df,100))


write.parquet(skyid_imps_aggr_step6_df, "/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step6.parquet")

skyid_imps_aggr1_df <- read.parquet("/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step6.parquet")
createOrReplaceTempView(skyid_imps_aggr1_df, "skyid_imps_aggr1")


write.parquet(skyid_imps_aggr_step7_df, "/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step7.parquet")

skyid_imps_aggr2_df <- read.parquet("/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step7.parquet")
createOrReplaceTempView(skyid_imps_aggr2_df, "skyid_imps_aggr2")


write.parquet(skyid_imps_aggr_step4a_df, "/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step4a.parquet")

skyid_imps_aggr3_df <- read.parquet("/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step4a.parquet")
createOrReplaceTempView(skyid_imps_aggr3_df, "skyid_imps_aggr3")


write.parquet(skyid_imps_aggr_step4b_df, "/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step4b.parquet")

skyid_imps_aggr4_df <- read.parquet("/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step4b.parquet")
createOrReplaceTempView(skyid_imps_aggr4_df, "skyid_imps_aggr4")


write.parquet(skyid_imps_aggr_step4c_df, "/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step4c.parquet")

skyid_imps_aggr5_df <- read.parquet("/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step4c.parquet")
createOrReplaceTempView(skyid_imps_aggr5_df, "skyid_imps_aggr5")


write.parquet(skyid_imps_aggr_step8_df, "/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step8.parquet")

skyid_imps_aggr6_df <- read.parquet("/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step8.parquet")
createOrReplaceTempView(skyid_imps_aggr6_df, "skyid_imps_aggr6")


write.parquet(skyid_imps_aggr_step9_df, "/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step9.parquet")

skyid_imps_aggr7_df <- read.parquet("/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step9.parquet")
createOrReplaceTempView(skyid_imps_aggr7_df, "skyid_imps_aggr7")


write.parquet(skyid_imps_aggr_step10_df, "/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step10.parquet")

skyid_imps_aggr8_df <- read.parquet("/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_step10.parquet")
createOrReplaceTempView(skyid_imps_aggr8_df, "skyid_imps_aggr8")


skyid_imps_aggr_finale_df <- sql("select a.skyid, num_mesi , num_week, num_giorni, imps_tot, imps_mobile, imps_tablet, imps_desktop, 
                                        num_device, num_mobile, num_tablet, num_desktop, num_monday, num_tuesday, num_wednesday, num_thursday, num_friday, 
                                        num_saturday, num_sunday, num_imps_monday, num_imps_tuesday, num_imps_wednesday, num_imps_thursday, num_imps_friday, 
                                        num_imps_saturday, num_imps_sunday, num_imps_0_1, num_imps_1_2, num_imps_2_3, num_imps_3_4, num_imps_4_5, num_imps_5_6, 
                                        num_imps_6_7, num_imps_7_8, num_imps_8_9, num_imps_9_10, num_imps_10_11, num_imps_11_12, num_imps_12_13, num_imps_13_14, 
                                        num_imps_14_15, num_imps_15_16, num_imps_16_17, num_imps_17_18, num_imps_18_19, num_imps_19_20, num_imps_20_21, 
                                        num_imps_21_22, num_imps_22_23, num_imps_23_0 
                                 from (select a.skyid, num_mesi , num_week, num_giorni from skyid_imps_aggr3 as a 
                                      join skyid_imps_aggr4 as b 
                                      on a.skyid = b.skyid 
                                      join skyid_imps_aggr5 as c 
                                      on a.skyid=c.skyid) as a 
                                 inner join (select skyid, num_device, num_mobile, num_tablet, num_desktop from skyid_imps_aggr1) as b 
                                 on a.skyid = b.skyid 
                                 inner join (select skyid, imps_tot, imps_mobile, imps_tablet, imps_desktop from skyid_imps_aggr2) as c 
                                 on a.skyid = c.skyid 
                                 inner join (select a.*, num_monday, num_tuesday, num_wednesday, num_thursday, num_friday, num_saturday, num_sunday, 
                                                    num_imps_monday, num_imps_tuesday, num_imps_wednesday, num_imps_thursday, num_imps_friday, 
                                                    num_imps_saturday, num_imps_sunday 
                                            from skyid_imps_aggr8 as a 
                                            join skyid_imps_aggr7 as b 
                                            on a.skyid = b.skyid 
                                            join skyid_imps_aggr6 as c 
                                            on a.skyid = c.skyid) as d 
                                 on a.skyid = d.skyid")
# View(head(skyid_imps_aggr_finale_df,100))
# nrow(skyid_imps_aggr_finale_df)
# # 
createOrReplaceTempView(skyid_imps_aggr_finale_df, "skyid_imps_aggr_finale")


skyid_imps_aggr2_finale_df <- sql("select *, num_mobile/num_device as perc_mobile, num_tablet/num_device as perc_tablet, num_desktop/num_device as perc_desktop, 
                                          imps_mobile/imps_tot as perc_imps_mobile, imps_tablet/imps_tot as perc_imps_tablet, 
                                          imps_desktop/imps_tot as perc_imps_desktop, imps_tot/num_giorni as num_imps_per_giorno, 
                                          imps_tot/num_mesi as num_imps_per_mese, imps_tot/num_week as num_imps_per_week, num_monday/num_giorni as perc_monday, 
                                          num_tuesday/num_giorni as perc_tuesday, num_wednesday/num_giorni as perc_wednesday, 
                                          num_thursday/num_giorni as perc_thursday, num_friday/num_giorni as perc_friday, 
                                          num_saturday/num_giorni as perc_saturday, num_sunday/num_giorni as perc_sunday, 
                                          num_imps_monday/imps_tot as perc_imps_monday, num_imps_tuesday/imps_tot as perc_imps_tuesday, 
                                          num_imps_wednesday/imps_tot as perc_imps_wednesday, num_imps_thursday/imps_tot as perc_imps_thursday, 
                                          num_imps_friday/imps_tot as perc_imps_friday, num_imps_saturday/imps_tot as perc_imps_saturday, 
                                          num_imps_sunday/imps_tot as perc_imps_sunday, num_imps_0_1/imps_tot as perc_imps_0_1, 
                                          num_imps_1_2/imps_tot as perc_imps_1_2, num_imps_2_3/imps_tot as perc_imps_2_3, num_imps_3_4/imps_tot as perc_imps_3_4, 
                                          num_imps_4_5/imps_tot as perc_imps_4_5, num_imps_5_6/imps_tot as perc_imps_5_6, num_imps_6_7/imps_tot as perc_imps_6_7, 
                                          num_imps_7_8/imps_tot as perc_imps_7_8, num_imps_8_9/imps_tot as perc_imps_8_9, num_imps_9_10/imps_tot as perc_imps_9_10, 
                                          num_imps_10_11/imps_tot as perc_imps_10_11, num_imps_11_12/imps_tot as perc_imps_11_12, 
                                          num_imps_12_13/imps_tot as perc_imps_12_13, num_imps_13_14/imps_tot as perc_imps_13_14, 
                                          num_imps_14_15/imps_tot as perc_imps_14_15, num_imps_15_16/imps_tot as perc_imps_15_16, 
                                          num_imps_16_17/imps_tot as perc_imps_16_17, num_imps_17_18/imps_tot as perc_imps_17_18, 
                                          num_imps_18_19/imps_tot as perc_imps_18_19, num_imps_19_20/imps_tot as perc_imps_19_20, 
                                          num_imps_20_21/imps_tot as perc_imps_20_21, num_imps_21_22/imps_tot as perc_imps_21_22, 
                                          num_imps_22_23/imps_tot as perc_imps_22_23, num_imps_23_0/imps_tot as perc_imps_23_0 
                                  from skyid_imps_aggr_finale")
# View(head(skyid_imps_aggr2_finale_df,100))
# nrow(skyid_imps_aggr2_finale_df)
# # 


write.parquet(skyid_imps_aggr2_finale_df, "/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_finale.parquet", mode = "overwrite")

skyid_imps_aggr2_finale_df <- read.parquet("/user/stefano.mazzucca/digitalizzazione/201809_sm_skyid_imps_aggr_finale.parquet")
View(head(skyid_imps_aggr2_finale_df,100))
nrow(skyid_imps_aggr2_finale_df)
# 3.785.570


createOrReplaceTempView(skyid_imps_aggr2_finale_df, "skyid_imps_aggr2_finale")
count_skyid <- sql("select count(distinct skyid) 
                   from skyid_imps_aggr2_finale")
View(head(count_skyid,100))
# 3.785.570




#chiudi sessione
sparkR.stop()
