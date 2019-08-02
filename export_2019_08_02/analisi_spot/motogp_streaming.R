
## Streaming MOTO GP


#apri sessione
source("connection_R.R")
options(scipen = 1000)




adform_tp_1 <- read.parquet('hdfs:///STAGE/adform/table=Trackingpoint/Trackingpoint.parquet')
View(head(adform_tp_1,1000))


createOrReplaceTempView(adform_tp_1, "adform_tp_1")
adform_tp_2 <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                            CookieID
                    from adform_tp_1
                    having (skyid <> '' and length(skyid)= 48)")

write.parquet(adform_tp_2,"/user/stefano.mazzucca/motogp_streaming/diz_adform_20181119.parquet")

diz_id_coo_adform <- read.parquet("/user/stefano.mazzucca/motogp_streaming/diz_adform_20181119.parquet")
View(head(diz_id_coo_adform,1000))
nrow(diz_id_coo_adform)
# 72.526.312


adform_tp_3 <- withColumn(adform_tp_1, "data_ts", cast(unix_timestamp(adform_tp_1$Timestamp, 'yyyy-MM-dd HH:mm:ss'), 'timestamp'))
adform_tp_4 <- withColumn(adform_tp_3, "data_dt", cast(cast(unix_timestamp(adform_tp_3$data_ts, 'yyyy-MM-dd HH:mm:ss'), 'timestamp'), 'date'))
View(head(adform_tp_4,100))


## Valencia ########################################################################################################################################################

valencia <- filter(adform_tp_4, adform_tp_4$data_dt == '2018-11-18')
valencia_1 <- filter(valencia, valencia$data_ts >= '2018-11-18 14:00:00' & valencia$data_dt <= '2018-11-18 16:00:00')

createOrReplaceTempView(valencia_1, "valencia_1")

valencia_2 <- sql("select *,
                          case when PageURL LIKE '%sport.sky.it/motogp/2018/11/18/motogp-gp-valencia-2018-diretta.html' then 1 else 0 end as flg_url_diretta,
                          case when PageURL LIKE '%sport.sky.it/ledirettediskysport.html' then 1 else 0 end as flg_ledirettediskysport,
                          case when PageURL LIKE '%player.sky.it/player/external.html' then 1 else 0 end as flg_player
                  from valencia_1
                  where (PageURL LIKE '%sport.sky.it/motogp/2018/11/18/motogp-gp-valencia-2018-diretta.html' OR 
                        PageURL LIKE '%sport.sky.it/ledirettediskysport.html' OR 
                        PageURL LIKE '%player.sky.it/player/external.html' )")

write.parquet(valencia_2, "/user/stefano.mazzucca/motogp_streaming/valencia_20181118.parquet")


##########################################################
valencia <- read.parquet("/user/stefano.mazzucca/motogp_streaming/valencia_20181118.parquet")
View(head(valencia, 100))
nrow(valencia)
# 20.176


createOrReplaceTempView(valencia, "valencia")
valencia_coo <- sql("select CookieID, count(*) as count_hit, 
                            max(flg_url_diretta) as flg_url_diretta,
                            max(flg_ledirettediskysport) as flg_ledirettediskysport,
                            max(flg_player) as flg_player
                   from valencia
                   group by CookieID, flg_url_diretta, flg_ledirettediskysport, flg_player")
View(head(valencia_coo,100))
nrow(valencia_coo)
# 14.436

valdist_cookie <- distinct(select(valencia, "CookieID"))
View(head(valdist_cookie,100))
nrow(valdist_cookie)
# 13.855


createOrReplaceTempView(valdist_cookie, "valdist_cookie")
createOrReplaceTempView(diz_id_coo_adform, "diz_id_coo_adform")
skyid_coo_valencia <- sql("select distinct t2.*
                          from valdist_cookie t1
                          inner join diz_id_coo_adform t2
                          on t1.CookieID = t2.CookieID ")

write.parquet(skyid_coo_valencia, "/user/stefano.mazzucca/motogp_streaming/valencia_skyid_coo_20181118.parquet", mode = "overwrite")

skyid_coo_valencia <- read.parquet("/user/stefano.mazzucca/motogp_streaming/valencia_skyid_coo_20181118.parquet")
View(head(skyid_coo_valencia,100))
nrow(skyid_coo_valencia)
# 3.386.459

skyid_coo_valencia_1 <- filter(skyid_coo_valencia, "CookieID not like '0'")
View(head(skyid_coo_valencia_1,100))
nrow(skyid_coo_valencia_1)
# 34.931

createOrReplaceTempView(skyid_coo_valencia_1, "skyid_coo_valencia_1")
valdist_skyid <- sql("select skyid, count(*) as count
                     from skyid_coo_valencia_1
                     group by skyid")
View(head(valdist_skyid,100))
nrow(valdist_skyid)
# 33.532 skyid unici
# 3.353 cookie unici



# createOrReplaceTempView(valencia, "valencia")
# createOrReplaceTempView(diz_id_coo_adform, "diz_id_coo_adform")
# 
# valencia_coo <- merge(valencia, diz_id_coo_adform, by.x = "CookieID", by.y = "CookieID", all = TRUE)
# 
# # sql("select distinct t1.skyid, t2.*
# #                     from diz_id_coo_adform  t1
# #                     inner join valencia t2
# #                     on t1.CookieID = t2.CookieID")
# 
# write.parquet(valencia_coo, "/user/stefano.mazzucca/motogp_streaming/valencia_coo_20181118_bis.parquet", mode = "overwrite")


###################################################################################################################################################################

## Rep. Ceca

repceca <- filter(adform_tp_4, adform_tp_4$data_dt == '2018-08-05')
repceca_1 <- filter(repceca, repceca$data_ts >= '2018-08-05 14:00:00' & repceca$data_dt <= '2018-08-05 15:35:00')

createOrReplaceTempView(repceca_1, "repceca_1")
repceca_2 <- sql("select *,
                          case when PageURL LIKE '%sport.sky.it/motogp/2018/08/05/motogp-gp-brno-2018-diretta.html' then 1 else 0 end as flg_url_diretta,
                          case when PageURL LIKE '%sport.sky.it/ledirettediskysport.html' then 1 else 0 end as flg_ledirettediskysport,
                          case when PageURL LIKE '%player.sky.it/player/external.html' then 1 else 0 end as flg_player
                  from repceca_1
                  where (PageURL LIKE '%sport.sky.it/motogp/2018/08/05/motogp-gp-brno-2018-diretta.html' OR 
                        PageURL LIKE '%sport.sky.it/ledirettediskysport.html' OR 
                        PageURL LIKE '%player.sky.it/player/external.html' )")

write.parquet(repceca_2, "/user/stefano.mazzucca/motogp_streaming/repceca_20180805.parquet")


##########################################################
repceca <- read.parquet("/user/stefano.mazzucca/motogp_streaming/repceca_20180805.parquet")
View(head(repceca, 100))
nrow(repceca)
# 81.321

createOrReplaceTempView(repceca, "repceca")
repceca_coo <- sql("select CookieID, count(*) as count_hit, 
                            max(flg_url_diretta) as flg_url_diretta,
                            max(flg_ledirettediskysport) as flg_ledirettediskysport,
                            max(flg_player) as flg_player
                   from repceca
                   group by CookieID, flg_url_diretta, flg_ledirettediskysport, flg_player")
View(head(repceca_coo,100))
nrow(repceca_coo)
# 22.331

valdist_cookie <- distinct(select(repceca, "CookieID"))
nrow(valdist_cookie)
# 20.784


createOrReplaceTempView(valdist_cookie, "valdist_cookie")
createOrReplaceTempView(diz_id_coo_adform, "diz_id_coo_adform")
skyid_coo_repceca <- sql("select distinct t2.*
                          from valdist_cookie t1
                          inner join diz_id_coo_adform t2
                          on t1.CookieID = t2.CookieID ")

write.parquet(skyid_coo_repceca, "/user/stefano.mazzucca/motogp_streaming/repceca_skyid_coo_20181118.parquet", mode = "overwrite")

skyid_coo_repceca <- read.parquet("/user/stefano.mazzucca/motogp_streaming/repceca_skyid_coo_20181118.parquet")
View(head(skyid_coo_repceca,100))
nrow(skyid_coo_repceca)
# 3.431.394

skyid_coo_repceca_1 <- filter(skyid_coo_repceca, "CookieID not like '0'")
View(head(skyid_coo_repceca_1,100))
nrow(skyid_coo_repceca_1)
# 79.866

createOrReplaceTempView(skyid_coo_repceca_1, "skyid_coo_repceca_1")
valdist_skyid <- sql("select CookieID, count(*) as count
                     from skyid_coo_repceca_1
                     group by CookieID")
View(head(valdist_skyid,100))
nrow(valdist_skyid)
# 70.134 skyid unici
# 5.163 cookie unici



# createOrReplaceTempView(repceca, "repceca")
# createOrReplaceTempView(diz_id_coo_adform, "diz_id_coo_adform")
# 
# repceca_coo <- sql("select distinct t1.*, t2.skyid
#                     from repceca t1
#                     left join diz_id_coo_adform t2
#                     on t1.CookieID = t2.CookieID")
# 
# write.parquet(repceca_coo, "/user/stefano.mazzucca/motogp_streaming/repceca_coo_20180805.parquet")
# 
# repceca_coo <- read.parquet("/user/stefano.mazzucca/motogp_streaming/repceca_coo_20180805.parquet")
# View(head(repceca_coo,100))
# nrow(repceca_coo)
# # 


## Austria ########################################################################################################################################################

austria <- filter(adform_tp_4, adform_tp_4$data_dt == '2018-08-12')
austria_1 <- filter(austria, austria$data_ts >= '2018-08-12 14:00:00' & austria$data_dt <= '2018-08-12 15:30:00')

createOrReplaceTempView(austria_1, "austria_1")
austria_2 <- sql("select *,
                          case when PageURL LIKE '%sport.sky.it/motogp/2018/08/12/motogp-gp-austria-2018-diretta.html' then 1 else 0 end as flg_url_diretta,
                          case when PageURL LIKE '%sport.sky.it/ledirettediskysport.html' then 1 else 0 end as flg_ledirettediskysport,
                          case when PageURL LIKE '%player.sky.it/player/external.html' then 1 else 0 end as flg_player
                  from austria_1
                  where (PageURL LIKE '%sport.sky.it/motogp/2018/08/12/motogp-gp-austria-2018-diretta.html' OR 
                        PageURL LIKE '%sport.sky.it/ledirettediskysport.html' OR 
                        PageURL LIKE '%player.sky.it/player/external.html' )")

write.parquet(austria_2, "/user/stefano.mazzucca/motogp_streaming/austria_20180812.parquet")


##########################################################
austria <- read.parquet("/user/stefano.mazzucca/motogp_streaming/austria_20180812.parquet")
View(head(austria, 100))
nrow(austria)
# 45.249

createOrReplaceTempView(austria, "austria")
austria_coo <- sql("select CookieID, count(*) as count_hit, 
                            max(flg_url_diretta) as flg_url_diretta,
                            max(flg_ledirettediskysport) as flg_ledirettediskysport,
                            max(flg_player) as flg_player
                   from austria
                   group by CookieID, flg_url_diretta, flg_ledirettediskysport, flg_player")
View(head(austria_coo,100))
nrow(austria_coo)
# 21.497

valdist_cookie <- distinct(select(austria, "CookieID"))
nrow(valdist_cookie)
# 20.056


createOrReplaceTempView(valdist_cookie, "valdist_cookie")
createOrReplaceTempView(diz_id_coo_adform, "diz_id_coo_adform")
skyid_coo_austria <- sql("select distinct t2.*
                          from valdist_cookie t1
                          inner join diz_id_coo_adform t2
                          on t1.CookieID = t2.CookieID ")

write.parquet(skyid_coo_austria, "/user/stefano.mazzucca/motogp_streaming/austria_skyid_coo_20180812.parquet", mode = "overwrite")

skyid_coo_austria <- read.parquet("/user/stefano.mazzucca/motogp_streaming/austria_skyid_coo_20180812.parquet")
View(head(skyid_coo_austria,100))
nrow(skyid_coo_austria)
# 3.420.688

skyid_coo_austria_1 <- filter(skyid_coo_austria, "CookieID not like '0'")
View(head(skyid_coo_austria_1,100))
nrow(skyid_coo_austria_1)
# 69.160

createOrReplaceTempView(skyid_coo_austria_1, "skyid_coo_austria_1")
valdist_skyid <- sql("select CookieID, count(*) as count
                     from skyid_coo_austria_1
                     group by CookieID")
View(head(valdist_skyid,100))
nrow(valdist_skyid)
# 60.929 skyid unici
# 4.956 cookie unici









#chiudi sessione
sparkR.stop()
