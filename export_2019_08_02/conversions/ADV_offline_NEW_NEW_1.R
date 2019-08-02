
## Impatto ADV su vendite offline NEW 1



#apri sessione
source("connection_R.R")
options(scipen = 1000)


# File per selezionare le sole campagne che ci interessano
# /user/silvia/campaign_attribute.csv

# File per scartare i codici delle promo "Special"
# /user/silvia/metadata_promo.csv

# Report suite IMPRESSION
# /STAGE/adform/table=Impression/Impression.parquet

# Report suite CLICK
# /STAGE/adform/table=Click/Click.parquet

# report suite TRACKING POINT
# /STAGE/adform/table=Trackingpoint/Trackingpoint.parquet

# File estrazioni delle nuove conversioni
# /user/stefano.mazzucca/20180614_Richiesta_estrazione_2.csv







## Carcio il file delle conversioni (su tutti i canali)  ####################################################################################################

conversions_all <- read.df("/user/stefano.mazzucca/20180614_Richiesta_estrazione_2.csv", source = "csv", header = "true", delimiter = ";")
View(head(conversions_all,100))
nrow(conversions_all)
# 246.886 

conversions_all_1 <- withColumn(conversions_all, "data_stipula_dt", cast(cast(unix_timestamp(conversions_all$DATA_STIPULA, 'dd/MM/yyyy'),'timestamp'), 'date'))
conversions_all_2 <- filter(conversions_all_1, "data_stipula_dt >= '2017-11-27' and data_stipula_dt <= '2018-03-04'")
conversions_all_3 <- filter(conversions_all_2, "DES_CLS_UTENZA_SALES = 'RESIDENTIAL'")
conversions_all_4 <- filter(conversions_all_3, "NUM_COD_VIS <> '5462'") # No "subentri"!

View(head(conversions_all_4,100))
nrow(conversions_all_4)
# 154.272





## Carico il metadata delle promo per filtrare le conversioni ##############################################################################################
metadata_promo <- read.df("/user/silvia/metadata_promo.csv", source = "csv", header = "true", delimiter = ";")
View(head(metadata_promo,100))
nrow(metadata_promo)
# 1.348

metadata_promo_1 <- filter(metadata_promo, "`Macro Type L1` NOT LIKE '%Special%'")
metadata_promo_2 <- filter(metadata_promo_1, "Cod <> '5462'")
nrow(metadata_promo_2)
# 1.151



## Filtro le conversioni non special, residential, no "subentri", tra Dicembre 2017 e Febbraio 2018 inclusi

createOrReplaceTempView(conversions_all_4, "conversions_all_4")
createOrReplaceTempView(metadata_promo_2, "metadata_promo_2")

conversions_all_filter <- sql("select t1.*, case when DES_CATEGORIA_CANALE = 'WEBSELLING' then 1 else 0 end as flg_digital 
                              from conversions_all_4 t1
                              inner join metadata_promo_2 t2
                                on t1.NUM_COD_VIS = t2.cod ")
View(head(conversions_all_filter,100))
nrow(conversions_all_filter)
# 128.829


createOrReplaceTempView(conversions_all_filter, "conversions_all_filter")

conversions_all_filter_2 <- sql("select *, 
                                      case when data_stipula_dt >= '2017-11-27' and data_stipula_dt <= '2017-12-03' then 'W22'
                                      when data_stipula_dt >= '2017-12-04' and data_stipula_dt <= '2017-12-10' then 'W23'
                                      when data_stipula_dt >= '2017-12-11' and data_stipula_dt <= '2017-12-17' then 'W24'
                                      when data_stipula_dt >= '2017-12-18' and data_stipula_dt <= '2017-12-24' then 'W25'
                                      when data_stipula_dt >= '2017-12-25' and data_stipula_dt <= '2017-12-31' then 'W26'
                                      when data_stipula_dt >= '2018-01-01' and data_stipula_dt <= '2018-01-07' then 'W27'
                                      when data_stipula_dt >= '2018-01-08' and data_stipula_dt <= '2018-01-14' then 'W28'
                                      when data_stipula_dt >= '2018-01-15' and data_stipula_dt <= '2018-01-21' then 'W29'
                                      when data_stipula_dt >= '2018-01-22' and data_stipula_dt <= '2018-01-28' then 'W30'
                                      when data_stipula_dt >= '2018-01-29' and data_stipula_dt <= '2018-02-04' then 'W31'
                                      when data_stipula_dt >= '2018-02-05' and data_stipula_dt <= '2018-02-11' then 'W32'
                                      when data_stipula_dt >= '2018-02-12' and data_stipula_dt <= '2018-02-18' then 'W33'
                                      when data_stipula_dt >= '2018-02-19' and data_stipula_dt <= '2018-02-25' then 'W34'
                                      when data_stipula_dt >= '2018-02-26' and data_stipula_dt <= '2018-03-04' then 'W35'
                                      else NULL end as num_week
                                from conversions_all_filter")
View(head(conversions_all_filter_2,100))
nrow(conversions_all_filter_2)
# 128.829



write.parquet(conversions_all_filter_2,"/user/stefano.mazzucca/conversion_all_filtered.parquet")



conversions_all_filter <- read.parquet("/user/stefano.mazzucca/conversion_all_filtered.parquet")
View(head(conversions_all_filter,100))
nrow(conversions_all_filter)
# 128.829


valdist_skyid_dat <- distinct(select(conversions_all_filter, "COD_CLIENTE_CIFRATO", "data_stipula_dt"))
nrow(valdist_skyid_dat)
# 127.515
valdist_skyid <- distinct(select(conversions_all_filter, "COD_CLIENTE_CIFRATO"))
nrow(valdist_skyid)
# 121.902
valdist_skyid_dat_stip_attiv <- distinct(select(conversions_all_filter, "COD_CLIENTE_CIFRATO", "data_stipula_dt", "DAT_PRIMA_ATTIVAZIONE"))
nrow(valdist_skyid_dat_stip_attiv)
# 128.322


## Leggo tracking point per costruire dizionario cookieid-skyid  ##########################################################################################

trackingpoint <- read.parquet("/STAGE/adform/table=Trackingpoint/Trackingpoint.parquet")
View(head(trackingpoint, 100))

trackingpoint_1 <- filter(trackingpoint, "IsRobot = 'No'")
trackingpoint_2 <- filter(trackingpoint_1, "IsNoRepeats = 'Yes'")
trackingpoint_3 <- filter(trackingpoint_2, "CookieID <> 0 and CookieID is NOT NULL")
trackingpoint_4 <- withColumn(trackingpoint_3, "data", cast(trackingpoint_3$yyyymmdd, 'string'))
trackingpoint_5 <- withColumn(trackingpoint_4, "data_dt", cast(cast(unix_timestamp(trackingpoint_4$data, 'yyyyMMdd'), 'timestamp'), 'date'))

createOrReplaceTempView(trackingpoint_5, "trackingpoint_5")

cookieid_skyid_adform <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                                      CookieID, `device-name`,`os-name`,`browser-name`
                             from trackingpoint_5
                             where customvars.systemvariables like '%13%' and CookieID <> '0' 
                             having (skyid <> '' and length(skyid)= 48)")

createOrReplaceTempView(cookieid_skyid_adform, "cookieid_skyid_adform")
count_ <- sql("select count(distinct skyid) 
              from cookieid_skyid_adform")
View(head(count_,100))
# -- (2.768.952)
# and data_dt >= '2017-11-27'


write.parquet(cookieid_skyid_adform, "/user/stefano.mazzucca/p2c_nd_coo_skyid.parquet")


cookieid_skyid_adform <- read.parquet("/user/stefano.mazzucca/p2c_nd_coo_skyid.parquet")
View(head(cookieid_skyid_adform,100))
nrow(cookieid_skyid_adform)
# 29.518.853



## Aggancio skyid al cookie su tutti i convertenti #########################################################################################################

createOrReplaceTempView(conversions_all_filter, "conversions_all_filter")
createOrReplaceTempView(cookieid_skyid_adform, "cookieid_skyid_adform")

conversions_all_coo <- sql("select distinct t1.*, t2.cookieid , t2.`device-name` , t2.`os-name` , t2.`browser-name`
                           from conversions_all_filter t1
                           left join cookieid_skyid_adform t2
                            on t1.COD_CLIENTE_CIFRATO = t2.skyid")



write.parquet(conversions_all_coo, "/user/stefano.mazzucca/p2c_nd_conv_all_coo.parquet")




conversions_all_coo <- read.parquet("/user/stefano.mazzucca/p2c_nd_conv_all_coo.parquet")
View(head(conversions_all_coo,100))
nrow(conversions_all_coo)
# 688.236

# prova <- filter(conversions_all_coo, "cookieid is NULL and flg_digital = 1")
# View(head(prova,100))
# nrow(prova)
# # 6.748
# prova_valdist <- distinct(select(prova, "COD_CLIENTE_CIFRATO"))
# nrow(prova_valdist)
# # 6.226



## Carico campagne da considerare per impression e click ##################################################################################################

campaign_attribute <- read.df("/user/silvia/campaign_attribute.csv", source = "csv", header = "true", delimiter = ";")
View(head(campaign_attribute,100))
nrow(campaign_attribute)
# 2.534

createOrReplaceTempView(campaign_attribute, "campaign_attribute")
campaign_attribute_unique <- sql("select `Campaign ID`, `Line Item ID`, Label, Search
                                 from campaign_attribute
                                 group by `Campaign ID`, `Line Item ID`, Label, Search")
View(head(campaign_attribute_unique,100))
nrow(campaign_attribute_unique)
# 2.276





## Carico le impression ####################################################################################################################################

impression <- read.parquet('/STAGE/adform/table=Impression/Impression.parquet')
View(head(impression,100))
# nrow(impression)
# # 9.310.724.907

impression_1 <- filter(impression, "IsRobot = 'No'")
# nrow(impression_1)
# # 9.269.091.226
impression_2 <- filter(impression_1, "CookieID <> 0 and CookieID is NOT NULL")
impression_3 <- withColumn(impression_2, "data_nav", cast(impression_2$yyyymmdd, 'string'))
impression_4 <- withColumn(impression_3, "data_nav_dt", cast(cast(unix_timestamp(impression_3$data_nav, 'yyyyMMdd'), 'timestamp'), 'date'))
# View(head(impression_4,100))
impression_5 <- filter(impression_4, "data_nav_dt <= '2018-03-04' and data_nav_dt >= '2017-04-01'")
# View(head(impression_5,100))
nrow(impression_5)
# -- (6.735.952.207)

createOrReplaceTempView(impression_5, "impression_5")
impression_6 <- sql("select *, 
                    regexp_extract(unloadvars.visibility, '\"visible1\":(\\\\w+)',1) as visibility
                    from impression_5
                    having visibility LIKE 'true'")


#write.parquet(impression_6, "/user/stefano.mazzucca/p2c_nd_impression.parquet")


createOrReplaceTempView(impression_6, "impression_6")
createOrReplaceTempView(campaign_attribute_unique, "campaign_attribute_unique")

impression_filter <- sql("select t1.CookieID, t1.data_nav_dt, t1.`device-name`, t1.`os-name`, t1.`browser-name`, t1.CampaignId, t1.`PlacementId-ActivityId`,
                                 t1.visibility, 
                                 t2.Label, t2.Search
                         from impression_6 t1
                         inner join campaign_attribute_unique t2
                          on t1.CampaignId = t2.`Campaign ID` and t1.`PlacementId-ActivityId` = t2.`Line Item ID`")


# write.parquet(impression_filter, "/user/stefano.mazzucca/p2c_nd_impression_filter_coo.parquet")
# 
# 
# impression_filter <- read.parquet("/user/stefano.mazzucca/p2c_nd_impression_filter_coo.parquet")
# View(head(impression_filter,100))
# nrow(impression_filter)
# # --

createOrReplaceTempView(conversions_all_coo, "conversions_all_coo")
createOrReplaceTempView(impression_filter, "impression_filter")

imp_skyid_coo <- sql("select t1.*, t2.data_nav_dt, t2.CampaignId, t2.`PlacementId-ActivityId`, t2.Label, t2.Search
                     from conversions_all_coo t1
                     left join impression_filter t2
                      on t1.cookieid = t2.cookieid and 
                         t1.`device-name` = t2.`device-name` and 
                         t1.`os-name` = t2.`os-name` and
                         t1.`browser-name` = t2.`browser-name` and 
                         t1.data_stipula_dt >= t2.data_nav_dt ")


write.parquet(imp_skyid_coo, "/user/stefano.mazzucca/p2c_nd_impression_skyid_coo.parquet")


imp_skyid_coo <- read.parquet("/user/stefano.mazzucca/p2c_nd_impression_skyid_coo.parquet")
View(head(imp_skyid_coo,100))
nrow(imp_skyid_coo)
# 10.628.067 (21.783.441)


# test1 <- filter(imp_skyid_coo, "COD_CLIENTE_CIFRATO = '0220ed8fc1ba7cdc15d5ca662d6f8531ef92472bcd4bcb20'")
# View(head(test1,100))


createOrReplaceTempView(imp_skyid_coo, "imp_skyid_coo")

imp_skyid_coo_day <- sql("select COD_CLIENTE_CIFRATO, data_stipula_dt, data_nav_dt, DAT_PRIMA_ATTIVAZIONE,
                                 count(data_nav_dt) as num_imp_day, 
                                 sum(case when search='Si' then 1 else 0 end) num_imps_search, 
                                 sum(case when search='NO' then 1 else 0 end) num_imps_search_no, 
                                 sum(case when label='Branding' then 1 else 0 end) num_imps_branding , 
                                 sum(case when label='Trading' then 1 else 0 end) num_imps_trading , 
                                 sum(case when label='Other' then 1 else 0 end) num_imps_other, 
                                 sum(case when label='Content & Engagement' then 1 else 0 end) num_imps_content_eng
                         from imp_skyid_coo
                         group by COD_CLIENTE_CIFRATO, data_stipula_dt, data_nav_dt, DAT_PRIMA_ATTIVAZIONE")


write.parquet(imp_skyid_coo_day, "/user/stefano.mazzucca/p2c_nd_impression_skyid_day.parquet")


imp_skyid_coo_day <- read.parquet("/user/stefano.mazzucca/p2c_nd_impression_skyid_day.parquet")
View(head(imp_skyid_coo_day,100))
nrow(imp_skyid_coo_day)
# 1.498.809


createOrReplaceTempView(imp_skyid_coo_day, "imp_skyid_coo_day")

imp_skyid_coo_tot <- sql("select COD_CLIENTE_CIFRATO, data_stipula_dt, DAT_PRIMA_ATTIVAZIONE,
                                 min(data_nav_dt) as min_date_imp,
                                 max(data_nav_dt) as max_date_imp,
                                 sum(num_imp_day) as num_imp_tot,
                                 sum(num_imps_search) as num_imps_search_tot,
                                 sum(num_imps_search_no) as num_imps_search_no_tot,
                                 sum(num_imps_branding) as num_imps_branding_tot,
                                 sum(num_imps_trading) as num_imps_trading_tot,
                                 sum(num_imps_other) as num_imps_other,
                                 sum(num_imps_content_eng) as num_imps_content_eng_tot
                         from imp_skyid_coo_day
                         group by COD_CLIENTE_CIFRATO, data_stipula_dt, DAT_PRIMA_ATTIVAZIONE")


write.parquet(imp_skyid_coo_tot, "/user/stefano.mazzucca/p2c_nd_impression_skyid_tot.parquet")


imp_skyid_coo_tot <- read.parquet("/user/stefano.mazzucca/p2c_nd_impression_skyid_tot.parquet")
View(head(imp_skyid_coo_tot,100))
nrow(imp_skyid_coo_tot)
# 128.322 (vs. 128.322 valdist_skyid_dat_stip_attiv)



# ## prova
# imp_skyid_with_imp <- filter(imp_skyid_coo_tot, "num_imp_tot <> 0")
# View(head(imp_skyid_with_imp,100))
# nrow(imp_skyid_with_imp)
# # 39.539



## Carico i click #########################################################################################################################################

click <- read.parquet('/STAGE/adform/table=Click/Click.parquet')
View(head(click,100))
# nrow(click)
# # --

click_1 <- filter(click, "IsRobot = 'No'")
# nrow(click_1)
# # --
click_2 <- filter(click_1, "CookieID <> 0 and CookieID is NOT NULL")
click_3 <- withColumn(click_2, "data_nav", cast(click_2$yyyymmdd, 'string'))
click_4 <- withColumn(click_3, "data_nav_dt", cast(cast(unix_timestamp(click_3$data_nav, 'yyyyMMdd'), 'timestamp'), 'date'))
# View(head(click_4,100))
click_5 <- filter(click_4, "data_nav_dt <= '2018-02-28' and data_nav_dt >= '2017-04-01'")
# nrow(click_5)
# # --

createOrReplaceTempView(click_5, "click_5")
click_6 <- sql("select *, 
               regexp_extract(unloadvars.visibility, '\"visible1\":(\\\\w+)',1) as visibility
               from click_5
               having (visibility <> 'false' or visibility is NOT NULL)")


#write.parquet(click_6, "/user/stefano.mazzucca/p2c_nd_click.parquet")


createOrReplaceTempView(click_6, "click_6")
createOrReplaceTempView(campaign_attribute_unique, "campaign_attribute_unique")

click_filter <- sql("select t1.CookieID, t1.data_nav_dt, t1.`device-name`, t1.`os-name`, t1.`browser-name`, t1.CampaignId, t1.`PlacementId-ActivityId`,
                            t1.visibility, 
                            t2.Label, t2.Search
                    from click_6 t1
                    inner join campaign_attribute_unique t2
                      on t1.CampaignId = t2.`Campaign ID` and t1.`PlacementId-ActivityId` = t2.`Line Item ID`")


# write.parquet(click_filter, "/user/stefano.mazzucca/p2c_nd_click_filter_coo.parquet")
# 
# 
# click_filter <- read.parquet("/user/stefano.mazzucca/p2c_nd_click_filter_coo.parquet")
# View(head(click_filter,100))
# nrow(click_filter)
# # --


createOrReplaceTempView(conversions_all_coo, "conversions_all_coo")
createOrReplaceTempView(click_filter, "click_filter")

clic_skyid_coo <- sql("select t1.*, t2.data_nav_dt, t2.CampaignId, t2.`PlacementId-ActivityId`, t2.Label, t2.Search
                      from conversions_all_coo t1
                      left join click_filter t2
                        on t1.cookieid = t2.cookieid and 
                           t1.`device-name` = t2.`device-name` and 
                           t1.`os-name` = t2.`os-name` and
                           t1.`browser-name` = t2.`browser-name` and 
                           t1.data_stipula_dt >= t2.data_nav_dt ")


write.parquet(clic_skyid_coo, "/user/stefano.mazzucca/p2c_nd_click_skyid_coo.parquet")


clic_skyid_coo <- read.parquet("/user/stefano.mazzucca/p2c_nd_click_skyid_coo.parquet")
View(head(clic_skyid_coo,100))
nrow(clic_skyid_coo)
# 744.987


createOrReplaceTempView(clic_skyid_coo, "clic_skyid_coo")

clic_skyid_coo_day <- sql("select COD_CLIENTE_CIFRATO, data_stipula_dt, data_nav_dt, DAT_PRIMA_ATTIVAZIONE,
                                  count(data_nav_dt) as num_clic_day, 
                                  sum(case when search='Si' then 1 else 0 end) num_clic_search, 
                                  sum(case when search='NO' then 1 else 0 end) num_clic_search_no, 
                                  sum(case when label='Branding' then 1 else 0 end) num_clic_branding , 
                                  sum(case when label='Trading' then 1 else 0 end) num_clic_trading , 
                                  sum(case when label='Other' then 1 else 0 end) num_clic_other, 
                                  sum(case when label='Content & Engagement' then 1 else 0 end) num_clic_content_eng
                          from clic_skyid_coo
                          group by COD_CLIENTE_CIFRATO, data_stipula_dt, data_nav_dt, DAT_PRIMA_ATTIVAZIONE")


write.parquet(clic_skyid_coo_day, "/user/stefano.mazzucca/p2c_nd_clic_skyid_day.parquet")


clic_skyid_coo_day <- read.parquet("/user/stefano.mazzucca/p2c_nd_clic_skyid_day.parquet")
View(head(clic_skyid_coo_day,100))
nrow(clic_skyid_coo_day)
# 180.698


createOrReplaceTempView(clic_skyid_coo_day, "clic_skyid_coo_day")

clic_skyid_coo_tot <- sql("select COD_CLIENTE_CIFRATO, data_stipula_dt, DAT_PRIMA_ATTIVAZIONE,
                                  min(data_nav_dt) as min_date_clic,
                                  max(data_nav_dt) as max_date_clic,
                                  sum(num_clic_day) as num_clic_tot,
                                  sum(num_clic_search) as num_clic_search_tot,
                                  sum(num_clic_search_no) as num_clic_search_no_tot,
                                  sum(num_clic_branding) as num_clic_branding_tot,
                                  sum(num_clic_trading) as num_clic_trading_tot,
                                  sum(num_clic_other) as num_clic_other_tot,
                                  sum(num_clic_content_eng) as num_clic_content_eng_tot
                          from clic_skyid_coo_day
                          group by COD_CLIENTE_CIFRATO, data_stipula_dt, DAT_PRIMA_ATTIVAZIONE")


write.parquet(clic_skyid_coo_tot, "/user/stefano.mazzucca/p2c_nd_clic_skyid_tot.parquet")


clic_skyid_coo_tot <- read.parquet("/user/stefano.mazzucca/p2c_nd_clic_skyid_tot.parquet")
View(head(clic_skyid_coo_tot,100))
nrow(clic_skyid_coo_tot)
# 128.322 (vs. 128.322 valdist_skyid_dat_stip_attiv)



#############################################################################################################################################################
#############################################################################################################################################################
## Aggrego i dati di impression e click su tutto il target
#############################################################################################################################################################
#############################################################################################################################################################


imp_skyid_coo_tot <- read.parquet("/user/stefano.mazzucca/p2c_nd_impression_skyid_tot.parquet")
View(head(imp_skyid_coo_tot,100))
nrow(imp_skyid_coo_tot)
# 128.322

clic_skyid_coo_tot <- read.parquet("/user/stefano.mazzucca/p2c_nd_clic_skyid_tot.parquet")
View(head(clic_skyid_coo_tot,100))
nrow(clic_skyid_coo_tot)
# 128.322

############################################################################################################


valdist_skyid_imp <- distinct(select(imp_skyid_coo_tot, "COD_CLIENTE_CIFRATO"))
nrow(valdist_skyid_imp)
# 121.902
valdist_skyid_clic <- distinct(select(clic_skyid_coo_tot, "COD_CLIENTE_CIFRATO"))
nrow(valdist_skyid_clic)
# 121.902

valdist_skyid_tot <- union(valdist_skyid_imp, valdist_skyid_clic)
valdist_skyid_tot_2 <- distinct(select(valdist_skyid_tot, "COD_CLIENTE_CIFRATO"))
nrow(valdist_skyid_tot_2)
# 121.902 (Ceck OK)



valdist_skyid_dat_imp <- distinct(select(imp_skyid_coo_tot, "COD_CLIENTE_CIFRATO", "data_stipula_dt"))
nrow(valdist_skyid_dat_imp)
# 127.515
valdist_skyid_dat_clic <- distinct(select(clic_skyid_coo_tot, "COD_CLIENTE_CIFRATO", "data_stipula_dt"))
nrow(valdist_skyid_dat_clic)
# 127.515

valdist_skyid_dat_tot <- union(valdist_skyid_dat_imp, valdist_skyid_dat_clic)
valdist_skyid_dat_tot_2 <- distinct(select(valdist_skyid_dat_tot, "COD_CLIENTE_CIFRATO", "data_stipula_dt"))
nrow(valdist_skyid_dat_tot_2)
# 127.515 (Ceck OK)

############################################################################################################


createOrReplaceTempView(imp_skyid_coo_tot, "imp_skyid_coo_tot")
createOrReplaceTempView(clic_skyid_coo_tot, "clic_skyid_coo_tot")

skyid_coo_tot <- sql("select t1.*,
                               t2.min_date_clic,
                               t2.max_date_clic,
                               t2.num_clic_tot,
                               t2.num_clic_search_tot,
                               t2.num_clic_search_no_tot,
                               t2.num_clic_branding_tot,
                               t2.num_clic_trading_tot,
                               t2.num_clic_other_tot,
                               t2.num_clic_content_eng_tot
                     from imp_skyid_coo_tot t1
                     full outer join clic_skyid_coo_tot t2
                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and 
                         t1.data_stipula_dt = t2.data_stipula_dt and 
                         t1.DAT_PRIMA_ATTIVAZIONE = t2.DAT_PRIMA_ATTIVAZIONE")
View(head(skyid_coo_tot,1000))
nrow(skyid_coo_tot)
# 128.322 (vs. 128.322 valdist_skyid_dat_stip_attiv) --> Ceck OK!
cache(skyid_coo_tot)


write.parquet(skyid_coo_tot, "/user/stefano.mazzucca/p2c_nd_skyid_coo_tot.parquet")
write.df(repartition( skyid_coo_tot, 1), path = "/user/stefano.mazzucca/p2c_nd_skyid_coo_tot.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



###########################################################################################################################################################
###########################################################################################################################################################

###########################################################################################################################################################
###########################################################################################################################################################


#skyid_coo_tot <- read.parquet("/user/stefano.mazzucca/p2c_nd_skyid_coo_tot.parquet")
skyid_coo_tot <- read.df("/user/stefano.mazzucca/p2c_nd_skyid_coo_tot.csv", source = "csv", header = "true", delimiter = ";")
View(head(skyid_coo_tot,100))
nrow(skyid_coo_tot)
# 128.322

skyid_coo_tot_2 <- withColumn(skyid_coo_tot, "data_stipula_dt", cast(skyid_coo_tot$data_stipula_dt, 'date'))
skyid_coo_tot_2 <- withColumn(skyid_coo_tot, "dat_prima_attivazione_dt", cast(cast(unix_timestamp(skyid_coo_tot$DAT_PRIMA_ATTIVAZIONE, 'dd/MM/yyyy'),'timestamp'), 'date'))

View(head(skyid_coo_tot_2,100))
nrow(skyid_coo_tot_2)
# 128.322
printSchema(skyid_coo_tot_2)



# skyid_coo_tot_navig <- filter(skyid_coo_tot_2, "min_date_imp is NOT NULL or min_date_clic is NOT NULL")
# View(head(skyid_coo_tot_navig,100))
# nrow(skyid_coo_tot_navig)
# # 39.548  ## Con il solo "min_date_imp is NOT NULL" -> 39.539



# skyid_coo_tot_2_bis <- fillna(skyid_coo_tot, "0") # se non prende la colonna, fare con l'intero data frame
# View(head(skyid_coo_tot_2_bis,100))
# printSchema(skyid_coo_tot_2_bis)



createOrReplaceTempView(skyid_coo_tot_2, "skyid_coo_tot_2")

skyid_coo_tot_3 <- sql("select *, 
                             case when min_date_imp <= min_date_clic then min_date_imp
                             else min_date_clic end as min_data_interactions_provv,
                             case when max_date_imp >= max_date_clic then max_date_imp
                             else max_date_clic end as max_data_interactions_provv,
                             (num_imp_tot + num_clic_tot) as num_tot_interactions,
                             (num_imps_search_tot + num_clic_search_tot) as num_tot_inter_search,
                             (num_imps_search_no_tot + num_clic_search_no_tot) as num_tot_inter_search_no,
                             (num_imps_branding_tot + num_clic_branding_tot) as num_tot_inter_branding,
                             (num_imps_trading_tot + num_clic_trading_tot) as num_tot_inter_trading,
                             (num_imps_other + num_clic_other_tot) as num_tot_inter_other,
                             (num_imps_content_eng_tot + num_clic_content_eng_tot) as num_tot_inter_content_eng
                       from skyid_coo_tot_2")
# View(head(skyid_coo_tot_3,100))

skyid_coo_tot_3$min_data_interactions_provv2 <- ifelse(isNull(skyid_coo_tot_3$min_data_interactions_provv) == TRUE & isNull(skyid_coo_tot_3$min_date_imp) == TRUE,
                                                        skyid_coo_tot_3$min_date_clic, skyid_coo_tot_3$min_data_interactions_provv)
skyid_coo_tot_3$min_data_interactions <- ifelse(isNull(skyid_coo_tot_3$min_data_interactions_provv2) == TRUE & isNull(skyid_coo_tot_3$min_date_clic) == TRUE,
                                                skyid_coo_tot_3$min_date_imp, skyid_coo_tot_3$min_data_interactions_provv2)

skyid_coo_tot_3$max_data_interactions_provv2 <- ifelse(isNull(skyid_coo_tot_3$max_data_interactions_provv) == TRUE & isNull(skyid_coo_tot_3$max_date_imp) == TRUE, 
                                                skyid_coo_tot_3$max_date_clic, skyid_coo_tot_3$max_data_interactions_provv)
skyid_coo_tot_3$max_data_interactions <- ifelse(isNull(skyid_coo_tot_3$max_data_interactions_provv2) == TRUE & isNull(skyid_coo_tot_3$max_date_clic) == TRUE, 
                                                skyid_coo_tot_3$max_date_imp, skyid_coo_tot_3$max_data_interactions_provv2)

skyid_coo_tot_3$min_data_interactions_provv <- NULL
skyid_coo_tot_3$min_data_interactions_provv2 <- NULL
skyid_coo_tot_3$max_data_interactions_provv <- NULL
skyid_coo_tot_3$max_data_interactions_provv2 <- NULL


View(head(skyid_coo_tot_3,100))
nrow(skyid_coo_tot_3)
# 128.322



write.parquet(skyid_coo_tot_3, "/user/stefano.mazzucca/p2c_nd_skyid_tot.parquet")
write.df(repartition( skyid_coo_tot_3, 1), path = "/user/stefano.mazzucca/p2c_nd_skyid_tot.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




skyid_tot <- read.parquet("/user/stefano.mazzucca/p2c_nd_skyid_tot.parquet")
View(head(skyid_tot,100))
nrow(skyid_tot)
# 128.322





#chiudi sessione
sparkR.stop()
