
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







## Carcio il file delle conversioni (su tutti i canali)  ##############################################################################################################

conversions_all <- read.df("/user/stefano.mazzucca/20180614_Richiesta_estrazione_2.csv", source = "csv", header = "true", delimiter = ";")
View(head(conversions_all,100))
nrow(conversions_all)
# 246.886

conversions_all_1 <- withColumn(conversions_all, "data_stipula_dt", cast(cast(unix_timestamp(conversions_all$DATA_STIPULA, 'dd/MM/yyyy'),'timestamp'), 'date'))
conversions_all_2 <- filter(conversions_all_1, "data_stipula_dt >= '2017-12-01' and data_stipula_dt <= '2018-02-28'")
conversions_all_3 <- filter(conversions_all_2, "DES_CLS_UTENZA_SALES = 'RESIDENTIAL'")
conversions_all_4 <- filter(conversions_all_3, "NUM_COD_VIS <> '5462'") # No "subentri"!

View(head(conversions_all_4,100))
nrow(conversions_all_4)
# 141.242





## Carico il metadata delle promo per filtrare le conversioni #########################################################################################################

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
# 117.464





## Leggo tracking point per costruire dizionario cookieid-skyid  ######################################################################################################

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
                             where customvars.systemvariables like '%13%' and CookieID <> '0' and 
                                        data_dt >= '2017-12-01' and data_dt <= '2018-05-20'
                             having (skyid <> '' and length(skyid)= 48)")

createOrReplaceTempView(cookieid_skyid_adform, "cookieid_skyid_adform")
count_ <- sql("select count(distinct skyid) 
              from cookieid_skyid_adform")
View(head(count_,100))
# 2.768.952


write.parquet(cookieid_skyid_adform, "/user/stefano.mazzucca/p2c_nd_coo_skyid.parquet")





## Aggancio skyid al cookie su tutti i convertenti ####################################################################################################################


cookieid_skyid_adform <- read.parquet("/user/stefano.mazzucca/p2c_nd_coo_skyid.parquet")
View(head(cookieid_skyid_adform,100))
nrow(cookieid_skyid_adform)
# 25.508.421

createOrReplaceTempView(conversions_all_filter, "conversions_all_filter")
createOrReplaceTempView(cookieid_skyid_adform, "cookieid_skyid_adform")

conversions_all_coo <- sql("select t1.*, t2.cookieid , t2.`device-name` , t2.`os-name` , t2.`browser-name`
                           from conversions_all_filter t1
                           left join cookieid_skyid_adform t2
                            on t1.COD_CLIENTE_CIFRATO = t2.skyid")



write.parquet(conversions_all_coo, "/user/stefano.mazzucca/p2c_nd_conv_all_coo.parquet")




conversions_all_coo <- read.parquet("/user/stefano.mazzucca/p2c_nd_conv_all_coo.parquet")
View(head(conversions_all_coo,100))
nrow(conversions_all_coo)
# 557.111



## Carico campagne da considerare per impression e click ##############################################################################################################

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





## Carico le impression ###############################################################################################################################################

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
impression_5 <- filter(impression_4, "data_nav_dt <= '2018-02-28' and data_nav_dt >= '2017-04-01'")
# View(head(impression_5,100))
nrow(impression_5)
# 6.735.952.207


# createOrReplaceTempView(impression_5, "impression_5")
# impression_6 <- sql("select *
#                     from impression_5
#                     where regexp_extract(unloadvars.visibility, '\"visible1\":(\\w+)',1) = 'true'")
# # distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid
# 
# 
# write.parquet(impression_6, "/user/stefano.mazzucca/p2c_nd_impression.parquet")
# 
# 
# impression_6 <- read.parquet("/user/stefano.mazzucca/p2c_nd_impression.parquet")
# View(head(impression_6,100))
# nrow(impression_6)
# # 0

# impression_6_bis <- filter(impression_5, "regexp_extract(unloadvars.visibility, '\"visible1\":(\\w+)',1) = 'true'")
# 
# 
# write.parquet(impression_6_bis, "/user/stefano.mazzucca/p2c_nd_impression_bis.parquet")
# 
# 
# impression_6_bis <- read.parquet("/user/stefano.mazzucca/p2c_nd_impression_bis.parquet")
# View(head(impression_6_bis,100))
# nrow(impression_6_bis)
# # 0




createOrReplaceTempView(impression_5, "impression_5")
createOrReplaceTempView(campaign_attribute_unique, "campaign_attribute_unique")

impression_filter <- sql("select t1.CookieID, t1.data_nav_dt, t1.`device-name`, t1.`os-name`, t1.`browser-name`, 
                                  t1.CampaignId, t1.`PlacementId-ActivityId`,
                                  t2.Label, t2.Search
                          from impression_5 t1
                          inner join campaign_attribute_unique t2
                            on t1.CampaignId = t2.`Campaign ID` and t1.`PlacementId-ActivityId` = t2.`Line Item ID`")



# write.parquet(impression_filter, "/user/stefano.mazzucca/p2c_nd_impression_filter_coo.parquet")
# 
# impression_filter <- read.parquet("/user/stefano.mazzucca/p2c_nd_impression_filter_coo.parquet")
# View(head(impression_filter,100))
# nrow(impression_filter)
# # 0



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
# 21.783.441

prova<- filter(imp_skyid_coo, "cookieid is NOT NULL")
View(head(prova,100))


imp_skyid_coo_day <- summarize(groupBy(imp_skyid_coo, "COD_CLIENTE_CIFRATO", "data_stipula_dt"), 
                               num_imp = count(imp_skyid_coo$cookieid),
                               min_imp_date = min(imp_skyid_coo$data_nav_dt),
                               max_imp_date = max(imp_skyid_coo$data_nav_dt))
View(head(imp_skyid_coo_day,100))
nrow(imp_skyid_coo_day)
# 116.277



write.parquet(imp_skyid_coo_day, "/user/stefano.mazzucca/p2c_nd_impression_skyid_day.parquet")




imp_skyid_day <- read.parquet("/user/stefano.mazzucca/p2c_nd_impression_skyid_day.parquet")
View(head(imp_skyid_day,100))
nrow(imp_skyid_day)
# 116.277


imp_skyid_with_imp <- filter(imp_skyid_day, "num_imp <> 0")
View(head(imp_skyid_with_imp,100))
nrow(imp_skyid_with_imp)
# 0




## Carico i click #####################################################################################################################################################

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
click_6 <- sql("select *
                from click_5
                where regexp_extract(unloadvars.visibility, '\"visible1\":(\\w+)',1) = 'true'")
# distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid


write.parquet(click_6, "/user/stefano.mazzucca/p2c_nd_click.parquet")


click_6 <- read.parquet("/user/stefano.mazzucca/p2c_nd_click.parquet")
View(head(click_6,100))
nrow(click_6)
# --


createOrReplaceTempView(click_6, "click_6")
createOrReplaceTempView(campaign_attribute_unique, "campaign_attribute_unique")

click_filter <- sql("select t1.*, t2.Label, t2.Search
                          from click_6 t1
                          inner join campaign_attribute_unique t2
                            on t1.CampaignId = t2.`Campaign ID` and t1.`PlacementId-ActivityId` = t2.`Line Item ID`")



createOrReplaceTempView(conversions_all_coo, "conversions_all_coo")
createOrReplaceTempView(click_filter, "click_filter")

clic_skyid_day <- sql("select t1.COD_CLIENTE_CIFRATO, t1.data_stipula_dt, count(t2.cookieid) as num_imp, 
                              min(t2.data_nav_dt) as min_imp_date, max(t2.data_nav_dt) as max_imp_date
                     from conversions_all_coo t1
                     left join click_filter t2
                      on t1.cookieid = t2.cookieid and 
                          t1.`device-name` = t2.`device-name` and 
                          t1.`os-name` = t2.`os-name` and
                          t1.`browser-name` = t2.`browser-name` and 
                          t1.data_stipula_dt >= t2.data_nav_dt
                     group by t1.COD_CLIENTE_CIFRATO, t1.data_stipula_dt")




write.parquet(clic_skyid_day, "/user/stefano.mazzucca/p2c_nd_cliclk_skyid_day.parquet")




clic_skyid_day <- read.parquet("/user/stefano.mazzucca/p2c_nd_cliclk_skyid_day.parquet")
View(head(clic_skyid_day,100))
nrow(clic_skyid_day)
# --

















































write.parquet(cookieid_skyid_adform, "/user/stefano.mazzucca/p2c_nd_coo_skyid.parquet")

write.parquet(conversions_all_coo, "/user/stefano.mazzucca/p2c_nd_conv_all_coo.parquet")



write.parquet(impression_filter, "/user/stefano.mazzucca/p2c_nd_impression_filter.parquet")

write.parquet(click_filter, "/user/stefano.mazzucca/p2c_nd_click_filter.parquet")





#chiudi sessione
sparkR.stop()
