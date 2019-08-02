
## Lenti-veloci (4)
## Conversioni IBRIDI


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
# /user/stefano.mazzucca/20180726_stipulati_Nov17_Giu18.csv







## Carcio il file delle conversioni (su tutti i canali)  ####################################################################################################

conversions_all <- read.df("/user/stefano.mazzucca/20180726_stipulati_Nov17_Giu18.csv", source = "csv", header = "true", delimiter = ";")
View(head(conversions_all,100))
nrow(conversions_all)
# 403.360 

conversions_all_1 <- withColumn(conversions_all, "data_stipula_dt", cast(cast(unix_timestamp(conversions_all$DATA_STIPULA, 'dd/MM/yyyy'),'timestamp'), 'date'))
#conversions_all_2 <- filter(conversions_all_1, "data_stipula_dt >= '2017-11-27' and data_stipula_dt <= '2018-03-04'")

conversions_all_2 <- filter(conversions_all_1, "DES_CATEGORIA_CANALE not like '%WEBSELLING%'")
nrow(conversions_all_2)
# 347.523

# createOrReplaceTempView(conversions_all_2, "conversions_all_2")
# count_promo <- sql("select DES_PROMO, count(COD_CLIENTE_CIFRATO) as count
#                    from conversions_all_2
#                    group by DES_PROMO")
# View(head(count_promo, 1000))

conversions_all_3 <- filter(conversions_all_2, "DES_PROMO not like '%PREP%' and DES_PROMO not like '%FASTWEB%'")
conversions_all_4 <- filter(conversions_all_3, "DES_PROMO not like '%HP%' and DES_PROMO not like '%T&B%'")
nrow(conversions_all_4)
# 246.873
conversions_all_5 <- filter(conversions_all_4, "NUM_COD_VIS <> '5462'") # No "subentri"!
View(head(conversions_all_5,100))
nrow(conversions_all_5)
# 241.899


## Carico il metadata delle promo per filtrare le conversioni ##############################################################################################
metadata_promo <- read.df("/user/silvia/metadata_promo.csv", source = "csv", header = "true", delimiter = ";")
View(head(metadata_promo,100))
nrow(metadata_promo)
# 1.348

metadata_promo_1 <- filter(metadata_promo, "`Macro Type L1` NOT LIKE '%Special%'")
metadata_promo_2 <- filter(metadata_promo_1, "Cod <> '5462'")
nrow(metadata_promo_2)
# 1.151


## Filtro le conversioni non special, residential, no "subentri"

createOrReplaceTempView(conversions_all_5, "conversions_all_5")
createOrReplaceTempView(metadata_promo_2, "metadata_promo_2")
conversions_target_filter <- sql("select t1.*
                                 from conversions_all_5 t1
                                 inner join metadata_promo_2 t2
                                 on t1.NUM_COD_VIS = t2.cod ")
View(head(conversions_target_filter,100))
nrow(conversions_target_filter)
# 190.984


## VERIFICHE ##
valdist_skyid_dat <- distinct(select(conversions_target_filter, "COD_CLIENTE_CIFRATO", "data_stipula_dt"))
nrow(valdist_skyid_dat)
# 189.223
valdist_skyid <- distinct(select(conversions_target_filter, "COD_CLIENTE_CIFRATO"))
nrow(valdist_skyid)
# 181.553
valdist_skyid_dat_stip_attiv <- distinct(select(conversions_target_filter, "COD_CLIENTE_CIFRATO", "data_stipula_dt", "DAT_PRIMA_ATTIVAZIONE"))
nrow(valdist_skyid_dat_stip_attiv)
# 189.865


## Leggo tracking point per costruire dizionario cookieid-skyid  ##########################################################################################

# trackingpoint <- read.parquet("/STAGE/adform/table=Trackingpoint/Trackingpoint.parquet")
# View(head(trackingpoint, 100))
# 
# trackingpoint_1 <- filter(trackingpoint, "IsRobot = 'No'")
# trackingpoint_2 <- filter(trackingpoint_1, "IsNoRepeats = 'Yes'")
# trackingpoint_3 <- filter(trackingpoint_2, "CookieID <> 0 and CookieID is NOT NULL")
# trackingpoint_4 <- withColumn(trackingpoint_3, "data", cast(trackingpoint_3$yyyymmdd, 'string'))
# trackingpoint_5 <- withColumn(trackingpoint_4, "data_dt", cast(cast(unix_timestamp(trackingpoint_4$data, 'yyyyMMdd'), 'timestamp'), 'date'))
# 
# createOrReplaceTempView(trackingpoint_5, "trackingpoint_5")
# cookieid_skyid_adform <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
#                              CookieID, `device-name`,`os-name`,`browser-name`
#                              from trackingpoint_5
#                              where customvars.systemvariables like '%13%' and CookieID <> '0' 
#                              having (skyid <> '' and length(skyid)= 48)")
# 
# 
# write.parquet(cookieid_skyid_adform, "/user/stefano.mazzucca/lenti_veloci_coo_skyid.parquet")


cookieid_skyid_adform <- read.parquet("/user/stefano.mazzucca/lenti_veloci_coo_skyid.parquet")
View(head(cookieid_skyid_adform,100))
nrow(cookieid_skyid_adform)
# 56.494.840

# createOrReplaceTempView(cookieid_skyid_adform, "cookieid_skyid_adform")
# count_skyid <- sql("select count(distinct skyid)
#                    from cookieid_skyid_adform")
# View(head(count_skyid,100))
# # 3.519.855


## Aggancio skyid al cookie sui convertenti #########################################################################################################

createOrReplaceTempView(conversions_target_filter, "conversions_target_filter")
createOrReplaceTempView(cookieid_skyid_adform, "cookieid_skyid_adform")
conversions_target_coo <- sql("select distinct t1.*, t2.cookieid , t2.`device-name` , t2.`os-name` , t2.`browser-name`
                              from conversions_target_filter t1
                              left join cookieid_skyid_adform t2
                              on t1.COD_CLIENTE_CIFRATO = t2.skyid")


write.parquet(conversions_target_coo, "/user/stefano.mazzucca/lenti_veloci_ibridi_coo.parquet")



conversions_target_coo <- read.parquet("/user/stefano.mazzucca/lenti_veloci_ibridi_coo.parquet")
View(head(conversions_target_coo,100))
nrow(conversions_target_coo)
# 1.059.337

# prova <- filter(conversions_target_coo, "cookieid is NULL")
# View(head(prova,100))
# nrow(prova)
# # 94.264
# prova_valdist <- distinct(select(prova, "COD_CLIENTE_CIFRATO"))
# nrow(prova_valdist)
# # 89.762


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
# View(head(impression,100))
# nrow(impression)
# # 9.310.724.907

impression_1 <- filter(impression, "IsRobot = 'No'")
# nrow(impression_1)
# # 9.269.091.226
impression_2 <- filter(impression_1, "CookieID <> 0 and CookieID is NOT NULL")
impression_3 <- withColumn(impression_2, "data_nav", cast(impression_2$yyyymmdd, 'string'))
impression_4 <- withColumn(impression_3, "data_nav_dt", cast(cast(unix_timestamp(impression_3$data_nav, 'yyyyMMdd'), 'timestamp'), 'date'))
# View(head(impression_4,100))
impression_5 <- filter(impression_4, "data_nav_dt <= '2018-07-01' and data_nav_dt >= '2017-04-01'")
# View(head(impression_5,100))
# nrow(impression_5)
# -- (6.735.952.207)

createOrReplaceTempView(impression_5, "impression_5")
impression_6 <- sql("select *, 
                    regexp_extract(unloadvars.visibility, '\"visible1\":(\\\\w+)',1) as visibility
                    from impression_5
                    having visibility LIKE 'true'")

createOrReplaceTempView(impression_6, "impression_6")
createOrReplaceTempView(campaign_attribute_unique, "campaign_attribute_unique")
impression_filter <- sql("select t1.CookieID, t1.data_nav_dt, t1.`device-name`, t1.`os-name`, t1.`browser-name`, t1.CampaignId, t1.`PlacementId-ActivityId`,
                         t1.visibility, 
                         t2.Label, t2.Search
                         from impression_6 t1
                         inner join campaign_attribute_unique t2
                         on t1.CampaignId = t2.`Campaign ID` and t1.`PlacementId-ActivityId` = t2.`Line Item ID`")

createOrReplaceTempView(conversions_target_coo, "conversions_target_coo")
createOrReplaceTempView(impression_filter, "impression_filter")
imp_skyid_coo <- sql("select t1.*, t2.data_nav_dt, t2.CampaignId, t2.`PlacementId-ActivityId`, t2.Label, t2.Search
                     from conversions_target_coo t1
                     left join impression_filter t2
                     on t1.cookieid = t2.cookieid and 
                     t1.`device-name` = t2.`device-name` and 
                     t1.`os-name` = t2.`os-name` and
                     t1.`browser-name` = t2.`browser-name` and 
                     t1.data_stipula_dt >= t2.data_nav_dt ")


write.parquet(imp_skyid_coo, "/user/stefano.mazzucca/lenti_veloci_impression_ibridi.parquet")



imp_skyid_coo <- read.parquet("/user/stefano.mazzucca/lenti_veloci_impression_ibridi.parquet")
View(head(imp_skyid_coo,100))
nrow(imp_skyid_coo)
# 14.455.334 

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


write.parquet(imp_skyid_coo_day, "/user/stefano.mazzucca/lenti_veloci_impression_irbridi_day.parquet")



imp_skyid_coo_day <- read.parquet("/user/stefano.mazzucca/lenti_veloci_impression_irbridi_day.parquet")
View(head(imp_skyid_coo_day,100))
nrow(imp_skyid_coo_day)
# 2.088.428

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


write.parquet(imp_skyid_coo_tot, "/user/stefano.mazzucca/lenti_veloci_impression_ibridi_tot.parquet")


imp_skyid_coo_tot <- read.parquet("/user/stefano.mazzucca/lenti_veloci_impression_ibridi_tot.parquet")
View(head(imp_skyid_coo_tot,100))
nrow(imp_skyid_coo_tot)
# 189.865 (vs. 189.865 valdist_skyid_dat_stip_attiv)


# ## prova
# imp_skyid_with_imp <- filter(imp_skyid_coo_tot, "num_imp_tot <> 0")
# View(head(imp_skyid_with_imp,100))
# nrow(imp_skyid_with_imp)
# # 56.280


## Carico i click #########################################################################################################################################

click <- read.parquet('/STAGE/adform/table=Click/Click.parquet')
# View(head(click,100))
# nrow(click)
# # --

click_1 <- filter(click, "IsRobot = 'No'")
# nrow(click_1)
# # --
click_2 <- filter(click_1, "CookieID <> 0 and CookieID is NOT NULL")
click_3 <- withColumn(click_2, "data_nav", cast(click_2$yyyymmdd, 'string'))
click_4 <- withColumn(click_3, "data_nav_dt", cast(cast(unix_timestamp(click_3$data_nav, 'yyyyMMdd'), 'timestamp'), 'date'))
# View(head(click_4,100))
click_5 <- filter(click_4, "data_nav_dt <= '2018-07-01' and data_nav_dt >= '2017-04-01'")
# nrow(click_5)
# # --

createOrReplaceTempView(click_5, "click_5")
click_6 <- sql("select *, 
               regexp_extract(unloadvars.visibility, '\"visible1\":(\\\\w+)',1) as visibility
               from click_5
               having (visibility <> 'false' or visibility is NOT NULL)")

createOrReplaceTempView(click_6, "click_6")
createOrReplaceTempView(campaign_attribute_unique, "campaign_attribute_unique")
click_filter <- sql("select t1.CookieID, t1.data_nav_dt, t1.`device-name`, t1.`os-name`, t1.`browser-name`, t1.CampaignId, t1.`PlacementId-ActivityId`,
                    t1.visibility, 
                    t2.Label, t2.Search
                    from click_6 t1
                    inner join campaign_attribute_unique t2
                    on t1.CampaignId = t2.`Campaign ID` and t1.`PlacementId-ActivityId` = t2.`Line Item ID`")

createOrReplaceTempView(conversions_target_coo, "conversions_target_coo")
createOrReplaceTempView(click_filter, "click_filter")
clic_skyid_coo <- sql("select t1.*, t2.data_nav_dt, t2.CampaignId, t2.`PlacementId-ActivityId`, t2.Label, t2.Search
                      from conversions_target_coo t1
                      left join click_filter t2
                      on t1.cookieid = t2.cookieid and 
                      t1.`device-name` = t2.`device-name` and 
                      t1.`os-name` = t2.`os-name` and
                      t1.`browser-name` = t2.`browser-name` and 
                      t1.data_stipula_dt >= t2.data_nav_dt ")


write.parquet(clic_skyid_coo, "/user/stefano.mazzucca/lenti_veloci_click_ibridi.parquet")



clic_skyid_coo <- read.parquet("/user/stefano.mazzucca/lenti_veloci_click_ibridi.parquet")
View(head(clic_skyid_coo,100))
nrow(clic_skyid_coo)
# 1.137.376

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


write.parquet(clic_skyid_coo_day, "/user/stefano.mazzucca/lenti_veloci_clic_ibridi_day.parquet")



clic_skyid_coo_day <- read.parquet("/user/stefano.mazzucca/lenti_veloci_clic_ibridi_day.parquet")
View(head(clic_skyid_coo_day,100))
nrow(clic_skyid_coo_day)
# 261.475

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


write.parquet(clic_skyid_coo_tot, "/user/stefano.mazzucca/lenti_veloci_clic_ibridi_tot.parquet")



clic_skyid_coo_tot <- read.parquet("/user/stefano.mazzucca/lenti_veloci_clic_ibridi_tot.parquet")
View(head(clic_skyid_coo_tot,100))
nrow(clic_skyid_coo_tot)
# 189.865 (vs. 189.865 valdist_skyid_dat_stip_attiv)

# ## prova
# clic_skyid_with_imp <- filter(clic_skyid_coo_tot, "num_clic_tot <> 0")
# View(head(clic_skyid_with_imp,100))
# nrow(clic_skyid_with_imp)
# # 18.932


#############################################################################################################################################################
#############################################################################################################################################################
## Aggrego i dati di impression e click su tutto il target
#############################################################################################################################################################
#############################################################################################################################################################


imp_skyid_coo_tot <- read.parquet("/user/stefano.mazzucca/lenti_veloci_impression_ibridi_tot.parquet")
View(head(imp_skyid_coo_tot,100))
nrow(imp_skyid_coo_tot)
# 189.865

clic_skyid_coo_tot <- read.parquet("/user/stefano.mazzucca/lenti_veloci_clic_ibridi_tot.parquet")
View(head(clic_skyid_coo_tot,100))
nrow(clic_skyid_coo_tot)
# 189.865

############################################################################################################

# valdist_skyid_imp <- distinct(select(imp_skyid_coo_tot, "COD_CLIENTE_CIFRATO"))
# nrow(valdist_skyid_imp)
# # 181.553
# valdist_skyid_clic <- distinct(select(clic_skyid_coo_tot, "COD_CLIENTE_CIFRATO"))
# nrow(valdist_skyid_clic)
# # 181.553
# 
# valdist_skyid_tot <- union(valdist_skyid_imp, valdist_skyid_clic)
# valdist_skyid_tot_2 <- distinct(select(valdist_skyid_tot, "COD_CLIENTE_CIFRATO"))
# nrow(valdist_skyid_tot_2)
# # 181.553 (Ceck OK)
# 
# 
# valdist_skyid_dat_imp <- distinct(select(imp_skyid_coo_tot, "COD_CLIENTE_CIFRATO", "data_stipula_dt"))
# nrow(valdist_skyid_dat_imp)
# # 189.223
# valdist_skyid_dat_clic <- distinct(select(clic_skyid_coo_tot, "COD_CLIENTE_CIFRATO", "data_stipula_dt"))
# nrow(valdist_skyid_dat_clic)
# # 189.223
# 
# valdist_skyid_dat_tot <- union(valdist_skyid_dat_imp, valdist_skyid_dat_clic)
# valdist_skyid_dat_tot_2 <- distinct(select(valdist_skyid_dat_tot, "COD_CLIENTE_CIFRATO", "data_stipula_dt"))
# nrow(valdist_skyid_dat_tot_2)
# # 189.223 (Ceck OK)

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
# 189.865 (vs. 189.865 valdist_skyid_dat_stip_attiv) --> Ceck OK!
cache(skyid_coo_tot)



write.parquet(skyid_coo_tot, "/user/stefano.mazzucca/lenti_veloci_ibridi_tot.parquet")


###########################################################################################################################################################
###########################################################################################################################################################

###########################################################################################################################################################
###########################################################################################################################################################


skyid_coo_tot <- read.parquet("/user/stefano.mazzucca/lenti_veloci_ibridi_tot.parquet")
View(head(skyid_coo_tot,100))
nrow(skyid_coo_tot)
# 189.865

skyid_coo_tot_2 <- withColumn(skyid_coo_tot, "dat_prima_attivazione_dt", cast(cast(unix_timestamp(skyid_coo_tot$DAT_PRIMA_ATTIVAZIONE, 'dd/MM/yyyy'),'timestamp'), 'date'))
View(head(skyid_coo_tot_2,100))
nrow(skyid_coo_tot_2)
# 189.865

# skyid_coo_tot_navig <- filter(skyid_coo_tot_2, "min_date_imp is NOT NULL or min_date_clic is NOT NULL")
# View(head(skyid_coo_tot_navig,100))
# nrow(skyid_coo_tot_navig)
# # 56.292  ## Con il solo "min_date_imp is NOT NULL" -> 56.280

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
# 189.865


write.parquet(skyid_coo_tot_3, "/user/stefano.mazzucca/lenti_veloci_ibridi_finale.parquet")
write.df(repartition( skyid_coo_tot_3, 1), path = "/user/stefano.mazzucca/lenti_veloci_ibridi_finale.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



skyid_tot <- read.parquet("/user/stefano.mazzucca/lenti_veloci_ibridi_finale.parquet")
View(head(skyid_tot,100))
nrow(skyid_tot)
# 189.865


## SCARICA NAVIGAZIONE IBIDI e procedo con l'aggancio delle navigazioni organiche

## Prendi lista 
# conversions_target_filter
nrow(conversions_target_filter) 
# 190.984

## Associo cookies ed external_id ADOBE #####################################################################################################

base_ext_id <- distinct(select(conversions_target_filter,"COD_CLIENTE_CIFRATO"))
nrow(base_ext_id) 
# 181.553

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated"))

#join
createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
createOrReplaceTempView(base_ext_id,"base_ext_id")
base_ext_id_2 <- sql("select *
                     from base_ext_id  t1 left join skyitdev_df_3 t2
                     on t1.COD_CLIENTE_CIFRATO=t2.external_id_post_evar")


write.parquet(base_ext_id_2,"/user/stefano.mazzucca/lenti_veloci_estrazione_ibridi_CookiesADOBE.parquet")



## Creo data-set per estrazioni successive

base_cookies <- read.parquet("/user/stefano.mazzucca/lenti_veloci_estrazione_ibridi_CookiesADOBE.parquet")
nrow(base_cookies) 
# 347.845

base_cookies_2 <- filter(base_cookies,"external_id_post_evar is not NULL")
nrow(base_cookies_2) 
# 241.314

## Seleziono la max data di attivaz associata all'ext-id
createOrReplaceTempView(conversions_target_filter,"conversions_target_filter")
max_data_stipula <- sql("select COD_CLIENTE_CIFRATO, max(data_stipula_dt) as max_Data_stipula_dt
                        from conversions_target_filter
                        group by COD_CLIENTE_CIFRATO")
View(head(max_data_stipula))
nrow(max_data_stipula) 
# 181.553 ceck OK (vs. nrow(conversions_target_filter) = 190.984 )


#join 
createOrReplaceTempView(base_cookies_2,"base_cookies_2")
createOrReplaceTempView(max_data_stipula,"max_data_stipula")
base_cookies_3 <- sql("select t1.*, t2.max_Data_stipula_dt
                      from base_cookies_2 t1 
                      inner join max_data_stipula t2
                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
nrow(base_cookies_3)  
# 241.314

#########################################################################################################################################################
# scarico tutta la navigazione sul sito da prima dell'attivazione
#########################################################################################################################################################

skyitdev <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")

#names(skyitdev)
# post_campaign: ha persistenza di 7gg (trackingcode)
# va_closer_id: ha persistenza legata alla singola visita, non rimane nel tempo

skyitdev <- select(skyitdev, "external_id_post_evar",
                   "post_pagename",
                   "post_visid_high",
                   "post_visid_low",
                   "post_visid_concatenated",
                   "date_time",
                   "page_url_post_evar","page_url_post_prop",
                   "secondo_livello_post_evar",
                   "terzo_livello_post_evar",
                   "visit_num",
                   "hit_source",
                   "exclude_hit",
                   "external_id_con_NL_post_evar",
                   "post_campaign",
                   "va_closer_id",
                   "canale_corporate_post_prop"
)

## FILTRO DATE
skyitdev <- withColumn(skyitdev, "date_time_dt", cast(skyitdev$date_time, "date"))
skyitdev <- withColumn(skyitdev, "date_time_ts", cast(skyitdev$date_time, "timestamp"))

skyitdev <- filter(skyitdev,"date_time_dt >= '2017-04-01'")

## Associa e scarica navigazioni
createOrReplaceTempView(skyitdev,"skyitdev")
createOrReplaceTempView(base_cookies_3,"base_cookies_3")
scarico_naigazione_1 <- sql("select t1.*,
                           t2.COD_CLIENTE_CIFRATO, t2.max_Data_stipula_dt
                           from skyitdev t1 
                           inner join base_cookies_3 t2
                           on t1.post_visid_concatenated = t2.post_visid_concatenated
                           ")

scarico_naigazione_2 <- filter(scarico_naigazione_1,"date_time_dt <= max_Data_stipula_dt")


write.parquet(scarico_naigazione_2,"/user/stefano.mazzucca/lenti_veloci_IBRIDI_scarico_naigazione.parquet")



nav_tot <- read.parquet("/user/stefano.mazzucca/lenti_veloci_IBRIDI_scarico_naigazione.parquet")
View(head(nav_tot,100))
nrow(nav_tot)
# 6.530.837
nav_organica <- filter(nav_tot, "post_campaign is NULL and canale_corporate_post_prop is NOT NULL")
View(head(nav_organica,100))
nrow(nav_organica)
# 1.851.947

lista_sezioni <- distinct(select(nav_organica, "canale_corporate_post_prop"))
View(head(lista_sezioni,1000))

nav_organica_1 <- filter(nav_organica, "canale_corporate_post_prop LIKE 'acquista' or 
                         canale_corporate_post_prop LIKE 'aol' or 
                         canale_corporate_post_prop LIKE 'pacchetti-offerte'")
nrow(nav_organica_1)
# 259.825
createOrReplaceTempView(nav_organica_1, "nav_organica_1")
nav_organica_2 <- sql("select COD_CLIENTE_CIFRATO, max_Data_stipula_dt,
                      count(COD_CLIENTE_CIFRATO) as num_tot_page_views,
                      sum(case when canale_corporate_post_prop = 'acquista' then 1 else 0 end) as num_views_ACQUISTA,
                      sum(case when canale_corporate_post_prop = 'aol' then 1 else 0 end) as num_views_AOL,
                      sum(case when canale_corporate_post_prop = 'pacchetti-offerte' then 1 else 0 end) as num_views_PACCHETTI_OFFERTE,
                      min(date_time_dt) as min_date_organic,
                      max(date_time_dt) as max_date_organic
                      from nav_organica_1
                      group by COD_CLIENTE_CIFRATO, max_Data_stipula_dt
                      order by COD_CLIENTE_CIFRATO")
View(head(nav_organica_2,100))
nrow(nav_organica_2)
# 19.357


## Riprendo le interazioni ADV (click e impression) e integro le 2 fonti di informazioni #######################################################################

skyid_tot <- read.parquet("/user/stefano.mazzucca/lenti_veloci_ibridi_finale.parquet")
View(head(skyid_tot,100))
nrow(skyid_tot)
# 189.865

createOrReplaceTempView(skyid_tot, "skyid_tot")
createOrReplaceTempView(nav_organica_2, "nav_organica_2")
skyid_completo <- sql("select distinct t1.*, t2.min_date_organic, t2.max_date_organic
                      from skyid_tot t1
                      left join nav_organica_2 t2
                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t1.data_stipula_dt = t2.max_Data_stipula_dt")
View(head(skyid_completo,100))
nrow(skyid_completo)
# 189.865

skyid_completo_1 <- select(skyid_completo, "COD_CLIENTE_CIFRATO", "data_stipula_dt", "DAT_PRIMA_ATTIVAZIONE",
                           "min_data_interactions", "max_data_interactions", "min_date_organic", "max_date_organic")

createOrReplaceTempView(skyid_completo_1, "skyid_completo_1")
skyid_completo_2 <- sql("select *, 
                        case when min_data_interactions <= min_date_organic then min_data_interactions
                        else min_date_organic end as min_data_provv,
                        case when max_data_interactions >= max_date_organic then max_data_interactions
                        else max_date_organic end as max_data_provv
                        from skyid_completo_1")
View(head(skyid_completo_2,100))
nrow(skyid_completo_2)
# 189.865

skyid_completo_2$min_data_provv2 <- ifelse(isNull(skyid_completo_2$min_data_provv) == TRUE & isNull(skyid_completo_2$min_data_interactions) == TRUE,
                                           skyid_completo_2$min_date_organic, skyid_completo_2$min_data_provv)
skyid_completo_2$min_data <- ifelse(isNull(skyid_completo_2$min_data_provv2) == TRUE & isNull(skyid_completo_2$min_date_organic) == TRUE,
                                    skyid_completo_2$min_data_interactions, skyid_completo_2$min_data_provv2)

skyid_completo_2$max_data_provv2 <- ifelse(isNull(skyid_completo_2$max_data_provv) == TRUE & isNull(skyid_completo_2$max_data_interactions) == TRUE, 
                                           skyid_completo_2$max_date_organic, skyid_completo_2$max_data_provv)
skyid_completo_2$max_data <- ifelse(isNull(skyid_completo_2$max_data_provv2) == TRUE & isNull(skyid_completo_2$max_date_organic) == TRUE, 
                                    skyid_completo_2$max_data_interactions, skyid_completo_2$max_data_provv2)

skyid_completo_2$min_data_provv <- NULL
skyid_completo_2$min_data_provv2 <- NULL
skyid_completo_2$max_data_provv <- NULL
skyid_completo_2$max_data_provv2 <- NULL

View(head(skyid_completo_2,100))
nrow(skyid_completo_2)
# 189.865


write.parquet(skyid_completo_2, "/user/stefano.mazzucca/lenti_veloci_ibridi_completo_finale.parquet")
write.df(repartition( skyid_completo_2, 1), path = "/user/stefano.mazzucca/lenti_veloci_ibridi_completo_finale.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



###########################################################################################################################################################
## Per ogni COD_CLIENTE ricavo le frequenze di visita delle sezioni corporate navigate in maniera ORGANICA nelle ultime 2 settimane prima della conversione

skyid_completo <- read.df("/user/stefano.mazzucca/lenti_veloci_ibridi_completo_finale.csv", source = "csv", header = "true", delimiter = ";")
View(head(skyid_completo,100))
nrow(skyid_completo)
# 189.865

createOrReplaceTempView(skyid_completo, "skyid_completo")
skyid_completo_1 <- sql("SELECT *, 
                        datediff(data_stipula_dt, min_data) as diff_gg 
                        from skyid_completo")
View(head(skyid_completo_1,100))


## classificazione per definire le fasce di "velocita'", da applicare alla base con tutti i movimenti (organico+ad-form)
createOrReplaceTempView(skyid_completo_1,"skyid_completo_1")
skyid_completo_2 <- sql("select *,
                        case when diff_gg <= 7 then '1_<=7gg'
                        when diff_gg <= 14 then '2_<=14gg'
                        when diff_gg <= 21 then '3_<=21gg'
                        when diff_gg <= 28 then '4_<=28gg'
                        when diff_gg <= 60 then '5_<=60gg'
                        when diff_gg > 60 then '6_>60gg'
                        else Null end as FASCIA_P2C
                        from skyid_completo_1")
View(head(skyid_completo_2,100))
nrow(skyid_completo_2)
# 189.865

skyid_completo_2$classificazione_cliente <- ifelse(skyid_completo_2$diff_gg <= 28, 'veloci', 'lenti')
skyid_completo_2$classificazione_cliente <- ifelse(isNull(skyid_completo_2$FASCIA_P2C) == TRUE, NULL, skyid_completo_2$classificazione_cliente)
View(head(skyid_completo_2,100))


write.parquet(skyid_completo_2, "/user/stefano.mazzucca/lenti_veloci_ibridi_completo_finale_info.parquet")
write.df(repartition( skyid_completo_2, 1), path = "/user/stefano.mazzucca/lenti_veloci_ibridi_completo_finale_info.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



info_finali <- read.parquet("/user/stefano.mazzucca/lenti_veloci_ibridi_completo_finale_info.parquet")
View(head(info_finali,100))
nrow(info_finali)
# 189.865

info_finali_ibridi <- filter(info_finali, "classificazione_cliente is NOT null")
View(head(info_finali_ibridi,100))
nrow(info_finali_ibridi)
# 60.272


## Rianggancio le info iniziali
nrow(conversions_target_filter)
# 190.984

createOrReplaceTempView(info_finali_ibridi, "info_finali_ibridi")
createOrReplaceTempView(conversions_target_filter, "conversions_target_filter")
info_finali_1 <- sql("select distinct t1.*, t2.FASCIA_P2C, t2.classificazione_cliente
                     from conversions_target_filter  t1
                     inner join info_finali_ibridi t2
                     on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and 
                      t1.data_stipula_dt = t2.data_stipula_dt and
                      t1.DAT_PRIMA_ATTIVAZIONE = t2.DAT_PRIMA_ATTIVAZIONE")
View(head(info_finali_1,100))
nrow(info_finali_1)
# 60.386
## ATTENZIONE !! vs 60.272 clienti.. ci sono doppioni!


write.df(repartition( info_finali_1, 1), path = "/user/stefano.mazzucca/lenti_veloci_ibridi_def.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
write.parquet(info_finali_1, "/user/stefano.mazzucca/lenti_veloci_ibridi_def.parquet")



lv_ibridi <- read.parquet("/user/stefano.mazzucca/lenti_veloci_ibridi_def.parquet")
View(head(lv_ibridi,100))
nrow(lv_ibridi)
# 60.386



#chiudi sessione
sparkR.stop()
