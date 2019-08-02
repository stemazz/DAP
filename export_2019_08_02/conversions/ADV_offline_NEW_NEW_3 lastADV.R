
## Impatto ADV su vendite offline NEW 3

#apri sessione
source("connection_R.R")
options(scipen = 1000)



impression_target <- read.parquet("/user/stefano.mazzucca/p2c_nd_impression_skyid_coo.parquet")
View(head(impression_target,100))
nrow(impression_target)
# 10.628.067

# utenti_imp_si_cookie <- filter(impression_target, "cookieid is NOT NULL")
# View(head(utenti_imp_si_cookie,100))
# nrow(utenti_imp_si_cookie)
# # 10.564.412

createOrReplaceTempView(impression_target, "impression_target")
utenti_imp_last_ADV <- sql ("select COD_CLIENTE_CIFRATO, data_stipula_dt, DAT_PRIMA_ATTIVAZIONE, last(flg_digital) as flg_digital,
                                    max(data_nav_dt) as ultima_nav,
                                    last(CampaignId) as last_CampaignId,
                                    last(`PlacementId-ActivityId`) as last_PlacementId,
                                    last(Label) as last_Label, 
                                    last(Search) as last_Search
                            from impression_target
                            group by COD_CLIENTE_CIFRATO, data_stipula_dt, DAT_PRIMA_ATTIVAZIONE")
View(head(utenti_imp_last_ADV,100))
nrow(utenti_imp_last_ADV)
# 128.322

utenti_imp_last_ADV_1 <- filter(utenti_imp_last_ADV, "ultima_nav is NOT NULL  and flg_digital = 0")
View(head(utenti_imp_last_ADV_1,100))
nrow(utenti_imp_last_ADV_1)
# 31.749

####################################################################################################################################

click_target <- read.parquet("/user/stefano.mazzucca/p2c_nd_click_skyid_coo.parquet")
View(head(click_target,100))
nrow(click_target)
# 744.987

# utenti_clic_si_cookie <- filter(click_target, "cookieid is NOT NULL")
# View(head(utenti_clic_si_cookie,100))
# nrow(utenti_clic_si_cookie)
# # 681.332

createOrReplaceTempView(click_target, "click_target")
utenti_clic_last_ADV <- sql ("select COD_CLIENTE_CIFRATO, data_stipula_dt, DAT_PRIMA_ATTIVAZIONE, last(flg_digital) as flg_digital,
                                    max(data_nav_dt) as ultima_nav,
                                    last(CampaignId) as last_CampaignId,
                                    last(`PlacementId-ActivityId`) as last_PlacementId,
                                    last(Label) as last_Label, 
                                    last(Search) as last_Search
                            from click_target
                            group by COD_CLIENTE_CIFRATO, data_stipula_dt, DAT_PRIMA_ATTIVAZIONE")
View(head(utenti_clic_last_ADV,100))
nrow(utenti_clic_last_ADV)
# 128.322

utenti_clic_last_ADV_1 <- filter(utenti_clic_last_ADV, "ultima_nav is NOT NULL  and flg_digital = 0")
View(head(utenti_clic_last_ADV_1,100))
nrow(utenti_clic_last_ADV_1)
# 10.815


## join completa ##########################################################################################################################################

createOrReplaceTempView(utenti_imp_last_ADV, "utenti_imp_last_ADV")
createOrReplaceTempView(utenti_clic_last_ADV, "utenti_clic_last_ADV")

utenti_last_ADV <- sql("select distinct t1.*,
                                t2.ultima_nav as ultima_nav_clic,
                                t2.last_CampaignId as last_CampaignId_clic,
                                t2.last_PlacementId as last_PlacementId_clic,
                                t2.last_Label as last_Label_clic,
                                t2.last_Search as last_Search_clic
                       from utenti_imp_last_ADV t1
                       inner join utenti_clic_last_ADV t2 
                        on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t1.data_stipula_dt = t2.data_stipula_dt 
                       and t1.DAT_PRIMA_ATTIVAZIONE = t2.DAT_PRIMA_ATTIVAZIONE")
View(head(utenti_last_ADV,100))
nrow(utenti_last_ADV)
# 128.322


## join target convNOdigital ##############################################################################################################################

createOrReplaceTempView(utenti_imp_last_ADV_1, "utenti_imp_last_ADV_1")
createOrReplaceTempView(utenti_clic_last_ADV_1, "utenti_clic_last_ADV_1")

utenti_last_ADV_target <- sql("select distinct t1.*,
                                      t2.ultima_nav as ultima_nav_clic,
                                      t2.last_CampaignId as last_CampaignId_clic,
                                      t2.last_PlacementId as last_PlacementId_clic,
                                      t2.last_Label as last_Label_clic,
                                      t2.last_Search as last_Search_clic
                             from utenti_imp_last_ADV_1 t1
                             left join utenti_clic_last_ADV_1 t2 
                              on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t1.data_stipula_dt = t2.data_stipula_dt and
                                    t1.DAT_PRIMA_ATTIVAZIONE = t2.DAT_PRIMA_ATTIVAZIONE")
View(head(utenti_last_ADV_target,100))
nrow(utenti_last_ADV_target)
# 31.749





createOrReplaceTempView(utenti_last_ADV_target, "utenti_last_ADV_target")

utenti_last_ADV_target_1 <- sql("select COD_CLIENTE_CIFRATO, data_stipula_dt, DAT_PRIMA_ATTIVAZIONE, 
                                      ultima_nav, ultima_nav_clic,
                                      last_CampaignId, last_CampaignId_clic,
                                      last_PlacementId, last_PlacementId_clic,
                                      case when ultima_nav > ultima_nav_clic then ultima_nav else ultima_nav_clic end as ultima_nav_provv,
                                      last_Label,
                                      last_Label_clic,
                                      last_Search,
                                      last_Search_clic                    
                                from utenti_last_ADV_target")

utenti_last_ADV_target_1$ultima_nav_provv2 <- ifelse(isNull(utenti_last_ADV_target_1$ultima_nav_provv) == TRUE & isNull(utenti_last_ADV_target_1$ultima_nav) == TRUE,
                                                     utenti_last_ADV_target_1$ultima_nav_clic, utenti_last_ADV_target_1$ultima_nav_provv)
utenti_last_ADV_target_1$ultima_nav_def <- ifelse(isNull(utenti_last_ADV_target_1$ultima_nav_provv2) == TRUE & isNull(utenti_last_ADV_target_1$ultima_nav_clic) == TRUE,
                                              utenti_last_ADV_target_1$ultima_nav, utenti_last_ADV_target_1$ultima_nav_provv2)

utenti_last_ADV_target_1$last_CampaignId_def <- ifelse(utenti_last_ADV_target_1$ultima_nav_def == utenti_last_ADV_target_1$ultima_nav, 
                                                       utenti_last_ADV_target_1$last_CampaignId, utenti_last_ADV_target_1$last_CampaignId_clic)

utenti_last_ADV_target_1$last_PlacementId_def <- ifelse(utenti_last_ADV_target_1$ultima_nav_def == utenti_last_ADV_target_1$ultima_nav, 
                                                       utenti_last_ADV_target_1$last_PlacementId, utenti_last_ADV_target_1$last_PlacementId_clic)

utenti_last_ADV_target_1$last_Label_def <- ifelse(utenti_last_ADV_target_1$ultima_nav_def == utenti_last_ADV_target_1$ultima_nav, 
                                                       utenti_last_ADV_target_1$last_Label, utenti_last_ADV_target_1$last_Label_clic)

utenti_last_ADV_target_1$last_Search_def <- ifelse(utenti_last_ADV_target_1$ultima_nav_def == utenti_last_ADV_target_1$ultima_nav, 
                                                       utenti_last_ADV_target_1$last_Search, utenti_last_ADV_target_1$last_Search_clic)

utenti_last_ADV_target_1$ultima_nav <- NULL
utenti_last_ADV_target_1$ultima_nav_clic <- NULL
utenti_last_ADV_target_1$ultima_nav_provv <- NULL
utenti_last_ADV_target_1$ultima_nav_provv2 <- NULL

utenti_last_ADV_target_1$last_CampaignId <- NULL
utenti_last_ADV_target_1$last_CampaignId_clic <- NULL

utenti_last_ADV_target_1$last_PlacementId <- NULL
utenti_last_ADV_target_1$last_PlacementId_clic <- NULL

utenti_last_ADV_target_1$last_Label <- NULL
utenti_last_ADV_target_1$last_Label_clic <- NULL

utenti_last_ADV_target_1$last_Search <- NULL
utenti_last_ADV_target_1$last_Search_clic <- NULL


View(head(utenti_last_ADV_target_1,100))
nrow(utenti_last_ADV_target_1)
# 31.749


createOrReplaceTempView(utenti_last_ADV_target_1, "utenti_last_ADV_target_1")

utenti_last_ADV_target_2 <- sql("select COD_CLIENTE_CIFRATO, data_stipula_dt, DAT_PRIMA_ATTIVAZIONE, ultima_nav_def,
                                        last_CampaignId_def, last_PlacementId_def,
                                        case when last_Search_def = 'Si' then 1 else 0 end as last_ADV_Search,
                                        case when last_Search_def = 'NO' then 1 else 0 end as last_ADV_Search_no,
                                        case when last_Label_def = 'Branding' then 1 else 0 end as last_ADV_Branding,
                                        case when last_Label_def = 'Trading' then 1 else 0 end as last_ADV_Trading,
                                        case when last_Label_def = 'Other' then 1 else 0 end as last_ADV_Other,
                                        case when last_Label_def = 'Content & Engagement' then 1 else 0 end as last_ADV_content_eng
                                from utenti_last_ADV_target_1")
View(head(utenti_last_ADV_target_2,100))
nrow(utenti_last_ADV_target_2)
# 31.749




write.parquet(utenti_last_ADV_target_2, "/user/stefano.mazzucca/p2c_nd_last_ADV_no_digital.parquet")


ut_no_dig_last_ADV <- read.parquet("/user/stefano.mazzucca/p2c_nd_last_ADV_no_digital.parquet")
View(head(ut_no_dig_last_ADV,100))
nrow(ut_no_dig_last_ADV)
# 31.749



##########################################################################################################################################################
## Analisi ##
##########################################################################################################################################################


createOrReplaceTempView(ut_no_dig_last_ADV, "ut_no_dig_last_ADV")

distr_last_ADV <- sql("select sum(last_ADV_Search) as last_ADV_Search,
                              sum(last_ADV_Search_no) as last_ADV_Search_no,
                              sum(last_ADV_Branding) as last_ADV_Branding,
                              sum(last_ADV_Trading) as last_ADV_Trading,
                              sum(last_ADV_Other) as last_ADV_Other,
                              sum(last_ADV_content_eng) as last_ADV_content_eng
                      from ut_no_dig_last_ADV")
View(head(distr_last_ADV,100))











#chiudi sessione
sparkR.stop()
