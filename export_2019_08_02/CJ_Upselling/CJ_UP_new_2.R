
## CJ_UPSELLING_new 2
## Raccolta contatti CRM


#apri sessione
source("connection_R.R")
options(scipen = 1000)




## Lista finale DIGITAL ------------------------------------------------------------------------
lista_eventi_digital <- read.parquet("/user/stefano.mazzucca/CJ_UP_lista_eventi_digital.parquet")
View(head(lista_eventi_digital,1000))
nrow(lista_eventi_digital)
# 8.044.554

valdist_eventi_digital <- distinct(select(lista_eventi_digital, "evento_digital"))
View(head(valdist_eventi_digital,100))

valdist_ext_id <- distinct(select(lista_eventi_digital, "COD_CLIENTE_CIFRATO"))
nrow(valdist_ext_id)
# 332.372 (erano 310k circa con le sole navigazioni.. si sono aggiunti circa 22k dagli ADV online)


#write.df(repartition( lista_eventi_digital, 1), path = "/user/stefano.mazzucca/CJ_UP_lista_eventi_digital.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



# Impression ADV (tutti)
# write.parquet(impression_adform_target_6, "/user/stefano.mazzucca/CJ_UP_impression_adv_target.parquet")
adv_impression <- read.parquet("/user/stefano.mazzucca/CJ_UP_impression_adv_target.parquet")
View(head(adv_impression,100))
nrow(adv_impression)
# 425.745.997

adv_impression_1 <- distinct(select(adv_impression, "COD_CONTRATTO", "COD_CLIENTE_CIFRATO", "data_upselling", "date_time_dt", "date_time_ts", "days_from_up", 
                                    "evento_digital", "flg_app_wsc"))
adv_impression_2 <- arrange(adv_impression_1, desc(adv_impression_1$COD_CONTRATTO), asc(adv_impression_1$date_time_ts))
View(head(adv_impression_2,1000))



## Recupero dati CRM --------------------------------------------------------------------------------
lista_crm_contatti <- read.df("/user/stefano.mazzucca/DB_CONTATTI_UPGRADERS.csv", source = "csv", header = "true", delimiter = ";")
View(head(lista_crm_contatti,100))
nrow(lista_crm_contatti)
# 268.779

up_canale <- read.df("/user/stefano.mazzucca/Base_dati_upg_canale.csv", source = "csv", header = "true", delimiter = ";")
View(head(up_canale,100))
nrow(up_canale)
# 517.573


## Esplorazione dati...
createOrReplaceTempView(lista_crm_contatti, "lista_crm_contatti")
groupby_cl_lista_crm <- sql("select COD_CLIENTE_CIFRATO, count(COD_CLIENTE_CIFRATO)
                            from lista_crm_contatti
                            group by COD_CLIENTE_CIFRATO")
groupby_cl_lista_crm_1 <- arrange(groupby_cl_lista_crm, desc(groupby_cl_lista_crm$`count(COD_CLIENTE_CIFRATO)`))
View(head(groupby_cl_lista_crm_1,100))
nrow(groupby_cl_lista_crm_1)
# 123.666


createOrReplaceTempView(up_canale, "up_canale")
valdist_up_canale <- sql("select CANALE_UPGD, count(COD_CLIENTE_CIFRATO)
                         from up_canale
                         group by CANALE_UPGD")
valdist_up_canale_1 <- arrange(valdist_up_canale, desc(valdist_up_canale$`count(COD_CLIENTE_CIFRATO)`))
View(head(valdist_up_canale_1,100))




#chiudi sessione
sparkR.stop()
