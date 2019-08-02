
# Creazione del "dizionario" external_id - cookies su Adform

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


write.parquet(adform_tp_2,"/user/stefano.mazzucca/diz_adform_20181119.parquet")


diz_id_coo_adform <- read.parquet("/user/stefano.mazzucca/diz_adform_20181119.parquet")
View(head(diz_id_coo_adform,1000))
nrow(diz_id_coo_adform)
# 72.526.312

# createOrReplaceTempView(diz_id_coo_adform, "diz_id_coo_adform")
# 
# diz_id_coo_adform_2 <- sql("select *
#                            from diz_id_coo_adform
#                            order by skyid")
# View(head(diz_id_coo_adform_2,1000))
# nrow(diz_id_coo_adform_2)
# # 45.572.725


diz_id_coo_adform_3 <- filter(diz_id_coo_adform, "CookieID <> 0")
nrow(diz_id_coo_adform_3)
# 69.174.784


valdist_skyid_diz <- distinct(select(diz_id_coo_adform_3, "skyid"))
nrow(valdist_skyid_diz)
# 3.902.042

valdist_coo_diz <- distinct(select(diz_id_coo_adform_3, "CookieID"))
nrow(valdist_coo_diz)
# 40.650.072





## diz con gli altri campi identificativi

adform_tp_2_bis <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                            CookieID,
                            `device-name`,
                            `os-name`, 
                            `browser-name`
                    from adform_tp_1
                    having (skyid <> '' and length(skyid)= 48)
                    order by skyid")

write.parquet(adform_tp_2_bis,"/user/stefano.mazzucca/adfrom_diz_plus_skyid_cookies.parquet")


diz_plus_id_coo_adform <- read.parquet("/user/stefano.mazzucca/adfrom_diz_plus_skyid_cookies.parquet")
View(head(diz_plus_id_coo_adform,1000))
nrow(diz_plus_id_coo_adform)
# 50.809.126


diz_plus_id_coo_adform_2 <- filter(diz_plus_id_coo_adform, "CookieID <> 0")
nrow(diz_plus_id_coo_adform_2)
# 45.187.844


valdist_skyid_diz_plus <- distinct(select(diz_plus_id_coo_adform_2, "skyid"))
nrow(valdist_skyid_diz_plus)
# 3.329.016 (vs 3.329.016 del diz precedemte)

valdist_coo_diz_plus <- distinct(select(diz_plus_id_coo_adform_2, "CookieID"))
nrow(valdist_coo_diz_plus)
# 24.894.905 (vs 24.894.905 del diz precedente)


createOrReplaceTempView(diz_plus_id_coo_adform_2, "diz_plus_id_coo_adform_2")
verifica_skyid <- sql("select *
                      from diz_plus_id_coo_adform_2
                      where (skyid <> '' and length(skyid)= 48)")
nrow(verifica_skyid)
# 45.187.844 (Ceck OK!)



## Salvataggio dizionario definitivo skyid - cookies da Adform

write.parquet(diz_plus_id_coo_adform_2,"/user/stefano.mazzucca/def_diz_adform_skyid_coo.parquet")

diz_adform <- read.parquet("/user/stefano.mazzucca/def_diz_adform_skyid_coo.parquet")
View(head(diz_adform,1000))
nrow(diz_adform)
# 45.187.844








#chiudi sessione
sparkR.stop()
