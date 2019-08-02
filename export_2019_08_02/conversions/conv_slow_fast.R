# transaction ID e purchase id su adone


#apri sessione
source("connection_R.r")
options(scipen = 1000)


##prendi estrazione ad-form
cookieid_skyid_adform <- read.parquet("/user/valentina/cookieid_skyid_adform_p2cDigital_201807_v2.parquet")
nrow(cookieid_skyid_adform) #177271

cookieid_skyid_adform_2 <- filter(cookieid_skyid_adform,"skyid is not NULL")
nrow(cookieid_skyid_adform_2) # 129579

verifica <- distinct(select(cookieid_skyid_adform_2,"skyid"))
nrow(verifica) #93740


###### carico estrazione crm
estrazione_date <- read.df("/user/valentina/20180726_p2c_lentiVSveloci_dateCRM.csv", source = "csv", header = "true", delimiter = ";")
names(estrazione_date)
nrow(estrazione_date) #99041

##join x extid
createOrReplaceTempView(cookieid_skyid_adform_2,"cookieid_skyid_adform_2")
createOrReplaceTempView(estrazione_date,"estrazione_date")

cookieid_skyid_adform_3 <- sql("select *
                               from cookieid_skyid_adform_2 t1 inner join estrazione_date t2
                               on t1.skyid=t2.COD_CLIENTE_CIFRATO")
nrow(cookieid_skyid_adform_3) #118562
View(head(cookieid_skyid_adform_3,300))


## modifica formato data
cookieid_skyid_adform_4 <- withColumn(cookieid_skyid_adform_3,"DAT_STIPULA_CNTR_substr",substr(cookieid_skyid_adform_3$DAT_STIPULA_CNTR, 1,9))

cookieid_skyid_adform_4 <-   withColumn(cookieid_skyid_adform_4,"DAT_STIPULA_CNTR_ts",cast(unix_timestamp(cookieid_skyid_adform_4$DAT_STIPULA_CNTR_substr, 'ddMMMyyyy'),'timestamp'))
cookieid_skyid_adform_4 <- withColumn(cookieid_skyid_adform_4, "DAT_STIPULA_CNTR_dt", cast(cookieid_skyid_adform_4$DAT_STIPULA_CNTR_ts, "date"))

View(head(cookieid_skyid_adform_4,200))
names(cookieid_skyid_adform_4)

cookieid_skyid_adform_5 <- filter(cookieid_skyid_adform_4,"DAT_STIPULA_dt=DAT_STIPULA_CNTR_dt")
nrow(cookieid_skyid_adform_5) #5394
printSchema(cookieid_skyid_adform_5)

View(head(cookieid_skyid_adform_5,200))

write.df(repartition( cookieid_skyid_adform_5, 1),path = "/user/valentina/20180726_Export_lista_Ricerca_p2c.csv", "csv", sep=";", mode = "overwrite", header=TRUE)

#chiudi sessione
sparkR.stop()



  
# Programma per creare l'associz cookieID e ext_id di adForm:
#######################################################################################################################################################################
####### prendi dati P2C Digital
#######################################################################################################################################################################

attivazioni_digital_p2cAmedeo <- read.parquet("/user/silvia/p2c_length_digitali_puri")
View(head(attivazioni_digital_p2cAmedeo,300))
printSchema(attivazioni_digital_p2cAmedeo)

#converto date
attivazioni_digital_p2cAmedeo_2 <- withColumn(attivazioni_digital_p2cAmedeo,"DAT_CONVERSION_ts",cast(unix_timestamp(attivazioni_digital_p2cAmedeo$Conversion, 'yyyy-MM-dd'),'timestamp'))
attivazioni_digital_p2cAmedeo_2 <- withColumn(attivazioni_digital_p2cAmedeo_2, "DAT_CONVERSION_dt", cast(attivazioni_digital_p2cAmedeo_2$DAT_CONVERSION_ts, "date"))

attivazioni_digital_p2cAmedeo_2 <- withColumn(attivazioni_digital_p2cAmedeo_2,"First_touch_ts",cast(unix_timestamp(attivazioni_digital_p2cAmedeo_2$First_touch, 'yyyy-MM-dd'),'timestamp'))
attivazioni_digital_p2cAmedeo_2 <- withColumn(attivazioni_digital_p2cAmedeo_2, "First_touch_dt", cast(attivazioni_digital_p2cAmedeo_2$First_touch_ts, "date"))

View(head(attivazioni_digital_p2cAmedeo_2,300))
printSchema(attivazioni_digital_p2cAmedeo_2)

names(attivazioni_digital_p2cAmedeo_2)
attivazioni_digital_p2cAmedeo_3 <- select(attivazioni_digital_p2cAmedeo_2,"device-name","browser-name","os-name",
"CookieID","skyid","DAT_CONVERSION_dt","First_touch_dt")

createOrReplaceTempView(attivazioni_digital_p2cAmedeo_3,"attivazioni_digital_p2cAmedeo_3")
attivazioni_digital_p2cAmedeo_4 <- sql("select *,
datediff(DAT_CONVERSION_dt,First_touch_dt) as diff_gg
from attivazioni_digital_p2cAmedeo_3")
View(head(attivazioni_digital_p2cAmedeo_4,300))



#crea classificazione
createOrReplaceTempView(attivazioni_digital_p2cAmedeo_4,"attivazioni_digital_p2cAmedeo_4")
attivazioni_digital_p2cAmedeo_5 <- sql("select *,
case when diff_gg<=7 then '1_<=7gg'
when diff_gg<=14 then '2_<=14gg'
when diff_gg<=21 then '3_<=21gg'
when diff_gg<=28 then '4_<=28gg'
when diff_gg<=60 then '5_<=60gg'
when diff_gg>60 then '6_>60gg'
else 'NA' end as FASCIA_P2C
from attivazioni_digital_p2cAmedeo_4")
View(head(attivazioni_digital_p2cAmedeo_5,2000))
###riconduco i cookies di Ad-Form agli external_id
trackingpoint <- read.parquet("/STAGE/adform/table=Trackingpoint/Trackingpoint.parquet")

trackingpoint_1 <- filter(trackingpoint, "IsRobot = 'No'")
trackingpoint_2 <- filter(trackingpoint_1, "IsNoRepeats = 'Yes'")
trackingpoint_3 <- filter(trackingpoint_2, "CookieID <> 0 and CookieID is NOT NULL")
trackingpoint_4 <- withColumn(trackingpoint_3, "data", cast(trackingpoint_3$yyyymmdd, 'string'))
trackingpoint_5 <- withColumn(trackingpoint_4, "data_dt", cast(cast(unix_timestamp(trackingpoint_4$data, 'yyyyMMdd'), 'timestamp'), 'date'))

printSchema(trackingpoint_5)
createOrReplaceTempView(trackingpoint_5, "trackingpoint_5")

trackingpoint_6 <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid,
                            CookieID, `device-name`,`os-name`,`browser-name`,data_dt
                            from trackingpoint_5
                            where customvars.systemvariables like '%13%' and CookieID <> '0'
                            having (skyid <> '' and length(skyid)= 48)")
View(head(trackingpoint_6,200))

#filtro data
trackingpoint_7 <- filter(trackingpoint_6,"data_dt>= '2017-11-01'")

#join
createOrReplaceTempView(trackingpoint_7, "trackingpoint_7")
createOrReplaceTempView(attivazioni_digital_p2cAmedeo_5, "attivazioni_digital_p2cAmedeo_5")

nrow(attivazioni_digital_p2cAmedeo_5) # 82291
names(trackingpoint_7)
names(attivazioni_digital_p2cAmedeo_5)

cookieid_skyid_adform_join <- sql("select distinct t1.CookieID,t1.FASCIA_P2C,
                                   t1.DAT_CONVERSION_dt as DAT_STIPULA_dt,
                                   t2.skyid
                                 from attivazioni_digital_p2cAmedeo_5 t1 left join trackingpoint_7 t2
                                  on t1.CookieID = t2.CookieID
                                 and t1. `device-name`=t2. `device-name`
                                 and t1.`os-name`=t2.`os-name`
                                 and t1.`browser-name`=t2.`browser-name`")

#write.parquet(cookieid_skyid_adform_join, "/user/valentina/cookieid_skyid_adform_p2cDigital_201807.parquet")
write.parquet(cookieid_skyid_adform_join, "/user/valentina/cookieid_skyid_adform_p2cDigital_201807_v2.parquet")






# alfa:direttrice web - (CMN)
# numerico: progressivo anagrafica (AOL)





# attivazioni_digital_p2cAmedeo <- read.parquet("/user/silvia/p2c_length_digitali_puri")





