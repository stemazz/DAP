
## 05_kpi_adform ##

source("connection_R.R")
options(scipen = 10000)

source("Digitalizzazione/NEW_kpi_digital/Production_Code/00_Utils_kpi_digital.R")


impression = read.parquet(path_impression)

impression1 = select(impression, 'Timestamp',
                     'CookieID',
                     'PublisherDomain',
                     'IsRobot',
                     'brandsafetyvars',
                     'device-name',
                     'browser-name',
                     'os-name',
                     'yyyymmdd')

impression2 <- withColumn(impression1, "date_time_dt", cast(impression1$Timestamp, "date"))

impression3 = filter(impression2, impression2$date_time_dt >= data_inizio)
impression4 = filter(impression3, impression3$date_time_dt <= data_fine)

# verifica_filtro= summarize(impression4, min_date= min(impression4$date_time_dt), max_date=max(impression4$date_time_dt)) date filtrate
# View(head(verifica_filtro,20))

write.parquet(impression4, path_scarico_impression)


# Join con dizionario:

impression_df = read.parquet(path_scarico_impression)


adform <- read.parquet(path_tp)

adform1 <- withColumn(adform, "date_time_dt", cast(adform$Timestamp, "date"))

adform2 <- filter(adform1, adform1$date_time_dt >= (data_inizio - 185))
adform3 <- filter(adform2, adform2$date_time_dt <= data_fine)

createOrReplaceTempView(adform3, "adform3")

adform_tp_2 <- sql("select distinct regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid,
                   CookieID
                   from adform3
                   having (skyid <> '' and length(skyid)= 48)")

adform_tp_3 <- filter(adform_tp_2, "CookieID <> 0" )

write.parquet(adform_tp_3, path_dizionario_adform)


diz = read.parquet(path_dizionario_adform)
# View(head(diz,100))
# nrow(diz) # 47.268.363

createOrReplaceTempView(impression_df, 'impr')
createOrReplaceTempView(diz, 'diz')

join1= sql('SELECT DISTINCT impr.*, diz.skyid as ext_id_adform
           FROM impr
           INNER JOIN diz
           ON impr.CookieID = diz.CookieID')

write.parquet(join1, path_impression_con_diz) 


################## CREAZIONE VARIABILI ################## 

impression_df = read.parquet(path_impression_con_diz)
#nrow(impression_df) #6.671.683.923

impression_df = withColumn(impression_df, 'weekday', date_format(impression_df$Timestamp, 'EEEE'))
impression_df = withColumn(impression_df, 'hour', hour(impression_df$Timestamp))
#View(head(impression_df))

createOrReplaceTempView(impression_df, 'impression_df')
imp_df0 = sql ("select ext_id_adform, Cookieid, yyyymmdd, `device-name`, `os-name`, `browser-name`,
               substr(cast(yyyymmdd as varchar(8)),1,6) as mese,
               weekofyear(to_date(Timestamp)) as week,
               weekday,
               hour,
               case when (`device-name`=='TV' or `device-name`=='Unknown') then 1 else 0 end as flag_tv_unknown,
               case when (`device-name`=='Mobile') then 1 else 0 end as flag_mobile,
               case when (`device-name`='Tablet') then 1 else 0 end as flag_tablet,
               case when (`device-name`='Desktop and Laptop') then 1 else 0 end as flag_desktop,
               case when (weekday = 'Monday' or weekday ='Tuesday' or weekday = 'Wednesday' or weekday ='Thursday' or weekday='Friday') then 1 else 0 end as flag_week,
               case when (weekday = 'Saturday' or weekday ='Sunday') then 1 else 0 end as flag_weekend,
               case when (hour >= 0 and hour < 6) then 1 else 0 end as flag_notte,
               case when (hour >= 6 and hour < 12) then 1 else 0 end as flag_mattina,
               case when (hour >= 12 and hour < 18) then 1 else 0 end as flag_pomeriggio,
               case when (hour >= 18 and hour <= 23) then 1 else 0 end as flag_sera,
               Timestamp
               from impression_df")

# Ho eliminato: concat(`device-name`,`os-name`) as device_os perch?? lo riaggiungo dopo.
write.parquet(imp_df0, path_impression_variabile0) #, mode='overwrite')

# imp_df=read.parquet(path_impression_variabile0_genn_giu)
# nrow(imp_df) #4.690.414.925
# View(head(imp_df,100))







# # PROBLEMA CONCAT(DEVICE_NAME, OS_NAME) --> UNKOWN + OS_NAME (NULL) --> DIVENTA NULL E QUINDI NON VIENE CONTATO COME DEVCIE:
# 
# prova_device_name = distinct(select(imp_df, imp_df$`device-name`)) #ci sono 8.525 ext. id che hanno navigato tramite TV o Unknown (di conseguenza mettiamo 0)
# View(head(prova_device_name))
# 
# device_unknown = filter(imp_df, imp_df$`device-name`=='Unknown')
# nrow(device_unknown) #552.865 hit con uknown
# View(head(device,100))
# 
# 
# prova_os_name = distinct(select(imp_df, imp_df$`os-name`))
# View(head(prova_os_name,100))
# nrow(prova_os_name) # 34



# 1) NUMERO MESI, NUMERO WEEK, NUMERO GIORNI, IMPRESSION TOTALI, IMPRESSION_TV_UKNOWN, IMPRESSION_MOBILE, IMPRESSION_DESKTOP, IMPRESSION_TABLET, IMPRESSION_SETTIMANA, IMPRESSION_WEEKEND, IMPRESSION FASCE ORARIE:

imp_df = read.parquet(path_impression_variabile0)

createOrReplaceTempView(imp_df,'imp_df')

var_impres= sql('SELECT ext_id_adform AS ext_id_impression,
                COUNT(DISTINCT(mese)) as numero_mesi,
                COUNT(DISTINCT(week)) as numero_week,
                COUNT(DISTINCT(yyyymmdd)) as numero_giorni,
                COUNT(*) as impression_tot,
                SUM(flag_tv_unknown) as impression_tv_unknown,
                SUM(flag_mobile) as impression_mobile,
                SUM(flag_desktop) as impression_desktop,
                SUM(flag_tablet) as impression_tablet,
                SUM(flag_week) as impression_week,
                SUM(flag_weekend) as impression_weekend,
                SUM(flag_mattina) as impression_mattina,
                SUM(flag_pomeriggio) as impression_pomeriggio,
                SUM(flag_sera) as impression_sera,
                SUM(flag_notte) as impression_notte
                FROM imp_df
                GROUP BY ext_id_adform')

write.parquet(var_impres,path_impression_variabili1) #, mode='overwrite')

impression_var1 = read.parquet(path_impression_variabili1)
# View(head(impression_var1,100))
# nrow(impression_var1) # 2941450

# path_impression_variabili1_RITENTA = "/user/emma.cambieri/impression_variabili1_20180101_20180630_tentativo2.parquet"
# write.parquet(var_impres,path_impression_variabili1_RITENTA)




# 2) AVG IMPRESSION PER MESI, WEEK, GIORNI, PERCENTUALE IMPRESSION PER MOBILE, DESKTOP, TABLET:

createOrReplaceTempView(impression_var1, 'impr')

var_impres1= sql('SELECT *, impression_tot/numero_mesi as avg_impression_mese,
                 impression_tot/numero_week as avg_impression_week,
                 impression_tot/numero_giorni as avg_impression_giorno,
                 impression_tv_unknown/impression_tot as perc_impression_tv_unknown,
                 impression_mobile/impression_tot as perc_impression_mobile,
                 impression_desktop/impression_tot as perc_impression_desktop,
                 impression_tablet/impression_tot as perc_impression_tablet
                 FROM impr')

write.parquet(var_impres1, path_impression_variabili2)
impression_var2 = read.parquet(path_impression_variabili2)
# View(head(impression_var2,100))
# nrow(impression_var2) #2941450


# 3) NUMERO DEVICE, NUMERO MOBILE, NUMERO DESKTOP, NUMERO TABLET:

imp_df = withColumn(imp_df, 'os-name', ifelse(isNull(imp_df$`os-name`), 'os_non_riconosciuto', imp_df$`os-name`))
imp_df = withColumn(imp_df, 'device_os', concat(imp_df$`device-name`,imp_df$`os-name`))

# prova= filter(imp_df, isNull(imp_df$device_os))
# nrow(prova) #0

#View(head(imp_df,100))
# prova=filter(imp_df, imp_df$`os-name`=='device_non_riconosciuto')
# View(head(prova,100))
# printSchema(imp_df)

createOrReplaceTempView(imp_df,'imp_df')

#step 1
var_device = sql("select ext_id_adform, `device-name`, `os-name`, `browser-name`, device_os,
                 max(flag_tv_unknown) as flag_tv_unknown,
                 max(flag_mobile) as flag_mobile, 
                 max(flag_tablet) as flag_tablet,
                 max(flag_desktop) as flag_desktop
                 from imp_df
                 group by ext_id_adform, `device-name`, `os-name`, `browser-name`, device_os ")

#var_device_tv = filter(var_device, var_device$`device-name`=='TV')
#View(head(var_device_tv,100))

#var_device_uknown = filter(var_device, var_device$`device-name`=='Unknown')
#View(head(var_device_uknown,100))


#step 2

createOrReplaceTempView(var_device, "var_device")

var_device1 = sql("select ext_id_adform,
                  count(distinct device_os) as numero_device,
                  count(distinct case when flag_tv_unknown=1 then device_os  else null end) as numero_tv_unknown,
                  count(distinct case when flag_mobile=1 then device_os  else null end) as numero_mobile,
                  count(distinct case when flag_tablet=1 then device_os  else null end) as numero_tablet,
                  count(distinct case when flag_desktop=1 then device_os else null end) as numero_desktop  
                  from  var_device
                  group by ext_id_adform")


# var_device1 = sql("select skyid,
#                   count(distinct device_os) as num_device,
#                   count(distinct case when flag_mobile=1 then device_os  else null end) as num_mobile,
#                   count(distinct case when flag_tablet=1 then device_os  else null end) as num_tablet,
#                   count(distinct case when flag_desktop=1 then device_os else null end) as num_desktop  
#                   # from (select skyid, cookieid, `device-name`, `os-name`, `browser-name` 
#                   #       from cookieid_skyid where length(skyid)=48) as a  #?? il tuo imp_df
#                   left join (select cookieid, `device-name`, `os-name`, `browser-name`, flag_mobile, flag_tablet, flag_desktop, concat(`device-name`,`os-name`) as device_os
#                             from  skyid_imps_aggr_step5) as b 
#                   on (a.cookieid = b.cookieid and a.`device-name`=b.`device-name` and a.`os-name`=b.`os-name` and a.`browser-name`=b.`browser-name`) group by 1")

write.parquet(var_device1, path_impression_variabili3)
impression_device = read.parquet(path_impression_variabili3)
#View(head(impression_device,100))
#nrow(impression_device) #2941450



# 4) PERCENTUALE NUMERO MOBILE, DESKTOP, TABLET:

createOrReplaceTempView(impression_device,'impression_device')

var_device2= sql('select *,
                 numero_tv_unknown/numero_device as perc_tv_unknown,
                 numero_mobile/numero_device as perc_mobile,
                 numero_desktop/numero_device as perc_desktop,
                 numero_tablet/numero_device as perc_tablet
                 from impression_device')

write.parquet(var_device2, path_impression_variabili4) #, mode='overwrite')

impression_device_perc= read.parquet(path_impression_variabili4)
#View(head(impression_device_perc,100))
#nrow(impression_device_perc) #2941450


#5) SOCIAL:

# nrow(pub_dom) #3.642.383  sia facebook che instagram
# 
# pub_dom_face= filter(impression_df, "PublisherDomain like '%acebook%'")
# nrow(pub_dom_face) #3.640.410
# View(head(pub_dom_face,100))
# 
# pub_dom_insta= filter(impression_df, "PublisherDomain like '%nstagram%'")
# nrow(pub_dom_insta) #1.973
# View(head(pub_dom_insta,100))
# 
# pub_dom_tweet= filter(impression_df, "PublisherDomain like '%witter%'")
# nrow(pub_dom_tweet) #18.596
# View(head(pub_dom_tweet,100))

# pubdom_social= summarize(groupBy(impression_df, impression_df$PublisherDomain), count=count(impression_df$ext_id_adform))
# pubdom_social2=filter(pubdom_social, "PublisherDomain like '%facebook%' or PublisherDomain like '%nstagram%' or PublisherDomain like '%witter%' or PublisherDomain like '%napchat%'")
# View(head(pubdom_social2,2000))

# pubdom= distinct(select(impression_df, impression_df$PublisherDomain))
# pubdom2=filter(pubdom, "PublisherDomain like '%facebook%' or PublisherDomain like '%nstagram%' or PublisherDomain like '%witter%' or PublisherDomain like '%napchat%'")

#impression_social= filter(impression_df, "PublisherDomain like '%acebook%' or PublisherDomain like '%nstagram%' or PublisherDomain like '%witter%'")


impression_df = read.parquet(path_impression_con_diz)
#nrow(impression_df) #6.671.683.923

impression_df$social_network= ifelse((impression_df$PublisherDomain =='*.facebook.com' | impression_df$PublisherDomain =='apps.facebook.com' | impression_df$PublisherDomain =='*.copertinafacebook.com' | impression_df$PublisherDomain =='www.facebook.com'), 'facebook',
                                     ifelse((impression_df$PublisherDomain =='*.downloadtwittervideo.com' | impression_df$PublisherDomain =='twittervideodownloader.com'), 'twitter',
                                            ifelse(impression_df$PublisherDomain =='*.instagramtags.com', 'instagram', 'Null')))

# prova1 = filter(impression_df, impression_df$social_network != 'facebook')
# View(head(prova1,100))

createOrReplaceTempView(impression_df, 'impression_df')

impression_social = sql('SELECT ext_id_adform,
                        count(distinct case when social_network like "facebook" or social_network like "twitter" or social_network like "instagram" then social_network else Null end) as numero_social_provenienza,
                        sum(case when social_network like "facebook" or social_network like "twitter" or social_network like "instagram" then 1 else 0 end) as numero_impression_social
                        FROM impression_df 
                        GROUP BY ext_id_adform')

impression_social$flag_social = ifelse(impression_social$numero_impression_social > 0, 1, 0)

write.parquet(impression_social, path_impression_social) #, mode='overwrite')

impression_soc= read.parquet(path_impression_social)
#View(head(impression_soc,100))
#nrow(impression_soc) #2941450



#JOIN:

impression_var2 = read.parquet(path_impression_variabili2) # 20 colonne
impression_device_perc= read.parquet(path_impression_variabili4) # 8 colonne
impression_soc= read.parquet(path_impression_social) # 4 colonne

createOrReplaceTempView(impression_var2, 'impression_var2') #ext_id_impression
createOrReplaceTempView(impression_device_perc,'impression_device_perc')
createOrReplaceTempView(impression_soc,'social')

join1 = sql('SELECT I.*, D.numero_device, D.numero_tv_unknown, D.numero_mobile, D.numero_desktop, D.numero_tablet, D.perc_mobile, D.perc_desktop, D.perc_tablet, D.perc_tv_unknown
           FROM impression_var2 I
           LEFT JOIN impression_device_perc D
           ON I.ext_id_impression = D.ext_id_adform')

createOrReplaceTempView(join1,'j')

join2 = sql('SELECT j.*, S.numero_social_provenienza, S.numero_impression_social, S.flag_social
           FROM j
           LEFT JOIN social S
           ON j.ext_id_impression = S.ext_id_adform')



df_numero_cookie = summarize(groupBy(diz, diz$skyid), numero_cookies = countDistinct(diz$CookieID))

#View(head(impression_finale_pulito,100))
#printSchema(impression_finale_pulito)

createOrReplaceTempView(join2, 'join2')
createOrReplaceTempView(df_numero_cookie, 'df_numero_cookie')

join_cookie = sql('SELECT I.*, C.numero_cookies
                  FROM join2 I
                  LEFT JOIN df_numero_cookie C
                  ON I.ext_id_impression == C.skyid')




write.parquet(join_cookie, path_impression_join_finale)
# 
# impression_finale = read.parquet(path_impression_join_finale)
# #View(head(impression_finale,100))
# #nrow(impression_finale) #2941450
# 