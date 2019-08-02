
## Verifica cookie negativi Adform

#apri sessione
source("connection_R.R")
options(scipen = 1000)





# impression <- read.parquet('/STAGE/adform/table=Impression/Impression.parquet')
# 
# impression_2 <- withColumn(impression, "data_nav", cast(impression$yyyymmdd, 'string'))
# impression_3 <- withColumn(impression_2, "data_nav_dt", cast(cast(unix_timestamp(impression_2$data_nav, 'yyyyMMdd'), 'timestamp'), 'date'))
# impression_4 <- filter(impression_3, "data_nav_dt >= '2019-06-01' and data_nav_dt <= '2019-06-30'")
# 
# 
# write.parquet(impression_4, "/user/stefano.mazzucca/info_check_impression_coo.parquet")


trackingpoint <- read.parquet("/STAGE/adform/table=Trackingpoint/Trackingpoint.parquet")

trackingpoint_2 <- withColumn(trackingpoint, "data", cast(trackingpoint$yyyymmdd, 'string'))
trackingpoint_3 <- withColumn(trackingpoint_2, "data_dt", cast(cast(unix_timestamp(trackingpoint_2$data, 'yyyyMMdd'), 'timestamp'), 'date'))
trackingpoint_4 <- filter(trackingpoint_3, "data_dt >= '2019-06-01' and data_dt <= '2019-06-30'")


write.parquet(trackingpoint_4, "/user/stefano.mazzucca/analisi_coo_negativi_adform/info_check_tp_coo.parquet")




tp_tab <- read.parquet("/user/stefano.mazzucca/analisi_coo_negativi_adform/info_check_tp_coo.parquet")
View(head(tp_tab,100))
nrow(tp_tab)
# 195.555.953

createOrReplaceTempView(tp_tab, "tp_tab")

tp_tab_2 <- sql("select distinct *, regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid
                   from tp_tab")

tp_tab_2 <- withColumn(tp_tab_2, "flag_coo_pos", ifelse(tp_tab_2$CookieID >= 0, 1, 0))
View(head(tp_tab_2,100))


write.parquet(tp_tab_2, "/user/stefano.mazzucca/analisi_coo_negativi_adform/info_check_tp_coo_1.parquet")



tp <- read.parquet("/user/stefano.mazzucca/analisi_coo_negativi_adform/info_check_tp_coo_1.parquet")
View(head(tp,100))
nrow(tp)
# 195.328.053

gp_coo <- summarize(groupBy(tp, tp$data_dt), 
                    count_tot = count(tp$CookieID),
                    count_tot_dist = countDistinct(tp$CookieID),
                    count_coo_pos = sum(tp$flag_coo_pos))

gp_coo_1 <- withColumn(gp_coo, "perc_pos", gp_coo$count_coo_pos/gp_coo$count_tot)
View(head(gp_coo_1,100))


# associaz_extid_coo <- distinct(select(tp, tp$CookieID, tp$skyid))
# View(head(associaz_extid_coo,100))
# nrow(associaz_extid_coo)
# # 20.743.065

associaz_extid_coo <- filter(tp, tp$CookieID != 0 | isNotNull(tp$CookieID) | tp$skyid != 0 | isNotNull(tp$skyid))

associaz_extid_coo_1 <- summarize(groupBy(tp, tp$CookieID, associaz_extid_coo$skyid), 
                                  count_coppie = count(concat(tp$CookieID, tp$skyid)), coo_pos = max(associaz_extid_coo$flag_coo_pos))
View(head(associaz_extid_coo_1,100))
nrow(associaz_extid_coo_1)
# 20.743.065

associaz_extid_coo_2 <- filter(associaz_extid_coo_1, associaz_extid_coo_1$CookieID != 0 & isNotNull(associaz_extid_coo_1$skyid) & 
                                 isNotNull(associaz_extid_coo_1$CookieID) & associaz_extid_coo_1$skyid != "" & associaz_extid_coo_2$skyid != "${EXTERNAL_ID}$" &
                                 associaz_extid_coo_2$skyid != "trial" & associaz_extid_coo_2$skyid != "N-A")
View(head(associaz_extid_coo_2,100))
nrow(associaz_extid_coo_2)
# 2.650.165


write.parquet(associaz_extid_coo_2, "/user/stefano.mazzucca/analisi_coo_negativi_adform/count_check_tp_coo.parquet")




#### PARTIRE da qui ###############################################################################################################################

analisi <- read.parquet("/user/stefano.mazzucca/analisi_coo_negativi_adform/count_check_tp_coo.parquet")
View(head(analisi,100))
nrow(analisi)
# 2.650.165


analisi_1 <- summarize(groupBy(analisi, analisi$coo_pos), count_coppie_distinte = count(analisi$CookieID), 
                       sum_count_hit = sum(analisi$count_coppie))
View(head(analisi_1,100))
# coo_pos     count_coppie_distinte    sum_count_hit
# 0           1.206.222                 10.974.826
# 1           1.443.943                 12.661.331


coo_neg <- filter(analisi, analisi$coo_pos == 0)
nrow(coo_neg)
# 1.206.222

## COUNT COOKIE NEGATIVI associati a SKYID ##
skyid_coo_neg_analisi <- summarize(groupBy(coo_neg, coo_neg$skyid), count_cookie_neg = countDistinct(coo_neg$CookieID))
skyid_coo_neg_analisi <- filter(skyid_coo_neg_analisi, skyid_coo_neg_analisi$skyid != "null" & skyid_coo_neg_analisi$skyid != "n-a" & 
                                  skyid_coo_neg_analisi$skyid != "P17691b8e892656f" & skyid_coo_neg_analisi$skyid != "EXTERNAL_ID" & 
                                  skyid_coo_neg_analisi$skyid != "${COD_CLIENTE_EXTERNAL_ID}$")
skyid_coo_neg_analisi <- arrange(skyid_coo_neg_analisi, desc(skyid_coo_neg_analisi$count_cookie_neg))
View(head(skyid_coo_neg_analisi,100))
nrow(skyid_coo_neg_analisi)
# 616.658

## COUNT SKYID associati a COOKIE NEGATIVI ##
coo_neg_skyid_analisi <- summarize(groupBy(coo_neg, coo_neg$CookieID), count_skyid = countDistinct(coo_neg$skyid))
coo_neg_skyid_analisi <- arrange(coo_neg_skyid_analisi, desc(coo_neg_skyid_analisi$count_skyid))
View(head(coo_neg_skyid_analisi,100))
nrow(coo_neg_skyid_analisi)
# 968.665
# CookieID                  count_skyid
# -2195512363153315328        53
# -1519049657266653952        49
# -2598899428500596736        48
# -8890564831992898560        47
# -987893835040438784         47


# skyid                                               count_cookie_neg
# c9167301e0551638ec5fdd83150e31429cae7bde1a979b3e    29674
# 3259644c26f1c9d50a99d35634f29c5b1af5851da9442584    141
# 8902473940877b43a5055b0dcfa2b67227d7c501eabce74d    80
# ef50ba6029a3015d5ca6f026f287065e8104e300dc7dad07    80
# a38f1a38f2388ee376f507b83fd544040a02fef40b2a844f    75
# 7a55a54805d13293481c0cc6e7b93b674f13c4b9481f270f    70
# 0b8bd9451e756adc88d96574e298c8435e0e80f6c3cd42ac    65
# 529f4aa6060a0afb41c0ec3f3859b121368ee90c1f1b4779    64
# 302ff7f022a609f5478c0b1d92e40ab86ad283807419fa29    64
# 3e55383bf8cd007c544d2f25d07b75f7ef8176368d28d2df    63


## RECAP cookie NEGATIVI:
#### skyid unici: 616.658
#### cookie unici: 968.665



## RECAP cookie POSITIVI:
#### skyid unici: 92.719
#### cookie unici: 1.334.651



## Vado alla ricerca dei 5 cookie nagativi con alte associazioni a skyid: 
tp <- read.parquet("/user/stefano.mazzucca/analisi_coo_negativi_adform/info_check_tp_coo_1.parquet")
View(head(tp,100))
nrow(tp)
# 195.328.053

tp_es_1 <- filter(tp, tp$CookieID == "-2195512363153315328" | tp$CookieID == "-1519049657266653952" |
                    tp$CookieID == "-2598899428500596736" | tp$CookieID == "-8890564831992898560" | 
                    tp$CookieID == "-987893835040438784")
View(head(tp_es_1,1000))
nrow(tp_es_1)
# 938 (solo su cookieID = "-2195512363153315328")

tp_es_2 <- select(tp_es_1, "CookieID", "skyid", "VisitNumber", "flag_coo_pos", "yyyymmdd", "data_dt", "Timestamp", "TrackingPointId", "website",
                  "PageURL", "page", "IP", "city", "device-name")

write.df(repartition(tp_es_2, 1), path = "/user/stefano.mazzucca/analisi_coo_negativi_adform/esempio_cookie_neg.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



## POSITIVI
# tp_es_pos_1 <- filter(tp, tp$CookieID == "8988131919378016256" | tp$CookieID == "4026000633885638144" |
#                     tp$CookieID == "1568752803317034496" | tp$CookieID == "5235619168790161408" | 
#                     tp$CookieID == "1336636948709946880")
# View(head(tp_es_pos_1,1000))
# nrow(tp_es_pos_1)
# # 938 (solo su cookieID = "8988131919378016256")
# 
# tp_es_pos_2 <- select(tp_es_pos_1, "CookieID", "skyid", "VisitNumber", "flag_coo_pos", "yyyymmdd", "data_dt", "Timestamp", "TrackingPointId", "website",
#                   "PageURL", "page", "IP", "city", "device-name")
# 
# write.df(repartition(tp_es_pos_2, 1), path = "/user/stefano.mazzucca/analisi_coo_negativi_adform/esempio_cookie_pos.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





## CONFRONTO CON ADOBE ###########################################################################################################################################

skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
skyit_1 <- select(skyitdev, "external_id_post_evar",
                  "post_visid_concatenated",
                  "visit_num",
                  "date_time",
                  "post_channel", 
                  "page_name_post_evar",
                  "page_url_post_evar",
                  "secondo_livello_post_prop",
                  "terzo_livello_post_prop",
                  "hit_source",
                  "exclude_hit",
                  "post_page_event_value"
)
skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))

# filtro temporale
skyit_4 <- filter(skyit_3, skyit_3$date_time_dt >= "2019-06-01" & skyit_3$date_time_dt <= "2019-06-30")

write.parquet(skyit_4, "/user/stefano.mazzucca/analisi_coo_negativi_adform/scarico_adobe.parquet")



adobe <- read.parquet("/user/stefano.mazzucca/analisi_coo_negativi_adform/scarico_adobe.parquet")
View(head(adobe, 100))
nrow(adobe)
# 826.424.492

# adobe_es <- filter(adobe, adobe$external_id_post_evar == "c9167301e0551638ec5fdd83150e31429cae7bde1a979b3e" | 
#                      adobe$external_id_post_evar == "3259644c26f1c9d50a99d35634f29c5b1af5851da9442584" | 
#                      adobe$external_id_post_evar == "8902473940877b43a5055b0dcfa2b67227d7c501eabce74d" | 
#                      adobe$external_id_post_evar == "ef50ba6029a3015d5ca6f026f287065e8104e300dc7dad07" | 
#                      adobe$external_id_post_evar == "a38f1a38f2388ee376f507b83fd544040a02fef40b2a844f" | 
#                      adobe$external_id_post_evar == "7a55a54805d13293481c0cc6e7b93b674f13c4b9481f270f" | 
#                      adobe$external_id_post_evar == "0b8bd9451e756adc88d96574e298c8435e0e80f6c3cd42ac" | 
#                      adobe$external_id_post_evar == "529f4aa6060a0afb41c0ec3f3859b121368ee90c1f1b4779" | 
#                      adobe$external_id_post_evar == "302ff7f022a609f5478c0b1d92e40ab86ad283807419fa29" | 
#                      adobe$external_id_post_evar == "3e55383bf8cd007c544d2f25d07b75f7ef8176368d28d2df")
# 
# write.df(repartition(adobe_es, 1), path = "/user/stefano.mazzucca/analisi_coo_negativi_adform/esempio_adobe.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


# prendo qualche extenalID da stesso cookie neg adform:
adobe_es_2 <- filter(adobe, adobe$external_id_post_evar == "84bd4605df9c2cb1837b7d94bdc65979534eb9b5d5cbfe84")

View(head(adobe_es_2,10000))



# e confronto con quelli Adform
adform_neg <- read.df("/user/stefano.mazzucca/analisi_coo_negativi_adform/esempio_cookie_neg.csv",source = "csv", header = "true", delimiter = ";")
View(head(adform_neg,100))
nrow(adform_neg)
# 7.024

adform_neg_x_adobe <- filter(adform_neg, adform_neg$skyid == "c18dc2bf7ce2b1df676c20302988f9b765780b86938fa263")
View(head(adform_neg_x_adobe,1000))





#####################################################################################################################################################################
#####################################################################################################################################################################

adform_neg <- read.df("/user/stefano.mazzucca/analisi_coo_negativi_adform/esempio_cookie_neg.csv",source = "csv", header = "true", delimiter = ";")
View(head(adform_neg,100))
nrow(adform_neg)
# 7.024


adobe <- read.df("/user/stefano.mazzucca/analisi_coo_negativi_adform/esempio_adobe.csv", source = "csv", header = "true", delimiter = ";")
View(head(adobe,100))
nrow(adobe)
# 7.056



#chiudi sessione
sparkR.stop()
