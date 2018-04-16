

#CJ_UP vers03

#apri sessione
source("connection_R.R")
options(scipen = 1000)


## Costruisco il campione su cui fare le analisi


sample_flg_0 <- read.parquet("/user/stefano.mazzucca/CJ_UP_sample_stratified_flg_0.parquet")
View(head(sample_flg_0,100))
nrow(sample_flg_0)
# 62.247


base_up_filtrata <- read.parquet("/user/valentina/CJ_UP_base_filtrata_3.parquet")
View(head(base_up_filtrata,100))
nrow(base_up_filtrata)
# 3.694.583

base_up_1 <- filter(base_up_filtrata, "flg_up_3m = 1")
View(head(base_up_1,100))
nrow(base_up_1)
# 31.127



campione <- union(sample_flg_0, base_up_1)
View(head(campione,100))
nrow(campione)
# 93.374

ver_distr_flg <- summarize(groupBy(campione, campione$flg_up_3m), COUNT = count(campione$COD_CONTRATTO_ok))
View(head(ver_distr_flg,100))

ver_dist_data <- summarize(groupBy(campione, campione$DATA_t0_ts), COUNT = count(campione$COD_CONTRATTO_ok))
View(head(ver_dist_data,100))


# Salvataggio del campione

write.parquet(campione,"/user/stefano.mazzucca/CJ_UP_campione.parquet")

# prova <- read.parquet("/user/stefano.mazzucca/CJ_UP_campione.parquet")
# View(head(prova))
# nrow(prova)
# # 93.374
# 
# ver_filt_up <- filter(prova, "flg_up_3m = 1")
# nrow(ver_filt_up)
# # 31.127
# createOrReplaceTempView(ver_filt_up,"ver_filt_up")
# ver_filt_up_2 <- sql("select SKY_ID
#                      from ver_filt_up
#                      group by SKY_ID")
# nrow(ver_filt_up_2)
# # 30.630



######### Recupero le chiavi

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
# skyitdev_df_1 <- filter(skyitdev_df,"external_id_post_evar is not NULL")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low","post_visid_concatenated")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low","post_visid_concatenated"))


valdist_campione <- distinct(select(campione, "SKY_ID"))
nrow(valdist_campione)
# 92.499


createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
createOrReplaceTempView(valdist_campione,"valdist_campione")

recupera_cookies <- sql("select *
                       from skyitdev_df_3 t1 
                       inner join valdist_campione t2
                       on t2.SKY_ID=t1.external_id_post_evar")
View(head(recupera_cookies,100))
nrow(recupera_cookies)


# Salvataggio delle chiavi

write.parquet(recupera_cookies,"/user/stefano.mazzucca/CJ_UP_chiavi_cookies_campione.parquet")

# prova <- read.parquet("/user/stefano.mazzucca/CJ_UP_chiavi_cookies_campione.parquet")
# View(head(prova,100))
# nrow(prova)
# # 791.032
# ver_filt_skyid <- filter(prova, "SKY_ID is NULL")
# nrow(ver_filt_skyid) 
# # 0
# ver_filt_coo <- filter(prova, "post_visid_concatenated is NULL")
# nrow(ver_filt_coo) 
# # 3
# ver_ <- distinct(select(prova, "SKY_ID"))
# nrow(ver_)
# # 87.871



######### Imposto le pagine e i parametri per l'analisi
### SEZIONE PUBBLICA sito SKY

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")

# skyitdev_df_2 <- select(skyitdev_df,
#                         "first_hit_page_url","first_hit_ref_domain","first_hit_referrer",                    
#                         "page_name_post_evar","previous_page_post_evar","external_id_post_evar",    
#                         "post_visid_high", "post_visid_low","post_visid_concatenated","visit_num",
#                         "page_url_post_evar","page_url_pulita_post_evar","canale_corporate_post_evar",
#                         "sottocanale_corporate_post_evar","secondo_livello_post_evar","terzo_livello_post_evar",
#                         "chat_type_post_evar","page_url_post_prop","page_url_pulita_post_prop",
#                         "sottocanale_editoriale_post_prop","secondo_livello_post_prop",
#                         "terzo_livello_post_prop","date_time",
#                         "stack_tracking_code_post_evar","hit_source",
#                         "exclude_hit","first_hit_ref_domain")

skyitdev_df_2 <- select(skyitdev_df,
                        "external_id_post_evar", 
                        "post_visid_high", "post_visid_low","post_visid_concatenated","date_time",
                        "secondo_livello_post_evar","terzo_livello_post_evar","hit_source",
                        "exclude_hit")

# skyitdev_df_3 <- filter(skyitdev_df_2,"external_id_post_evar is not NULL and stack_tracking_code_post_evar is not NULL")

skyitdev_df_4 <- withColumn(skyitdev_df_2, "ts", cast(skyitdev_df_2$date_time, "timestamp"))
skyitdev_df_5 <- withColumn(skyitdev_df_4, "date", cast(skyitdev_df_4$date_time, "date"))
skyitdev_df_6 <- filter(skyitdev_df_5,"ts>='2017-03-01 00:00:00' and  ts<='2017-10-01 00:00:00' ")
skyitdev_df_7 <- filter(skyitdev_df_6,"
                   secondo_livello_post_evar like '%guidatv%' or
                   secondo_livello_post_evar like '%Guidatv%' or
                   secondo_livello_post_evar like '%pagine di servizio%' or
                   secondo_livello_post_evar like '%assistenza%' or
                   secondo_livello_post_evar like '%fai da te%' or
                   secondo_livello_post_evar like '%extra%' or
                   secondo_livello_post_evar like '%tecnologia%' or
                   secondo_livello_post_evar like '%pacchetti-offerte%' or
                   terzo_livello_post_evar like '%conosci%' or
                   terzo_livello_post_evar like '%gestisci%' or
                   terzo_livello_post_evar like '%contatta%' or
                   terzo_livello_post_evar like '%home%' or
                   terzo_livello_post_evar like '%ricerca%' or
                   terzo_livello_post_evar like '%risolvi%'
                   ")
cache(skyitdev_df_7)
View(head(skyitdev_df_7,200))


################## Di queste mi tengo solo quei record valorizzati da un cookies "rintracciabile" con ext_id 

## Recupero le chiavi
chiavi_id_coo <- read.parquet("/user/stefano.mazzucca/CJ_UP_chiavi_cookies_campione.parquet")

## join con il campione
createOrReplaceTempView(campione,"campione")
createOrReplaceTempView(chiavi_id_coo,"chiavi_id_coo")

campione_coo <- sql("select t1.*, t2.post_visid_concatenated
                    from campione t1
                    left join chiavi_id_coo t2
                      on t1.SKY_ID = t2.SKY_ID")
View(head(campione_coo,100))
nrow(campione_coo)
# 804.035


# Salvataggio del campione con i relativi cookies

write.parquet(campione_coo,"/user/stefano.mazzucca/CJ_UP_campione_coo.parquet")

# prova <- read.parquet("/user/stefano.mazzucca/CJ_UP_campione_coo.parquet")
# View(head(prova))
# nrow(prova)
# # 804.035




### Adesso join campione-parametri per pagina web

createOrReplaceTempView(campione_coo,"campione_coo")
createOrReplaceTempView(skyitdev_df_7,"skyitdev_df_7")


join_base_sez_pubblica <- sql("select t1.*, 
                                t2.secondo_livello_post_evar,t2.terzo_livello_post_evar,
                                t2.ts as data_visualizzaz_pagina
                              from campione_coo t1 
                              inner join skyitdev_df_7 t2
                                on t1.post_visid_concatenated=t2.post_visid_concatenated
                                and t2.ts>=t1.DATA_t0_4m_ts and t2.ts<=t1.DATA_t0_ts")

write.parquet(join_base_sez_pubblica,"/user/valentina/CJ_UP_scarico_pag_viste_sito.parquet")





### Per le analisi di upselling su WSC e APP vedi ------> "CJ_UP_Vale_pub_wsc_app.r" ######################################################################################






#chiudi sessione
sparkR.stop()
