
#CJ_UP vers04 

#apri sessione
source("connection_R.R")
options(scipen = 1000)



## Ricostruisco la base



######### Recupero le chiavi dal sito pubblico

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
# skyitdev_df_1 <- filter(skyitdev_df,"external_id_post_evar is not NULL")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated"))




######### Recupero il campione per l'UP

campione_up <- read.parquet("/user/stefano.mazzucca/CJ_UP_campione.parquet")
View(head(campione_up,100))
nrow(campione_up)
# 93.374

valdist_campione <- distinct(select(campione_up, "SKY_ID"))
nrow(valdist_campione)
# 92.499


########################################################################################################################################################################
## Sistemo le date del campione !! ####################################################################################################################################
########################################################################################################################################################################


campione_up <- withColumn(campione_up, "DATA_t0_4m_ts", cast(campione_up$DATA_t0_4m_ts, "timestamp"))
View(head(campione_up,100))
nrow(campione_up)
# 93.374




# createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
# createOrReplaceTempView(valdist_campione,"valdist_campione")
# 
# recupera_cookies <- sql("select *
#                        from valdist_campione  t1 
#                        left join skyitdev_df_3 t2
#                        on t1.SKY_ID=t2.external_id_post_evar")
# View(head(recupera_cookies,100))
# nrow(recupera_cookies)
# 
# # Salvataggio delle chiavi
# 
# write.parquet(recupera_cookies,"/user/stefano.mazzucca/CJ_UP_chiavi_cookies_campione_2.parquet") ##NON ha girato...disconnesso da R prima.





######### Aggancio le informazioni al campione


chiavi_campione_coo <- read.parquet("/user/stefano.mazzucca/CJ_UP_chiavi_cookies_campione.parquet")

#### Di queste mi tengo solo quei record valorizzati da un cookies "rintracciabile" con ext_id

createOrReplaceTempView(chiavi_campione_coo,"chiavi_campione_coo")
verifica_count <- sql("select
                        count(distinct external_id_post_evar) as num_external_id_post_evar,
                        count(distinct post_visid_concatenated) as num_post_visid_concatenated,
                        count(distinct SKY_ID) as num_SKY_ID
                      from chiavi_campione_coo")
View(head(verifica_count))
# 87.871
# 789.991
# 87.871

## join con il campione

createOrReplaceTempView(campione_up,"campione_up")
createOrReplaceTempView(chiavi_campione_coo,"chiavi_campione_coo")

campione_up_coo <- sql("select t1.*, t2.post_visid_concatenated
                    from campione_up t1
                    left join chiavi_campione_coo t2
                      on t1.SKY_ID = t2.external_id_post_evar")
View(head(campione_up_coo,100))
nrow(campione_up_coo)
# 804.035

ver_valdist_clienti <- distinct(select(campione_up_coo, "SKY_ID"))
nrow(ver_valdist_clienti)
# 92.499

ver_ext_id <- filter(campione_up_coo, "SKY_ID is NULL")
nrow(ver_ext_id)
# 0
ver_coo <- filter(campione_up_coo, "post_visid_concatenated is NULL")
View(head(ver_coo,100))
nrow(ver_coo)
# 4.651 <<<--------------------------------- SI PERDONO i cookies di 4.651 clienti !!!!!!!!
##################################################################################################
ver_coo_2 <- filter(ver_coo, "canale_ordine_3m = 'APP'")
nrow(ver_coo_2)
# 460 <<----- DI questi possiamo sapere solo gli EXTERNAL_ID perch?? non hanno un cookie associato!!
##################################################################################################
ver_coo_flg1 <- filter(ver_coo, "flg_up_3m = 1")
nrow(ver_coo_flg1)
# 467 <<<----------------------------------- DI CUI 467 con flg_up_3m = 1 !!!!!!!!
ver_coo_flg0 <- filter(ver_coo, "flg_up_3m = 0")
nrow(ver_coo_flg0)
# 4.184 <<<--------------------------------- DI CUI 4.184 con flg_up_3m = 0 !!!!!!!!!





################################################################################################################################################################################
################################################################################################################################################################################
## Recupero gli ext_id e i cookies dall'APP ###################################################################################################################################
################################################################################################################################################################################
################################################################################################################################################################################



######### Recupero le chiavi dall'APP

skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")

skyappwsc_2 <- withColumn(skyappwsc, "ts", cast(skyappwsc$date_time, "timestamp"))
skyappwsc_3 <- filter(skyappwsc_2,"ts >= '2017-03-01 00:00:00' and ts <= '2017-10-01 00:00:00' ")

skyappwsc_4 <- select(skyappwsc_3,
                      "external_id_v0",
                      "site_section",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "post_pagename",
                      "date_time",
                      "ts")

# skyappwsc_4 <- filter(skyappwsc_4,"
#                      site_section like '%dati e fatture%' or
#                      site_section like '%il mio abbonamento%' or
#                      site_section like '%sky extra%' or
#                      site_section like '%comunicazioni%' or
#                      site_section like '%assistenza%' or
#                      site_section like '%widget%' or
#                      site_section like '%impostazioni%' or
#                      site_section like '%i miei dati%' or
#                      page_name_post_evar like '%assistenza:home%' or
#                      page_name_post_evar like '%assistenza:contatta%' or
#                      page_name_post_evar like '%assistenza:conosci%' or
#                      page_name_post_evar like '%assistenza:ricerca%' or
#                      page_name_post_evar like '%assistenza:gestisci%' or
#                      page_name_post_evar like '%assistenza:risolvi%' or
#                      page_name_post_evar like '%widget:dispositivi%' or
#                      page_name_post_evar like '%widget:ultimafattura%' or
#                      page_name_post_evar like '%widget:contatta sky%' or
#                      page_name_post_evar like '%widget:gestisci%'
#                      ")




######### Recupero il campione per l'UP

campione_up <- read.parquet("/user/stefano.mazzucca/CJ_UP_campione.parquet")
View(head(campione_up,100))
nrow(campione_up)
# 93.374

valdist_campione <- distinct(select(campione_up, "SKY_ID"))
nrow(valdist_campione)
# 92.499




createOrReplaceTempView(skyappwsc_4,"skyappwsc_4")
createOrReplaceTempView(valdist_campione,"valdist_campione")

join_campione_app <- sql("select *
                            from valdist_campione  t1
                            left join skyappwsc_4 t2
                              on t1.SKY_ID=t2.external_id_v0")
View(head(join_campione_app,100))
nrow(join_campione_app)


# Salvataggio delle chiavi

write.parquet(join_campione_app,"/user/stefano.mazzucca/CJ_UP_join_campione_app.parquet")





######################## Verifiche #######################################################################################################################################

join_campione_app <- read.parquet("/user/stefano.mazzucca/CJ_UP_join_campione_app.parquet")
View(head(join_campione_app,100))
nrow(join_campione_app)
# 3.577.468

ver_clienti_campione_app <- distinct(select(join_campione_app, "external_id_v0"))
nrow(ver_clienti_campione_app)
# 49.410

ver_2_clienti_campione_app <- filter(join_campione_app, "external_id_v0 is NULL")
nrow(ver_2_clienti_campione_app)
# 43.090



## join con il campione 
##### per verificare se almeno i 4.651 clienti NON agganciati sul sito pubblico, siano passati almeno dall'APP

createOrReplaceTempView(campione_up,"campione_up")
createOrReplaceTempView(join_campione_app,"join_campione_app")

campione_up_app <- sql("select t1.*, t2.external_id_v0, t2.site_section, t2.page_name_post_evar, t2.page_name_post_prop, t2.post_pagename, t2.ts
                        from campione_up t1
                        left join join_campione_app t2
                          on t1.SKY_ID = t2.external_id_v0")
View(head(campione_up_app,100))
nrow(campione_up_app)
# 3.622.539

# ver_valdist_clienti <- distinct(select(campione_up_app, "external_id_v0"))
# nrow(ver_valdist_clienti)
# # 45.816
# 
# ver_app <- filter(campione_up_app, "external_id_v0 is NULL")
# nrow(ver_app)
# # 47.039 <<<--------------------------------- SI PERDONO 47.039 clienti !!!!!!!!
# ver_app_flg1 <- filter(ver_app, "flg_up_3m = 1")
# nrow(ver_app_flg1)
# # 12.443 <<<----------------------------------- DI CUI 12.443 con flg_up_3m = 1 !!!!!!!!
# ver_app_flg0 <- filter(ver_app, "flg_up_3m = 0")
# nrow(ver_app_flg0)
# # 34.596 <<<--------------------------------- DI CUI 34.596 con flg_up_3m = 0 !!!!!!!!!



# ---->>> ver_coo sono i 4.651 clienti mancanti dalla precedente join

createOrReplaceTempView(ver_coo,"ver_coo")
createOrReplaceTempView(campione_up_app,"campione_up_app")

join_missing <- sql("select t1.external_id_v0, t1.flg_up_3m
                    from campione_up_app t1
                    inner join ver_coo t2
                      on t1.external_id_v0 = t2.SKY_ID")
View(head(join_missing))
nrow(join_missing)
# 67.569

join_missing_2 <- distinct(select(join_missing, "external_id_v0","flg_up_3m"))
nrow(join_missing_2)
# 1.468 <<-------------------------------------------------- Si possono recuperare 1.468 clienti (perch?? navigano solo su APP e non su device) dei 4.651 mancanti
##################################################################################################################################################################

ver_missing_app_flg1 <- filter(join_missing_2, "flg_up_3m = 1")
nrow(ver_missing_app_flg1)
# 455 <<<----------------------------------- DI CUI 455 con flg_up_3m = 1 (dei 467 che mancavano all'appello) !!
ver_missing_app_flg0 <- filter(join_missing_2, "flg_up_3m = 0")
nrow(ver_missing_app_flg0)
# 1.013 <<---------------------------------- DI CUI 1.013 con flg_up_3m = 0 (dei 4.184 che mancavano all'appello) !!







################################################################################################################################################################################
################################################################################################################################################################################
## Ricostruisco il campione con gli ext_id anche dei clienti che navigano solo su APP #########################################################################################
################################################################################################################################################################################
################################################################################################################################################################################














#chiudi sessione
sparkR.stop()
