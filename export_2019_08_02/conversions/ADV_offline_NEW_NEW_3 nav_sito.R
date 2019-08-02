
#apri sessione
source("connection_R.R")
options(scipen = 1000)


############################# recupera basi ########################################################################################################################
# qua trovi i 128k records che rientrano nei filtri:
# - residenziali
# - tra 27nov e 4mar
# - no subentri
# - no special


conversions_all_filter <- read.parquet("/user/stefano.mazzucca/conversion_all_filtered.parquet")
nrow(conversions_all_filter) #128.829
View(head(conversions_all_filter,300))
names(conversions_all_filter)

# verifica_date_errate <- filter(conversions_all_filter,"DAT_PRIMA_ATTIVAZIONE='01/01/1900'")
# nrow(verifica_date_errate) #26.129


conversions_all_filter_2 <- withColumn(conversions_all_filter,"DAT_PRIMA_ATTIVAZIONE_ts",cast(unix_timestamp(conversions_all_filter$DAT_PRIMA_ATTIVAZIONE, 'dd/MM/yyyy'),'timestamp'))

conversions_all_filter_3 <- withColumn(conversions_all_filter_2, "DAT_PRIMA_ATTIVAZIONE_dt", cast(conversions_all_filter_2$DAT_PRIMA_ATTIVAZIONE_ts, "date"))


######################################################################################################################################################################
########################## associo cookies ed external_id ADOBE #####################################################################################################


base_ext_id <- distinct(select(conversions_all_filter,"COD_CLIENTE_CIFRATO"))
nrow(base_ext_id) # 121.902
printSchema(base_ext_id)

skyitdev_df <- read.parquet("hdfs:///STAGE/adobe/reportsuite=skyitdev")
skyitdev_df_2 <- select(skyitdev_df,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated")
skyitdev_df_3 <- distinct(select(skyitdev_df_2,"external_id_post_evar","post_visid_high", "post_visid_low", "post_visid_concatenated"))

#printSchema(skyitdev_df_3)

## join
createOrReplaceTempView(skyitdev_df_3,"skyitdev_df_3")
createOrReplaceTempView(base_ext_id,"base_ext_id")

base_ext_id_2 <- sql("select *
                     from base_ext_id  t1 left join skyitdev_df_3 t2
                     on t1.COD_CLIENTE_CIFRATO=t2.external_id_post_evar")


write.parquet(base_ext_id_2,"/user/valentina/IMPATTO_ADV_BASE_TOT_CookiesADOBE.parquet")





#########################################################################################################################################################
####### creo data-set per estrazioni successive

base_cookies <- read.parquet("/user/valentina/IMPATTO_ADV_BASE_TOT_CookiesADOBE.parquet")
nrow(base_cookies) #239.312

base_cookies_2 <- filter(base_cookies,"external_id_post_evar is not NULL")
nrow(base_cookies_2) #168.044
#names(base_cookies_2)


## seleziono la max data di attivaz associata all'ext-id
createOrReplaceTempView(conversions_all_filter_3,"conversions_all_filter_3")
max_data_stipula <- sql("select COD_CLIENTE_CIFRATO, max(data_stipula_dt) as max_Data_stipula_dt
                        from conversions_all_filter_3
                        group by COD_CLIENTE_CIFRATO")
nrow(conversions_all_filter_3)  #128.829
nrow(max_data_stipula) #121.902
View(head(max_data_stipula,200))


#join 
createOrReplaceTempView(base_cookies_2,"base_cookies_2")
createOrReplaceTempView(max_data_stipula,"max_data_stipula")

base_cookies_3 <- sql("select t1.*, t2.max_Data_stipula_dt
                      from base_cookies_2 t1 inner join max_data_stipula t2
                      on t1.COD_CLIENTE_CIFRATO=t2.COD_CLIENTE_CIFRATO")
nrow(base_cookies_3)  #168.044



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

#View(head(skyitdev,100))

## FILTRO DATE
skyitdev <- withColumn(skyitdev, "date_time_dt", cast(skyitdev$date_time, "date"))
skyitdev <- withColumn(skyitdev, "date_time_ts", cast(skyitdev$date_time, "timestamp"))

skyitdev <- filter(skyitdev,"date_time_dt >= '2017-03-01'")


#### associa e scarica navigazioni
createOrReplaceTempView(skyitdev,"skyitdev")
createOrReplaceTempView(base_cookies_3,"base_cookies_3")

#View(head(base_cookies_3,200))

scarico_navigazione_1 <- sql("select t1.*,
                            t2.COD_CLIENTE_CIFRATO, t2.max_Data_stipula_dt
                            from skyitdev t1 inner join base_cookies_3 t2
                            on t1.post_visid_concatenated=t2.post_visid_concatenated
                            ")

scarico_navigazione_2 <- filter(scarico_navigazione_1,"date_time_dt<=max_Data_stipula_dt")


write.parquet(scarico_navigazione_2,"/user/valentina/IMPATTO_ADV_scarico_naigazione.parquet")



#########################################################################################################################################################
#########################################################################################################################################################
### TO DO:

#prendere solo la navigazione organica e poi continuare, da decidere che campo utilizzare









#########################################################################################################################################################
#########################################################################################################################################################

#### riprendi la base dati 

base_scarico <- read.parquet("/user/valentina/IMPATTO_ADV_scarico_naigazione.parquet")
nrow(base_scarico)  #4236648
View(head(base_scarico,300))
names(base_scarico)

# count_distinct_ext_id <- distinct(select(base_scarico,"COD_CLIENTE_CIFRATO"))
# nrow(count_distinct_ext_id)  #29250

base_scarico <- withColumnRenamed(base_scarico,"COD_CLIENTE_CIFRATO","SKY_ID") 


#DIFF GG PDISC
createOrReplaceTempView(base_scarico, "base_scarico")
base_scarico <- sql("SELECT *, datediff(max_Data_stipula_dt, date_time_dt) as diff_gg_view from base_scarico")


skyid_distinti <- distinct(select(base_scarico,"SKY_ID"))



####### start Ric


## FUNZIONE RENAME LIST --> aggiunge il suffisso alle variabili 
rename_with_list <- function(df, suffix){
  nomi_vecchi <- colnames(df)
  nomi_nuovi <- unlist(lapply(nomi_vecchi, paste0, suffix))
  for(i in 1:length(nomi_vecchi)){
    df <- withColumnRenamed(df, nomi_vecchi[i], nomi_nuovi[i])
  }
  
  return(df)
}


## FUNZIONE DI INNESCO

start_kpi_sito <- function(df, clienti){
  
  # Calcolo i vari df per i kpi
  df_1w <- filter(df, "diff_gg_view <= 7")
  df_2w <- filter(df, "diff_gg_view <= 15")
  df_1m <- filter(df, "diff_gg_view <= 30")
  df_2m <- filter(df, "diff_gg_view <= 60")
  df_4m <- filter(df, "diff_gg_view > 60")
  
  # KPI
  df_1w_kpi <- group_kpi_sito(df_1w)
  df_1w_kpi <- rename_with_list(df_1w_kpi, "_7gg")
  df_kpi <- merge(clienti, df_1w_kpi, all.x = T, by.x = "SKY_ID", by.y = "SKY_ID_7gg")
  
  df_2w_kpi <- group_kpi_sito(df_2w)
  df_2w_kpi <- rename_with_list(df_2w_kpi, "_15gg")
  df_kpi <- merge(df_kpi, df_2w_kpi, all.x = T, by.x = "SKY_ID", by.y = "SKY_ID_15gg")
  
  df_1m_kpi <- group_kpi_sito(df_1m)
  df_1m_kpi <- rename_with_list(df_1m_kpi, "_30gg")
  df_kpi <- merge(df_kpi, df_1m_kpi, all.x = T, by.x = "SKY_ID", by.y = "SKY_ID_30gg")
  
  df_2m_kpi <- group_kpi_sito(df_2m)
  df_2m_kpi <- rename_with_list(df_2m_kpi, "_60gg")
  df_kpi <- merge(df_kpi, df_2m_kpi, all.x = T, by.x = "SKY_ID", by.y = "SKY_ID_60gg")
  
  df_4m_kpi <- group_kpi_sito(df_4m)
  df_4m_kpi <- rename_with_list(df_4m_kpi, "_piu60gg")
  df_kpi <- merge(df_kpi, df_4m_kpi, all.x = T, by.x = "SKY_ID", by.y = "SKY_ID_piu60gg")
  
  df_kpi <- drop(df_kpi, c("SKY_ID_7gg", "SKY_ID_15gg", "SKY_ID_30gg", "SKY_ID_60gg", "SKY_ID_piu60gg"))
  
  ## LASTY CLEANS
  df_kpi <- fillna(df_kpi, 0)
  
  return(df_kpi)
  
}



## FUNZIONE PER CALCOLARE TUTTI I KPI SITO
add_kpi_sito <- function(df) {
  
  
  # Aggiugno colonne necessarie ai nuovi kpi
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,
            -- Hit x categoria
            CASE WHEN (secondo_livello_post_evar LIKE '%guidatv%' OR secondo_livello_post_evar LIKE '%Guidatv%') THEN 1 ELSE 0 END AS is_GuidaTv,
            CASE WHEN secondo_livello_post_evar LIKE 'pagine di servizio' THEN 1 ELSE 0 END AS is_PaginaDiServizio,
            CASE WHEN secondo_livello_post_evar LIKE 'conosci' THEN 1 ELSE 0 END AS is_AssistenzaConosci,
            CASE WHEN terzo_livello_post_evar LIKE 'gestisci' THEN 1 ELSE 0 END AS is_AssistenzaGestisci,
            CASE WHEN terzo_livello_post_evar LIKE 'contatta' THEN 1 ELSE 0 END AS is_AssistenzaContatta,
            CASE WHEN terzo_livello_post_evar LIKE 'home' THEN 1 ELSE 0 END AS is_AssistenzaHome,
            CASE WHEN terzo_livello_post_evar LIKE 'ricerca' THEN 1 ELSE 0 END AS is_AssistenzaRicerca,
            CASE WHEN terzo_livello_post_evar LIKE 'risolvi' THEN 1 ELSE 0 END AS is_AssistenzaRisolvi,
            CASE WHEN terzo_livello_post_evar LIKE 'extra' THEN 1 ELSE 0 END AS is_Extra,
            CASE WHEN terzo_livello_post_evar LIKE 'tecnologia' THEN 1 ELSE 0 END AS is_Tecnologia,
            CASE WHEN terzo_livello_post_evar LIKE 'pacchetti-offerte' THEN 1 ELSE 0 END AS is_PacchettiOfferte,
            CASE WHEN terzo_livello_post_evar LIKE 'info disdetta' THEN 1 ELSE 0 END AS is_InfoDisdetta,
            CASE WHEN terzo_livello_post_evar LIKE 'trova sky service' THEN 1 ELSE 0 END AS is_TrovaSkyService,
            CASE WHEN page_url_post_evar LIKE '%query=disdetta%' THEN 1 ELSE 0 END AS is_SearchDisdetta,
            CASE WHEN page_url_post_evar LIKE '%query=contratto%' THEN 1 ELSE 0 END AS is_SearchContratto,
            
            CASE WHEN page_url_post_evar LIKE '%abbonamento.sky.it/aol%' THEN 1 ELSE 0 END AS is_AOL,
            CASE WHEN (page_url_post_evar LIKE 'http://www.sky.it/' or page_url_post_evar LIKE 'https://www.sky.it/') THEN 1 ELSE 0 END AS is_SKY_IT_HOME,
            
            
            -- Visite per categoria
            CASE WHEN (secondo_livello_post_evar LIKE '%guidatv%' OR secondo_livello_post_evar LIKE '%Guidatv%') THEN visit_num ELSE Null END AS visit_GuidaTv,
            CASE WHEN secondo_livello_post_evar LIKE 'pagine di servizio' THEN visit_num ELSE Null END AS visit_PaginaDiServizio,
            CASE WHEN secondo_livello_post_evar LIKE 'conosci' THEN visit_num ELSE Null END AS visit_AssistenzaConosci,
            CASE WHEN terzo_livello_post_evar LIKE 'gestisci' THEN visit_num ELSE Null END AS visit_AssistenzaGestisci,
            CASE WHEN terzo_livello_post_evar LIKE 'contatta' THEN visit_num ELSE Null END AS visit_AssistenzaContatta,
            CASE WHEN terzo_livello_post_evar LIKE 'home' THEN visit_num ELSE Null END AS visit_AssistenzaHome,
            CASE WHEN terzo_livello_post_evar LIKE 'ricerca' THEN visit_num ELSE Null END AS visit_AssistenzaRicerca,
            CASE WHEN terzo_livello_post_evar LIKE 'risolvi' THEN visit_num ELSE Null END AS visit_AssistenzaRisolvi,
            CASE WHEN terzo_livello_post_evar LIKE 'extra' THEN visit_num ELSE Null END AS visit_Extra,
            CASE WHEN terzo_livello_post_evar LIKE 'tecnologia' THEN visit_num ELSE Null END AS visit_Tecnologia,
            CASE WHEN terzo_livello_post_evar LIKE 'pacchetti-offerte' THEN visit_num ELSE Null END AS visit_PacchettiOfferte,
            CASE WHEN terzo_livello_post_evar LIKE 'info disdetta' THEN visit_num ELSE Null END AS visit_InfoDisdetta,
            CASE WHEN terzo_livello_post_evar LIKE 'trova sky service' THEN visit_num ELSE Null END AS visit_TrovaSkyService,
            CASE WHEN page_url_post_evar LIKE '%query=disdetta%' THEN visit_num ELSE Null END AS visit_SearchDisdetta,
            CASE WHEN page_url_post_evar LIKE '%query=contratto%' THEN visit_num ELSE Null END AS visit_SearchContratto,
            
            CASE WHEN page_url_post_evar LIKE '%abbonamento.sky.it/aol%' THEN visit_num ELSE Null END AS visit_AOL,
            CASE WHEN (page_url_post_evar LIKE 'http://www.sky.it/' or page_url_post_evar LIKE 'https://www.sky.it/') THEN 1 ELSE 0 END AS visit_SKY_IT_HOME
            
            
            FROM df"
  )
  
  return(df)
}



## FUNZIONE PER SCORE KPI
group_kpi_sito <- function(df) {
  
  df <- add_kpi_sito(df)
  
  df_kpi <- agg(
    groupBy(df, "SKY_ID"),
    # hit
    hit_totali = sum(df$hit_source),
    hit_GuidaTv = sum(df$is_GuidaTv),
    hit_PaginaDiServizio = sum(df$is_PaginaDiServizio),
    hit_AssistenzaConosci = sum(df$is_AssistenzaConosci),
    hit_AssistenzaGestisci = sum(df$is_AssistenzaGestisci),
    hit_AssistenzaContatta = sum(df$is_AssistenzaContatta),
    hit_AssistenzaHome = sum(df$is_AssistenzaHome),
    hit_AssistenzaRicerca = sum(df$is_AssistenzaRicerca),
    hit_AssistenzaRisolvi = sum(df$is_AssistenzaRisolvi),
    hit_Extra = sum(df$is_Extra),
    hit_Tecnologia = sum(df$is_Tecnologia),
    hit_PacchettiOfferta = sum(df$is_PacchettiOfferte),
    hit_InfoDisdetta = sum(df$is_InfoDisdetta),
    hit_TrovaSkyService = sum(df$is_TrovaSkyService),
    hit_SearchDisdetta = sum(df$is_SearchDisdetta),
    hit_SearchContratto = sum(df$is_SearchContratto),
    
    hit_AOL = sum(df$is_AOL),
    hit_SKY_IT_HOME = sum(df$is_SKY_IT_HOME),
    
    # visite
    visite_totali = max(df$visit_num) - min(df$visit_num),
    visite_GuidaTv = max(df$visit_GuidaTv, na.rm = T) - min(df$visit_GuidaTv, na.rm = T),
    visite_PaginaDiServizio = max(df$visit_PaginaDiServizio, na.rm = T) - min(df$visit_PaginaDiServizio, na.rm = T),
    visite_AssistenzaConosci = max(df$visit_AssistenzaConosci, na.rm = T) - min(df$visit_AssistenzaConosci, na.rm = T),
    visite_AssistenzaGestisci = max(df$visit_AssistenzaGestisci, na.rm = T) - min(df$visit_AssistenzaGestisci, na.rm = T),
    visite_AssistenzaContatta = max(df$visit_AssistenzaContatta, na.rm = T) - min(df$visit_AssistenzaContatta, na.rm = T),
    visite_AssistenzaHome = max(df$visit_AssistenzaHome, na.rm = T) - min(df$visit_AssistenzaHome, na.rm = T),
    visite_AssistenzaRicerca = max(df$visit_AssistenzaRicerca, na.rm = T) - min(df$visit_AssistenzaRicerca, na.rm = T),
    visite_AssistenzaRisolvi = max(df$visit_AssistenzaRisolvi, na.rm = T) - min(df$visit_AssistenzaRisolvi, na.rm = T),
    visite_Extra = max(df$visit_Extra, na.rm = T) - min(df$visit_Extra, na.rm = T),
    visite_Tecnologia = max(df$visit_Tecnologia, na.rm = T) - min(df$visit_Tecnologia, na.rm = T),
    visite_PacchettiOfferta = max(df$visit_PacchettiOfferte, na.rm = T) - min(df$visit_PacchettiOfferte, na.rm = T),
    visite_InfoDisdetta = max(df$visit_InfoDisdetta, na.rm = T) - min(df$visit_InfoDisdetta, na.rm = T),
    visite_TrovaSkyService = max(df$visit_TrovaSkyService, na.rm = T) - min(df$visit_TrovaSkyService, na.rm = T),
    visite_SearchDisdetta = max(df$visit_SearchDisdetta, na.rm = T) - min(df$visit_SearchDisdetta, na.rm = T),
    visite_SearchContratto = max(df$visit_SearchContratto, na.rm = T) - min(df$visit_SearchContratto, na.rm = T),
    
    visite_AOL = max(df$visit_AOL, na.rm = T) - min(df$visit_AOL, na.rm = T),
    visite_SKY_IT_HOME = max(df$visit_SKY_IT_HOME, na.rm = T) - min(df$visit_SKY_IT_HOME, na.rm = T)
    
  )
  
  return(df_kpi)
}

#### END FUNCTIONS, START WORKING


#VISIT_NUM TO INTEGER
base_scarico$visit_num <- cast(base_scarico$visit_num, "integer")

pdisc_kpi <- start_kpi_sito(base_scarico, skyid_distinti)

write.parquet(pdisc_kpi,"/user/valentina/IMPATTO_ADV_sommarizzaz_sito.parquet")












#chiudi sessione
sparkR.stop()
